package block

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"cosmossdk.io/log"
	abci "github.com/cometbft/cometbft/abci/types"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/tellor-io/layer/cryptoriums/db"
	"github.com/tellor-io/layer/cryptoriums/monitors/block/processor"
)

const ComponentName = "block-monitor"

// Config controls the RPC-based monitor behaviour.
type Config struct {
	Nodes        []string      `yaml:"nodes"`
	Backfill     bool          `yaml:"backfill"`
	PollInterval time.Duration `yaml:"poll_interval"`
}

// Monitor polls ABCI endpoints to ingest blocks without using websockets.
type Monitor struct {
	cfg          Config
	logger       log.Logger
	clients      []*rpchttp.HTTP
	db           db.Db
	pollInterval time.Duration
	processor    processor.BlockProcessor
}

// New creates a new RPC monitor.
func New(ctx context.Context, logger log.Logger, cfg Config, reg prometheus.Registerer, db db.Db) (*Monitor, error) {

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 800 * time.Millisecond
	}

	clients := make([]*rpchttp.HTTP, 0, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		client, err := rpchttp.New(node, "/websocket")
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	if len(clients) == 0 {
		return nil, errors.New("no RPC nodes configured")
	}

	proc := processor.New(
		logger,
		db,
		reg,
		nil,
	)

	return &Monitor{
		cfg:          cfg,
		logger:       logger.With("component", ComponentName+"-rpc"),
		clients:      clients,
		db:           db,
		pollInterval: cfg.PollInterval,
		processor:    proc,
	}, nil
}

// Run starts the polling loop until ctx is cancelled.
func (m *Monitor) Run(ctx context.Context) error {
	nextHeight, err := m.startHeight(ctx)
	if err != nil {
		return err
	}

	m.logger.Debug("starting", "height", nextHeight)

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		lastProcessed, err := m.catchUp(ctx, nextHeight)
		if err != nil {
			m.logger.Error("catch up failed", "error", err)
		}
		if lastProcessed >= nextHeight {
			nextHeight = lastProcessed + 1
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Monitor) catchUp(ctx context.Context, nextHeight int64) (lastProcessed int64, err error) {
	latest, err := m.latestChainHeight(ctx)
	if err != nil {
		return nextHeight - 1, err
	}

	m.logger.Debug("chain height", "num", latest)
	if latest < nextHeight {
		return nextHeight - 1, nil
	}

	for h := nextHeight; h <= latest; h++ {
		event, err := m.fetchBlock(ctx, h)
		if err != nil {
			return h - 1, fmt.Errorf("fetch block %d: %w", h, err)
		}
		m.processor.ProcessBlock(ctx, event)
		lastProcessed = h
	}
	return lastProcessed, nil
}

func (m *Monitor) startHeight(ctx context.Context) (int64, error) {
	last, err := m.lastStoredHeight(ctx)
	if err != nil {
		return 0, err
	}

	// If not backfilling, skip to current chain height (only monitor new blocks)
	if !m.cfg.Backfill {
		latest, err := m.latestChainHeight(ctx)
		if err != nil {
			return 0, err
		}
		if latest > last {
			last = latest
		}
		return last + 1, nil
	}

	// Backfill mode: if DB has data, continue from where we left off
	if last > 0 {
		return last + 1, nil
	}

	// Backfill mode with empty DB: start from earliest available block
	// Can't start from 1 because chain's genesis height might be higher
	earliest, err := m.earliestChainHeight(ctx)
	if err != nil {
		return 0, err
	}
	return earliest, nil
}

func (m *Monitor) lastStoredHeight(ctx context.Context) (int64, error) {
	query := fmt.Sprintf("SELECT COALESCE(MAX(block_height), 0) FROM %s", db.TableNameTxs)
	rows, err := m.db.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var height sql.NullInt64
	if rows.Next() {
		if err := rows.Scan(&height); err != nil {
			return 0, err
		}
	}
	if !height.Valid {
		return 0, nil
	}
	return height.Int64, nil
}

func (m *Monitor) latestChainHeight(ctx context.Context) (int64, error) {
	type res struct {
		height int64
		err    error
	}
	resCh := make(chan res, len(m.clients))
	var wg sync.WaitGroup
	for _, client := range m.clients {
		wg.Add(1)
		go func(cli *rpchttp.HTTP) {
			defer wg.Done()
			resp, err := cli.Status(ctx)
			if err != nil {
				resCh <- res{err: err}
				return
			}
			resCh <- res{height: resp.SyncInfo.LatestBlockHeight}
		}(client)
	}

	var firstErr error
	for i := 0; i < len(m.clients); i++ {
		select {
		case r := <-resCh:
			if r.err == nil {
				return r.height, nil
			}
			if firstErr == nil {
				firstErr = r.err
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	wg.Wait()
	if firstErr != nil {
		return 0, firstErr
	}
	return 0, errors.New("unable to query any node")
}

func (m *Monitor) earliestChainHeight(ctx context.Context) (int64, error) {
	type res struct {
		height int64
		err    error
	}
	resCh := make(chan res, len(m.clients))
	var wg sync.WaitGroup
	for _, client := range m.clients {
		wg.Add(1)
		go func(cli *rpchttp.HTTP) {
			defer wg.Done()
			resp, err := cli.Status(ctx)
			if err != nil {
				resCh <- res{err: err}
				return
			}
			resCh <- res{height: resp.SyncInfo.EarliestBlockHeight}
		}(client)
	}

	var firstErr error
	for i := 0; i < len(m.clients); i++ {
		select {
		case r := <-resCh:
			if r.err == nil {
				return r.height, nil
			}
			if firstErr == nil {
				firstErr = r.err
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	wg.Wait()
	if firstErr != nil {
		return 0, firstErr
	}
	return 0, errors.New("unable to query any node for earliest height")
}

func (m *Monitor) fetchBlock(ctx context.Context, height int64) (ctypes.EventDataNewBlock, error) {
	type res struct {
		event ctypes.EventDataNewBlock
		err   error
	}
	resCh := make(chan res, len(m.clients))
	var wg sync.WaitGroup
	for _, client := range m.clients {
		wg.Add(1)
		go func(cli *rpchttp.HTTP) {
			defer wg.Done()
			blockRes, err := cli.Block(ctx, &height)
			if err != nil {
				resCh <- res{err: err}
				return
			}
			resultsRes, err := cli.BlockResults(ctx, &height)
			if err != nil {
				resCh <- res{err: err}
				return
			}
			event := ctypes.EventDataNewBlock{
				Block:               blockRes.Block,
				BlockID:             blockRes.BlockID,
				ResultFinalizeBlock: finalizeToResponse(resultsRes),
			}
			resCh <- res{event: event}
		}(client)
	}

	var firstErr error
	var fallbackEvent ctypes.EventDataNewBlock
	hasFallback := false

	for i := 0; i < len(m.clients); i++ {
		select {
		case r := <-resCh:
			if r.err == nil && r.event.Block != nil {
				// Prefer results with non-empty TxResults (from nodes with full data)
				if len(r.event.ResultFinalizeBlock.TxResults) > 0 {
					return r.event, nil
				}
				// Keep first valid result as fallback
				if !hasFallback {
					fallbackEvent = r.event
					hasFallback = true
				}
			}
			if r.err != nil && firstErr == nil {
				firstErr = r.err
			}
		case <-ctx.Done():
			return ctypes.EventDataNewBlock{}, ctx.Err()
		}
	}
	wg.Wait()

	// Use fallback if we have one (block with empty TxResults is better than nothing)
	if hasFallback {
		return fallbackEvent, nil
	}
	if firstErr != nil {
		return ctypes.EventDataNewBlock{}, firstErr
	}
	return ctypes.EventDataNewBlock{}, errors.New("no block data available")
}

func finalizeToResponse(results *coretypes.ResultBlockResults) abci.ResponseFinalizeBlock {
	if results == nil {
		return abci.ResponseFinalizeBlock{}
	}

	resp := abci.ResponseFinalizeBlock{
		Events:                results.FinalizeBlockEvents,
		TxResults:             results.TxsResults,
		ValidatorUpdates:      results.ValidatorUpdates,
		ConsensusParamUpdates: results.ConsensusParamUpdates,
		AppHash:               results.AppHash,
	}

	return resp
}
