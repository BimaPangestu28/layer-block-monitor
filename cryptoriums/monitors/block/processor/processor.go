package processor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cosmossdk.io/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	abci "github.com/cometbft/cometbft/abci/types"
	ctypes "github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authsigning "github.com/cosmos/cosmos-sdk/x/auth/signing"

	"github.com/tellor-io/layer/cryptoriums"
	blockdb "github.com/tellor-io/layer/cryptoriums/db"
	"github.com/tellor-io/layer/x/oracle/types"
)

const (
	ComponentName     = "block_processor"
	MetricErrCount    = "errors_total"
	MetricReportCount = "reports_total"

	// DefaultDBTimeout is the timeout for database operations.
	DefaultDBTimeout = 5 * time.Second
)

type DisputeEventHandler interface {
	HandleDisputeEvent(abci.Event)
}

type BlockProcessor interface {
	ProcessBlock(context.Context, ctypes.EventDataNewBlock) error
}

type Processor struct {
	logger       log.Logger
	db           blockdb.Db
	txDecoder    sdk.TxDecoder
	errCount     *prometheus.CounterVec
	reportCount  prometheus.Counter
	reportsCount *prometheus.CounterVec

	disputeHandler DisputeEventHandler

	mtx              sync.Mutex
	processedHeights map[int64]struct{}
	heightQueue      []int64
}

func New(
	logger log.Logger,
	db blockdb.Db,
	reg prometheus.Registerer,
	disputeHandler DisputeEventHandler,
) *Processor {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	errCount := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: cryptoriums.MetricsNamespace,
		Subsystem: ComponentName,
		Name:      MetricErrCount,
		Help:      "Errors in " + ComponentName,
	}, []string{"error"})

	reportCount := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Namespace: cryptoriums.MetricsNamespace,
		Subsystem: ComponentName,
		Name:      MetricReportCount,
		Help:      "Reports processed by " + ComponentName,
	})

	reportsCount := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: cryptoriums.MetricsNamespace,
		Subsystem: ComponentName,
		Name:      "reports_count",
		Help:      "Reports observed per reporter by " + ComponentName,
	}, []string{"reporter"})

	return &Processor{
		logger:           logger.With("component", ComponentName),
		db:               db,
		txDecoder:        NewTxDecoder(),
		errCount:         errCount,
		reportCount:      reportCount,
		reportsCount:     reportsCount,
		disputeHandler:   disputeHandler,
		processedHeights: make(map[int64]struct{}),
	}
}

func (p *Processor) ProcessBlock(ctx context.Context, blockEv ctypes.EventDataNewBlock) error {
	if blockEv.Block == nil {
		return nil
	}
	if !p.shouldProcess(blockEv.Block.Header.Height) {
		return nil
	}
	var errs []error
	if err := p.insertTx(ctx, blockEv); err != nil {
		errs = append(errs, err)
	}
	if err := p.insertEvents(ctx, blockEv); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

const processedHeightsLimit = 1000

func (p *Processor) shouldProcess(height int64) bool {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if _, seen := p.processedHeights[height]; seen {
		return false
	}

	p.processedHeights[height] = struct{}{}
	p.heightQueue = append(p.heightQueue, height)
	if len(p.heightQueue) > processedHeightsLimit {
		oldest := p.heightQueue[0]
		p.heightQueue = p.heightQueue[1:]
		delete(p.processedHeights, oldest)
	}
	return true
}

func (p *Processor) insertTx(ctx context.Context, blockEv ctypes.EventDataNewBlock) error {
	txs := blockEv.Block.Data.Txs
	txsResults := blockEv.ResultFinalizeBlock.TxResults
	if len(txs) != len(txsResults) {
		p.logger.Warn("txs length mismatch", "txs", len(txs), "results", len(txsResults))
	}

	var errs []error
	for i := 0; i < len(txs) && i < len(txsResults); i++ {
		raw := txs[i]

		if voteExtTx, ok := ParseVoteExtensionTx(raw); ok {
			p.logger.Debug("skipping vote extension tx", "height", voteExtTx.BlockHeight)
			continue
		}

		tx, err := p.txDecoder(raw)
		if err != nil {
			p.logger.Error("decode tx", "error", err)
			p.errCount.WithLabelValues("txDecode").Inc()
			continue
		}
		feeTx, ok := tx.(sdk.FeeTx)
		if !ok {
			continue
		}
		feeCoins := feeTx.GetFee()
		if feeCoins.Len() == 0 {
			continue
		}
		fee := feeCoins[0].Amount.String()

		txResp := txsResults[i]
		sender := findSenderFromTx(tx)
		if sender == "unknown" {
			sender = findSenderFromEvents(txResp.GetEvents())
		}

		insertCtx, cancel := context.WithTimeout(ctx, DefaultDBTimeout)
		_, err = p.db.Exec(insertCtx,
			fmt.Sprintf("INSERT INTO %s (block_height, tx_hash, sender, gas_used, fee_amount) VALUES (?, ?, ?, ?, ?)", blockdb.TableNameTxs),
			blockEv.Block.Header.Height,
			fmt.Sprintf("%X", ctypes.Tx(raw).Hash()),
			sender,
			txResp.GasUsed,
			fee,
		)
		cancel()
		if err != nil {
			p.logger.Error("inserting tx", "err", err)
			errs = append(errs, fmt.Errorf("insert tx %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (p *Processor) insertEvents(ctx context.Context, blockEv ctypes.EventDataNewBlock) error {
	height := blockEv.Block.Height
	var errs []error

	processEvents := func(events []abci.Event) {
		var currentQueryID string
		for _, ev := range events {
			switch ev.Type {
			case "aggregate_report":
				currentQueryID = getAttribute(ev, "query_id")
			case "new_report":
				report, err := DecodeReportEvent(height, ev)
				if err != nil {
					p.logger.Error("failed to decode report event", "error", err)
					p.errCount.WithLabelValues("reportDecode").Inc()
					errs = append(errs, fmt.Errorf("decode report: %w", err))
					continue
				}
				p.reportsCount.WithLabelValues(report.Reporter).Inc()
				if err := p.storeReport(ctx, *report); err != nil {
					p.logger.Error("failed to store report", "error", err)
					p.errCount.WithLabelValues("reportInsert").Inc()
					errs = append(errs, fmt.Errorf("store report: %w", err))
				}
			case "rewards_added":
				if err := p.insertReward(ctx, height, ev, currentQueryID); err != nil {
					p.logger.Error("failed to store reward", "error", err)
					p.errCount.WithLabelValues("rewardInsert").Inc()
					errs = append(errs, fmt.Errorf("store reward: %w", err))
				}
				currentQueryID = ""
			case "new_dispute":
				if p.disputeHandler != nil {
					p.disputeHandler.HandleDisputeEvent(ev)
				}
			}
		}
	}

	processEvents(blockEv.ResultFinalizeBlock.Events)
	for _, txResult := range blockEv.ResultFinalizeBlock.TxResults {
		if txResult == nil {
			continue
		}
		processEvents(txResult.Events)
	}
	return errors.Join(errs...)
}

func (p *Processor) insertReward(ctx context.Context, height int64, ev abci.Event, queryID string) error {
	delegatorRaw := getAttribute(ev, "delegator")
	if delegatorRaw == "" {
		return fmt.Errorf("reward missing delegator")
	}
	addrBytes := []byte(delegatorRaw)
	if err := sdk.VerifyAddressFormat(addrBytes); err != nil {
		return fmt.Errorf("verify address format: %w", err)
	}
	reporter := sdk.AccAddress(addrBytes).String()

	amountStr := getAttribute(ev, "amount")
	if amountStr == "" {
		return fmt.Errorf("reward missing amount")
	}

	insertCtx, cancel := context.WithTimeout(ctx, DefaultDBTimeout)
	defer cancel()

	_, err := p.db.Exec(insertCtx,
		fmt.Sprintf("INSERT INTO %s (block_height, reporter, query_id, amount) VALUES (?, ?, ?, ?)", blockdb.TableNameRewards),
		height,
		reporter,
		queryID,
		amountStr,
	)
	return err
}

func (p *Processor) storeReport(ctx context.Context, r types.MicroReport) error {
	ctx, cancel := context.WithTimeout(ctx, DefaultDBTimeout)
	defer cancel()

	var cycle uint8
	if r.Cyclelist {
		cycle = 1
	}

	type columnVal struct {
		name string
		val  any
	}
	queryIDHex, err := EncodeQueryID(r.QueryId)
	if err != nil {
		return err
	}

	cols := []columnVal{
		{name: reporterCol, val: r.Reporter},
		{name: powerCol, val: r.Power},
		{name: queryTypeCol, val: r.QueryType},
		{name: queryIdCol, val: queryIDHex},
		{name: aggregateMethodCol, val: r.AggregateMethod},
		{name: valueCol, val: r.Value},
		{name: timestampCol, val: r.Timestamp},
		{name: cyclelistCol, val: cycle},
		{name: blockNumberCol, val: r.BlockNumber},
		{name: metaIdCol, val: r.MetaId},
	}

	var colNames []string
	var vals []any
	for _, cv := range cols {
		colNames = append(colNames, cv.name)
		vals = append(vals, cv.val)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		blockdb.TableNameReports,
		strings.Join(colNames, ", "),
		strings.TrimRight(strings.Repeat("?, ", len(colNames)), ", "),
	)

	if _, err := p.db.Exec(ctx, query, vals...); err != nil {
		return err
	}

	p.reportCount.Inc()
	return nil
}

func getAttribute(ev abci.Event, key string) string {
	for _, attr := range ev.Attributes {
		if attr.Key == key {
			return attr.Value
		}
	}
	return ""
}

func findSenderFromEvents(events []abci.Event) string {
	for _, ev := range events {
		if ev.Type != "message" {
			continue
		}
		if sender := getAttribute(ev, "sender"); sender != "" {
			return sender
		}
	}
	return "unknown"
}

func findSenderFromTx(tx sdk.Tx) string {
	if signingTx, ok := tx.(authsigning.Tx); ok {
		signers, err := signingTx.GetSigners()
		if err == nil && len(signers) > 0 {
			return sdk.AccAddress(signers[0]).String()
		}
	}
	return "unknown"
}

const (
	reporterCol        = "reporter"
	powerCol           = "power"
	queryTypeCol       = "query_type"
	queryIdCol         = "query_id"
	aggregateMethodCol = "aggregate_method"
	valueCol           = "value"
	timestampCol       = "timestamp"
	cyclelistCol       = "cyclelist"
	blockNumberCol     = "block_number"
	metaIdCol          = "meta_id"
)
