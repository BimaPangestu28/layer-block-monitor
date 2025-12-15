package block

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cometbft/cometbft/abci/example/kvstore"
	abci "github.com/cometbft/cometbft/abci/types"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
	blockdb "github.com/tellor-io/layer/cryptoriums/db"
	blockprocessor "github.com/tellor-io/layer/cryptoriums/monitors/block/processor"
	"github.com/tellor-io/layer/x/oracle/types"
)

func loadFixtures(t *testing.T) []ctypes.EventDataNewBlock {
	t.Helper()

	path := fixturePath(t, "blocks_fixtures.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var fixtures []ctypes.EventDataNewBlock
	require.NoError(t, json.Unmarshal(data, &fixtures))

	return fixtures
}

func fixturePath(t *testing.T, name string) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to determine caller path")

	dir := filepath.Dir(filename)
	return filepath.Join(dir, name)
}

func fixtureHeight(f ctypes.EventDataNewBlock) int64 {
	if f.Block == nil {
		return 0
	}
	return f.Block.Header.Height
}

func extractExpectedReports(t *testing.T, fixtures []ctypes.EventDataNewBlock) []types.MicroReport {
	t.Helper()

	var reports []types.MicroReport
	for _, fixture := range fixtures {
		height := fixtureHeight(fixture)
		for _, tx := range fixture.ResultFinalizeBlock.TxResults {
			if tx == nil {
				continue
			}
			for _, ev := range tx.Events {
				if ev.Type != "new_report" {
					continue
				}
				report, err := blockprocessor.DecodeReportEvent(height, ev)
				require.NoError(t, err)

				reports = append(reports, *report)
			}
		}
	}

	return reports
}

func fetchReportsFromDB(t *testing.T, db *sql.DB) []types.MicroReport {
	t.Helper()

	query := fmt.Sprintf("SELECT reporter, power, query_type, query_id, aggregate_method, value, timestamp, cyclelist, block_number, meta_id FROM %s ORDER BY block_number, reporter", blockdb.TableNameReports)
	rows, err := db.QueryContext(context.Background(), query)
	require.NoError(t, err)
	defer rows.Close()

	var reports []types.MicroReport
	for rows.Next() {
		var (
			reporter        string
			power           uint64
			queryType       string
			queryIDStr      string
			aggregateMethod string
			value           string
			ts              time.Time
			cycle           uint8
			blockNumber     uint64
			metaID          uint64
		)
		require.NoError(t, rows.Scan(&reporter, &power, &queryType, &queryIDStr, &aggregateMethod, &value, &ts, &cycle, &blockNumber, &metaID))

		queryID, err := blockprocessor.DecodeQueryID(queryIDStr)
		require.NoError(t, err)

		reports = append(reports, types.MicroReport{
			Reporter:        reporter,
			Power:           power,
			QueryType:       queryType,
			QueryId:         queryID,
			AggregateMethod: aggregateMethod,
			Value:           value,
			Timestamp:       ts.UTC(),
			Cyclelist:       cycle == 1,
			BlockNumber:     blockNumber,
			MetaId:          metaID,
		})
	}
	require.NoError(t, rows.Err())

	return reports
}

func sortReports(t *testing.T, reports []types.MicroReport) []types.MicroReport {
	t.Helper()

	sort.Slice(reports, func(i, j int) bool {
		if reports[i].BlockNumber != reports[j].BlockNumber {
			return reports[i].BlockNumber < reports[j].BlockNumber
		}
		if reports[i].Reporter != reports[j].Reporter {
			return reports[i].Reporter < reports[j].Reporter
		}
		// Use QueryId as tie-breaker for same reporter in same block
		return string(reports[i].QueryId) < string(reports[j].QueryId)
	})

	return reports
}

func copyReports(reports []types.MicroReport) []types.MicroReport {
	if reports == nil {
		return nil
	}
	out := make([]types.MicroReport, len(reports))
	for i, r := range reports {
		out[i] = r
		if len(r.QueryId) > 0 {
			out[i].QueryId = append([]byte(nil), r.QueryId...)
		}
	}
	return out
}

func prepareBatchToSend(idxToSend []int, fixtures []ctypes.EventDataNewBlock) []ctypes.EventDataNewBlock {
	emit := make(map[int]struct{}, len(idxToSend))
	for _, idx := range idxToSend {
		emit[idx] = struct{}{}
	}

	batch := make([]ctypes.EventDataNewBlock, 0, len(fixtures))
	for idx, fx := range fixtures {
		event := ctypes.EventDataNewBlock{
			Block:   fx.Block,
			BlockID: fx.BlockID,
		}
		if _, ok := emit[idx]; ok {
			event = fx
		}
		batch = append(batch, event)
	}
	return batch
}

type testApp struct {
	kvstore.Application
	t *testing.T

	mu            sync.Mutex
	currentHeight int64

	payloadMu sync.Mutex
	payloads  map[int64]ctypes.EventDataNewBlock
	notifyCh  chan struct{}
}

func newTestApp(t *testing.T) *testApp {
	return &testApp{
		Application: *kvstore.NewInMemoryApplication(),
		payloads:    make(map[int64]ctypes.EventDataNewBlock),
		notifyCh:    make(chan struct{}, 1),
		t:           t,
	}
}

func (app *testApp) SetBatch(blocks []ctypes.EventDataNewBlock) {
	if len(blocks) == 0 {
		return
	}

	app.payloadMu.Lock()
	for _, block := range blocks {
		if block.Block == nil {
			continue
		}
		height := block.Block.Header.Height
		app.payloads[height] = block
	}
	app.payloadMu.Unlock()
	app.signal()
}

func (app *testApp) CurrentHeight() int64 {
	app.mu.Lock()
	defer app.mu.Unlock()
	return app.currentHeight
}

func (app *testApp) FinalizeBlock(ctx context.Context, req *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, error) {
	app.mu.Lock()
	app.currentHeight = int64(req.Height)
	app.mu.Unlock()

	payload, err := app.waitForPayload(ctx, int64(req.Height))
	if err != nil {
		return nil, err
	}
	app.removePayload(int64(req.Height))

	if isEmptyEventDataNewBlock(payload) {
		return &abci.ResponseFinalizeBlock{}, nil
	}

	resp := payload.ResultFinalizeBlock

	// Clear ConsensusParamUpdates to avoid vote extensions validation error
	// when using production fixtures with high block heights
	resp.ConsensusParamUpdates = nil

	// CometBFT validates that len(TxResults) == len(req.Txs)
	// req.Txs comes from what CometBFT actually included in the block.
	// We need to match this count while preserving fixture events.
	txCount := len(req.Txs)
	fixtureTxResults := resp.TxResults

	// Build new TxResults matching req.Txs count
	newTxResults := make([]*abci.ExecTxResult, txCount)
	for i := 0; i < txCount; i++ {
		if i < len(fixtureTxResults) && fixtureTxResults[i] != nil {
			// Use fixture's TxResult if available
			newTxResults[i] = fixtureTxResults[i]
		} else {
			// Pad with empty result for extra transactions (e.g., vote extensions)
			newTxResults[i] = &abci.ExecTxResult{}
		}
	}
	resp.TxResults = newTxResults

	return &resp, nil
}

func (app *testApp) PrepareProposal(ctx context.Context, req *abci.RequestPrepareProposal) (*abci.ResponsePrepareProposal, error) {
	payload, ok := app.peekPayload(int64(req.Height))
	if !ok || payload.Block == nil || isEmptyEventDataNewBlock(payload) {
		return &abci.ResponsePrepareProposal{}, nil
	}
	return &abci.ResponsePrepareProposal{Txs: cloneTxs(payload.Block.Data.Txs)}, nil
}

func (app *testApp) ProcessProposal(context.Context, *abci.RequestProcessProposal) (*abci.ResponseProcessProposal, error) {
	return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_ACCEPT}, nil
}

func (app *testApp) CheckTx(context.Context, *abci.RequestCheckTx) (*abci.ResponseCheckTx, error) {
	return &abci.ResponseCheckTx{Code: kvstore.CodeTypeOK, GasWanted: 1}, nil
}

func (app *testApp) Commit(context.Context, *abci.RequestCommit) (*abci.ResponseCommit, error) {
	return &abci.ResponseCommit{}, nil
}

func (app *testApp) waitForPayload(ctx context.Context, height int64) (ctypes.EventDataNewBlock, error) {
	// Quick check without waiting - if no payload exists, return empty immediately
	// This handles heights beyond our fixtures (e.g., 10037795 when we only have 10037791-10037794)
	app.payloadMu.Lock()
	payload, ok := app.payloads[height]
	app.payloadMu.Unlock()
	if ok {
		return payload, nil
	}

	// Wait briefly for payload in case it's being added
	timeout := time.After(100 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return ctypes.EventDataNewBlock{}, ctx.Err()
		case <-timeout:
			// No payload after timeout - return empty
			return ctypes.EventDataNewBlock{}, nil
		case <-app.notifyCh:
			app.payloadMu.Lock()
			payload, ok := app.payloads[height]
			app.payloadMu.Unlock()
			if ok {
				return payload, nil
			}
		}
	}
}

func (app *testApp) removePayload(height int64) {
	app.payloadMu.Lock()
	delete(app.payloads, height)
	app.payloadMu.Unlock()
}

func (app *testApp) peekPayload(height int64) (ctypes.EventDataNewBlock, bool) {
	app.payloadMu.Lock()
	defer app.payloadMu.Unlock()
	payload, ok := app.payloads[height]
	return payload, ok
}

func (app *testApp) signal() {
	select {
	case app.notifyCh <- struct{}{}:
	default:
	}
}

func cloneTxs(txs ctypes.Txs) [][]byte {
	if len(txs) == 0 {
		return nil
	}
	out := make([][]byte, len(txs))
	for i, tx := range txs {
		out[i] = append([]byte(nil), tx...)
	}
	return out
}

func isEmptyEventDataNewBlock(ev ctypes.EventDataNewBlock) bool {
	return ev.Block == nil && len(ev.ResultFinalizeBlock.Events) == 0 && len(ev.ResultFinalizeBlock.TxResults) == 0
}

// waitForNodeReady polls the RPC node until it reports a valid chain height.
// This replaces hardcoded sleeps with a proper readiness check.
func waitForNodeReady(t *testing.T, nodeAddr string) {
	t.Helper()

	client, err := rpchttp.New(nodeAddr, "/websocket")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.Eventually(t, func() bool {
		status, err := client.Status(ctx)
		if err != nil {
			return false
		}
		return status.SyncInfo.LatestBlockHeight > 0
	}, 10*time.Second, 100*time.Millisecond, "node at %s did not become ready", nodeAddr)
}
