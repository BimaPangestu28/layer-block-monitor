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
		// Compare QueryId as hex string for stable ordering
		queryIdI := fmt.Sprintf("%x", reports[i].QueryId)
		queryIdJ := fmt.Sprintf("%x", reports[j].QueryId)
		if queryIdI != queryIdJ {
			return queryIdI < queryIdJ
		}
		// Compare MetaId as final tiebreaker
		return reports[i].MetaId < reports[j].MetaId
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
		if _, ok := emit[idx]; ok {
			batch = append(batch, fx)
		} else {
			// Create empty placeholder to maintain index alignment
			// Empty block means the node doesn't have this block data
			batch = append(batch, ctypes.EventDataNewBlock{})
		}
	}
	return batch
}

// normalizeFixtureHeights transforms fixtures to use sequential heights starting from 1
// This avoids consensus issues when using high production block heights
func normalizeFixtureHeights(fixtures []ctypes.EventDataNewBlock) []ctypes.EventDataNewBlock {
	normalized := make([]ctypes.EventDataNewBlock, len(fixtures))
	for i, fx := range fixtures {
		normalized[i] = fx
		newHeight := int64(i + 1)

		if fx.Block != nil {
			// Modify height directly - fixtures are test data, we can mutate them
			fx.Block.Header.Height = newHeight
			normalized[i].Block = fx.Block
		}

		// Update block number in TxResults events if present
		if len(fx.ResultFinalizeBlock.TxResults) > 0 {
			normalized[i].ResultFinalizeBlock.TxResults = make([]*abci.ExecTxResult, len(fx.ResultFinalizeBlock.TxResults))
			for j, txResult := range fx.ResultFinalizeBlock.TxResults {
				if txResult != nil {
					txCopy := *txResult
					// Update block_number in events
					txCopy.Events = updateBlockNumberInEvents(txCopy.Events, newHeight)
					normalized[i].ResultFinalizeBlock.TxResults[j] = &txCopy
				}
			}
		}

		// Update block height events
		normalized[i].ResultFinalizeBlock.Events = updateBlockNumberInEvents(fx.ResultFinalizeBlock.Events, newHeight)
	}
	return normalized
}

func updateBlockNumberInEvents(events []abci.Event, newHeight int64) []abci.Event {
	result := make([]abci.Event, len(events))
	for i, ev := range events {
		result[i] = ev
		attrs := make([]abci.EventAttribute, len(ev.Attributes))
		for j, attr := range ev.Attributes {
			attrs[j] = attr
			if attr.Key == "block_number" {
				attrs[j].Value = fmt.Sprintf("%d", newHeight)
			}
		}
		result[i].Attributes = attrs
	}
	return result
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
	// Clear ConsensusParamUpdates to avoid vote extensions validation errors
	// when running tests with high block heights from production fixtures
	resp.ConsensusParamUpdates = nil
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
	for {
		app.payloadMu.Lock()
		payload, ok := app.payloads[height]
		app.payloadMu.Unlock()

		if ok {
			return payload, nil
		}

		select {
		case <-ctx.Done():
			return ctypes.EventDataNewBlock{}, ctx.Err()
		case <-app.notifyCh:
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
