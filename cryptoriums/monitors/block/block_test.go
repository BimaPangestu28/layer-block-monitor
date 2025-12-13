package block

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	rpctest "github.com/cometbft/cometbft/rpc/test"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	blockdb "github.com/tellor-io/layer/cryptoriums/db"
	cryptolog "github.com/tellor-io/layer/cryptoriums/log"
)

// For all tests use only public module functions.
// For matching exp vs act, use the db or the prometheus metrics.

// TODO
func TestBackfill(t *testing.T) {
}

func TestDeduplication(t *testing.T) {
	type nodeStream struct {
		name      string
		idxToSend []int
	}
	fixtures := loadFixtures(t)
	require.NotEmpty(t, fixtures)

	expected := extractExpectedReports(t, fixtures)
	require.NotEmpty(t, expected)

	cases := []struct {
		name  string
		nodes []nodeStream
	}{
		{
			name: "second node stopped after first block",
			nodes: []nodeStream{
				{name: "primary", idxToSend: []int{0, 1, 2, 3}},
				{name: "secondary", idxToSend: []int{0}},
			},
		},
		{
			name: "mixed blocks from different nodes",
			nodes: []nodeStream{
				{name: "primary", idxToSend: []int{0, 1}},
				{name: "secondary", idxToSend: []int{1, 2, 3}},
			},
		},
		{
			name: "second node starts sending later",
			nodes: []nodeStream{
				{name: "primary", idxToSend: []int{0, 1, 2, 3}},
				{name: "secondary", idxToSend: []int{2, 3}},
			},
		},
		{
			name: "three nodes emit identical blocks",
			nodes: []nodeStream{
				{name: "node-a", idxToSend: []int{0, 1, 2, 3}},
				{name: "node-b", idxToSend: []int{0, 1, 2, 3}},
				{name: "node-c", idxToSend: []int{0, 1, 2, 3}},
			},
		},
		{
			name: "multiple nodes send each different blocks",
			nodes: []nodeStream{
				{name: "node-1", idxToSend: []int{0, 1}},
				{name: "node-2", idxToSend: []int{2}},
				{name: "node-3", idxToSend: []int{3}},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			_ = cancel // cancel is called in cleanup

			sqlDB, err := sql.Open("chdb", "")
			require.NoError(t, err)
			// Register DB close FIRST so it runs LAST (after monitor and nodes stop)
			t.Cleanup(func() { _ = sqlDB.Close() })

			wrappedDB, err := blockdb.New(ctx, sqlDB)
			require.NoError(t, err)

			rpcCfg := Config{Backfill: true, PollInterval: 200 * time.Millisecond}
			for _, nodeCfg := range tc.nodes {
				fixturesToSend := prepareBatchToSend(nodeCfg.idxToSend, fixtures)
				app := newTestApp(t)
				app.SetBatch(fixturesToSend)

				cfg := rpctest.GetConfig(true)
				genDoc, err := ctypes.GenesisDocFromFile(cfg.GenesisFile())
				require.NoError(t, err)
				genDoc.InitialHeight = fixtureHeight(fixturesToSend[0])
				require.NoError(t, genDoc.SaveAs(cfg.GenesisFile()))

				node := rpctest.StartTendermint(app)

				t.Cleanup(func() {
					rpctest.StopTendermint(node)
				})

				rpcCfg.Nodes = append(rpcCfg.Nodes, node.Config().RPC.ListenAddress)
			}

			monitor, err := New(
				ctx,
				cryptolog.New(),
				rpcCfg,
				prometheus.NewRegistry(),
				wrappedDB,
			)
			require.NoError(t, err)

			runErr := make(chan error, 1)
			go func() {
				time.Sleep(2 * time.Second)
				runErr <- monitor.Run(ctx)
			}()

			// Register cleanup for monitor LAST so it runs FIRST (before StopTendermint)
			// This ensures we cancel context and wait for monitor to stop before stopping nodes
			t.Cleanup(func() {
				cancel() // Stop the monitor first
				select {
				case err := <-runErr:
					if err != nil && err != context.Canceled {
						t.Fatalf("monitor run failed: %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("monitor did not stop")
				}
			})

			expectedCount := len(expected)
			require.Eventually(t, func() bool {
				actual := fetchReportsFromDB(t, sqlDB)
				return len(actual) == expectedCount
			}, 5*time.Second, 200*time.Millisecond, "expected %d reports, got %d", expectedCount, len(fetchReportsFromDB(t, sqlDB)))

			actualReports := sortReports(t, copyReports(fetchReportsFromDB(t, sqlDB)))
			expectedSorted := sortReports(t, copyReports(expected))
			require.Equal(t, expectedSorted, actualReports)
		})
	}
}
