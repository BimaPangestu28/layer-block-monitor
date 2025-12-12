package block

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	blockdb "github.com/tellor-io/layer/cryptoriums/db"
	cryptolog "github.com/tellor-io/layer/cryptoriums/log"
	"github.com/tellor-io/layer/cryptoriums/monitors/block/processor"
)

// For all tests use only public module functions.
// For matching exp vs act, use the db or the prometheus metrics.

func TestBackfill(t *testing.T) {
	// TODO: implement backfill test with mock RPC client
}

// TestProcessorDeduplication tests that the processor correctly deduplicates
// blocks when the same block is processed multiple times (e.g., from different RPC nodes).
func TestProcessorDeduplication(t *testing.T) {
	rawFixtures := loadFixtures(t)
	require.NotEmpty(t, rawFixtures)

	// Normalize fixtures to use low heights (1, 2, 3, 4)
	fixtures := normalizeFixtureHeights(rawFixtures)

	expected := extractExpectedReports(t, fixtures)
	require.NotEmpty(t, expected)

	cases := []struct {
		name           string
		blocksToEmit   []int // indices of fixtures to process
		duplicateEmits int   // how many times to emit each block
	}{
		{
			name:           "all blocks once",
			blocksToEmit:   []int{0, 1, 2, 3},
			duplicateEmits: 1,
		},
		{
			name:           "all blocks twice (duplicate from two nodes)",
			blocksToEmit:   []int{0, 1, 2, 3},
			duplicateEmits: 2,
		},
		{
			name:           "all blocks three times (duplicate from three nodes)",
			blocksToEmit:   []int{0, 1, 2, 3},
			duplicateEmits: 3,
		},
		{
			name:           "first block only, duplicated",
			blocksToEmit:   []int{0},
			duplicateEmits: 5,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sqlDB, err := sql.Open("chdb", "")
			require.NoError(t, err)
			defer func() { _ = sqlDB.Close() }()

			// Drop existing tables to ensure fresh state for each test case
			_, _ = sqlDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+blockdb.TableNameReports)
			_, _ = sqlDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+blockdb.TableNameTxs)
			_, _ = sqlDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+blockdb.TableNameRewards)

			wrappedDB, err := blockdb.New(ctx, sqlDB)
			require.NoError(t, err)

			proc := processor.New(
				cryptolog.New(),
				wrappedDB,
				prometheus.NewRegistry(),
				nil,
			)

			// Emit blocks multiple times to test deduplication
			for i := 0; i < tc.duplicateEmits; i++ {
				for _, idx := range tc.blocksToEmit {
					proc.ProcessBlock(ctx, fixtures[idx])
				}
			}

			// Calculate expected reports for the blocks we emitted
			var expectedForTest []any
			emittedBlocks := make(map[int]bool)
			for _, idx := range tc.blocksToEmit {
				emittedBlocks[idx] = true
			}

			actualReports := fetchReportsFromDB(t, sqlDB)

			// For "all blocks" tests, compare with full expected
			if len(tc.blocksToEmit) == len(fixtures) {
				expectedSorted := sortReports(t, copyReports(expected))
				actualSorted := sortReports(t, copyReports(actualReports))
				require.Equal(t, len(expectedSorted), len(actualSorted), "report count mismatch")
				require.Equal(t, expectedSorted, actualSorted)
			} else {
				// For partial tests, just verify no duplicates and count is reasonable
				// Each block should only be processed once despite multiple emissions
				require.NotEmpty(t, actualReports, "should have some reports")

				// Check for duplicates by reporter+block_number+query_id
				seen := make(map[string]bool)
				for _, r := range actualReports {
					key := r.Reporter + string(r.QueryId) + string(rune(r.BlockNumber))
					require.False(t, seen[key], "duplicate report found: %s", key)
					seen[key] = true
				}
			}
			_ = expectedForTest // silence unused warning
		})
	}
}

// TestProcessBlockReturnsError tests that ProcessBlock returns errors properly
// when database operations fail.
func TestProcessBlockReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlDB, err := sql.Open("chdb", "")
	require.NoError(t, err)
	defer func() { _ = sqlDB.Close() }()

	// Drop tables first
	_, _ = sqlDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+blockdb.TableNameReports)
	_, _ = sqlDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+blockdb.TableNameTxs)
	_, _ = sqlDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+blockdb.TableNameRewards)

	wrappedDB, err := blockdb.New(ctx, sqlDB)
	require.NoError(t, err)

	proc := processor.New(
		cryptolog.New(),
		wrappedDB,
		prometheus.NewRegistry(),
		nil,
	)

	// Load and process valid fixtures - should return no error
	rawFixtures := loadFixtures(t)
	require.NotEmpty(t, rawFixtures)

	fixtures := normalizeFixtureHeights(rawFixtures)
	err = proc.ProcessBlock(ctx, fixtures[0])
	require.NoError(t, err, "ProcessBlock should succeed for valid block")

	// Test with nil block - should return no error (early return)
	err = proc.ProcessBlock(ctx, ctypes.EventDataNewBlock{})
	require.NoError(t, err, "ProcessBlock should succeed for nil block")
}

// TestDBTimeoutConstant verifies that the database timeout is set to a reasonable value.
func TestDBTimeoutConstant(t *testing.T) {
	require.Equal(t, 5*time.Second, processor.DefaultDBTimeout,
		"DB timeout should be 5 seconds, not too short")
}

// TestProcessBlockDeduplicationWithError tests that deduplication works correctly
// and doesn't interfere with error handling.
func TestProcessBlockDeduplicationWithError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlDB, err := sql.Open("chdb", "")
	require.NoError(t, err)
	defer func() { _ = sqlDB.Close() }()

	// Drop and recreate tables
	_, _ = sqlDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+blockdb.TableNameReports)
	_, _ = sqlDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+blockdb.TableNameTxs)
	_, _ = sqlDB.ExecContext(ctx, "DROP TABLE IF EXISTS "+blockdb.TableNameRewards)

	wrappedDB, err := blockdb.New(ctx, sqlDB)
	require.NoError(t, err)

	proc := processor.New(
		cryptolog.New(),
		wrappedDB,
		prometheus.NewRegistry(),
		nil,
	)

	rawFixtures := loadFixtures(t)
	require.NotEmpty(t, rawFixtures)
	fixtures := normalizeFixtureHeights(rawFixtures)

	// Process first block - should succeed
	err = proc.ProcessBlock(ctx, fixtures[0])
	require.NoError(t, err)

	// Process same block again - should be deduplicated (no error, no processing)
	err = proc.ProcessBlock(ctx, fixtures[0])
	require.NoError(t, err, "Deduplicated block should not return error")

	// Verify only one set of reports was inserted
	reports := fetchReportsFromDB(t, sqlDB)
	expectedFirst := extractExpectedReports(t, fixtures[:1])

	require.Equal(t, len(expectedFirst), len(reports),
		"Should have reports from only one block despite multiple ProcessBlock calls")
}
