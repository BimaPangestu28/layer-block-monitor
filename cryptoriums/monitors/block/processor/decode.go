package processor

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	evidencemodule "cosmossdk.io/x/evidence"
	feegrantmodule "cosmossdk.io/x/feegrant/module"
	upgrademodule "cosmossdk.io/x/upgrade"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/std"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/module"
	authmodule "github.com/cosmos/cosmos-sdk/x/auth"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	vestingmodule "github.com/cosmos/cosmos-sdk/x/auth/vesting"
	authzmodule "github.com/cosmos/cosmos-sdk/x/authz/module"
	"github.com/cosmos/cosmos-sdk/x/bank"
	consensusmodule "github.com/cosmos/cosmos-sdk/x/consensus"
	distr "github.com/cosmos/cosmos-sdk/x/distribution"
	genutil "github.com/cosmos/cosmos-sdk/x/genutil"
	gov "github.com/cosmos/cosmos-sdk/x/gov"
	govclient "github.com/cosmos/cosmos-sdk/x/gov/client"
	groupmodule "github.com/cosmos/cosmos-sdk/x/group/module"
	slashingmodule "github.com/cosmos/cosmos-sdk/x/slashing"
	stakingmodule "github.com/cosmos/cosmos-sdk/x/staking"
	icq "github.com/cosmos/ibc-apps/modules/async-icq/v8"
	ica "github.com/cosmos/ibc-go/v8/modules/apps/27-interchain-accounts"
	ibctransfer "github.com/cosmos/ibc-go/v8/modules/apps/transfer"
	ibc "github.com/cosmos/ibc-go/v8/modules/core"
	ibctm "github.com/cosmos/ibc-go/v8/modules/light-clients/07-tendermint"
	"github.com/strangelove-ventures/globalfee/x/globalfee"

	"github.com/tellor-io/layer/app"
	bridgemodule "github.com/tellor-io/layer/x/bridge"
	disputemodule "github.com/tellor-io/layer/x/dispute"
	mintmodule "github.com/tellor-io/layer/x/mint"
	oraclemodule "github.com/tellor-io/layer/x/oracle"
	"github.com/tellor-io/layer/x/oracle/types"
	registrymodule "github.com/tellor-io/layer/x/registry/module"
	reportermodule "github.com/tellor-io/layer/x/reporter/module"
)

// ParseTimestamp parses timestamps stored on chain into UTC time.
func ParseTimestamp(val string) (time.Time, error) {
	if ts, err := time.Parse(time.RFC3339Nano, val); err == nil {
		return ts.UTC(), nil
	}
	ms, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}

// ParseReporterPower parses the reporter power column.
func ParseReporterPower(val string) (uint64, error) {
	return parseUint64(val)
}

// ParseBlockNumber parses the block_number column.
func ParseBlockNumber(val string) (uint64, error) {
	return parseUint64(val)
}

// ParseMetaID parses the meta_id column.
func ParseMetaID(val string) (uint64, error) {
	return parseUint64(val)
}

// parseUint64 converts decimal strings to uint64.
func parseUint64(val string) (uint64, error) {
	return strconv.ParseUint(val, 10, 64)
}

// EncodeQueryID converts 32-byte query IDs to a hex string.
func EncodeQueryID(queryID []byte) (string, error) {
	if len(queryID) != 32 {
		return "", fmt.Errorf("query_id must be 32 bytes, got %d", len(queryID))
	}
	return hex.EncodeToString(queryID), nil
}

// DecodeQueryID converts a hex string query ID to bytes.
func DecodeQueryID(val string) ([]byte, error) {
	if val == "" {
		return nil, fmt.Errorf("query_id is empty")
	}
	return hex.DecodeString(val)
}

// DecodeReportEvent extracts a MicroReport from a CometBFT event payload.
func DecodeReportEvent(height int64, ev abci.Event) (*types.MicroReport, error) {
	var report types.MicroReport

	for _, attr := range ev.Attributes {
		attrVal := attr.Value
		switch attr.Key {
		case reporterCol:
			report.Reporter = attrVal
		case powerCol, "reporter_power":
			power, err := ParseReporterPower(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse reporter power: %w", err)
			}
			report.Power = power
		case queryTypeCol:
			report.QueryType = attrVal
		case queryIdCol:
			queryIDBytes, err := DecodeQueryID(attrVal)
			if err != nil {
				return nil, fmt.Errorf("decode query_id: %w", err)
			}
			report.QueryId = queryIDBytes
		case aggregateMethodCol:
			report.AggregateMethod = attrVal
		case valueCol:
			report.Value = attrVal
		case timestampCol:
			ts, err := ParseTimestamp(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse timestamp: %w", err)
			}
			report.Timestamp = ts
		case cyclelistCol:
			report.Cyclelist = attrVal == "true"
		case blockNumberCol:
			blockNumber, err := ParseBlockNumber(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse block number: %w", err)
			}
			report.BlockNumber = blockNumber
			if report.BlockNumber == 0 && height > 0 {
				report.BlockNumber = uint64(height)
			}
		case metaIdCol:
			metaId, err := ParseMetaID(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse meta_id: %w", err)
			}
			report.MetaId = metaId
		}
	}

	// If BlockNumber wasn't set from attributes (or was set to 0), use the height parameter
	if report.BlockNumber == 0 && height > 0 {
		report.BlockNumber = uint64(height)
	}

	return &report, nil
}

// NewTxDecoder builds a Cosmos SDK TxDecoder capable of handling Layer transactions.
func NewTxDecoder() sdk.TxDecoder {
	interfaceRegistry := codectypes.NewInterfaceRegistry()
	std.RegisterInterfaces(interfaceRegistry)

	moduleBasics := module.NewBasicManager(
		genutil.AppModuleBasic{},
		authmodule.AppModuleBasic{},
		authzmodule.AppModuleBasic{},
		vestingmodule.AppModuleBasic{},
		bank.AppModuleBasic{},
		feegrantmodule.AppModuleBasic{},
		groupmodule.AppModuleBasic{},
		gov.NewAppModuleBasic([]govclient.ProposalHandler{}),
		slashingmodule.AppModuleBasic{},
		distr.AppModuleBasic{},
		stakingmodule.AppModuleBasic{},
		upgrademodule.AppModuleBasic{},
		evidencemodule.AppModuleBasic{},
		consensusmodule.AppModuleBasic{},
		ica.AppModuleBasic{},
		icq.AppModuleBasic{},
		ibc.AppModuleBasic{},
		ibctransfer.AppModuleBasic{},
		ibctm.AppModuleBasic{},
		mintmodule.AppModuleBasic{},
		oraclemodule.AppModuleBasic{},
		registrymodule.AppModuleBasic{},
		disputemodule.AppModuleBasic{},
		bridgemodule.AppModuleBasic{},
		reportermodule.AppModuleBasic{},
		globalfee.AppModuleBasic{},
	)
	moduleBasics.RegisterInterfaces(interfaceRegistry)

	protoCodec := codec.NewProtoCodec(interfaceRegistry)
	txConfig := authtx.NewTxConfig(protoCodec, authtx.DefaultSignModes)

	protoDecoder := txConfig.TxDecoder()
	jsonDecoder := txConfig.TxJSONDecoder()

	return func(txBytes []byte) (sdk.Tx, error) {
		if tx, err := protoDecoder(txBytes); err == nil {
			return tx, nil
		}
		return jsonDecoder(txBytes)
	}
}

// ParseVoteExtensionTx attempts to decode vote extension payloads emitted as JSON.
func ParseVoteExtensionTx(raw []byte) (*app.VoteExtTx, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || (trimmed[0] != '{' && trimmed[0] != '[') {
		return nil, false
	}

	var voteTx app.VoteExtTx
	if err := json.Unmarshal(trimmed, &voteTx); err != nil {
		return nil, false
	}

	if voteTx.BlockHeight == 0 && voteTx.ExtendedCommitInfo.Round == 0 && len(voteTx.ExtendedCommitInfo.Votes) == 0 &&
		len(voteTx.OpAndEVMAddrs.OperatorAddresses) == 0 &&
		len(voteTx.ValsetSigs.OperatorAddresses) == 0 &&
		len(voteTx.OracleAttestations.OperatorAddresses) == 0 {
		return nil, false
	}

	return &voteTx, true
}
