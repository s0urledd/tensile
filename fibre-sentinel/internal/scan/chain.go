package scan

import (
	"context"
	cryptoed25519 "crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	signaltypes "github.com/celestiaorg/celestia-app/v10/x/signal/types"
	valaddrtypes "github.com/celestiaorg/celestia-app/v10/x/valaddr/types"
	abci "github.com/cometbft/cometbft/abci/types"
	cmtbytes "github.com/cometbft/cometbft/libs/bytes"
	rpcclient "github.com/cometbft/cometbft/rpc/client"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	cmttypes "github.com/cometbft/cometbft/types"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/ed25519"
	sdkquery "github.com/cosmos/cosmos-sdk/types/query"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
)

// Chain is a thin, timeout-bounded wrapper over a single CometBFT RPC endpoint.
// Every call takes a fresh child context with Timeout; nothing here can block
// forever. Chain only reads, request by request; the scanner's one
// subscription, to new block headers in follow mode, is heads.go's, and it
// only wakes the follow loop.
type Chain struct {
	rpc     *rpchttp.HTTP
	timeout time.Duration
	log     *Logger
}

// NewChain dials rpcURL (e.g. http://127.0.0.1:26657). It does not verify
// connectivity; the first real call will surface a dead endpoint.
//
// The HTTP client is ours rather than CometBFT's default, for one reason: the
// default builds a Transport with a hand-rolled dialer and no Proxy function,
// so it ignores HTTPS_PROXY and connects straight out. On a host that only has
// egress through a proxy — a corporate network, a locked-down VPS, a CI
// sandbox — every call fails with something that looks like the chain refusing
// us rather than like a proxy we never asked. http.ProxyFromEnvironment is the
// standard library's own rule and is a no-op when no proxy is configured, so
// the common case is unchanged.
func NewChain(rpcURL string, timeout time.Duration, log *Logger) (*Chain, error) {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	httpc := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext,
			TLSHandshakeTimeout:   timeout,
			ResponseHeaderTimeout: timeout,
			MaxIdleConns:          16,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
		},
	}
	c, err := rpchttp.NewWithClient(rpcURL, "/websocket", httpc)
	if err != nil {
		return nil, fmt.Errorf("rpc client for %s: %w", rpcURL, err)
	}
	return &Chain{rpc: c, timeout: timeout, log: log}, nil
}

func (c *Chain) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

// Status returns (chainID, latestHeight).
func (c *Chain) Status(parent context.Context) (string, int64, error) {
	id, h, _, err := c.StatusAt(parent)
	return id, h, err
}

// StatusAt is Status with the tip's block time, which is the only thing that
// says whether the chain is still moving. A halted chain, a node stuck
// mid-sync and a public endpoint that fell behind all keep answering /status
// with a height that does not change, and every check drawn from that same
// node then reads as healthy: the scanner is parked waiting for a height
// rather than failing, so it reports nothing, and the lag between the
// scanner and the tip is zero because both are the same stopped number.
func (c *Chain) StatusAt(parent context.Context) (string, int64, time.Time, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()
	s, err := c.rpc.Status(ctx)
	if err != nil {
		return "", 0, time.Time{}, fmt.Errorf("status: %w", err)
	}
	return s.NodeInfo.Network, s.SyncInfo.LatestBlockHeight, s.SyncInfo.LatestBlockTime.UTC(), nil
}

// AppVersion is the application version the chain is currently running, from
// ABCIInfo. It is the one number that says whether Fibre exists here at all:
// x/fibre and x/valaddr are introduced in app version 10, so on a chain below
// that every Fibre query fails for a reason that has nothing to do with any
// validator. An observer that cannot tell "the module is not there" from "the
// module is there and empty" will publish the second when the first is true.
func (c *Chain) AppVersion(parent context.Context) (uint64, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()
	info, err := c.rpc.ABCIInfo(ctx)
	if err != nil {
		return 0, fmt.Errorf("abci_info: %w", err)
	}
	return info.Response.AppVersion, nil
}

// FibreAppVersion is the app version x/fibre and x/valaddr first exist at.
const FibreAppVersion = 10

// Block holds only what the scanner needs from one block.
type Block struct {
	Height int64
	Time   time.Time
	Txs    []cmttypes.Tx
	// AppVersion is the header's app version: the version block Height was
	// executed under. The first block at FibreAppVersion is the first whose
	// state has x/fibre and x/valaddr in it.
	AppVersion uint64
}

// Block fetches block header + raw txs at height.
func (c *Chain) Block(parent context.Context, height int64) (*Block, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()
	res, err := c.rpc.Block(ctx, &height)
	if err != nil {
		return nil, fmt.Errorf("block %d: %w", height, err)
	}
	return &Block{
		Height:     res.Block.Height,
		Time:       res.Block.Time,
		Txs:        res.Block.Data.Txs,
		AppVersion: res.Block.Header.Version.App,
	}, nil
}

// headerTime is the block time of height, from its header alone. Used to
// place an operator-skipped height on the chain's clock without reading the
// block it was skipped for.
func (c *Chain) headerTime(parent context.Context, height int64) (time.Time, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()
	res, err := c.rpc.Header(ctx, &height)
	if err != nil {
		return time.Time{}, fmt.Errorf("header %d: %w", height, err)
	}
	if res.Header == nil {
		return time.Time{}, fmt.Errorf("header %d: empty response", height)
	}
	return res.Header.Time, nil
}

// headerAppVersion is the app version block height was executed under.
func (c *Chain) headerAppVersion(parent context.Context, height int64) (uint64, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()
	res, err := c.rpc.Header(ctx, &height)
	if err != nil {
		return 0, fmt.Errorf("header %d: %w", height, err)
	}
	if res.Header == nil {
		return 0, fmt.Errorf("header %d: empty response", height)
	}
	return res.Header.Version.App, nil
}

// abciPanicCode is cosmos-sdk's ErrPanic: the node recovered a panic while
// answering. A node on app version 10 or later answers every x/fibre and
// x/valaddr query at a height from before the upgrade with it, because the
// module's store did not exist at that version and the query reads through
// a nil store. The same node below the upgrade (and any node on an older
// binary) says "unknown query path" instead.
const abciPanicCode = 111222

// queryError turns a non-zero ABCI answer to a Fibre-module query at height
// into an ABCIError, and names the one case it can prove: a panic at a
// height whose block ran below FibreAppVersion is the module not existing
// yet at that height, which no retry changes. Anything else, a panic at a
// height where the module exists included, is left for the caller to judge.
func (c *Chain) queryError(parent context.Context, path string, height int64, r abci.ResponseQuery) *ABCIError {
	e := &ABCIError{Path: path, Height: height, Code: r.Code, Codespace: r.Codespace, Log: r.Log}
	if r.Code == abciPanicCode && height > 0 {
		if v, err := c.headerAppVersion(parent, height); err == nil && v < FibreAppVersion {
			e.BeforeModule = true
			e.AppVersion = v
		}
	}
	return e
}

// BlockResults holds the per-tx result codes and the events the scanner scans
// for EventUpdateFibreParams (both tx events and FinalizeBlock events).
type BlockResults struct {
	Height       int64
	TxCodes      []uint32
	TxEvents     [][]abci.Event
	FinalizeEvts []abci.Event
}

// BlockResults fetches execution results at height.
func (c *Chain) BlockResults(parent context.Context, height int64) (*BlockResults, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()
	res, err := c.rpc.BlockResults(ctx, &height)
	if err != nil {
		return nil, fmt.Errorf("block_results %d: %w", height, err)
	}
	out := &BlockResults{
		Height:       res.Height,
		TxCodes:      make([]uint32, len(res.TxsResults)),
		TxEvents:     make([][]abci.Event, len(res.TxsResults)),
		FinalizeEvts: res.FinalizeBlockEvents,
	}
	for i, r := range res.TxsResults {
		out.TxCodes[i] = r.Code
		out.TxEvents[i] = r.Events
	}
	return out, nil
}

// ValSetMember is one validator at a height.
type ValSetMember struct {
	Address     []byte // 20-byte consensus address
	PubKey      []byte // 32-byte ed25519 consensus key
	VotingPower int64
}

// ValidatorSet returns the full consensus validator set at height, paging until
// it has every member.
func (c *Chain) ValidatorSet(parent context.Context, height int64) ([]ValSetMember, error) {
	const perPage = 100
	var out []ValSetMember
	for page := 1; ; page++ {
		ctx, cancel := c.ctx(parent)
		p := page
		pp := perPage
		res, err := c.rpc.Validators(ctx, &height, &p, &pp)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("validators h=%d page=%d: %w", height, page, err)
		}
		for _, v := range res.Validators {
			out = append(out, ValSetMember{
				Address:     append([]byte(nil), v.Address.Bytes()...),
				PubKey:      append([]byte(nil), v.PubKey.Bytes()...),
				VotingPower: v.VotingPower,
			})
		}
		if len(out) >= res.Total || len(res.Validators) == 0 {
			break
		}
	}
	return out, nil
}

// FibreParamsAt reads the on-chain fibre module params as of height, straight
// from the chain (ABCI query, no gRPC). height <= 0 means latest.
func (c *Chain) FibreParamsAt(parent context.Context, height int64) (fibretypes.Params, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()

	req := fibretypes.QueryParamsRequest{}
	data, err := req.Marshal()
	if err != nil {
		return fibretypes.Params{}, fmt.Errorf("marshal params request: %w", err)
	}
	opts := rpcclient.ABCIQueryOptions{Height: height, Prove: false}
	res, err := c.rpc.ABCIQueryWithOptions(ctx, "/celestia.fibre.v1.Query/Params", cmtbytes.HexBytes(data), opts)
	if err != nil {
		return fibretypes.Params{}, fmt.Errorf("abci query params h=%d: %w", height, err)
	}
	if res.Response.Code != 0 {
		return fibretypes.Params{}, c.queryError(parent, "/celestia.fibre.v1.Query/Params", height, res.Response)
	}
	var resp fibretypes.QueryParamsResponse
	if err := resp.Unmarshal(res.Response.Value); err != nil {
		return fibretypes.Params{}, fmt.Errorf("unmarshal params response: %w", err)
	}
	return resp.Params, nil
}

// UpgradeSignal is what x/signal says about one app version: how much of the
// bonded voting power has signalled for it, the threshold it needs, which
// validators have not signalled (by moniker, which is how the module answers
// — it has no per-validator query by address), and the height the upgrade is
// scheduled at once the threshold was met and a TryUpgrade landed. Before
// Fibre exists on a chain this is the one Fibre-relevant fact the chain
// carries: who is ready for the version that brings it.
type UpgradeSignal struct {
	Version          uint64
	VotingPower      uint64
	ThresholdPower   uint64
	TotalVotingPower uint64
	// Missing is the monikers x/signal reports as not having signalled for
	// Version; only bonded validators are counted by the module.
	Missing []string
	// UpgradeHeight is set once an upgrade is scheduled; zero until then.
	// UpgradeAppVersion is the version it schedules, which need not be
	// Version.
	UpgradeHeight     int64
	UpgradeAppVersion uint64
}

// UpgradeSignal reads the tally, the missing validators and any scheduled
// upgrade for version from x/signal, at the latest height.
func (c *Chain) UpgradeSignal(parent context.Context, version uint64) (UpgradeSignal, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()
	out := UpgradeSignal{Version: version}
	query := func(path string, req interface{ Marshal() ([]byte, error) }, resp interface{ Unmarshal([]byte) error }) error {
		data, err := req.Marshal()
		if err != nil {
			return fmt.Errorf("marshal %s: %w", path, err)
		}
		res, err := c.rpc.ABCIQueryWithOptions(ctx, path, cmtbytes.HexBytes(data), rpcclient.ABCIQueryOptions{Prove: false})
		if err != nil {
			return fmt.Errorf("abci query %s: %w", path, err)
		}
		if res.Response.Code != 0 {
			return &ABCIError{Path: path, Code: res.Response.Code, Codespace: res.Response.Codespace, Log: res.Response.Log}
		}
		if err := resp.Unmarshal(res.Response.Value); err != nil {
			return fmt.Errorf("unmarshal %s: %w", path, err)
		}
		return nil
	}
	var tally signaltypes.QueryVersionTallyResponse
	if err := query("/celestia.signal.v1.Query/VersionTally", &signaltypes.QueryVersionTallyRequest{Version: version}, &tally); err != nil {
		return out, err
	}
	out.VotingPower, out.ThresholdPower, out.TotalVotingPower = tally.VotingPower, tally.ThresholdPower, tally.TotalVotingPower
	var missing signaltypes.QueryGetMissingValidatorsResponse
	if err := query("/celestia.signal.v1.Query/GetMissingValidators", &signaltypes.QueryGetMissingValidatorsRequest{Version: version}, &missing); err != nil {
		return out, err
	}
	out.Missing = missing.MissingValidators
	var up signaltypes.QueryGetUpgradeResponse
	if err := query("/celestia.signal.v1.Query/GetUpgrade", &signaltypes.QueryGetUpgradeRequest{}, &up); err != nil {
		return out, err
	}
	if up.Upgrade != nil {
		out.UpgradeHeight, out.UpgradeAppVersion = up.Upgrade.UpgradeHeight, up.Upgrade.AppVersion
	}
	return out, nil
}

// Escrow is one publisher's x/fibre escrow account as the chain holds it.
type Escrow struct {
	Signer        string
	Denom         string
	BalanceUtia   uint64
	AvailableUtia uint64
	Height        int64 // the height the state was read at (0 = latest)
	Found         bool
}

// EscrowAccount reads a publisher's escrow balance at height (<= 0 means
// latest). A publisher that never deposited is returned with Found=false,
// not as an error.
func (c *Chain) EscrowAccount(parent context.Context, signer string, height int64) (Escrow, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()

	const path = "/celestia.fibre.v1.Query/EscrowAccount"
	req := fibretypes.QueryEscrowAccountRequest{Signer: signer}
	data, err := req.Marshal()
	if err != nil {
		return Escrow{}, fmt.Errorf("marshal escrow request: %w", err)
	}
	opts := rpcclient.ABCIQueryOptions{Height: height, Prove: false}
	res, err := c.rpc.ABCIQueryWithOptions(ctx, path, cmtbytes.HexBytes(data), opts)
	if err != nil {
		return Escrow{}, fmt.Errorf("abci query escrow %s h=%d: %w", signer, height, err)
	}
	if res.Response.Code != 0 {
		return Escrow{}, c.queryError(parent, path, height, res.Response)
	}
	var resp fibretypes.QueryEscrowAccountResponse
	if err := resp.Unmarshal(res.Response.Value); err != nil {
		return Escrow{}, fmt.Errorf("unmarshal escrow response: %w", err)
	}
	e := Escrow{Signer: signer, Height: res.Response.Height, Found: resp.Found}
	if !resp.Found {
		return e, nil
	}
	e.Denom, e.BalanceUtia = coinAmount(resp.EscrowAccount.Balance)
	_, e.AvailableUtia = coinAmount(resp.EscrowAccount.AvailableBalance)
	return e, nil
}

// FibreProvider is one bonded validator's registered fibre service host.
type FibreProvider struct {
	ConsAddressBech32 string // celestiavalcons1...
	Host              string // host:port the validator serves fibre from
}

// BondedFibreProviders lists the fibre service host every currently-bonded
// validator has registered (x/valaddr AllBondedFibreProviders, latest height,
// ABCI query — no gRPC). Providers whose validator left the active set are
// already excluded by the chain.
func (c *Chain) BondedFibreProviders(parent context.Context) ([]FibreProvider, error) {
	return c.BondedFibreProvidersAt(parent, 0)
}

// BondedFibreProvidersAt is BondedFibreProviders as the chain state stood at
// height (0 = latest). It is how the scanner records the host each
// validator had registered when a promise settled, the host the upload
// went to; a node that has pruned that state answers with an error, which
// the record shows as "unknown", never as "no host".
func (c *Chain) BondedFibreProvidersAt(parent context.Context, height int64) ([]FibreProvider, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()

	req := valaddrtypes.QueryAllBondedFibreProvidersRequest{}
	data, err := req.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal providers request: %w", err)
	}
	const path = "/celestia.valaddr.v1.Query/AllBondedFibreProviders"
	res, err := c.rpc.ABCIQueryWithOptions(ctx, path, cmtbytes.HexBytes(data), rpcclient.ABCIQueryOptions{Height: height})
	if err != nil {
		return nil, fmt.Errorf("abci query bonded fibre providers: %w", err)
	}
	if res.Response.Code != 0 {
		return nil, c.queryError(parent, path, height, res.Response)
	}
	var resp valaddrtypes.QueryAllBondedFibreProvidersResponse
	if err := resp.Unmarshal(res.Response.Value); err != nil {
		return nil, fmt.Errorf("unmarshal providers response: %w", err)
	}
	out := make([]FibreProvider, 0, len(resp.Providers))
	for _, p := range resp.Providers {
		out = append(out, FibreProvider{ConsAddressBech32: p.ValidatorConsensusAddress, Host: p.Info.Host})
	}
	return out, nil
}

// FibreProviderInfoAt is one validator's Fibre host registration as the
// chain state stood at height (0 = latest), and whether one exists.
// Registration is independent of bonding, so this answers for a jailed or
// unbonding validator too, which AllBondedFibreProviders does not.
func (c *Chain) FibreProviderInfoAt(parent context.Context, consAddrBech32 string, height int64) (host string, found bool, err error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()

	req := valaddrtypes.QueryFibreProviderInfoRequest{ValidatorConsensusAddress: consAddrBech32}
	data, err := req.Marshal()
	if err != nil {
		return "", false, fmt.Errorf("marshal provider info request: %w", err)
	}
	const path = "/celestia.valaddr.v1.Query/FibreProviderInfo"
	res, err := c.rpc.ABCIQueryWithOptions(ctx, path, cmtbytes.HexBytes(data), rpcclient.ABCIQueryOptions{Height: height})
	if err != nil {
		return "", false, fmt.Errorf("abci query fibre provider info: %w", err)
	}
	if res.Response.Code != 0 {
		return "", false, c.queryError(parent, path, height, res.Response)
	}
	var resp valaddrtypes.QueryFibreProviderInfoResponse
	if err := resp.Unmarshal(res.Response.Value); err != nil {
		return "", false, fmt.Errorf("unmarshal provider info response: %w", err)
	}
	if !resp.Found || resp.Info == nil {
		return "", false, nil
	}
	return resp.Info.Host, true, nil
}

// ValidatorIdentity is what the staking module says about one validator:
// the name its operator chose and the facts a reader needs to recognise it.
//
// It is read from the chain, not from an explorer's API. An observer whose
// validator names come from somebody else's index is that much less
// independent, and it inherits that index's rate limits, attribution terms
// and coverage. The staking module carries all of this already, on every
// Cosmos chain, including the networks a third-party indexer has not got
// round to.
type ValidatorIdentity struct {
	// ConsAddressHex is the 20-byte consensus address, lower-case hex. It is
	// derived here from the validator's consensus public key so that it joins
	// directly against the address every probe row and assignment already
	// uses, with no bech32 round trip.
	ConsAddressHex string
	// OperatorAddress is the celestiavaloper... form, for linking out.
	OperatorAddress string
	Moniker         string
	// Identity is the operator's Keybase key suffix, when it set one. It is
	// how an avatar could be looked up later; it is not needed for a name.
	Identity string
	Website  string
	// Tokens is the staked amount as the chain reports it, and Jailed says
	// whether the validator is currently jailed. Both are the chain's own
	// words about the validator, unlike anything this observer measures.
	Tokens string
	Jailed bool
	// Status is BOND_STATUS_BONDED, _UNBONDING or _UNBONDED. A validator that
	// is not bonded still owes the shards it signed for, so this is shown
	// rather than used to filter anyone out.
	Status string
}

// ValidatorIdentities returns every validator the staking module knows,
// bonded or not, paging until the set is complete.
//
// Unbonded and jailed validators are deliberately included. A validator's
// Fibre obligation comes from the promise it signed, which outlives its
// bonding, and dropping it here would hide exactly the validator whose row a
// reader is most likely to be looking for.
func (c *Chain) ValidatorIdentities(parent context.Context) ([]ValidatorIdentity, error) {
	var out []ValidatorIdentity
	var nextKey []byte
	for page := 0; ; page++ {
		if page > 64 {
			return nil, fmt.Errorf("validator identities: more than 64 pages; refusing to keep paging")
		}
		batch, key, err := c.validatorIdentityPage(parent, nextKey)
		if err != nil {
			return nil, err
		}
		out = append(out, batch...)
		if len(key) == 0 {
			return out, nil
		}
		nextKey = key
	}
}

func (c *Chain) validatorIdentityPage(parent context.Context, key []byte) ([]ValidatorIdentity, []byte, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()

	req := stakingtypes.QueryValidatorsRequest{
		Pagination: &sdkquery.PageRequest{Key: key, Limit: 200},
	}
	data, err := req.Marshal()
	if err != nil {
		return nil, nil, fmt.Errorf("marshal validators request: %w", err)
	}
	res, err := c.rpc.ABCIQueryWithOptions(ctx, "/cosmos.staking.v1beta1.Query/Validators", cmtbytes.HexBytes(data), rpcclient.ABCIQueryOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("abci query validators: %w", err)
	}
	if res.Response.Code != 0 {
		return nil, nil, fmt.Errorf("abci query validators: code=%d log=%s", res.Response.Code, res.Response.Log)
	}
	var resp stakingtypes.QueryValidatorsResponse
	if err := resp.Unmarshal(res.Response.Value); err != nil {
		return nil, nil, fmt.Errorf("unmarshal validators response: %w", err)
	}

	out := make([]ValidatorIdentity, 0, len(resp.Validators))
	for _, v := range resp.Validators {
		id := ValidatorIdentity{
			OperatorAddress: v.OperatorAddress,
			Moniker:         v.Description.Moniker,
			Identity:        v.Description.Identity,
			Website:         v.Description.Website,
			Tokens:          v.Tokens.String(),
			Jailed:          v.Jailed,
			Status:          v.Status.String(),
		}
		if addr, err := consAddressFromAny(v.ConsensusPubkey); err == nil {
			id.ConsAddressHex = addr
		} else if c.log != nil {
			// A validator whose key this build cannot parse still belongs in
			// the list; it simply cannot be joined to a probe row, and a
			// silent drop would look like the validator not existing.
			c.log.Printf("validator %s: consensus key: %v", v.OperatorAddress, err)
		}
		out = append(out, id)
	}
	var next []byte
	if resp.Pagination != nil {
		next = resp.Pagination.NextKey
	}
	return out, next, nil
}

// consAddressFromAny derives the 20-byte consensus address from a validator's
// consensus public key, the same way CometBFT does: the first 20 bytes of the
// SHA-256 of the raw ed25519 key. Deriving it here means the staking view and
// the probe rows share one identifier with no bech32 conversion in between.
func consAddressFromAny(pk *codectypes.Any) (string, error) {
	if pk == nil {
		return "", fmt.Errorf("no consensus public key")
	}
	var key ed25519.PubKey
	if err := key.Unmarshal(pk.Value); err != nil {
		return "", fmt.Errorf("unmarshal consensus key: %w", err)
	}
	if len(key.Key) != cryptoed25519.PublicKeySize {
		return "", fmt.Errorf("consensus key is %d bytes, want %d", len(key.Key), cryptoed25519.PublicKeySize)
	}
	sum := sha256.Sum256(key.Key)
	return strings.ToLower(hex.EncodeToString(sum[:20])), nil
}

// ChainID returns the network id from /status.
func (c *Chain) ChainID(parent context.Context) (string, error) {
	id, _, err := c.Status(parent)
	return id, err
}

// LatestBlockTime returns the timestamp of the chain's latest block, for
// comparing the observer's own clock against the chain's.
func (c *Chain) LatestBlockTime(parent context.Context) (time.Time, error) {
	ctx, cancel := c.ctx(parent)
	defer cancel()
	s, err := c.rpc.Status(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("status: %w", err)
	}
	return s.SyncInfo.LatestBlockTime, nil
}

// ABCIError is a non-zero ABCI query response code.
type ABCIError struct {
	Path      string
	Height    int64
	Code      uint32
	Codespace string
	Log       string
	// BeforeModule is set when the answer is proven to mean the queried
	// module did not exist yet at Height: block Height ran under
	// AppVersion, below FibreAppVersion (see queryError).
	BeforeModule bool
	AppVersion   uint64
}

func (e *ABCIError) Error() string {
	if e.BeforeModule {
		return fmt.Sprintf("abci query %s h=%d: code=%d: block %d ran under app v%d, before the module existed", e.Path, e.Height, e.Code, e.Height, e.AppVersion)
	}
	return fmt.Sprintf("abci query %s h=%d: code=%d codespace=%s log=%s", e.Path, e.Height, e.Code, e.Codespace, e.Log)
}

// IsResultsNotPersisted reports the block_results error of a node that runs
// with storage.discard_abci_responses = true. Retrying never helps.
// CometBFT's own wording is "node is not persisting finalize block
// responses"; "not persisted" alone never matched it, so such a node held
// the scanner in the unbounded transient retry instead of the gap path.
func IsResultsNotPersisted(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "not persisting") || strings.Contains(s, "not persisted") || strings.Contains(s, "discard_abci_responses")
}

// IsHeightUnavailable reports an error that means the node does not have
// this height: pruned ("lowest height is N"), discarded ABCI responses, or a
// height it has not reached. The last one clears itself; the others do not.
func IsHeightUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if IsResultsNotPersisted(err) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "lowest height is") ||
		strings.Contains(s, "is not available") ||
		strings.Contains(s, "must be less than or equal to the current blockchain height") ||
		// CometBFT's own wording when the validator set for a height has
		// been pruned. It matches none of the phrases above, so without it
		// the retry loop had no exit at all: an unbounded retry on a height
		// the node can never serve, with nothing advancing and no gap
		// recorded. A promise may be up to PaymentPromiseHeightWindow
		// blocks older than the block that settles it, so any node whose
		// state base sits between the two reaches this.
		strings.Contains(s, "could not find validator set for height") ||
		// cosmos-sdk's answer to a query at a height whose state was pruned.
		strings.Contains(s, "failed to load state at height")
}
