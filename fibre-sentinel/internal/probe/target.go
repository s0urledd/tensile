package probe

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"
	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// Target is one validator to probe for a given publication.
type Target struct {
	Address     assign.Address // 20-byte consensus address
	AddressHex  string
	PubKey      ed25519.PublicKey // consensus key, for the TLS identity check
	Host        string            // host:port registered in x/valaddr (may be "")
	VotingPower int64
	Assigned    bool
	// Attested mirrors the publication's per-validator attestation: the
	// settled promise carries a signature from this validator that verified
	// against its consensus key. Only an attested validator is provably under
	// the retention obligation for this blob.
	Attested bool
	// AttestationUnknown: the publication record predates signature
	// verification (scan schema 1), so Attested carries no evidence.
	AttestationUnknown bool
	AssignedRows       []int // recomputed by fibre-assign; empty if unassigned
	RowCount           int

	// HostSource says where Host came from. "bonded" means the validator is
	// in AllBondedFibreProviders right now. "last_known" means it is not, but
	// this observer saw it register that host earlier and is still probing
	// it: jailing and unbonding drop a provider from the bonded list while
	// the chain keeps its x/valaddr entry for the jailed grace period, and a
	// validator's retention obligation comes from the promise it signed, not
	// from its bonding status. Dropping it would stop collecting evidence
	// about a server that may well still be serving. "" means no host was
	// ever seen for this validator.
	HostSource string
	// HostSeenAt is when the registry entry behind Host was last confirmed.
	HostSeenAt time.Time
	// HostAtSettlement is the host registered when the promise settled, from
	// the publication record ("" when none, or when the scanner could not
	// read the registry at that height). It is the last fallback for Host
	// and the evidence probe's target when Host differs from it.
	HostAtSettlement string
}

// Resolver turns a publication into probe targets: it fetches the validator set
// at the promise height (for consensus keys), the current host registry, and
// recomputes the assignment with fibre-assign. Host registry and validator sets
// are cached briefly so a burst of probes does not hammer the node.
type Resolver struct {
	chain *scan.Chain

	mu            sync.Mutex
	hostCacheAt   time.Time
	hostCacheTTL  time.Duration
	hostByConsHex map[string]string // 20-byte hex -> host:port, bonded only
	// lastKnown keeps the newest host this observer ever saw for a validator,
	// with the time it was last confirmed. It is the fallback when a
	// validator leaves the bonded set, which happens on every jailing and
	// every unbonding without the validator doing anything to its Fibre
	// service.
	lastKnown map[string]knownHost

	valSetCache map[int64][]scan.ValSetMember
}

type knownHost struct {
	host string
	at   time.Time
}

// maxLastKnownHosts bounds the fallback map. It is one entry per validator
// that has ever registered, which is small, but it must still be bounded.
const maxLastKnownHosts = 4096

// maxValSetCache bounds the per-height validator-set cache; publications
// arrive at many distinct heights and the map would otherwise grow for the
// life of the process.
const maxValSetCache = 256

// NewResolver builds a Resolver over an RPC chain client.
func NewResolver(chain *scan.Chain, hostCacheTTL time.Duration) *Resolver {
	if hostCacheTTL <= 0 {
		hostCacheTTL = 60 * time.Second
	}
	return &Resolver{
		chain:        chain,
		hostCacheTTL: hostCacheTTL,
		valSetCache:  map[int64][]scan.ValSetMember{},
		lastKnown:    map[string]knownHost{},
	}
}

// hostMap returns consHex(20-byte) -> host:port, refreshed at most every TTL.
func (r *Resolver) hostMap(ctx context.Context) (map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hostByConsHex != nil && time.Since(r.hostCacheAt) < r.hostCacheTTL {
		return r.hostByConsHex, nil
	}
	providers, err := r.chain.BondedFibreProviders(ctx)
	if err != nil {
		if r.hostByConsHex != nil {
			// Serve stale rather than fail a probe. The staleness is bounded
			// and every target carries the time behind its host, so a probe
			// taken against an old registry says so rather than looking like
			// a fresh observation.
			return r.hostByConsHex, nil
		}
		return nil, err
	}
	now := time.Now()
	m := make(map[string]string, len(providers))
	for _, p := range providers {
		_, raw, err := bech32.DecodeAndConvert(p.ConsAddressBech32)
		if err != nil {
			continue
		}
		key := strings.ToLower(hex.EncodeToString(raw))
		m[key] = p.Host
		r.lastKnown[key] = knownHost{host: p.Host, at: now}
	}
	r.evictLastKnown()
	r.hostByConsHex = m
	r.hostCacheAt = now
	return m, nil
}

// seedLastKnown records a host this observer saw registered for a validator
// at time at, from the durable registry (see hostRegistry). A record older
// than what is already known is ignored: the live bonded poll and the
// registry describe the same history, and the newer confirmation wins.
func (r *Resolver) seedLastKnown(addrHex, host string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if k, ok := r.lastKnown[addrHex]; ok && !k.at.Before(at) {
		return
	}
	r.lastKnown[addrHex] = knownHost{host: host, at: at}
	r.evictLastKnown()
}

// evictLastKnown keeps the fallback map bounded, dropping the entries
// confirmed longest ago first. Emptying it wholesale would throw away exactly
// the validators that have been gone longest, which are the ones the fallback
// exists for.
func (r *Resolver) evictLastKnown() {
	if len(r.lastKnown) <= maxLastKnownHosts {
		return
	}
	type ent struct {
		key string
		at  time.Time
	}
	all := make([]ent, 0, len(r.lastKnown))
	for k, v := range r.lastKnown {
		all = append(all, ent{k, v.at})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].at.Equal(all[j].at) {
			return all[i].key < all[j].key
		}
		return all[i].at.Before(all[j].at)
	})
	for i := 0; i < len(all)-maxLastKnownHosts; i++ {
		delete(r.lastKnown, all[i].key)
	}
}

// knownHosts is how many validators have a host on record, bonded or not.
func (r *Resolver) knownHosts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.lastKnown)
}

// hostFor resolves one validator's host, preferring the bonded registry and
// falling back to the last host this observer saw it register.
func (r *Resolver) hostFor(bonded map[string]string, addrHex string) (host, source string, at time.Time) {
	if h, ok := bonded[addrHex]; ok && h != "" {
		r.mu.Lock()
		seen := r.lastKnown[addrHex].at
		r.mu.Unlock()
		return h, "bonded", seen
	}
	r.mu.Lock()
	k, ok := r.lastKnown[addrHex]
	r.mu.Unlock()
	if ok && k.host != "" {
		return k.host, "last_known", k.at
	}
	return "", "", time.Time{}
}

func (r *Resolver) validatorSet(ctx context.Context, height int64) ([]scan.ValSetMember, error) {
	r.mu.Lock()
	if v, ok := r.valSetCache[height]; ok {
		r.mu.Unlock()
		return v, nil
	}
	r.mu.Unlock()

	v, err := r.chain.ValidatorSet(ctx, height)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.valSetCache[height] = v
	if len(r.valSetCache) > maxValSetCache {
		// Evict the lowest heights, which belong to the oldest publications.
		// Emptying the map instead cost a full round trip for every height
		// still in flight, at the moment the process was busiest.
		heights := make([]int64, 0, len(r.valSetCache))
		for h := range r.valSetCache {
			heights = append(heights, h)
		}
		sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
		for i := 0; i < len(heights)-maxValSetCache; i++ {
			delete(r.valSetCache, heights[i])
		}
	}
	r.mu.Unlock()
	return v, nil
}

// TargetsFor resolves every assigned validator of a publication into probe
// Targets.
func (r *Resolver) TargetsFor(ctx context.Context, p scan.Publication) ([]Target, error) {
	members, err := r.validatorSet(ctx, p.Assignment.ValidatorSetHeight)
	if err != nil {
		return nil, fmt.Errorf("validator set at height %d: %w", p.Assignment.ValidatorSetHeight, err)
	}
	hosts, err := r.hostMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("host registry: %w", err)
	}

	// recompute the assignment independently from the record's own inputs.
	pp := assign.ProtocolParams{
		OriginalRows:        p.Assignment.ProtocolParams.OriginalRows,
		TotalRows:           p.Assignment.ProtocolParams.TotalRows,
		MinRowsPerValidator: p.Assignment.ProtocolParams.MinRowsPerValidator,
		LivenessThreshold: assign.Fraction{
			Numerator:   p.Assignment.ProtocolParams.LivenessThresholdNum,
			Denominator: p.Assignment.ProtocolParams.LivenessThresholdDen,
		},
	}
	var commitment [32]byte
	cb, err := hex.DecodeString(p.Promise.Commitment)
	if err != nil || len(cb) != 32 {
		return nil, fmt.Errorf("bad commitment hex %q", p.Promise.Commitment)
	}
	copy(commitment[:], cb)

	vals := make([]assign.Validator, 0, len(members))
	pubByAddr := map[assign.Address]ed25519.PublicKey{}
	powerByAddr := map[assign.Address]int64{}
	for _, m := range members {
		var a assign.Address
		if len(m.Address) != len(a) {
			return nil, fmt.Errorf("consensus address %d bytes, want 20", len(m.Address))
		}
		copy(a[:], m.Address)
		vals = append(vals, assign.Validator{Address: a, VotingPower: m.VotingPower})
		if len(m.PubKey) == ed25519.PublicKeySize {
			pubByAddr[a] = ed25519.PublicKey(append([]byte(nil), m.PubKey...))
		}
		powerByAddr[a] = m.VotingPower
	}

	sm, err := assign.Assign(commitment, vals, pp)
	if err != nil {
		return nil, fmt.Errorf("recompute assignment: %w", err)
	}

	// cross-check recomputed counts against the record, and carry the
	// scanner's per-validator attestation across: the observer verified those
	// signatures once, at scan time, against the consensus keys at the promise
	// height, and the result is part of the record.
	recByAddr := map[string]int{}
	for a, rows := range sm {
		recByAddr[a.String()] = len(rows)
	}
	attestedByAddr := map[string]bool{}
	settlementHost := map[string]string{}
	for _, v := range p.Assignment.Validators {
		if v.Host != "" {
			settlementHost[strings.ToLower(v.Address)] = v.Host
		}
		if recByAddr[v.Address] != v.RowCount {
			return nil, fmt.Errorf("assignment mismatch for %s: record says %d rows, recompute says %d — record and chain disagree",
				v.Address, v.RowCount, recByAddr[v.Address])
		}
		if v.Attested {
			attestedByAddr[strings.ToLower(v.Address)] = true
		}
	}

	var out []Target
	for _, v := range vals {
		rows := sm[v.Address]
		assigned := len(rows) > 0
		if !assigned {
			continue
		}
		addrHex := v.Address.String()
		host, source, seenAt := r.hostFor(hosts, addrHex)
		atSettlement := settlementHost[strings.ToLower(addrHex)]
		host, source, seenAt = withSettlementFallback(host, source, seenAt, atSettlement, p.SettlementTime)
		out = append(out, Target{
			HostAtSettlement:   atSettlement,
			Address:            v.Address,
			AddressHex:         addrHex,
			PubKey:             pubByAddr[v.Address],
			Host:               host,
			HostSource:         source,
			HostSeenAt:         seenAt,
			VotingPower:        powerByAddr[v.Address],
			Assigned:           assigned,
			Attested:           attestedByAddr[strings.ToLower(addrHex)],
			AttestationUnknown: !p.HasAttestation(),
			AssignedRows:       append([]int(nil), rows...),
			RowCount:           len(rows),
		})
	}
	return out, nil
}

// withSettlementFallback is the last step of host resolution: with nothing
// in the live registry and nothing this observer ever saw, the publication
// record still says where the shard went, and the row says the host came
// from there ("settlement", seen at the settlement time).
func withSettlementFallback(host, source string, at time.Time, atSettlement string, settled time.Time) (string, string, time.Time) {
	if host == "" && atSettlement != "" {
		return atSettlement, "settlement", settled
	}
	return host, source, at
}
