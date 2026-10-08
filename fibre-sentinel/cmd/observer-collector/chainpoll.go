package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The chain poll: what the collector reads from the chain itself on every
// endpoint-poll pass (-endpoints-every), after the files. The app version
// and whether Fibre is live, the upgrade signal, the chain's id and tip,
// the bonded Fibre endpoints and the validators' names.
//
// It runs whatever the ingest before it did. It used to run only after a
// pass that ingested every file, so one line the store kept refusing, in a
// file that has nothing to do with the chain, stopped the endpoint history
// (a validator that moved host or left kept its old one on the site, and
// nothing reached registry.jsonl) and froze the chain's tip on record, and
// ten minutes later /v1/health reported the chain halted on top of the real
// fault. Only the status file's OK still waits on a clean ingest, which is
// what it says; and the endpoint diff waits on the one step it needs, the
// replay of registry.jsonl (collect.Collector.RegistryReplayed).
//
// Each poll that succeeds is dated in meta, <poll>_polled_at, so /v1/health
// can tell a value that is current from one a failing poll left behind:
// chain_status, endpoints, identities here, signal (signal_polled_at), and
// escrow beside it in main.go.

// pollChain is the part of scan.Chain the chain poll uses, so the poll can
// be tested against a fake chain.
type pollChain interface {
	AppVersion(ctx context.Context) (uint64, error)
	FibreParamsAt(ctx context.Context, height int64) (fibretypes.Params, error)
	UpgradeSignal(ctx context.Context, version uint64) (scan.UpgradeSignal, error)
	StatusAt(ctx context.Context) (string, int64, time.Time, error)
	BondedFibreProviders(ctx context.Context) ([]scan.FibreProvider, error)
	ValidatorIdentities(ctx context.Context) ([]scan.ValidatorIdentity, error)
}

// liveStatus is the part of the status file (internal/status) the collector
// reports to.
type liveStatus interface {
	OK()
	Progress(height int64)
	Error(msg string)
	Set(key string, v any)
}

// chainPoll is the chain poll's state.
type chainPoll struct {
	chain pollChain
	st    *store.Store
	live  liveStatus
	logf  logf
	// appendRegistry writes endpoint events to registry.jsonl, fsynced, and
	// says whether they are on record.
	appendRegistry func([]store.EndpointEvent) error
	// waiting: the endpoint diff is held back for the registry replay, and
	// the log has said so.
	waiting bool
}

// run is one poll at now. ingested says the pass before it ingested every
// file; registryReplayed that it read registry.jsonl to its end.
func (p *chainPoll) run(ctx context.Context, now time.Time, ingested, registryReplayed bool) {
	p.appVersion(ctx, now)
	p.status(ctx, now, ingested, registryReplayed)
	p.identities(ctx, now)
}

func (p *chainPoll) setMeta(key, value string, now time.Time) {
	if err := p.st.SetMeta(key, value, now); err != nil {
		p.logf("meta %s: %v", key, err)
	}
}

// appVersion records whether Fibre exists on this chain at all, recorded
// rather than inferred. x/fibre and x/valaddr are introduced in app version
// 10, so below that every Fibre query fails for a reason that has nothing to
// do with any validator — and a site that cannot tell "the module is not
// there" from "the module is there and nobody registered" will show the
// second while the first is true. Both the version and the verdict are
// stored, so the page can say which chain it is watching and what state
// that chain is in.
func (p *chainPoll) appVersion(ctx context.Context, now time.Time) {
	av, err := p.chain.AppVersion(ctx)
	if err != nil {
		p.logf("app version: %v", err)
		return
	}
	p.setMeta("app_version", itoa(int64(av)), now)
	active, known := fibreActive(av, func() error {
		_, err := p.chain.FibreParamsAt(ctx, 0)
		return err
	})
	if known {
		p.setMeta("fibre_active", active, now)
	} else {
		p.logf("fibre_active: app v%d but x/fibre did not answer; left as it was", av)
	}
	p.setMeta("fibre_app_version", itoa(scan.FibreAppVersion), now)
	// Until Fibre is live, the one Fibre-relevant fact the chain carries is
	// who has signalled for the version that brings it. Read from x/signal
	// on the same cadence as the rest; the site shows the tally, the
	// scheduled height and, per validator, whether it has signalled.
	// Dropped the moment the chain is on that version.
	if av >= scan.FibreAppVersion {
		return
	}
	sig, err := p.chain.UpgradeSignal(ctx, scan.FibreAppVersion)
	if err != nil {
		p.logf("upgrade signal: %v", err)
		return
	}
	if sig.Missing == nil {
		sig.Missing = []string{} // nobody missing is a list, not null
	}
	missing, _ := json.Marshal(sig.Missing)
	for k, v := range map[string]string{
		"signal_version":            fmt.Sprint(sig.Version),
		"signal_voting_power":       fmt.Sprint(sig.VotingPower),
		"signal_threshold_power":    fmt.Sprint(sig.ThresholdPower),
		"signal_total_voting_power": fmt.Sprint(sig.TotalVotingPower),
		"signal_upgrade_height":     fmt.Sprint(sig.UpgradeHeight),
		// The version that height upgrades to. x/signal schedules whichever
		// version reaches quorum, which need not be Fibre's: a chain two
		// versions below it schedules the one in between first, and a
		// countdown that names that height "Fibre" is wrong until it lands.
		// The API publishes the height only when this is Fibre's version.
		"signal_upgrade_app_version": fmt.Sprint(sig.UpgradeAppVersion),
		"signal_missing":             string(missing),
		"signal_polled_at":           store.TS(now),
	} {
		p.setMeta(k, v, now)
	}
}

// status records the chain's identity and tip and, once registry.jsonl has
// been replayed, diffs the bonded endpoints against the open rows.
func (p *chainPoll) status(ctx context.Context, now time.Time, ingested, registryReplayed bool) {
	chainID, height, tipTime, err := p.chain.StatusAt(ctx)
	if err != nil {
		p.logf("endpoints: status: %v", err)
		p.live.Error(fmt.Sprintf("chain status: %v", err))
		return
	}
	// The status file's OK: a pass that ingested every file and reached the
	// chain. A pass that failed to ingest is named on the status file by
	// main.go, after this.
	if ingested {
		p.live.OK()
		p.live.Progress(height)
	}
	// The chain's own identity and tip, recorded here rather than only by
	// the scanner: before Fibre activates there are no publications to
	// carry them, and "which chain is this, and how far along is it" is the
	// whole content of the site until then. Kept separate from
	// last_scanned_height, which is how far the SCANNER has read;
	// conflating the two would report the chain's progress as our own.
	p.setMeta("chain_id", chainID, now)
	p.setMeta("chain_height", itoa(height), now)
	// The tip's own clock, so /v1/health can tell a chain that is running
	// from one that stopped. Everything else here is drawn from the same
	// node, and a node whose height stands still looks identical to a
	// network at rest.
	if !tipTime.IsZero() {
		p.setMeta("chain_tip_time", store.TS(tipTime), now)
		// And the anchors behind the chain's recent block time, which the
		// site needs to say when a scheduled upgrade height is due (see
		// store.NotePace).
		if err := p.st.NotePace(height, tipTime, now); err != nil {
			p.logf("chain pace: %v", err)
		}
	}
	p.setMeta("chain_status_polled_at", store.TS(now), now)

	if !registryReplayed {
		if !p.waiting {
			p.logf("endpoints: registry.jsonl did not replay to its end this pass; the endpoint poll waits for it (the ingest error says why)")
			p.waiting = true
		}
		return
	}
	if p.waiting {
		p.logf("endpoints: registry.jsonl replayed; polling the endpoints again")
		p.waiting = false
	}
	provs, err := p.chain.BondedFibreProviders(ctx)
	if err != nil {
		// Before v10 the module does not exist; that is a normal state,
		// logged but not fatal. fibre_active above says which of the two
		// this is, and a chain that answered "no such module" was polled.
		p.logf("endpoints: %v", err)
		if scan.IsModuleInactive(err) {
			p.setMeta("endpoints_polled_at", store.TS(now), now)
		}
		return
	}
	// The openings and closings go to registry.jsonl, fsynced, before the
	// store has them, the order the amendments and corrections keep. The
	// file is the endpoint history's only record, and the store's diff,
	// once committed, is never drawn again: events the store had and the
	// file did not were lost to every export and every rebuild. A line on
	// record whose rows did not change replays into the store on the next
	// pass, before the next diff (RegistryReplayed); a write that fails
	// leaves the store as it was, so the next poll finds the same diff.
	open, err := p.st.CurrentEndpoints(ctx)
	if err != nil {
		p.logf("endpoints: store: %v", err)
		return
	}
	evs := endpointDiff(open, provs, height, now)
	if len(evs) > 0 {
		if err := p.appendRegistry(evs); err != nil {
			p.logf("endpoints: registry.jsonl: %v; the endpoints are left as they were, and the next poll diffs them again", err)
			return
		}
	}
	got, err := p.st.ObserveEndpointEvents(ctx, provs, height, now)
	if err != nil {
		p.logf("endpoints: store: %v", err)
		return
	}
	if !sameEvents(evs, got) {
		// Nothing but this poll writes the endpoint rows between the read
		// above and this, so this is a bug, said where it can be seen.
		p.logf("endpoints: WARNING the store changed %d endpoint(s) where registry.jsonl was given %d: %+v, on record %+v", len(got), len(evs), got, evs)
		p.live.Error(fmt.Sprintf("endpoints: the store's diff (%d) is not the one on record (%d)", len(got), len(evs)))
	}
	if len(evs) > 0 {
		opened, closed := 0, 0
		for _, e := range evs {
			if e.Kind == store.EndpointOpened {
				opened++
			} else {
				closed++
			}
		}
		p.logf("endpoints: h=%d registered=%d opened=%d closed=%d", height, len(provs), opened, closed)
	}
	p.setMeta("endpoints_height", itoa(height), now)
	p.setMeta("endpoints_registered", itoa(int64(len(provs))), now)
	p.setMeta("endpoints_polled_at", store.TS(now), now)
}

// endpointDiff is what store.ObserveEndpointEvents will change, given the
// open endpoint rows: an opening for each provider with no open row, a
// closing for each open row no provider names, at height and now.
func endpointDiff(open []store.Endpoint, provs []scan.FibreProvider, height int64, now time.Time) []store.EndpointEvent {
	type key struct{ addr, host string }
	isOpen := map[key]bool{}
	for _, e := range open {
		isOpen[key{e.ValidatorConsAddress, e.Host}] = true
	}
	var evs []store.EndpointEvent
	seen := map[key]bool{}
	for _, p := range provs {
		k := key{p.ConsAddressBech32, p.Host}
		seen[k] = true
		if !isOpen[k] {
			evs = append(evs, store.EndpointEvent{Kind: store.EndpointOpened, ConsAddress: k.addr, Host: k.host, Height: height, At: now.UTC()})
		}
	}
	var gone []key
	for k := range isOpen {
		if !seen[k] {
			gone = append(gone, k)
		}
	}
	sort.Slice(gone, func(i, j int) bool {
		if gone[i].addr != gone[j].addr {
			return gone[i].addr < gone[j].addr
		}
		return gone[i].host < gone[j].host
	})
	for _, k := range gone {
		evs = append(evs, store.EndpointEvent{Kind: store.EndpointClosed, ConsAddress: k.addr, Host: k.host, Height: height, At: now.UTC(), Reason: "left_bonded_provider_list"})
	}
	return evs
}

// sameEvents reports whether a and b hold the same events, in any order.
func sameEvents(a, b []store.EndpointEvent) bool {
	if len(a) != len(b) {
		return false
	}
	count := map[string]int{}
	id := func(e store.EndpointEvent) string {
		return e.Kind + "|" + e.ConsAddress + "|" + e.Host + "|" + itoa(e.Height) + "|" + store.TS(e.At) + "|" + e.Reason
	}
	for _, e := range a {
		count[id(e)]++
	}
	for _, e := range b {
		if count[id(e)]--; count[id(e)] < 0 {
			return false
		}
	}
	return true
}

// identities stores the validators' names, from the chain's own staking
// module rather than from an explorer's API. A reader recognises a
// validator by the name its operator chose, not by twenty hex characters,
// and taking that name from a third-party index would make this observer
// depend on somebody else's coverage and terms.
func (p *chainPoll) identities(ctx context.Context, now time.Time) {
	ids, err := p.chain.ValidatorIdentities(ctx)
	if err != nil {
		p.logf("validator identities: %v", err)
		return
	}
	n, err := p.st.UpsertValidatorIdentities(ids, now)
	if err != nil {
		p.logf("validator identities: store: %v", err)
		return
	}
	if n > 0 {
		p.logf("validator identities: %d of %d stored", n, len(ids))
		p.setMeta("validator_identities", itoa(int64(n)), now)
	}
	p.setMeta("identities_polled_at", store.TS(now), now)
}
