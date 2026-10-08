package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// fakeLive records what the collector tells its status file.
type fakeLive struct {
	mu     sync.Mutex
	oks    int
	height int64
	errs   []string
	set    map[string]any
}

func (l *fakeLive) OK()                   { l.mu.Lock(); l.oks++; l.mu.Unlock() }
func (l *fakeLive) Progress(height int64) { l.mu.Lock(); l.height = height; l.mu.Unlock() }
func (l *fakeLive) Error(msg string)      { l.mu.Lock(); l.errs = append(l.errs, msg); l.mu.Unlock() }
func (l *fakeLive) Set(key string, v any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.set == nil {
		l.set = map[string]any{}
	}
	l.set[key] = v
}

func (l *fakeLive) get(key string) any {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.set[key]
}

// fakePollChain answers the chain poll.
type fakePollChain struct {
	appVersion uint64
	signal     scan.UpgradeSignal
	height     int64
	tipTime    time.Time
	statusErr  error
	providers  []scan.FibreProvider
	provErr    error
	ids        []scan.ValidatorIdentity
	idsErr     error
}

func (f *fakePollChain) AppVersion(context.Context) (uint64, error) { return f.appVersion, nil }
func (f *fakePollChain) FibreParamsAt(context.Context, int64) (fibretypes.Params, error) {
	if f.appVersion < scan.FibreAppVersion {
		return fibretypes.Params{}, &scan.ABCIError{Code: 6, Codespace: "sdk", Log: "unknown query path"}
	}
	return fibretypes.Params{}, nil
}
func (f *fakePollChain) UpgradeSignal(_ context.Context, v uint64) (scan.UpgradeSignal, error) {
	s := f.signal
	s.Version = v
	return s, nil
}
func (f *fakePollChain) StatusAt(context.Context) (string, int64, time.Time, error) {
	if f.statusErr != nil {
		return "", 0, time.Time{}, f.statusErr
	}
	return "test-1", f.height, f.tipTime, nil
}
func (f *fakePollChain) BondedFibreProviders(context.Context) ([]scan.FibreProvider, error) {
	return f.providers, f.provErr
}
func (f *fakePollChain) ValidatorIdentities(context.Context) ([]scan.ValidatorIdentity, error) {
	return f.ids, f.idsErr
}

func newPoll(t *testing.T, c pollChain) (*chainPoll, *store.Store, *fakeLive, *[]store.EndpointEvent) {
	t.Helper()
	st := openStore(t)
	live := &fakeLive{}
	var appended []store.EndpointEvent
	p := &chainPoll{chain: c, st: st, live: live, logf: quiet,
		appendRegistry: func(evs []store.EndpointEvent) error { appended = append(appended, evs...); return nil }}
	return p, st, live, &appended
}

// An opening or closing goes to registry.jsonl before the store has it. A
// write that fails leaves the endpoint rows as they were, so the next poll
// draws the same change again and puts it on record; it used to be in the
// store alone, for good, while every export and rebuild went without it.
func TestEndpointEventsAreOnRecordBeforeTheStore(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	c := &fakePollChain{appVersion: 10, height: 500, tipTime: now,
		providers: []scan.FibreProvider{{ConsAddressBech32: "celestiavalcons1aa", Host: "a:7980"}, {ConsAddressBech32: "celestiavalcons1bb", Host: "b:7980"}}}
	p, st, _, appended := newPoll(t, c)
	p.run(context.Background(), now, true, true)
	if len(*appended) != 2 {
		t.Fatalf("first poll put %+v on record", *appended)
	}
	// bb leaves and cc arrives while the disk is full.
	c.providers = []scan.FibreProvider{{ConsAddressBech32: "celestiavalcons1aa", Host: "a:7980"}, {ConsAddressBech32: "celestiavalcons1cc", Host: "c:7980"}}
	full := errors.New("no space left on device")
	p.appendRegistry = func([]store.EndpointEvent) error { return full }
	p.run(context.Background(), now.Add(time.Minute), true, true)
	open, err := st.CurrentEndpoints(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 2 || open[0].ValidatorConsAddress != "celestiavalcons1aa" || open[1].ValidatorConsAddress != "celestiavalcons1bb" {
		t.Fatalf("the store moved on with nothing on record: %+v", open)
	}
	if got := meta(t, st, "endpoints_polled_at"); got != store.TS(now) {
		t.Fatalf("a poll whose change is not on record was dated: %s", got)
	}
	// The disk has room again: the same change, on record and in the store.
	var later []store.EndpointEvent
	p.appendRegistry = func(evs []store.EndpointEvent) error { later = append(later, evs...); return nil }
	at := now.Add(2 * time.Minute)
	p.run(context.Background(), at, true, true)
	want := []store.EndpointEvent{
		{Kind: store.EndpointOpened, ConsAddress: "celestiavalcons1cc", Host: "c:7980", Height: 500, At: at},
		{Kind: store.EndpointClosed, ConsAddress: "celestiavalcons1bb", Host: "b:7980", Height: 500, At: at, Reason: "left_bonded_provider_list"},
	}
	if !sameEvents(later, want) {
		t.Fatalf("on record %+v, want %+v", later, want)
	}
	if open, err = st.CurrentEndpoints(context.Background()); err != nil || len(open) != 2 || open[1].ValidatorConsAddress != "celestiavalcons1cc" {
		t.Fatalf("open endpoints %+v, %v", open, err)
	}
}

func meta(t *testing.T, st *store.Store, k string) string {
	t.Helper()
	v, err := st.Meta(k)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// A pass that could not ingest a file still polls the chain: the tip, the
// endpoints and the names go on being recorded. Only the status file's OK
// waits for a clean ingest.
func TestTheChainPollRunsWhenTheIngestFailed(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	c := &fakePollChain{appVersion: 10, height: 500, tipTime: now.Add(-3 * time.Second),
		providers: []scan.FibreProvider{{ConsAddressBech32: "celestiavalcons1aa", Host: "a:7980"}},
		ids:       []scan.ValidatorIdentity{{ConsAddressHex: "aa", Moniker: "a"}}}
	p, st, live, appended := newPoll(t, c)

	p.run(context.Background(), now, false, true)
	if live.oks != 0 {
		t.Fatalf("OK set after a pass that failed to ingest")
	}
	for k, want := range map[string]string{
		"chain_height": "500", "chain_tip_time": store.TS(c.tipTime), "chain_status_polled_at": store.TS(now),
		"endpoints_height": "500", "endpoints_polled_at": store.TS(now), "identities_polled_at": store.TS(now),
		"chain_id": "test-1", "fibre_active": "yes",
	} {
		if got := meta(t, st, k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if len(*appended) != 1 || (*appended)[0].Kind != store.EndpointOpened {
		t.Fatalf("registry events = %+v, want the endpoint opened", *appended)
	}

	// The next pass ingests cleanly: OK, at the tip.
	c.height = 510
	p.run(context.Background(), now.Add(time.Minute), true, true)
	if live.oks != 1 || live.height != 510 {
		t.Fatalf("clean pass: oks=%d height=%d", live.oks, live.height)
	}

	// The node does not answer /status: the error is said, nothing is
	// dated as polled, and the names are still read.
	c.statusErr = errors.New("connection refused")
	c.ids = append(c.ids, scan.ValidatorIdentity{ConsAddressHex: "bb", Moniker: "b"})
	p.run(context.Background(), now.Add(2*time.Minute), true, true)
	if live.oks != 1 || len(live.errs) != 1 {
		t.Fatalf("status down: oks=%d errs=%v", live.oks, live.errs)
	}
	if got := meta(t, st, "chain_status_polled_at"); got != store.TS(now.Add(time.Minute)) {
		t.Fatalf("chain_status_polled_at moved on a failed poll: %s", got)
	}
	if got := meta(t, st, "identities_polled_at"); got != store.TS(now.Add(2*time.Minute)) {
		t.Fatalf("identities_polled_at = %s", got)
	}
}

// The endpoint diff waits for registry.jsonl to be replayed: run ahead of
// it, it would open a row for every endpoint at this poll. The rest of the
// poll does not wait.
func TestTheEndpointPollWaitsForTheRegistryReplay(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	c := &fakePollChain{appVersion: 10, height: 500, tipTime: now,
		providers: []scan.FibreProvider{{ConsAddressBech32: "celestiavalcons1aa", Host: "a:7980"}}}
	p, st, _, appended := newPoll(t, c)
	p.run(context.Background(), now, true, false)
	if len(*appended) != 0 || meta(t, st, "endpoints_polled_at") != "" {
		t.Fatalf("endpoints polled ahead of the registry replay: %+v", *appended)
	}
	if meta(t, st, "chain_height") != "500" {
		t.Fatalf("the tip waited for the registry too")
	}
	p.run(context.Background(), now.Add(time.Minute), true, true)
	if len(*appended) != 1 || meta(t, st, "endpoints_polled_at") != store.TS(now.Add(time.Minute)) {
		t.Fatalf("after the replay: %+v", *appended)
	}
}

// Before Fibre: the module's absence is an answer, so the endpoint poll is
// dated; any other failure is not. The upgrade signal is stored with the
// version its height upgrades to.
func TestThePollBeforeFibre(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	c := &fakePollChain{appVersion: 8, height: 500, tipTime: now,
		provErr: &scan.ABCIError{Code: 6, Codespace: "sdk", Log: "unknown query path"},
		signal:  scan.UpgradeSignal{VotingPower: 70, ThresholdPower: 83, TotalVotingPower: 100, UpgradeHeight: 9000, UpgradeAppVersion: 9}}
	p, st, _, _ := newPoll(t, c)
	p.run(context.Background(), now, true, true)
	for k, want := range map[string]string{
		"fibre_active": "no", "endpoints_polled_at": store.TS(now),
		"signal_upgrade_height": "9000", "signal_upgrade_app_version": "9", "signal_version": "10",
	} {
		if got := meta(t, st, k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	c.provErr = errors.New("connection reset")
	p.run(context.Background(), now.Add(time.Minute), true, true)
	if got := meta(t, st, "endpoints_polled_at"); got != store.TS(now) {
		t.Fatalf("a failed endpoint poll was dated: %s", got)
	}
}
