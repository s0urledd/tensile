package probe

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tlsverify "github.com/plsgiveup/fibre/fibre-tlsverify"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// hangingEndpoint accepts connections and never answers: a validator whose
// endpoint takes the connect and then lets every handshake run out of time.
type hangingEndpoint struct {
	addr    string
	accepts atomic.Int32
}

func newHangingEndpoint(t *testing.T) *hangingEndpoint {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &hangingEndpoint{addr: ln.Addr().String()}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			h.accepts.Add(1)
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
	})
	return h
}

// A validator whose endpoint fails before any blob is asked for is judged
// on every blob it owes an attempt on, though only one attempt is in flight
// to it at a time: the answer of one request is the answer of every attempt
// of it that was due when the request began. Six blobs owe its endpoint,
// which lets every handshake run out its 1.5 s, twelve later attempts; one
// at a time they would need 18 s, and their cutoff leaves about 11. At
// least nine in ten are made, by fewer requests than attempts, each row
// naming the request it repeats.
func TestAnEndpointThatFailsAnswersEveryAttemptWaitingForIt(t *testing.T) {
	down := newHangingEndpoint(t)
	f := newReadFixture(t, 4, 16, []fakeVal{{rows: rowsOf(0, 4), serve: fakeServes}, {rows: rowsOf(1, 4), serve: fakeServes}})
	f.targets[1].Host = down.addr
	fs, pubs := []*readFixture{f}, []scan.Publication{f.pub}
	for i := 1; i < 6; i++ {
		g := *f
		g.pub.PromiseHash = fmt.Sprintf("%064x", i)
		fs, pubs = append(fs, &g), append(pubs, g.pub)
	}
	p := readProber(t, fs...)
	p.cfg.Timeouts.Download = 1500 * time.Millisecond
	p.cfg.RetrySpacing = 100 * time.Millisecond
	p.cfg.RequestStartMargin = time.Until(f.pub.MustServeUntil) - 14*time.Second
	ms := readFull(t, p, pubs...)

	v := f.targets[1].AddressHex
	keys := map[string]bool{}
	for _, m := range ms {
		keys[m.DedupeKey()] = true
	}
	byBlob := map[string][]Measurement{}
	for _, m := range ms {
		if m.ValidatorAddress == v {
			byBlob[m.PromiseHash] = append(byBlob[m.PromiseHash], m)
		}
	}
	firstPass := int32(0)
	made, owed, shared := 0, 0, 0
	for _, pb := range pubs {
		rows := byBlob[pb.PromiseHash]
		if len(rows) != 1+FullReadRetries {
			t.Fatalf("blob %s: %d rows for the validator, want every attempt", short(pb.PromiseHash), len(rows))
		}
		for k, m := range rows {
			if m.Attempt != k {
				t.Fatalf("blob %s: attempt %d in place %d", short(pb.PromiseHash), m.Attempt, k)
			}
			if k == 0 {
				firstPass++
				if m.Retry != nil {
					firstPass++ // the reading's own request has the client's re-dial
				}
				continue
			}
			owed++
			if m.Retry != nil {
				t.Fatalf("attempt %d re-dialled: a later attempt is one request", k)
			}
			if m.Classification == ClassNotProbed {
				continue
			}
			made++
			if m.Identity.OK || m.fullServed() || m.fullGap() {
				t.Fatalf("attempt %d: %s / %s, want the endpoint's failure", k, m.Outcome, m.Classification)
			}
			if m.SharedFrom != "" {
				shared++
				if !keys[m.SharedFrom] || m.SharedFrom == m.DedupeKey() || !strings.Contains(m.RawError, m.SharedFrom) {
					t.Fatalf("attempt %d shares %q, which is no other row on record", k, m.SharedFrom)
				}
			}
			if (k < FullReadRetries) != (m.NextAttemptDue != nil) {
				t.Fatalf("attempt %d: next attempt due %v", k, m.NextAttemptDue)
			}
		}
	}
	if made*10 < owed*9 {
		t.Fatalf("%d of %d owed attempts made, want at least nine in ten", made, owed)
	}
	requests := int(down.accepts.Load() - firstPass)
	if shared == 0 || requests >= made {
		t.Fatalf("%d attempts made by %d requests (%d shared), want fewer requests than attempts", made, requests, shared)
	}
	st := p.readStatus()
	if st["retries_shared_last_hour"].(int) != shared || st["retries_made_last_hour"].(int) != made-shared {
		t.Fatalf("status %+v, want %d made and %d shared", st, made-shared, shared)
	}
}

// A later attempt is one request, without the client's re-dial: the
// reading's own request re-dials a handshake that failed, each attempt
// after it connects once.
func TestALaterAttemptIsOneRequest(t *testing.T) {
	down := newHangingEndpoint(t)
	f := newReadFixture(t, 4, 16, []fakeVal{{rows: rowsOf(0, 4), serve: fakeServes}, {rows: rowsOf(1, 4), serve: fakeServes}})
	f.targets[1].Host = down.addr
	p := readProber(t, f)
	p.cfg.Timeouts.Download = 500 * time.Millisecond
	p.cfg.RetrySpacing = 50 * time.Millisecond
	ms := readFull(t, p, f.pub)
	rows := attemptsOf(ms)[f.targets[1].AddressHex]
	if len(rows) != 1+FullReadRetries || rows[0].Retry == nil || rows[1].Retry != nil || rows[2].Retry != nil {
		t.Fatalf("%d rows for the validator, want the reading's re-dialled request and two attempts of one request each", len(rows))
	}
	if n := down.accepts.Load(); n != 4 {
		t.Fatalf("the endpoint took %d connections, want 2 + 1 + 1", n)
	}
}

// An attempt this run makes after one a restart left it is this run's own:
// the next one it queues is not marked as owed by an earlier run.
func TestTheNextAttemptIsThisRunsOwn(t *testing.T) {
	p := testProber(t)
	now := time.Now().UTC()
	pub := scan.Publication{PromiseHash: "p1", MustServeUntil: now.Add(time.Hour)}
	j := &retryJob{pub: pub, point: SchedulePoint{At: now, Phase: PhaseInWindow, Label: FullReadLabel}, target: Target{AddressHex: "v1"},
		resolved: true, attempt: 1, due: now, cutoff: now.Add(30 * time.Minute), recovered: true}
	p.retries.push(j)
	if got, _ := p.retries.popDue(now); got != j {
		t.Fatal("the job is not due")
	}
	m := Measurement{Vantage: "v", PromiseHash: "p1", ValidatorAddress: "v1", ScheduleLabel: FullReadLabel, ScheduledAt: now,
		StartedAt: now, FinishedAt: now.Add(time.Second), Attempt: 1, Phase: PhaseInWindow, Outcome: OutcomeNotFound, Classification: ClassFault}
	p.recordAttempts([]*retryJob{j}, []Measurement{m})
	next, _ := p.retries.popDue(now.Add(time.Hour))
	if next == nil || next.attempt != 2 || next.recovered || !next.due.Equal(now.Add(time.Second+p.cfg.RetrySpacing)) {
		t.Fatalf("next job %+v, want attempt 2 of this run, due %s after the answer", next, p.cfg.RetrySpacing)
	}
	if !p.retries.owes("p1") {
		t.Fatal("the blob is let go while it owes an attempt")
	}
}

// A failure of a full reading that this observer cannot pin on the
// validator is its own gap: a connect that timed out while it reached no
// server, and none of the servers it reached last answers a connect now; a
// timeout after its own resolver took most of the request's time; a
// certificate read as out of its window when the window's edge is within
// its clock's offset and a minute. A connect that timed out while it
// reached other servers, or while one it reached answers now, stays the
// validator's; and only full readings are looked at.
func TestAFailureOnThisObserversSideIsItsOwnGap(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	row := func(label string, o Outcome) Measurement {
		m := Measurement{ValidatorAddress: "v9", ScheduleLabel: label, Phase: PhaseInWindow, Assigned: true, Attested: true,
			StartedAt: now.Add(-15 * time.Second), FinishedAt: now, Outcome: o, RawError: "dial tcp: i/o timeout"}
		m.TCP = StepResult{Attempted: true}
		classifyRow(&m)
		return m
	}
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	gone, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	goneAddr := gone.Addr().String()
	gone.Close()
	reached := func(at time.Time, ep string) Measurement {
		return Measurement{ValidatorAddress: "v1", StartedAt: at, TCP: StepResult{OK: true, Detail: "-> " + ep}}
	}

	for _, c := range []struct {
		name   string
		reach  []Measurement
		m      func() Measurement
		ownGap bool
	}{
		{"nothing reached, nothing to check", nil, func() Measurement { return row(FullReadLabel, OutcomeTCPTimeout) }, true},
		{"no route, nothing reached", nil, func() Measurement { return row(FullReadLabel, OutcomeTCPUnreachable) }, true},
		{"another request reached a server meanwhile", []Measurement{reached(now.Add(-20*time.Second), goneAddr)},
			func() Measurement { return row(FullReadLabel, OutcomeTCPTimeout) }, false},
		{"a server reached earlier answers now", []Measurement{reached(now.Add(-10*time.Minute), up.Addr().String())},
			func() Measurement { return row(FullReadLabel, OutcomeTCPTimeout) }, false},
		{"the servers reached earlier do not answer", []Measurement{reached(now.Add(-10*time.Minute), goneAddr)},
			func() Measurement { return row(FullReadLabel, OutcomeTCPTimeout) }, true},
		{"only the failing validator's own endpoint was reached earlier", []Measurement{func() Measurement {
			m := reached(now.Add(-10*time.Minute), up.Addr().String())
			m.ValidatorAddress = "v9"
			return m
		}()}, func() Measurement { return row(FullReadLabel, OutcomeTCPTimeout) }, true},
		{"not a full reading", nil, func() Measurement { return row(EnoughReadLabel, OutcomeTCPTimeout) }, false},
		{"refused: a server answered", nil, func() Measurement { return row(FullReadLabel, OutcomeTCPRefused) }, false},
		{"slow lookup, then out of time", nil, func() Measurement {
			m := row(FullReadLabel, OutcomeRPCTimeout)
			m.DNS, m.TCP.OK = StepResult{Attempted: true, OK: true, DurationMS: 6000}, true
			classifyRow(&m)
			return m
		}, true},
		{"a lookup in good time, then out of time", nil, func() Measurement {
			m := row(FullReadLabel, OutcomeRPCTimeout)
			m.DNS, m.TCP.OK = StepResult{Attempted: true, OK: true, DurationMS: 4000}, true
			classifyRow(&m)
			return m
		}, false},
		{"a window that ended within the clock's reach", nil, func() Measurement {
			m := row(FullReadLabel, OutcomeIdentityFail)
			m.TCP.OK, m.ClockOffsetMS = true, -20000
			// the check turns ClockSkew after the signed edge: 70 s before the request
			m.Identity = IdentityResult{Attempted: true, Stale: true, Reason: string(tlsverify.ReasonCertExpired),
				ClaimedNotAfter: m.StartedAt.Add(-tlsverify.ClockSkew - 70*time.Second).Format(time.RFC3339)}
			classifyRow(&m)
			return m
		}, true},
		{"a window that ended long before", nil, func() Measurement {
			m := row(FullReadLabel, OutcomeIdentityFail)
			m.TCP.OK = true
			m.Identity = IdentityResult{Attempted: true, Stale: true, Reason: string(tlsverify.ReasonCertExpired),
				ClaimedNotAfter: m.StartedAt.Add(-2 * time.Hour).Format(time.RFC3339)}
			classifyRow(&m)
			return m
		}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := testProber(t)
			for _, r := range c.reach {
				p.reach.note(r)
			}
			m := c.m()
			before := m.Outcome
			p.ownSide(ctx, &m)
			if got := m.Outcome == OutcomeProbeError; got != c.ownGap {
				t.Fatalf("%s became %s / %s %q", before, m.Outcome, m.Classification, m.RawError)
			}
			if c.ownGap && (m.Classification != ClassProbeError || !strings.Contains(m.RawError, "on the wire: "+string(before)) ||
				!strings.HasPrefix(m.ClassificationReason, "counted neither way: this observer's own")) {
				t.Fatalf("rewritten as %s %q / %q", m.Classification, m.RawError, m.ClassificationReason)
			}
		})
	}
}

// After a restart, the attempts a blob still owes resolve its validators
// once between them, not once each: three validators owed an attempt on
// one blob cost one resolution, and every attempt is made.
func TestARestartResolvesEachBlobOnce(t *testing.T) {
	var vals []fakeVal
	for i := 0; i < 4; i++ {
		serve := firstThen(1, fakeNotFound, fakeServes)
		if i == 0 {
			serve = fakeServes
		}
		vals = append(vals, fakeVal{rows: rowsOf(i, 2), serve: serve})
	}
	f := newReadFixture(t, 4, 16, vals)
	before := readProber(t, f)
	before.cfg.AskEveryEndorser = true
	before.cfg.RetrySpacing = 50 * time.Millisecond
	if ms := readNow(t, before, f.pub); len(ms) != 4 {
		t.Fatalf("first run: %d rows", len(ms))
	}
	dir := before.cfg.DataDir
	if err := before.store.Close(); err != nil {
		t.Fatal(err)
	}
	p := readProber(t, f)
	_ = p.store.Close()
	st, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	p.store, p.cfg.DataDir = st, dir
	p.cfg.AskEveryEndorser = true
	var resolutions atomic.Int32
	resolve := p.targetsFor
	p.targetsFor = func(ctx context.Context, pub scan.Publication) ([]Target, error) {
		resolutions.Add(1)
		return resolve(ctx, pub)
	}
	if owed := st.PendingAttempts(f.pub.PromiseHash); len(owed) != 3 {
		t.Fatalf("owed after the restart: %+v", owed)
	}
	p.planReads([]scan.Publication{f.pub}, time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runRetries(ctx)
	}()
	waitFor(t, 30*time.Second, p.retries.idle)
	cancel()
	<-done
	if n := resolutions.Load(); n != 1 {
		t.Fatalf("%d resolutions for one blob's three owed attempts, want 1", n)
	}
	ms, err := LoadMeasurements(st.Path())
	if err != nil {
		t.Fatal(err)
	}
	made := 0
	for _, m := range ms {
		if m.Attempt == 1 && m.Download.CommitmentVerified {
			made++
		}
	}
	if made != 3 {
		t.Fatalf("%d owed attempts made and served, want 3", made)
	}
}

// The rows of later attempts are written at once and made durable by the
// next sync, not one fsync each: they are on record (and owed attempts
// with them) before the sync, and the sync leaves nothing pending.
func TestAttemptRowsAreWrittenAndSyncedTogether(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	at := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	due := at.Add(2 * time.Minute)
	var ms []Measurement
	for i := 0; i < 3; i++ {
		m := Measurement{Vantage: "v1", PromiseHash: "p1", ValidatorAddress: fmt.Sprintf("v%d", i), ScheduleLabel: FullReadLabel,
			ScheduledAt: at, StartedAt: at, FinishedAt: at, Attempt: 1, Outcome: OutcomeNotFound, Classification: ClassFault}
		if i == 0 {
			m.NextAttemptDue = &due
		}
		ms = append(ms, m)
	}
	if err := st.AppendDeferredRows(ms); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	dirty := st.dirty
	st.mu.Unlock()
	if !dirty {
		t.Fatal("deferred rows were synced at once")
	}
	if got, err := LoadMeasurements(st.Path()); err != nil || len(got) != 3 {
		t.Fatalf("%d rows on record before the sync (%v), want 3", len(got), err)
	}
	if owed := st.PendingAttempts("p1"); len(owed) != 1 || owed[0].Validator != "v0" || owed[0].Attempt != 1 || !owed[0].Due.Equal(due) {
		t.Fatalf("owed %+v", owed)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	dirty = st.dirty
	st.mu.Unlock()
	if dirty {
		t.Fatal("rows still pending after the sync")
	}
	if err := st.AppendDeferredRows(ms); err != nil {
		t.Fatal(err)
	}
	if got, _ := LoadMeasurements(st.Path()); len(got) != 3 {
		t.Fatalf("%d rows after writing the same attempts again, want 3", len(got))
	}
}

// With the observer's link rate set, the shard bytes in flight are held to
// what it moves in half a request's time; without it the default stands.
func TestTheLinkBoundsTheBytesInFlight(t *testing.T) {
	if got := LinkBytesCap(1000, 15*time.Second); got != 937500000 {
		t.Fatalf("1000 Mbit/s over 7.5 s: %d bytes", got)
	}
	if c := (Config{LinkMbps: 100}).withDefaults(); c.InFlightBytes != LinkBytesCap(100, ClientRPCTimeout) {
		t.Fatalf("100 Mbit/s: %d bytes in flight", c.InFlightBytes)
	}
	if c := (Config{LinkMbps: 10000}).withDefaults(); c.InFlightBytes != defaultInFlightBytes {
		t.Fatalf("a link faster than the default needs: %d bytes in flight", c.InFlightBytes)
	}
	if c := (Config{}).withDefaults(); c.InFlightBytes != defaultInFlightBytes || c.Concurrency != DefaultConcurrency {
		t.Fatalf("defaults: %d bytes, %d requests", c.InFlightBytes, c.Concurrency)
	}
	// A later attempt's own verifier is charged beside its shard: about
	// 4 MiB at K = 4096 and 16384 rows of code.
	if b := verifierBytes(4096, 16384); b < 4<<20 || b > 5<<20 {
		t.Fatalf("a verifier at K = 4096 charged %d bytes", b)
	}
}
