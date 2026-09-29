package probe

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-app/v10/pkg/rsema1d/rlc"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// serveFn is one fake validator's answer to its call-th request (from 1);
// honest is the shard it holds.
type serveFn func(ctx context.Context, call int, honest *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error)

func fakeServes(_ context.Context, _ int, honest *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
	return &fibretypes.DownloadShardResponse{Shard: honest}, nil
}

func fakeNotFound(context.Context, int, *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
	return nil, status.Error(codes.NotFound, "no blob shard found")
}

// fakeHangs never answers: the request runs out of its time. When the
// deadline the client sent passes, the server gives up too, with the status
// a gRPC server sends then; a CANCELLED here could reach the client before
// its own deadline and read as the server's error.
func fakeHangs(ctx context.Context, _ int, _ *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
	<-ctx.Done()
	return nil, status.Error(codes.DeadlineExceeded, "gone")
}

// fakeCorrupt answers with rows that do not verify against the commitment.
func fakeCorrupt(_ context.Context, _ int, honest *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
	bad := &fibretypes.BlobShard{Rlcs: honest.Rlcs}
	for _, r := range honest.Rows {
		d := append([]byte(nil), r.Data...)
		d[0] ^= 0xff
		bad.Rows = append(bad.Rows, &fibretypes.BlobRow{Index: r.Index, Data: d, Proof: r.Proof})
	}
	return &fibretypes.DownloadShardResponse{Shard: bad}, nil
}

// fakeSlow serves after d.
func fakeSlow(d time.Duration) serveFn {
	return func(ctx context.Context, call int, honest *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}
		return fakeServes(ctx, call, honest)
	}
}

// firstThen answers the first n requests with first, the rest with then.
func firstThen(n int, first, then serveFn) serveFn {
	return func(ctx context.Context, call int, honest *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
		if call <= n {
			return first(ctx, call, honest)
		}
		return then(ctx, call, honest)
	}
}

type fakeVal struct {
	rows  []int
	serve serveFn
}

// readFixture is one blob, encoded for real, and a Fibre server per
// validator holding its rows. The validators are asked in the order given
// (their voting power falls with it).
type readFixture struct {
	pub     scan.Publication
	targets []Target
	calls   []*atomic.Int32
	maxBusy []*atomic.Int32
}

func newReadFixture(t *testing.T, k, total int, vals []fakeVal) *readFixture {
	t.Helper()
	coder, err := NewCoder(k, total)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([][]byte, total)
	for i := range rows {
		rows[i] = make([]byte, 64)
		if i < k {
			if _, err := rand.Read(rows[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	ed, err := coder.c.Encode(rows)
	if err != nil {
		t.Fatal(err)
	}
	commit := ed.Commitment()
	rlcs := rlc.Marshal(ed.RLC())
	hash := make([]byte, 32)
	_, _ = rand.Read(hash)
	now := time.Now().UTC()
	f := &readFixture{}
	f.pub = scan.Publication{PromiseHash: hex.EncodeToString(hash), SettlementTime: now.Add(-time.Hour), MustServeUntil: now.Add(time.Hour)}
	f.pub.Promise.ChainID = "test-chain"
	f.pub.Promise.Commitment = hex.EncodeToString(commit[:])
	f.pub.Promise.BlobSize = uint32(k * 64)
	f.pub.Assignment.ProtocolParams = scan.ProtocolParamsSnapshot{OriginalRows: k, TotalRows: total, MinRowsPerValidator: 1,
		LivenessThresholdNum: 1, LivenessThresholdDen: 3}
	for i, v := range vals {
		shard := &fibretypes.BlobShard{Rlcs: rlcs}
		for _, r := range v.rows {
			pr, err := ed.GenerateRowProof(r)
			if err != nil {
				t.Fatal(err)
			}
			shard.Rows = append(shard.Rows, &fibretypes.BlobRow{Index: uint32(r), Data: append([]byte(nil), pr.Row...), Proof: pr.RowProof})
		}
		consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
		cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
		calls, busy, maxBusy := new(atomic.Int32), new(atomic.Int32), new(atomic.Int32)
		serve := v.serve
		host, _ := startFibre(t, cert, &fakeFibre{download: func(ctx context.Context, _ *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
			n := busy.Add(1)
			defer busy.Add(-1)
			for {
				m := maxBusy.Load()
				if n <= m || maxBusy.CompareAndSwap(m, n) {
					break
				}
			}
			return serve(ctx, int(calls.Add(1)), shard)
		}})
		var a assign.Address // distinct across fixtures: a validator is its address
		_, _ = rand.Read(a[:])
		f.targets = append(f.targets, Target{Address: a, AddressHex: a.String(), PubKey: consPub, Host: host, HostSource: "bonded",
			Assigned: true, Attested: true, RowCount: len(v.rows), AssignedRows: append([]int(nil), v.rows...), VotingPower: int64(1000 - i)})
		f.calls = append(f.calls, calls)
		f.maxBusy = append(f.maxBusy, maxBusy)
	}
	return f
}

// rowsOf gives validator i rows [i*n, (i+1)*n).
func rowsOf(i, n int) []int {
	out := make([]int, n)
	for j := range out {
		out[j] = i*n + j
	}
	return out
}

// readProber reads the fixtures' blobs, asking each fixture's validators.
func readProber(t *testing.T, fs ...*readFixture) *Prober {
	t.Helper()
	p := testProber(t)
	p.chainID = "test-chain"
	p.feed = newPubFeed(p.cfg.DataDir + "/publications.jsonl")
	p.cfg.AllowUnroutableHosts = true
	p.cfg.Timeouts = StepTimeouts{DNS: time.Second, TCP: time.Second, TLS: 2 * time.Second, Download: 2 * time.Second, MinDownloadBytesPerSec: -1}
	p.cfg.Order = func(_ context.Context, _ scan.Publication, ts []Target) ([]readTarget, error) {
		return orderTargets(ts, nil, nil), nil
	}
	byHash := map[string][]Target{}
	for _, f := range fs {
		byHash[f.pub.PromiseHash] = f.targets
	}
	p.targetsFor = func(_ context.Context, pub scan.Publication) ([]Target, error) {
		return byHash[pub.PromiseHash], nil
	}
	return p
}

// readNow queues each blob's reading to start at once, runs the dispatcher
// until every reading is written, and returns the rows.
func readNow(t *testing.T, p *Prober, pubs ...scan.Publication) []Measurement {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.dispatch(ctx)
	}()
	cfg := p.schedCfg()
	now := time.Now()
	for _, pb := range pubs {
		p.sched.push(&readJob{pub: pb, point: ReadPoint(pb, cfg), start: now, latest: pb.MustServeUntil.Add(-cfg.ReadDeadline)})
	}
	deadline := time.Now().Add(90 * time.Second)
	for !p.sched.idle() {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("the readings did not finish")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	ms, err := LoadMeasurements(p.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

func byValidator(ms []Measurement) map[string]Measurement {
	out := map[string]Measurement{}
	for _, m := range ms {
		out[m.ValidatorAddress] = m
	}
	return out
}

// resultOf is the one result every row of a reading carries.
func resultOf(t *testing.T, ms []Measurement) (string, string) {
	t.Helper()
	if len(ms) == 0 {
		t.Fatal("no rows")
	}
	r, e := ms[0].Read.BlobResult, ms[0].Read.BlobError
	for _, m := range ms {
		if m.Read == nil || m.Read.BlobResult != r || m.Read.BlobError != e {
			t.Fatalf("rows disagree on the reading's result: %+v", m.Read)
		}
	}
	return r, e
}

// The reading asks the validators in order while the rows it still wants
// outnumber the rows already on their way, and stops at K = 4096 distinct
// verified rows: of sixteen validators holding 1024 rows each, only the
// first four are asked, and the other twelve have no row.
func TestAReadingStopsAtK(t *testing.T) {
	const k, total, per = 4096, 16384, 1024
	var vals []fakeVal
	for i := 0; i < total/per; i++ {
		vals = append(vals, fakeVal{rows: rowsOf(i, per), serve: fakeServes})
	}
	f := newReadFixture(t, k, total, vals)
	p := readProber(t, f)
	ms := readNow(t, p, f.pub)
	if len(ms) != 4 {
		t.Fatalf("%d rows, want the first 4 validators only", len(ms))
	}
	novel, most := 0, 0
	for _, m := range ms {
		if !m.Download.CommitmentVerified || m.Read == nil || m.Read.BlobResult != ReadAvailable || m.Read.BlobError != "" || m.Read.Order > 3 {
			t.Fatalf("row %+v read %+v", m.Download, m.Read)
		}
		novel += m.Read.NovelRows
		most = max(most, m.Read.BlobHaveAfter)
	}
	if novel != k || most != k {
		t.Fatalf("novel rows %d, rows held %d, want %d", novel, most, k)
	}
	for i := 4; i < len(f.calls); i++ {
		if n := f.calls[i].Load(); n != 0 {
			t.Fatalf("validator %d was asked %d times after the rows were enough", i, n)
		}
	}
}

// The reading ends as the client's Download does, three ways only:
// Available; Unavailable with "no shards retrieved" when no verified row
// came back; Unavailable with "not enough shards to reconstruct blob" when
// some did, fewer than K. On an Unavailable blob every validator is asked
// once, and nothing more.
func TestTheThreeOutcomes(t *testing.T) {
	half := func(_ context.Context, _ int, honest *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
		return &fibretypes.DownloadShardResponse{Shard: &fibretypes.BlobShard{Rlcs: honest.Rlcs, Rows: honest.Rows[:1]}}, nil
	}
	for _, c := range []struct {
		name          string
		serve         [4]serveFn
		result, error string
	}{
		{"available", [4]serveFn{fakeServes, fakeNotFound, fakeServes, fakeServes}, ReadAvailable, ""},
		{"no shards", [4]serveFn{fakeNotFound, fakeThrottled, fakeCorrupt, fakeNotFound}, ReadUnavailable, ClientErrNoShards},
		{"not enough", [4]serveFn{fakeServes, fakeNotFound, half, fakeNotFound}, ReadUnavailable, ClientErrNotEnoughShards},
	} {
		t.Run(c.name, func(t *testing.T) {
			var vals []fakeVal
			for i, s := range c.serve {
				vals = append(vals, fakeVal{rows: rowsOf(i, 2), serve: s})
			}
			f := newReadFixture(t, 4, 8, vals)
			p := readProber(t, f)
			ms := readNow(t, p, f.pub)
			if r, e := resultOf(t, ms); r != c.result || e != c.error {
				t.Fatalf("reading: %s %q, want %s %q", r, e, c.result, c.error)
			}
			if c.result == ReadUnavailable {
				if len(ms) != 4 {
					t.Fatalf("%d rows, want every validator asked", len(ms))
				}
				for i, n := range f.calls {
					if n.Load() != 1 {
						t.Fatalf("validator %d asked %d times, want once", i, n.Load())
					}
				}
			}
		})
	}
}

// A validator that times out is asked again at once (the client's re-dial),
// then its rows are wanted from the next one; one whose rows do not verify
// is skipped the same way. The blob is available.
func TestAFailureHandsItsRowsToTheNextValidator(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 2), serve: fakeServes},
		{rows: rowsOf(1, 2), serve: fakeHangs},
		{rows: rowsOf(2, 2), serve: fakeCorrupt},
		{rows: rowsOf(3, 2), serve: fakeServes},
		{rows: rowsOf(4, 2), serve: fakeServes},
	})
	p := readProber(t, f)
	p.cfg.Timeouts.Download = 300 * time.Millisecond
	ms := byValidator(readNow(t, p, f.pub))
	hung, bad := ms[f.targets[1].AddressHex], ms[f.targets[2].AddressHex]
	if hung.Outcome != OutcomeRPCTimeout || hung.Classification != ClassUnreachable ||
		hung.Retry == nil || hung.Retry.Attempts != 2 || f.calls[1].Load() != 2 {
		t.Fatalf("timed-out validator: %s / %s, read %+v, retry %+v, calls %d", hung.Outcome, hung.Classification, hung.Read, hung.Retry, f.calls[1].Load())
	}
	if bad.Outcome != OutcomeInvalidRows || bad.Download.CommitmentVerified || f.calls[2].Load() != 1 {
		t.Fatalf("corrupt validator: %s verified=%v calls %d", bad.Outcome, bad.Download.CommitmentVerified, f.calls[2].Load())
	}
	served := 0
	for _, m := range ms {
		if m.Read.BlobResult != ReadAvailable {
			t.Fatalf("%s: blob result %s", m.ValidatorAddress, m.Read.BlobResult)
		}
		if m.Download.CommitmentVerified {
			served++
		}
	}
	if served != 2 {
		t.Fatalf("%d validators served, want 2", served)
	}
}

// Rows two validators both return are counted once: the reading goes on
// until it holds K distinct rows.
func TestSharedRowsCountOnce(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: []int{0, 1}, serve: fakeServes},
		{rows: []int{0, 1}, serve: fakeServes},
		{rows: []int{2, 3}, serve: fakeServes},
		{rows: []int{4, 5}, serve: fakeServes},
	})
	p := readProber(t, f)
	ms := readNow(t, p, f.pub)
	if len(ms) != 3 {
		t.Fatalf("%d rows, want 3: the shared rows do not count twice, the fourth validator is not needed", len(ms))
	}
	novel := 0
	for _, m := range ms {
		novel += m.Read.NovelRows
	}
	if novel != 4 || f.calls[3].Load() != 0 {
		t.Fatalf("novel rows %d, fourth validator asked %d times", novel, f.calls[3].Load())
	}
}

// A reading in which not a single request reached a server did not happen:
// not read. Here two requests fail on this observer's side (no consensus
// key to check a certificate against, so no connection is opened) and the
// third validator has no host to connect to at all: nobody was reached,
// and a validator without a host is no answer that the reading happened.
func TestEveryRequestFailingHereIsNotRead(t *testing.T) {
	f := newReadFixture(t, 4, 8, []fakeVal{{rows: rowsOf(0, 2), serve: fakeServes}, {rows: rowsOf(1, 2), serve: fakeServes},
		{rows: rowsOf(2, 2), serve: fakeServes}})
	for i := range f.targets {
		f.targets[i].PubKey = nil
	}
	f.targets[2].Host = ""
	f.targets[2].Attested = false
	p := readProber(t, f)
	ms := readNow(t, p, f.pub)
	if len(ms) != 3 {
		t.Fatalf("%d rows, want every validator asked", len(ms))
	}
	if r, e := resultOf(t, ms); r != ReadNotRead || e != "" {
		t.Fatalf("reading: %s %q, want not read", r, e)
	}
	for _, m := range ms {
		if Reached(m.TCP.OK, m.Outcome, m.Download.CommitmentVerified) {
			t.Fatalf("row reached a server: %s / %s", m.Outcome, m.Classification)
		}
	}
	if o := byValidator(ms)[f.targets[2].AddressHex].Outcome; o != OutcomeNoHost {
		t.Fatalf("the hostless validator: %s", o)
	}
	for i := range f.calls {
		if f.calls[i].Load() != 0 {
			t.Fatal("a validator was reached")
		}
	}

	// One validator reached is enough for the reading to have happened: the
	// blob is Unavailable, as the client would say.
	g := newReadFixture(t, 4, 8, []fakeVal{{rows: rowsOf(0, 2), serve: fakeServes}, {rows: rowsOf(1, 2), serve: fakeNotFound}})
	g.targets[0].PubKey = nil
	p = readProber(t, g)
	if r, e := resultOf(t, readNow(t, p, g.pub)); r != ReadUnavailable || e != ClientErrNoShards {
		t.Fatalf("reading with one validator reached: %s %q", r, e)
	}
}

// A request is made again at once only after the client would make it
// again: a lookup or a dial that failed, for whatever reason, or an
// unreachable or timed-out peer; never after an answer.
func TestRedialsLikeTheClient(t *testing.T) {
	for o, want := range map[Outcome]bool{
		OutcomeTCPRefused: true, OutcomeTCPTimeout: true, OutcomeDNSFail: true, OutcomeTLSFail: true,
		OutcomeRPCUnavailable: true, OutcomeRPCTimeout: true,
		OutcomeNotFound: false, OutcomeServerError: false, OutcomeInvalidRows: false, OutcomeThrottled: false, OutcomeServedOK: false,
		OutcomeNoHost: false, OutcomeBadHost: false,
	} {
		if redials(Measurement{Outcome: o}) != want {
			t.Errorf("%s: redial %v, want %v", o, !want, want)
		}
	}
	for _, c := range []struct {
		name string
		m    Measurement
		want bool
	}{
		// this observer's resolver did not answer: gRPC's resolver error is Unavailable
		{"lookup", Measurement{Outcome: OutcomeProbeError, DNS: StepResult{Attempted: true}}, true},
		// every address failed to dial here (no route out, a local errno)
		{"dial", Measurement{Outcome: OutcomeProbeError, DNS: StepResult{OK: true}, TCP: StepResult{Attempted: true}}, true},
		// no key to check a certificate against: the request never started
		{"no key", Measurement{Outcome: OutcomeProbeError}, false},
		// connected, then this build could not handle the answer
		{"after connect", Measurement{Outcome: OutcomeProbeError, TCP: StepResult{Attempted: true, OK: true}}, false},
	} {
		if redials(c.m) != c.want {
			t.Errorf("PROBE_ERROR, %s: redial %v, want %v", c.name, !c.want, c.want)
		}
	}
	f := newReadFixture(t, 4, 8, []fakeVal{
		{rows: rowsOf(0, 2), serve: firstThen(1, fakeHangs, fakeServes)},
		{rows: rowsOf(1, 2), serve: fakeNotFound},
		{rows: rowsOf(2, 2), serve: fakeServes},
	})
	p := readProber(t, f)
	p.cfg.Timeouts.Download = 300 * time.Millisecond
	ms := byValidator(readNow(t, p, f.pub))
	m := ms[f.targets[0].AddressHex]
	if !m.Download.CommitmentVerified || m.Retry == nil || m.Retry.FirstOutcome != OutcomeRPCTimeout || f.calls[0].Load() != 2 {
		t.Fatalf("re-dialled validator: %s, read %+v, retry %+v", m.Outcome, m.Read, m.Retry)
	}
	if f.calls[1].Load() != 1 || ms[f.targets[1].AddressHex].Retry != nil {
		t.Fatalf("a NOT_FOUND was asked again at once (%d calls)", f.calls[1].Load())
	}
}

// A slow blob holds only its own reading: another blob's is read meanwhile.
func TestASlowBlobDoesNotHoldUpAnother(t *testing.T) {
	slow := newReadFixture(t, 4, 8, []fakeVal{{rows: []int{0, 1, 2, 3}, serve: fakeSlow(1500 * time.Millisecond)}})
	fast := newReadFixture(t, 4, 8, []fakeVal{{rows: []int{0, 1, 2, 3}, serve: fakeServes}})
	p := readProber(t, slow, fast)
	ms := readNow(t, p, slow.pub, fast.pub)
	fin := map[string]time.Time{}
	for _, m := range ms {
		fin[m.PromiseHash] = m.FinishedAt
	}
	if len(fin) != 2 || !fin[fast.pub.PromiseHash].Before(fin[slow.pub.PromiseHash].Add(-time.Second)) {
		t.Fatalf("finished: %v", fin)
	}
}

// A reading that cannot start before its latest start is not made: every
// endorsing validator gets a NOT_PROBED row, and none is asked.
func TestAReadingThatCannotStartInTimeIsNotRead(t *testing.T) {
	f := newReadFixture(t, 4, 8, []fakeVal{{rows: rowsOf(0, 2), serve: fakeServes}, {rows: rowsOf(1, 2), serve: fakeServes}})
	p := readProber(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.dispatch(ctx)
	}()
	p.sched.push(&readJob{pub: f.pub, point: ReadPoint(f.pub, p.schedCfg()), start: time.Now().Add(-time.Minute), latest: time.Now().Add(-time.Second)})
	for deadline := time.Now().Add(10 * time.Second); !p.sched.idle() && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	ms, err := LoadMeasurements(p.store.Path())
	if err != nil || len(ms) != 2 {
		t.Fatalf("%d rows, %v", len(ms), err)
	}
	for _, m := range ms {
		if m.Classification != ClassNotProbed || !strings.Contains(m.ClassificationReason, "not read in time") {
			t.Fatalf("row: %s (%s)", m.Classification, m.ClassificationReason)
		}
	}
	if f.calls[0].Load()+f.calls[1].Load() != 0 {
		t.Fatal("a validator was asked for a reading that was not made")
	}
}

// The client has no limit per validator, and neither has the reading: two
// promises over one blob, read at once, ask the validator they share at
// once.
func TestNoLimitPerValidator(t *testing.T) {
	f := newReadFixture(t, 4, 8, []fakeVal{{rows: []int{0, 1, 2, 3}, serve: fakeSlow(400 * time.Millisecond)}})
	other := *f
	other.pub.PromiseHash = strings.Repeat("ee", 32)
	p := readProber(t, f, &other)
	ms := readNow(t, p, f.pub, other.pub)
	if len(ms) != 2 || !ms[0].Download.CommitmentVerified || !ms[1].Download.CommitmentVerified {
		t.Fatalf("rows: %+v", ms)
	}
	if n := f.maxBusy[0].Load(); n != 2 {
		t.Fatalf("the validator had %d requests at once, want both readings' at once", n)
	}
}

// This observer's limit on requests in flight only delays a request: with
// room for one at a time, four slow validators are asked one after another,
// each gets the client's whole time from the moment it is let go, and none
// is dropped or changed. The reading takes longer than one request's time,
// and the blob is Available.
func TestTheRequestLimitOnlyDelays(t *testing.T) {
	var vals []fakeVal
	for i := 0; i < 4; i++ {
		vals = append(vals, fakeVal{rows: []int{i}, serve: fakeSlow(250 * time.Millisecond)})
	}
	f := newReadFixture(t, 4, 8, vals)
	p := readProber(t, f)
	p.cfg.Concurrency = 1
	p.initPace()
	p.cfg.Timeouts.Download = 400 * time.Millisecond
	start := time.Now()
	ms := readNow(t, p, f.pub)
	if r, _ := resultOf(t, ms); r != ReadAvailable || len(ms) != 4 {
		t.Fatalf("reading: %s over %d rows", r, len(ms))
	}
	for _, m := range ms {
		if !m.Download.CommitmentVerified || m.Retry != nil {
			t.Fatalf("row: %s, retry %+v", m.Outcome, m.Retry)
		}
	}
	if took := time.Since(start); took < 900*time.Millisecond {
		t.Fatalf("the reading took %s: the requests were not one at a time", took)
	}
}

// A request this observer's limit held back past must_serve_until carries
// the phase the reading started in, as the client, which asks every
// validator at once, would have asked it in the window: with room for one
// request at a time, four slow validators are asked one after another, the
// window ends while the later ones wait, and every row is in the window.
func TestARequestHeldBackCarriesTheReadingsPhase(t *testing.T) {
	var vals []fakeVal
	for i := 0; i < 4; i++ {
		vals = append(vals, fakeVal{rows: []int{i}, serve: fakeSlow(300 * time.Millisecond)})
	}
	f := newReadFixture(t, 4, 8, vals)
	p := readProber(t, f)
	p.cfg.Concurrency = 1
	p.initPace()
	p.cfg.Timeouts.Download = 2 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.dispatch(ctx)
	}()
	now := time.Now()
	f.pub.MustServeUntil = now.Add(500 * time.Millisecond)
	p.sched.push(&readJob{pub: f.pub, point: SchedulePoint{At: now, Phase: PhaseInWindow, Label: EndReadLabel}, start: now, latest: now.Add(time.Minute)})
	for deadline := time.Now().Add(20 * time.Second); !p.sched.idle() && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	ms, err := LoadMeasurements(p.store.Path())
	if err != nil || len(ms) != 4 {
		t.Fatalf("%d rows, %v", len(ms), err)
	}
	late := 0
	for _, m := range ms {
		if m.Phase != PhaseInWindow || !m.Download.CommitmentVerified || m.Read.BlobResult != ReadAvailable {
			t.Fatalf("row started %s after must_serve_until: %s %s / %s, read %+v", m.StartedAt.Sub(f.pub.MustServeUntil), m.Phase,
				m.Outcome, m.Classification, m.Read)
		}
		if m.StartedAt.After(f.pub.MustServeUntil) {
			late++
		}
	}
	if late == 0 {
		t.Fatal("no request was held past must_serve_until")
	}
}

// ClientOrder expects of each validator the rows fibre-assign gives it: the
// reading's reservations are the assignment the promise was made under.
func TestClientOrderExpectsTheRowsFibreAssignGives(t *testing.T) {
	ap := assign.ParamsV10BlobV0
	pp := scan.ProtocolParamsSnapshot{OriginalRows: ap.OriginalRows, TotalRows: ap.TotalRows, MinRowsPerValidator: ap.MinRowsPerValidator,
		LivenessThresholdNum: ap.LivenessThreshold.Numerator, LivenessThresholdDen: ap.LivenessThreshold.Denominator}
	var members []scan.ValSetMember
	var vals []assign.Validator
	for i := 0; i < 40; i++ {
		pk, _, _ := ed25519.GenerateKey(rand.Reader)
		addr := cmted25519.PubKey(pk).Address()
		power := int64(1000 + i*i*37)
		members = append(members, scan.ValSetMember{Address: addr, PubKey: pk, VotingPower: power})
		var a assign.Address
		copy(a[:], addr)
		vals = append(vals, assign.Validator{Address: a, VotingPower: power})
	}
	var commitment [32]byte
	_, _ = rand.Read(commitment[:])
	sm, err := assign.Assign(commitment, vals, ap)
	if err != nil {
		t.Fatal(err)
	}
	order, expected, err := ClientOrder(members, 100, pp)
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != len(members) {
		t.Fatalf("order names %d of %d validators", len(order), len(members))
	}
	seen := map[string]bool{}
	for _, a := range order {
		if seen[a] {
			t.Fatalf("%s ordered twice", a)
		}
		seen[a] = true
	}
	for _, v := range vals {
		if got, want := expected[v.Address.String()], len(sm[v.Address]); got != want {
			t.Fatalf("%s: client expects %d rows, fibre-assign gives %d", v.Address, got, want)
		}
	}
}
