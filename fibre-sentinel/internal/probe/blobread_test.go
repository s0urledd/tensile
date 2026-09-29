package probe

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
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

// fakeHangs never answers: the request runs out of its time.
func fakeHangs(ctx context.Context, _ int, _ *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
	<-ctx.Done()
	return nil, status.Error(codes.Canceled, "gone")
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
	p.requests = &requestLog{path: ConfirmRequestsPath(p.cfg.DataDir)}
	t.Cleanup(p.requests.close)
	p.cfg.AllowUnroutableHosts = true
	p.cfg.Timeouts = StepTimeouts{DNS: time.Second, TCP: time.Second, TLS: 2 * time.Second, Download: 2 * time.Second, MinDownloadBytesPerSec: -1}
	p.cfg.Schedule.RetryAfter = 100 * time.Millisecond
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
// until every reading, second passes included, is written, and returns the
// rows.
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
		p.sched.push(&readJob{pub: pb, point: ReadPoint(pb, cfg), start: now, latest: pb.MustServeUntil.Add(-cfg.RequestCutoff)})
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
		if !m.Download.CommitmentVerified || m.Read == nil || m.Read.BlobResult != ReadAvailable || m.Read.Pass != 1 || m.Read.Order > 3 {
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

// A validator that times out is asked again at once (the client's re-dial),
// then its rows are wanted from the next one; one whose rows do not verify
// is passed over the same way. The blob is available, and nothing is sent
// for confirmation.
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
	if hung.Outcome != OutcomeRPCTimeout || hung.Classification != ClassUnreachable || !hung.Read.Redialed ||
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
	if got := readRequests(t, ConfirmRequestsPath(p.cfg.DataDir)); len(got) != 0 {
		t.Fatalf("%d confirmation requests for an available blob", len(got))
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

// When every validator has been asked and the rows are short, the
// reading is made again a minute later (here 100 ms) before the blob is
// Unavailable. The second pass asks the validators again, and the rows
// record both answers; each not-served row is sent for confirmation.
func TestASecondPassComesBeforeUnavailable(t *testing.T) {
	var vals []fakeVal
	for i := 0; i < 4; i++ {
		vals = append(vals, fakeVal{rows: rowsOf(i, 2), serve: fakeNotFound})
	}
	f := newReadFixture(t, 4, 8, vals)
	p := readProber(t, f)
	ms := readNow(t, p, f.pub)
	if len(ms) != 4 {
		t.Fatalf("%d rows, want 4", len(ms))
	}
	for i, m := range ms {
		if m.Read.BlobResult != ReadUnavailable || m.Read.Pass != 2 || m.Classification != ClassFault ||
			m.Retry == nil || m.Retry.Attempts != 2 || m.Retry.FirstOutcome != OutcomeNotFound {
			t.Fatalf("row %d: %s, read %+v, retry %+v", i, m.Classification, m.Read, m.Retry)
		}
		if gap := m.StartedAt.Sub(m.Retry.FirstStartedAt); gap < 100*time.Millisecond {
			t.Fatalf("second pass %s after the first, want at least the retry delay", gap)
		}
	}
	for i, c := range f.calls {
		if c.Load() != 2 {
			t.Fatalf("validator %d asked %d times, want once per pass", i, c.Load())
		}
	}
	// Every validator failed at once: the correlated-failure guard sets the
	// reading aside, so none of it can count, and nothing is sent for
	// confirmation.
	if !GuardSetsAside(ms) {
		t.Fatal("the guard does not set aside a reading every validator failed")
	}
	if got := readRequests(t, ConfirmRequestsPath(p.cfg.DataDir)); len(got) != 0 {
		t.Fatalf("%d confirmation requests for a reading the guard sets aside, want none", len(got))
	}
}

// A second pass that finds the rows makes the blob available; the
// validators it did not need to ask keep their first answer, on an
// available blob.
func TestASecondPassCanFindTheRows(t *testing.T) {
	var vals []fakeVal
	for i := 0; i < 4; i++ {
		vals = append(vals, fakeVal{rows: rowsOf(i, 2), serve: firstThen(1, fakeNotFound, fakeServes)})
	}
	f := newReadFixture(t, 4, 8, vals)
	p := readProber(t, f)
	ms := byValidator(readNow(t, p, f.pub))
	for i, tg := range f.targets {
		m := ms[tg.AddressHex]
		if m.Read.BlobResult != ReadAvailable {
			t.Fatalf("validator %d: %s", i, m.Read.BlobResult)
		}
		if i < 2 && (!m.Download.CommitmentVerified || m.Read.Pass != 2) {
			t.Fatalf("validator %d: served=%v pass %d", i, m.Download.CommitmentVerified, m.Read.Pass)
		}
		if i >= 2 && (m.Outcome != OutcomeNotFound || m.Read.Pass != 1) {
			t.Fatalf("validator %d: %s pass %d, want its first answer", i, m.Outcome, m.Read.Pass)
		}
	}
}

// With no time left for the second pass, the reading is incomplete: no
// answer in it is the validator's, and nothing is sent for confirmation.
func TestNoTimeForASecondPassIsTheObserversGap(t *testing.T) {
	var vals []fakeVal
	for i := 0; i < 4; i++ {
		vals = append(vals, fakeVal{rows: rowsOf(i, 2), serve: fakeNotFound})
	}
	f := newReadFixture(t, 4, 8, vals)
	f.pub.MustServeUntil = time.Now().Add(100 * time.Second)
	p := readProber(t, f)
	p.cfg.Schedule.RetryAfter = 20 * time.Second // past must_serve_until - 90s
	ms := readNow(t, p, f.pub)
	if len(ms) != 4 {
		t.Fatalf("%d rows", len(ms))
	}
	for _, m := range ms {
		if m.Read.BlobResult != ReadIncomplete || m.Outcome != OutcomeProbeError || m.Classification != ClassProbeError ||
			!strings.Contains(m.ClassificationReason, "second pass") {
			t.Fatalf("row: %s / %s (%s), read %+v", m.Outcome, m.Classification, m.ClassificationReason, m.Read)
		}
	}
	if _, err := os.Stat(ConfirmRequestsPath(p.cfg.DataDir)); err == nil {
		if got := readRequests(t, ConfirmRequestsPath(p.cfg.DataDir)); len(got) != 0 {
			t.Fatalf("%d confirmation requests for an incomplete reading", len(got))
		}
	}
}

// A request is made again at once only after the client would make it
// again: a failed dial or an unreachable or timed-out peer, never an answer.
func TestRedialsLikeTheClient(t *testing.T) {
	for o, want := range map[Outcome]bool{
		OutcomeTCPRefused: true, OutcomeTCPTimeout: true, OutcomeDNSFail: true, OutcomeTLSFail: true,
		OutcomeRPCUnavailable: true, OutcomeRPCTimeout: true,
		OutcomeNotFound: false, OutcomeServerError: false, OutcomeInvalidRows: false, OutcomeThrottled: false, OutcomeServedOK: false,
	} {
		if redials(o) != want {
			t.Errorf("%s: redial %v, want %v", o, !want, want)
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
	if !m.Download.CommitmentVerified || !m.Read.Redialed || m.Retry == nil || m.Retry.FirstOutcome != OutcomeRPCTimeout || f.calls[0].Load() != 2 {
		t.Fatalf("re-dialled validator: %s, read %+v, retry %+v", m.Outcome, m.Read, m.Retry)
	}
	if f.calls[1].Load() != 1 || ms[f.targets[1].AddressHex].Read.Redialed {
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

// One validator has one request from this observer at a time, however many
// blobs it holds: two promises over one blob are read one after the other at
// the validator they share.
func TestOneRequestPerValidatorAtATime(t *testing.T) {
	f := newReadFixture(t, 4, 8, []fakeVal{{rows: []int{0, 1, 2, 3}, serve: fakeSlow(200 * time.Millisecond)}})
	other := *f
	other.pub.PromiseHash = strings.Repeat("ee", 32)
	p := readProber(t, f, &other)
	ms := readNow(t, p, f.pub, other.pub)
	if len(ms) != 2 || !ms[0].Download.CommitmentVerified || !ms[1].Download.CommitmentVerified {
		t.Fatalf("rows: %+v", ms)
	}
	if n := f.maxBusy[0].Load(); n != 1 {
		t.Fatalf("the validator had %d requests at once, want 1", n)
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
