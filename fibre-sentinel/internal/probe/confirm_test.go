package probe

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

func readRequests(t *testing.T, path string) []ConfirmRequest {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []ConfirmRequest
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var r ConfirmRequest
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("request line %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

// Only a FAULT is re-checked from another vantage; every other class, the
// ones held out of the rate included, costs the second vantage nothing.
func TestRequestConfirmation_OnlyForFault(t *testing.T) {
	for _, cls := range AllClassifications {
		t.Run(string(cls), func(t *testing.T) {
			p := testProber(t)
			path := filepath.Join(p.cfg.DataDir, ConfirmRequestsFile)
			p.requests = &requestLog{path: path}
			defer p.requests.close()
			now := time.Now().UTC()
			pb := pub(now.Add(-time.Hour), now.Add(time.Hour))
			m := rowFor(pb, "v1", "aa", SchedulePoint{At: now, Label: "w2"})
			m.Classification = cls
			p.requestConfirmation(pb, Target{AddressHex: "aa", AssignedRows: []int{1, 2}}, m)
			got := readRequests(t, path)
			if cls == ClassFault {
				if len(got) != 1 {
					t.Fatalf("a FAULT wrote %d requests, want 1", len(got))
				}
				r := got[0]
				if r.PromiseHash != pb.PromiseHash || r.ValidatorAddress != "aa" || !r.ScheduledAt.Equal(m.ScheduledAt) ||
					r.FromVantage != "v1" || len(r.AssignedRows) != 2 || r.Classification != ClassFault {
					t.Errorf("request = %+v", r)
				}
				if want := m.StartedAt.Add(ConfirmWindow); !r.Deadline.Equal(want) {
					t.Errorf("deadline %s, want the fault's start plus the window %s", r.Deadline, want)
				}
				return
			}
			if len(got) != 0 {
				t.Fatalf("%s wrote %d confirmation requests, want none", cls, len(got))
			}
		})
	}
}

// A fault whose grace phase is already over cannot be cleared by any
// answer, so no request is sent; one near the end of its window gets the
// end of the grace phase as its deadline.
func TestRequestConfirmation_DeadlineStopsAtTheEndOfGrace(t *testing.T) {
	p := testProber(t)
	path := filepath.Join(p.cfg.DataDir, ConfirmRequestsFile)
	p.requests = &requestLog{path: path}
	defer p.requests.close()
	tol := p.schedCfg().PruneTolerance
	now := time.Now().UTC()

	late := pub(now.Add(-5*time.Hour), now.Add(-tol-time.Minute))
	m := rowFor(late, "v1", "aa", SchedulePoint{At: now, Label: "grace"})
	m.Classification = ClassFault
	p.requestConfirmation(late, Target{AddressHex: "aa", AssignedRows: []int{1}}, m)
	if got := readRequests(t, path); len(got) != 0 {
		t.Fatalf("a fault past its grace phase was sent for confirmation: %+v", got)
	}

	near := pub(now.Add(-4*time.Hour), now.Add(2*time.Minute))
	m = rowFor(near, "v1", "aa", SchedulePoint{At: now, Label: "w4"})
	m.Classification = ClassFault
	p.requestConfirmation(near, Target{AddressHex: "aa", AssignedRows: []int{1}}, m)
	got := readRequests(t, path)
	if len(got) != 1 {
		t.Fatalf("got %d requests, want 1", len(got))
	}
	if want := near.MustServeUntil.Add(tol); !got[0].Deadline.Equal(want) {
		t.Errorf("deadline %s, want the end of the grace phase %s", got[0].Deadline, want)
	}
}

// ---- the confirming side ----

type fakeConfirmChain struct {
	chainID string
	set     []scan.ValSetMember
	setErr  error
	heights []int64
}

func (f *fakeConfirmChain) Status(context.Context) (string, int64, error) { return f.chainID, 100, nil }
func (f *fakeConfirmChain) ValidatorSet(_ context.Context, h int64) ([]scan.ValSetMember, error) {
	f.heights = append(f.heights, h)
	return f.set, f.setErr
}
func (f *fakeConfirmChain) LatestBlockTime(context.Context) (time.Time, error) {
	return time.Now(), nil
}
func (f *fakeConfirmChain) AppVersion(context.Context) (uint64, error) { return 1, nil }

// confirmFixture is a three-validator set, a commitment, the assignment
// fibre-assign gives it, and a request for the first validator's rows.
type confirmFixture struct {
	chain   *fakeConfirmChain
	pub     ed25519.PublicKey
	priv    ed25519.PrivateKey
	addrHex string
	rows    []int
	req     ConfirmRequest
}

func newConfirmFixture(t *testing.T, host string) confirmFixture {
	t.Helper()
	pp := scan.ProtocolParamsSnapshot{OriginalRows: 4, TotalRows: 8, MinRowsPerValidator: 1, LivenessThresholdNum: 1, LivenessThresholdDen: 3}
	var set []scan.ValSetMember
	var vals []assign.Validator
	var firstPub ed25519.PublicKey
	var firstPriv ed25519.PrivateKey
	var first assign.Address
	for i := 0; i < 3; i++ {
		pk, sk, _ := ed25519.GenerateKey(rand.Reader)
		var a assign.Address
		a[0], a[19] = byte(i+1), 0xab
		set = append(set, scan.ValSetMember{Address: append([]byte(nil), a[:]...), PubKey: pk, VotingPower: 100})
		vals = append(vals, assign.Validator{Address: a, VotingPower: 100})
		if i == 0 {
			firstPub, firstPriv, first = pk, sk, a
		}
	}
	var commitment [32]byte
	commitment[0] = 7
	sm, err := assign.Assign(commitment, vals, assign.ProtocolParams{OriginalRows: 4, TotalRows: 8, MinRowsPerValidator: 1,
		LivenessThreshold: assign.Fraction{Numerator: 1, Denominator: 3}})
	if err != nil {
		t.Fatal(err)
	}
	rows := sm[first]
	if len(rows) == 0 {
		t.Fatal("fixture validator has no rows")
	}
	now := time.Now().UTC()
	req := ConfirmRequest{
		SchemaVersion: ConfirmRequestSchemaVersion, RequestedAt: now, Deadline: now.Add(ConfirmWindow), FromVantage: "ut-1",
		ChainID: "test-chain", PromiseHash: "feedface", Commitment: hex.EncodeToString(commitment[:]), BlobSize: 1024,
		MustServeUntil: now.Add(time.Hour), ValidatorSetHeight: 42, ProtocolParams: pp, PruneToleranceS: 150,
		ValidatorAddress: first.String(), ValidatorHost: host, Attested: true, AssignedRows: rows,
		ScheduleLabel: "w2", ScheduledAt: now.Add(-time.Minute), StartedAt: now.Add(-time.Minute),
		Outcome: OutcomeNotFound, Classification: ClassFault,
	}
	return confirmFixture{chain: &fakeConfirmChain{chainID: "test-chain", set: set}, pub: firstPub, priv: firstPriv,
		addrHex: first.String(), rows: rows, req: req}
}

func writeRequests(t *testing.T, path string, rs ...ConfirmRequest) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, r := range rs {
		b, _ := json.Marshal(r)
		if _, err := f.Write(append(b, '\n')); err != nil {
			t.Fatal(err)
		}
	}
}

func newTestConfirmer(t *testing.T, dir, reqPath string, chain ConfirmChain) *Confirmer {
	t.Helper()
	c, err := NewConfirmer(ConfirmConfig{RequestsPath: reqPath, DataDir: dir, Vantage: "de-1", AllowUnroutableHosts: true,
		Timeouts: StepTimeouts{DNS: time.Second, TCP: time.Second, TLS: 2 * time.Second, Download: 3 * time.Second}}, chain, scan.NewLogger(50))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// The confirming vantage fetches exactly the failed slot from the host
// that failed, once, with the full probe (identity included), and writes
// the answer under its own vantage and the fault's schedule point, so the
// collector can match it to the fault. The consensus key and the rows come
// from its own chain reads at the promise height, not from the request.
func TestConfirmer_FetchesTheFailedRowsOnceAndStampsItsVantage(t *testing.T) {
	var asked atomic.Int32
	var fx confirmFixture
	now := time.Now()
	// The host is known only after the server starts; the key before.
	pk, sk, _ := ed25519.GenerateKey(rand.Reader)
	cert := fibreCert(t, sk, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	host, ln := startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		asked.Add(1)
		return nil, status.Error(codes.NotFound, "shard not found")
	}})
	fx = newConfirmFixture(t, host)
	fx.chain.set[0].PubKey = pk // the certificate's key is the validator's consensus key on chain

	dir := t.TempDir()
	reqPath := filepath.Join(dir, "inbox-requests.jsonl")
	writeRequests(t, reqPath, fx.req)
	c := newTestConfirmer(t, dir, reqPath, fx.chain)
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	ms, err := LoadMeasurements(filepath.Join(dir, "measurements.jsonl"))
	if err != nil || len(ms) != 1 {
		t.Fatalf("measurements = %d, %v; want 1", len(ms), err)
	}
	m := ms[0]
	if m.Vantage != "de-1" || m.PromiseHash != fx.req.PromiseHash || m.ValidatorAddress != fx.addrHex ||
		!m.ScheduledAt.Equal(fx.req.ScheduledAt) || m.ScheduleLabel != "w2" || m.ValidatorHost != host {
		t.Errorf("row identity = %s %s %s %s %s %s", m.Vantage, m.PromiseHash, m.ValidatorAddress, m.ScheduledAt, m.ScheduleLabel, m.ValidatorHost)
	}
	if !m.Identity.OK || m.Outcome != OutcomeNotFound || m.Classification != ClassFault {
		t.Errorf("outcome %s / %s identity=%v (%s)", m.Outcome, m.Classification, m.Identity.OK, m.RawError)
	}
	if m.AssignedRowCount != len(fx.rows) || m.Download.RowsExpected != len(fx.rows) {
		t.Errorf("rows expected %d / %d, want %d", m.AssignedRowCount, m.Download.RowsExpected, len(fx.rows))
	}
	if !strings.Contains(m.ClassificationReason, "confirmation from de-1 of the NOT_FOUND FAULT recorded by ut-1") {
		t.Errorf("reason = %q", m.ClassificationReason)
	}
	if len(fx.chain.heights) != 1 || fx.chain.heights[0] != 42 {
		t.Errorf("validator set read at %v, want the promise height 42", fx.chain.heights)
	}

	// Once: a second pass, and a restart on the same data dir, ask nothing.
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	c2 := newTestConfirmer(t, dir, reqPath, fx.chain)
	if err := c2.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if asked.Load() != 1 || ln.accepted.Load() != 1 {
		t.Errorf("the validator was asked %d times over %d connections, want once", asked.Load(), ln.accepted.Load())
	}
}

// Everything the verdict rests on is read again: a request whose rows the
// chain does not assign is recorded as an observer-side failure (no
// answer), without a connection to the validator.
func TestConfirmer_RefusesARequestTheChainDoesNotBearOut(t *testing.T) {
	fx := newConfirmFixture(t, "127.0.0.1:1")
	for _, c := range []struct {
		name string
		mod  func(*confirmFixture)
		want string
	}{
		{"rows", func(f *confirmFixture) { f.req.AssignedRows = append(append([]int(nil), f.rows...), 7) }, "assignment recomputed from the chain"},
		{"validator", func(f *confirmFixture) { f.req.ValidatorAddress = strings.Repeat("0", 40) }, "not in the set"},
		{"chain", func(f *confirmFixture) { f.req.ChainID = "other-chain" }, "request is for chain other-chain"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := fx
			f.chain = &fakeConfirmChain{chainID: "test-chain", set: fx.chain.set}
			c.mod(&f)
			dir := t.TempDir()
			reqPath := filepath.Join(dir, "requests.jsonl")
			writeRequests(t, reqPath, f.req)
			cf := newTestConfirmer(t, dir, reqPath, f.chain)
			cf.run = func(context.Context, Input, *Coder, StepTimeouts) Measurement {
				t.Fatal("probed a request the chain does not bear out")
				return Measurement{}
			}
			if err := cf.pass(context.Background()); err != nil {
				t.Fatal(err)
			}
			ms, _ := LoadMeasurements(filepath.Join(dir, "measurements.jsonl"))
			if len(ms) != 1 || ms[0].Classification != ClassProbeError || !strings.Contains(ms[0].RawError, c.want) {
				t.Fatalf("rows = %+v, want one PROBE_ERROR saying %q", ms, c.want)
			}
		})
	}
}

// A chain that does not answer is not a refusal: the request stays pending
// and is asked again on the next pass, until its deadline, and nothing is
// written meanwhile.
func TestConfirmer_RetriesWhenTheChainDoesNotAnswer(t *testing.T) {
	fx := newConfirmFixture(t, "203.0.113.9:7980")
	dir := t.TempDir()
	reqPath := filepath.Join(dir, "requests.jsonl")
	writeRequests(t, reqPath, fx.req)
	chain := &fakeConfirmChain{chainID: "test-chain", setErr: errors.New("height 42 is not available")}
	c := newTestConfirmer(t, dir, reqPath, chain)
	probed := 0
	c.run = func(_ context.Context, in Input, _ *Coder, _ StepTimeouts) Measurement {
		probed++
		return Measurement{SchemaVersion: MeasurementSchemaVersion, Vantage: in.Vantage, PromiseHash: in.PromiseHash,
			ValidatorAddress: in.Target.AddressHex, ScheduledAt: in.SchedulePoint.At, StartedAt: time.Now(),
			Outcome: OutcomeServedOK, Classification: ClassHealthy}
	}
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ms, _ := LoadMeasurements(filepath.Join(dir, "measurements.jsonl")); len(ms) != 0 || len(c.pending) != 1 {
		t.Fatalf("after a failed chain read: %d rows, %d pending; want 0 and 1", len(ms), len(c.pending))
	}
	chain.setErr, chain.set = nil, fx.chain.set
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ms, _ := LoadMeasurements(filepath.Join(dir, "measurements.jsonl")); len(ms) != 1 || probed != 1 || len(c.pending) != 0 {
		t.Fatalf("once the chain answers: %d rows, %d probes, %d pending; want 1, 1, 0", len(ms), probed, len(c.pending))
	}
}

// The probe the confirmer runs is built from the chain: the consensus key
// and the rows from the set at the promise height, the host and the
// schedule point from the request, the phase boundaries from the
// primary's tolerance.
func TestConfirmer_InputComesFromTheChainAndTheRequest(t *testing.T) {
	fx := newConfirmFixture(t, "203.0.113.9:7980")
	dir := t.TempDir()
	reqPath := filepath.Join(dir, "requests.jsonl")
	writeRequests(t, reqPath, fx.req)
	c := newTestConfirmer(t, dir, reqPath, fx.chain)
	var got Input
	c.run = func(_ context.Context, in Input, coder *Coder, _ StepTimeouts) Measurement {
		got = in
		if coder == nil {
			t.Error("no coder")
		}
		return Measurement{SchemaVersion: MeasurementSchemaVersion, Vantage: in.Vantage, PromiseHash: in.PromiseHash,
			ValidatorAddress: in.Target.AddressHex, ScheduledAt: in.SchedulePoint.At, StartedAt: time.Now(),
			Outcome: OutcomeServedOK, Classification: ClassHealthy}
	}
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !got.Target.PubKey.Equal(ed25519.PublicKey(fx.chain.set[0].PubKey)) {
		t.Error("consensus key not taken from the validator set")
	}
	if !sameRows(got.Target.AssignedRows, fx.rows) || !got.Target.Assigned || !got.Target.Attested {
		t.Errorf("target = %+v", got.Target)
	}
	if got.Target.Host != fx.req.ValidatorHost || got.Vantage != "de-1" || got.ChainID != "test-chain" ||
		!got.SchedulePoint.At.Equal(fx.req.ScheduledAt) || got.PruneTolerance != 150*time.Second || got.ValidatorSetHeight != 42 {
		t.Errorf("input = %+v", got)
	}
	ms, _ := LoadMeasurements(filepath.Join(dir, "measurements.jsonl"))
	if len(ms) != 1 || ms[0].Classification != ClassHealthy {
		t.Fatalf("rows = %+v", ms)
	}
}

// Requests past their deadline are never probed; the hourly cap holds the
// rest for a later pass; half a line (a copy in progress) waits for its
// newline; a request from this vantage's own name is refused.
func TestConfirmer_DeadlineCapAndPartialLines(t *testing.T) {
	fx := newConfirmFixture(t, "203.0.113.9:7980")
	dir := t.TempDir()
	reqPath := filepath.Join(dir, "requests.jsonl")
	expired := fx.req
	expired.ScheduledAt = expired.ScheduledAt.Add(-time.Hour)
	expired.Deadline = time.Now().Add(-time.Second)
	own := fx.req
	own.ScheduledAt = own.ScheduledAt.Add(-2 * time.Hour)
	own.FromVantage = "de-1"
	a, b := fx.req, fx.req
	b.ScheduledAt = b.ScheduledAt.Add(time.Second)
	writeRequests(t, reqPath, expired, own, a, b)

	c := newTestConfirmer(t, dir, reqPath, fx.chain)
	c.cfg.MaxPerHour = 1
	var probed []time.Time
	c.run = func(_ context.Context, in Input, _ *Coder, _ StepTimeouts) Measurement {
		probed = append(probed, in.SchedulePoint.At)
		return Measurement{SchemaVersion: MeasurementSchemaVersion, Vantage: in.Vantage, PromiseHash: in.PromiseHash,
			ValidatorAddress: in.Target.AddressHex, ScheduledAt: in.SchedulePoint.At, StartedAt: time.Now(),
			Outcome: OutcomeNotFound, Classification: ClassFault}
	}
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(probed) != 1 || len(c.pending) != 1 {
		t.Fatalf("first pass probed %d with %d pending, want 1 and 1 (cap of one an hour)", len(probed), len(c.pending))
	}
	c.cfg.MaxPerHour = 10
	// half a line: the next request is being copied in
	late := fx.req
	late.ScheduledAt = late.ScheduledAt.Add(2 * time.Second)
	line, _ := json.Marshal(late)
	f, _ := os.OpenFile(reqPath, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write(line[:len(line)/2])
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(probed) != 2 {
		t.Fatalf("second pass: %d probed in all, want 2 (the capped one, not the half line)", len(probed))
	}
	f.Write(append(line[len(line)/2:], '\n'))
	f.Close()
	if err := c.pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(probed) != 3 {
		t.Fatalf("third pass: %d probed in all, want 3 once the line is complete", len(probed))
	}
	for _, at := range probed {
		if at.Equal(expired.ScheduledAt) || at.Equal(own.ScheduledAt) {
			t.Errorf("probed %s: an expired or self-addressed request", at)
		}
	}
}
