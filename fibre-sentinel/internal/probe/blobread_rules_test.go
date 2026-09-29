package probe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// fakeThrottled refuses with the rate limit a Fibre server answers under
// load (ResourceExhausted).
func fakeThrottled(context.Context, int, *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
	return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
}

// The client asks every validator the assignment gives rows, endorsing or
// not, in its order: a validator that did not endorse is asked like the
// rest, its verified rows count toward the blob, and its row says it owes
// nothing (UNATTESTED).
func TestTheReadingAsksTheWholeSet(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 2), serve: fakeServes},
		{rows: rowsOf(1, 2), serve: fakeServes},
		{rows: rowsOf(2, 2), serve: fakeServes},
	})
	f.targets[1].Attested = false
	p := readProber(t, f)
	ms := byValidator(readNow(t, p, f.pub))
	other := ms[f.targets[1].AddressHex]
	if len(ms) != 2 || !other.Download.CommitmentVerified || other.Classification != ClassUnattested || other.Read.BlobResult != ReadAvailable {
		t.Fatalf("%d rows; the validator that did not endorse: %s / %s verified=%v read %+v", len(ms), other.Outcome, other.Classification,
			other.Download.CommitmentVerified, other.Read)
	}
	if f.calls[2].Load() != 0 {
		t.Fatal("a third validator was asked after the rows were enough")
	}

	none := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 2), serve: fakeNotFound},
		{rows: rowsOf(1, 2), serve: fakeNotFound},
		{rows: rowsOf(2, 2), serve: fakeNotFound},
	})
	none.targets[1].Attested = false
	p = readProber(t, none)
	rows := readNow(t, p, none.pub)
	if len(rows) != 3 {
		t.Fatalf("%d rows, want every validator asked", len(rows))
	}
	for _, m := range rows {
		if m.Read.BlobResult != ReadUnavailable || m.Read.BlobError != ClientErrNoShards {
			t.Fatalf("%s: read %+v, want Unavailable, no shards retrieved", m.ValidatorAddress, m.Read)
		}
	}
}

// A rate limit, a CANCELLED status the server sends, a timeout: each is
// that validator's rows not coming back, as the client meets it (a failed
// shard, skipped). None of them is this observer's gap, and a blob they
// leave short is Unavailable.
func TestAnAnswerWithoutRowsIsTheValidators(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 2), serve: fakeThrottled},
		{rows: rowsOf(1, 2), serve: fakeCancelled},
		{rows: rowsOf(2, 2), serve: fakeHangs},
		{rows: []int{6}, serve: fakeServes},
	})
	p := readProber(t, f)
	p.cfg.Timeouts.Download = 300 * time.Millisecond
	ms := byValidator(readNow(t, p, f.pub))
	thr, cc, hung := ms[f.targets[0].AddressHex], ms[f.targets[1].AddressHex], ms[f.targets[2].AddressHex]
	if thr.Outcome != OutcomeThrottled || thr.Classification != ClassThrottled || f.calls[0].Load() != 1 {
		t.Fatalf("throttled validator: %s / %s, asked %d times", thr.Outcome, thr.Classification, f.calls[0].Load())
	}
	if cc.Outcome != OutcomeServerError || cc.Classification != ClassServerError {
		t.Fatalf("the CANCELLED validator: %s / %s (%s), want the server's error", cc.Outcome, cc.Classification, cc.RawError)
	}
	if hung.Outcome != OutcomeRPCTimeout || hung.Retry == nil {
		t.Fatalf("the hanging validator: %s, retry %+v", hung.Outcome, hung.Retry)
	}
	for _, m := range ms {
		if m.Read.BlobResult != ReadUnavailable || m.Read.BlobError != ClientErrNotEnoughShards ||
			!OwnAnswer(m.Phase, m.Classification, m.Download.CommitmentVerified) {
			t.Fatalf("%s: %s / %s read %+v, want an answer of its own on an Unavailable reading", m.ValidatorAddress, m.Outcome, m.Classification, m.Read)
		}
	}
}

// Verified rows are rows that came back, whatever class the row is filed
// under while its verdict waits (the late shadow judgement, PROBE_ERROR):
// they count toward the blob and make the reading one that happened.
func TestVerifiedRowsAreAnAnswer(t *testing.T) {
	partial := func(_ context.Context, _ int, honest *fibretypes.BlobShard) (*fibretypes.DownloadShardResponse, error) {
		return &fibretypes.DownloadShardResponse{Shard: &fibretypes.BlobShard{Rlcs: honest.Rlcs, Rows: honest.Rows[:1]}}, nil
	}
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: []int{0, 1, 2, 3}, serve: partial},
		{rows: []int{4}, serve: fakeServes},
		{rows: []int{5}, serve: fakeServes},
	})
	p := readProber(t, f)
	ms := byValidator(readNow(t, p, f.pub))
	m := ms[f.targets[0].AddressHex]
	if !m.Download.CommitmentVerified || m.Read.BlobResult != ReadUnavailable || m.Read.BlobError != ClientErrNotEnoughShards {
		t.Fatalf("partial validator: %s / %s verified=%v read %+v, want Unavailable", m.Outcome, m.Classification, m.Download.CommitmentVerified, m.Read)
	}
	if !OwnAnswer(m.Phase, m.Classification, m.Download.CommitmentVerified) {
		t.Fatal("verified rows are not an answer")
	}
}

// Readings run beside the cycle loop, which refreshes and forgets
// publications and re-reads the scanner's state while the readings look
// up shadowing promises and the scan gaps. Run under -race.
func TestReadingsRunBesideTheCycleLoop(t *testing.T) {
	var vals []fakeVal
	for i := 0; i < 8; i++ {
		vals = append(vals, fakeVal{rows: rowsOf(i, 1), serve: fakeSlow(20 * time.Millisecond)})
	}
	f := newReadFixture(t, 8, 16, vals)
	var copies []*readFixture
	var pubs []scan.Publication
	for i := 0; i < 6; i++ {
		g := *f
		h := make([]byte, 32)
		_, _ = rand.Read(h)
		g.pub.PromiseHash = hex.EncodeToString(h)
		copies = append(copies, &g)
		pubs = append(pubs, g.pub)
	}
	p := readProber(t, copies...)
	path := filepath.Join(p.cfg.DataDir, "publications.jsonl")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	state, _ := json.Marshal(map[string]any{"gaps": []scan.ScanGap{{From: 1, To: 2}}, "last_scanned_height": 9, "last_scanned_time": time.Now().UTC()})
	if err := os.WriteFile(filepath.Join(p.cfg.DataDir, "state.json"), state, 0o644); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // what Prober.Run's loop does every cycle
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			h := make([]byte, 32)
			_, _ = rand.Read(h)
			pub := scan.Publication{PromiseHash: hex.EncodeToString(h)}
			pub.Promise.Commitment = f.pub.Promise.Commitment
			b, _ := json.Marshal(pub)
			fh, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
			if err == nil {
				_, _ = fh.Write(append(b, '\n'))
				fh.Close()
			}
			_, _ = p.feed.refresh()
			_ = p.feed.all()
			_ = p.feed.size()
			p.feed.forget(pub.PromiseHash)
			p.loadGaps()
			p.pollScanned(context.Background())
		}
	}()
	ms := readNow(t, p, pubs...)
	close(stop)
	wg.Wait()
	if len(ms) != 6*8 {
		t.Fatalf("%d rows, want every validator of every blob", len(ms))
	}
}
