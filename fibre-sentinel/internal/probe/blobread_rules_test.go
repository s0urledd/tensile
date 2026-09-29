package probe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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
// nothing (UNATTESTED). Only an endorsing validator's row is sent for
// confirmation on an Unavailable blob.
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
		if m.Read.BlobResult != ReadUnavailable || m.Read.Pass != 2 {
			t.Fatalf("%s: read %+v, want Unavailable after the second pass", m.ValidatorAddress, m.Read)
		}
	}
	reqs := readRequests(t, ConfirmRequestsPath(p.cfg.DataDir))
	if len(reqs) != 2 {
		t.Fatalf("%d confirmation requests, want one per endorsing validator", len(reqs))
	}
	for _, r := range reqs {
		if r.ValidatorAddress == none.targets[1].AddressHex {
			t.Fatal("a validator that did not endorse was sent for confirmation")
		}
	}
}

// A rate limit at the reading is the validator not serving, as the client
// meets it: no rows, not counted among the rows that might still come back,
// so the blob it leaves short is Unavailable, and the row is sent for
// confirmation like any other not-served row.
func TestAThrottleAtTheReadingIsNotServed(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{
		{rows: rowsOf(0, 2), serve: fakeNotFound},
		{rows: rowsOf(1, 2), serve: fakeThrottled},
		{rows: []int{4}, serve: fakeServes},
	})
	p := readProber(t, f)
	ms := byValidator(readNow(t, p, f.pub))
	thr := ms[f.targets[1].AddressHex]
	if thr.Outcome != OutcomeThrottled || thr.Classification != ClassThrottled || thr.Read.BlobResult != ReadUnavailable || f.calls[1].Load() != 2 {
		t.Fatalf("throttled validator: %s / %s read %+v, asked %d times", thr.Outcome, thr.Classification, thr.Read, f.calls[1].Load())
	}
	if got := readRequests(t, ConfirmRequestsPath(p.cfg.DataDir)); len(got) != 2 {
		t.Fatalf("%d confirmation requests, want the not-found and the throttled validator", len(got))
	}
}

// A second pass cut short by the deadline, before it asked again every
// validator that had not served, is incomplete: the first pass's answers of
// the ones it did not get to again are this observer's gap, never
// Unavailable on their word. Here the two big holders hang in the first
// pass; in the second only the first of them is asked before the cutoff,
// and the second, which would have served, is never asked again.
func TestACutShortSecondPassIsIncomplete(t *testing.T) {
	vals := []fakeVal{
		{rows: rowsOf(0, 10), serve: fakeHangs},
		{rows: rowsOf(1, 10), serve: firstThen(2, fakeHangs, fakeServes)},
	}
	for i := 0; i < 6; i++ {
		vals = append(vals, fakeVal{rows: []int{20 + 2*i, 21 + 2*i}, serve: fakeServes})
	}
	f := newReadFixture(t, 16, 32, vals)
	f.pub.MustServeUntil = time.Now().Add(10 * time.Second)
	p := readProber(t, f)
	p.cfg.Timeouts.Download = 300 * time.Millisecond
	p.cfg.Schedule.RetryDeadline = 7 * time.Second         // the second pass starts by now+3s
	p.cfg.Schedule.RequestCutoff = 8900 * time.Millisecond // no request starts after now+1.1s
	ms := byValidator(readNow(t, p, f.pub))
	if len(ms) != 8 {
		t.Fatalf("%d rows, want 8", len(ms))
	}
	for i, tg := range f.targets {
		m := ms[tg.AddressHex]
		if m.Read.BlobResult != ReadIncomplete {
			t.Fatalf("validator %d: %s / %s read %+v, want incomplete", i, m.Outcome, m.Classification, m.Read)
		}
		if i < 2 && (m.Classification != ClassProbeError || !strings.Contains(m.ClassificationReason, "second pass")) {
			t.Fatalf("validator %d: %s (%s), want this observer's gap", i, m.Classification, m.ClassificationReason)
		}
	}
	if n := f.calls[1].Load(); n != 2 {
		t.Fatalf("the second big holder was asked %d times, want only in the first pass (twice, with the re-dial)", n)
	}
	if _, err := os.Stat(ConfirmRequestsPath(p.cfg.DataDir)); err == nil {
		if got := readRequests(t, ConfirmRequestsPath(p.cfg.DataDir)); len(got) != 0 {
			t.Fatalf("%d confirmation requests for an incomplete reading", len(got))
		}
	}
}

// The client's one re-dial belongs to the request it follows: RequestCutoff
// leaves room for it, so a request started before the cutoff is re-dialled
// even when its first attempt ends after it.
func TestTheRedialRunsPastTheCutoff(t *testing.T) {
	f := newReadFixture(t, 4, 8, []fakeVal{{rows: []int{0, 1, 2, 3}, serve: firstThen(1, fakeHangs, fakeServes)}})
	f.pub.MustServeUntil = time.Now().Add(10 * time.Second)
	p := readProber(t, f)
	p.cfg.Timeouts.Download = 600 * time.Millisecond
	p.cfg.Schedule.RetryDeadline = 9 * time.Second
	p.cfg.Schedule.RequestCutoff = 9700 * time.Millisecond // the cutoff falls inside the first attempt
	ms := readNow(t, p, f.pub)
	if len(ms) != 1 || !ms[0].Download.CommitmentVerified || !ms[0].Read.Redialed || ms[0].Read.BlobResult != ReadAvailable {
		t.Fatalf("rows %d: %s read %+v", len(ms), ms[0].Outcome, ms[0].Read)
	}
}

// The prober records Unavailable on the verdict's own test (OwnAnswer): a
// validator whose verified rows are in hand, though its verdict waits for
// the late shadow judgement (PROBE_ERROR), has answered, so it holds no
// blob open.
func TestVerifiedRowsHoldNoBlobOpen(t *testing.T) {
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
	if !m.Download.CommitmentVerified || m.Read.BlobResult != ReadUnavailable {
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
