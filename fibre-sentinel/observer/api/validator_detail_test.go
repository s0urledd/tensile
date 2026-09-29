package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A hold that lands while a validator page is being computed withdraws
// verdicts that computation may already have read. The reader who asked
// before the hold can have that answer; one who arrives after it must get a
// fresh one, and the answer from before must not be kept for the next.
func TestAReaderAfterAHoldDoesNotShareTheComputationStartedBeforeIt(t *testing.T) {
	s := newSnapshotServer(t)
	addr := strings.Repeat("a1", 20)
	now := time.Now().UTC().Format(time.RFC3339)
	// on record as a bonded validator is, with nothing measured yet
	if _, err := s.st.DB().Exec(`INSERT INTO validator_identities (cons_address, status, first_seen_at, updated_at) VALUES (?, 'BOND_STATUS_BONDED', ?, ?)`, addr, now, now); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	s.details.compute = func(ctx context.Context, _ string, _ Window, _ time.Time) (int, any, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release // the rows it read still carried the fault
			return 200, map[string]int{"faults": 2}, nil
		}
		return 200, map[string]int{"faults": 0}, nil
	}
	win := testWindow("24h")
	read := func() <-chan int {
		out := make(chan int, 1)
		go func() {
			w := httptest.NewRecorder()
			s.serveValidatorDetail(w, httptest.NewRequest("GET", "/v1/validators/"+addr, nil), addr, win, time.Now())
			var d struct{ Faults int }
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &d) != nil {
				out <- -1
				return
			}
			out <- d.Faults
		}()
		return out
	}

	first := read()
	<-started
	if err := s.st.BumpParamHoldsRev(time.Now()); err != nil { // the hold lands
		t.Fatal(err)
	}
	second := read()
	select {
	case f := <-second:
		if f != 0 {
			t.Fatalf("the reader after the hold got faults=%d", f)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatalf("the reader after the hold waited on the computation started before it, and got faults=%d", <-second)
	}
	close(release)
	if f := <-first; f != 2 {
		t.Fatalf("the reader before the hold: faults=%d", f)
	}
	s.bg.Wait()
	if f := <-read(); f != 0 || calls.Load() != 2 {
		t.Fatalf("after both finished: faults=%d after %d computations; the answer from before the hold must not replace the one after it", f, calls.Load())
	}
}
