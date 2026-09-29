package api

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// On a pinned window "held now" is held at the pin: a promise settled after
// the pin is not counted, however long its retention runs, and neither is
// the assignment on a promise newer than the pin. Live, both are.
func TestHeldNowIsHeldAtThePin(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	t0 := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	pubs := []fxPub{
		{hash: "aa", height: 100, at: t0, msu: t0.Add(10 * 24 * time.Hour), size: 4096 * 4, orig: 4},
		{hash: "bb", height: 200, at: t0.Add(3 * 24 * time.Hour), msu: t0.Add(13 * 24 * time.Hour), size: 4096 * 8, orig: 4},
	}
	fxInsert(t, st.DB(), pubs, []fxAsg{
		{hash: "aa", val: "v1", rows: 1, attested: 1, host: "h:1"},
		{hash: "bb", val: "v1", rows: 2, attested: 1, host: "h:1"},
	})
	s := &Server{st: st}
	ctx := context.Background()
	now := t0.Add(5 * 24 * time.Hour)

	pin := t0.Add(24 * time.Hour)
	pinned := Window{Name: "24h", Span: 24 * time.Hour, Start: pin.Add(-24 * time.Hour), End: pin, AsOf: true}
	got, err := s.loadByValidatorAt(ctx, pinned, "", heldAt(pinned, now))
	if err != nil {
		t.Fatal(err)
	}
	// aa: one row of 4096 bytes; bb settled two days after the pin.
	if l := got["v1"]; l.StoredBytes != 4096 || l.RowsPerBlob != 1 {
		t.Fatalf("at the pin: stored %d, rows per blob %d; want 4096 and 1 (the promise settled after the pin left out)", l.StoredBytes, l.RowsPerBlob)
	}

	live := windowFor("24h", now)
	got, err = s.loadByValidatorAt(ctx, live, "", heldAt(live, now))
	if err != nil {
		t.Fatal(err)
	}
	// aa: 4096; bb: two rows of 8192.
	if l := got["v1"]; l.StoredBytes != 4096+2*8192 || l.RowsPerBlob != 2 {
		t.Fatalf("live: stored %d, rows per blob %d; want %d and 2", l.StoredBytes, l.RowsPerBlob, 4096+2*8192)
	}
}
