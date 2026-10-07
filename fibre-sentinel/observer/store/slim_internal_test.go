package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// A decoder that panics on a record (today: a slim publication without an
// assignment) is that record not decoding, not the process ending: a read
// gets ErrUndecodable for it, and a record that would be stored in that form
// keeps its line instead.
func TestARecordTheDecoderPanicsOnIsUndecodable(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	tb, err := st.tables(ctx, st.db, false)
	if err != nil {
		t.Fatal(err)
	}
	line := []byte(`{"promise_hash":"ab","note":"no assignment"}`)
	body, _, err := tb.EncodePublication(line)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := st.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := keepEntries(tx, tb.Pending()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Record(ctx, st.db, body); !errors.Is(err, ErrUndecodable) {
		t.Fatalf("a body the decoder panics on: %v", err)
	}
	if got, _, err := publicationBody(tb, line, tb.Stored()); err != nil || got != string(line) {
		t.Fatalf("stored as %v (%v), want its line", got, err)
	}
}
