package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/slim"
)

// A record that does not decode is that record's error, whatever the reason: a read gets ErrUndecodable for it. A
// slim body cut short, or one that starts with a tag the format does not have, never decodes; a decoding that panics
// (a record of a shape the decoder does not expect) is the same, not the process ending. A record that would be
// stored in a form that does not read back keeps its line: TestARecordTheSlimFormWouldNotGiveBackKeepsItsLine.
func TestARecordThatDoesNotDecodeIsUndecodable(t *testing.T) {
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
	line := []byte(`{"promise_hash":"ab","note":"a record"}`)
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
	for _, c := range []struct {
		name string
		body []byte
	}{
		{"a body cut short", body[:len(body)-1]},
		{"a body with a tag the format does not have", append([]byte{0xff}, body[1:]...)},
	} {
		// the decoder itself refuses the bytes, with no panic to recover
		if _, _, err := tb.DecodePublication(c.body); err == nil {
			t.Errorf("%s decodes", c.name)
		}
		if _, err := st.Record(ctx, st.db, c.body); !errors.Is(err, ErrUndecodable) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	err = st.decodeRetry(ctx, st.db, func(*slim.Tables) error { panic("a record of a shape the decoder does not expect") })
	if !errors.Is(err, ErrUndecodable) {
		t.Fatalf("a decoding that panics: %v", err)
	}
}
