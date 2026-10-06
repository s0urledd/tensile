package scan

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
)

// A publications or payments file whose older lines were archived loads exactly as it did before: the archived
// segments are read before the live file, so the whole history comes back, in order, line for line.
func TestLoadersReadTheArchivedRecord(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	var pubLines, payLines []byte
	for i := 0; i < 12; i++ {
		at := t0.Add(time.Duration(i) * 24 * time.Hour)
		p := Publication{SchemaVersion: 3, PromiseHash: fmt.Sprintf("%064x", i+1), SettlementHeight: int64(100 + i), SettlementTime: at, RecordedAt: at}
		l, _ := json.Marshal(p)
		pubLines = append(append(pubLines, l...), '\n')
		y := Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("pay-%d", i), Kind: "settlement", Height: int64(100 + i), Time: at, Publisher: "celestia1p", Denom: "utia"}
		l, _ = json.Marshal(y)
		payLines = append(append(payLines, l...), '\n')
	}
	pubs, pays := filepath.Join(dir, "publications.jsonl"), filepath.Join(dir, "payments.jsonl")
	if err := os.WriteFile(pubs, pubLines, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pays, payLines, 0o644); err != nil {
		t.Fatal(err)
	}
	wantPubs, err := LoadPublications(pubs)
	if err != nil || len(wantPubs) != 12 {
		t.Fatalf("before archiving: %d publications, err %v", len(wantPubs), err)
	}
	wantPays, err := LoadPayments(pays)
	if err != nil || len(wantPays) != 12 {
		t.Fatalf("before archiving: %d payments, err %v", len(wantPays), err)
	}

	cutoff := t0.Add(7 * 24 * time.Hour)
	for _, f := range []struct{ path, field string }{{pubs, "settlement_time"}, {pays, "time"}} {
		res, err := record.Archive(f.path, record.Options{Cutoff: cutoff, TimeField: f.field, Limit: -1})
		if errors.Is(err, record.ErrUnsupported) {
			t.Skip("rotation needs flock")
		}
		if err != nil || res.Lines != 7 {
			t.Fatalf("%s: archived %+v, err %v", filepath.Base(f.path), res, err)
		}
	}

	gotPubs, err := LoadPublications(pubs)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotPubs, wantPubs) {
		t.Fatalf("after archiving, publications read %d of %d, or not the same", len(gotPubs), len(wantPubs))
	}
	gotPays, err := LoadPayments(pays)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotPays, wantPays) {
		t.Fatalf("after archiving, payments read %d of %d, or not the same", len(gotPays), len(wantPays))
	}
}
