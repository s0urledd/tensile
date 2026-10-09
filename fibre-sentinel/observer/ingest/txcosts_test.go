package ingest_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
)

// txCost is a successful MsgPayForFibre's cost line as sentinel-scan writes it.
func txCost(height int64, txIndex int) txcost.Record {
	at := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC).Add(time.Duration(height) * time.Second)
	return txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(height, txIndex), Height: height, TxIndex: txIndex, Time: at,
		TxHash: strings.Repeat("0b", 31) + fmt.Sprintf("%02x", height&0xff), GasWanted: 400000, GasUsed: 219118, Fee: "8000utia",
		FeePayer: "celestia1pub", Messages: []failedtx.Msg{{Index: 0, TypeURL: "/celestia.fibre.v1.MsgPayForFibre", Signer: "celestia1pub"}},
		RecordedAt: at.Add(time.Second)}
}

// A line that is not a cost line the pages can use is a bad record: stepped
// over and counted, the cursor past it, the pass going on to the next line.
// A key that is not the line's own height and index is one: the table is
// keyed by the position and a restore counts the keys. The good line is
// stored once.
func TestATxCostLineThatCannotBeUsedIsSkipped(t *testing.T) {
	st := openStore(t)
	good := txCost(12, 3)
	noKey, badHash, noTime, noHeight, badIndex, otherKey := good, good, good, good, good, good
	noKey.DedupeKey = ""
	badHash.TxHash = "abcd"
	noTime.Time = time.Time{}
	noHeight.Height = 0
	badIndex.TxIndex = -1
	otherKey.DedupeKey = "h12:4"
	for name, line := range map[string]string{
		"undecodable":               `{"dedupe_key":"h12:3","tx_hash":` + "\n",
		"no key":                    jsonLine(t, noKey),
		"a bad hash":                jsonLine(t, badHash),
		"a zero time":               jsonLine(t, noTime),
		"no height":                 jsonLine(t, noHeight),
		"a bad tx index":            jsonLine(t, badIndex),
		"a key of another position": jsonLine(t, otherKey),
	} {
		path := filepath.Join(t.TempDir(), txcost.FileName)
		if err := os.WriteFile(path, []byte(line+jsonLine(t, good)), 0o644); err != nil {
			t.Fatal(err)
		}
		r, err := ingest.TxCosts(st, path, time.Now())
		if err != nil {
			t.Fatalf("%s: a bad line stopped the pass: %v", name, err)
		}
		if r.Skipped != 1 || r.Line != 2 || !strings.Contains(r.LastSkipped, "line 1: bad record") {
			t.Fatalf("%s: %+v", name, r)
		}
		if r, err := ingest.TxCosts(st, path, time.Now()); err != nil || r.Read != 0 {
			t.Fatalf("%s: the cursor did not move past the bad line: %+v %v", name, r, err)
		}
	}
	var n int
	var key string
	if err := st.DB().QueryRow(`SELECT COUNT(*), MAX(dedupe_key) FROM tx_costs`).Scan(&n, &key); err != nil || n != 1 || key != "h12:3" {
		t.Fatalf("%d rows (%s), want the good line once: %v", n, key, err)
	}
}

// No file yet (the scanner writes it at its first Fibre success): nothing is
// read and no cursor is written, so the health check has no row to watch.
func TestAMissingTxCostsFileWritesNoCursor(t *testing.T) {
	st := openStore(t)
	path := filepath.Join(t.TempDir(), txcost.FileName)
	r, err := ingest.TxCosts(st, path, time.Now())
	if err != nil || r.Read != 0 || r.Offset != 0 {
		t.Fatalf("%+v %v", r, err)
	}
	var n int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM ingest_cursors`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d cursor row(s) for a file that is not there: %v", n, err)
	}
}

// A re-ingest from the first byte (a rebuild, or a cursor reset) reads every
// line again and inserts nothing new: one row per position, the first line's.
func TestReIngestingTxCostsInsertsNothingNew(t *testing.T) {
	st := openStore(t)
	path := filepath.Join(t.TempDir(), txcost.FileName)
	var body string
	for i := 0; i < 3; i++ {
		body += jsonLine(t, txCost(200+int64(i), i))
	}
	// a re-scan wrote the first one again, later
	again := txCost(200, 0)
	again.RecordedAt = again.RecordedAt.Add(time.Hour)
	body += jsonLine(t, again)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	r, err := ingest.TxCosts(st, path, now)
	if err != nil || r.Inserted != 3 || r.Read != 4 || r.Skipped != 0 {
		t.Fatalf("first pass: %+v %v", r, err)
	}
	if err := st.SetCursor(path, 0, 0, now); err != nil {
		t.Fatal(err)
	}
	r, err = ingest.TxCosts(st, path, now)
	if err != nil || r.Inserted != 0 || r.Read != 4 {
		t.Fatalf("the re-ingest from 0: %+v %v", r, err)
	}
	var raw string
	if err := st.DB().QueryRow(`SELECT raw_json FROM tx_costs WHERE height = 200 AND tx_index = 0`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw+"\n" != jsonLine(t, txCost(200, 0)) {
		t.Fatalf("the row is not the first line:\n%s", raw)
	}
}
