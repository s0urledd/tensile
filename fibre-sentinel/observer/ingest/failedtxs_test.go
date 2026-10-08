package ingest_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
)

// failedTx is a failed MsgSetFibreProviderInfo as sentinel-scan writes it.
func failedTx(height int64, txIndex int) failedtx.Record {
	at := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC).Add(time.Duration(height) * time.Second)
	return failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(height, txIndex), Height: height, Time: at,
		AppVersion: 10, TxHash: strings.Repeat("0a", 31) + fmt.Sprintf("%02x", height&0xff), TxIndex: txIndex, Code: 2, Codespace: "valaddr",
		Log: "failed to execute message; message index: 0: validator not found: invalid validator", GasWanted: 100000, GasUsed: 40000,
		AntePassed: true, Fee: "1500utia",
		Messages: []failedtx.Msg{{Index: 0, TypeURL: "/celestia.valaddr.v1.MsgSetFibreProviderInfo", Signer: "celestiavaloper1x",
			Detail: &failedtx.MsgDetail{Host: "fibre.example:7980", Validator: "celestiavaloper1x"}}},
		RecordedAt: at.Add(time.Second)}
}

func jsonLine(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b) + "\n"
}

// A line that is not a failed transaction the lookup can use is a bad
// record: stepped over and counted, the cursor past it, the pass going on to
// the next line. The one that is stored is stored once.
func TestAFailedTxLineThatCannotBeUsedIsSkipped(t *testing.T) {
	st := openStore(t)
	good := failedTx(100, 1)
	noCode, noKey, badHash, noTime, noHeight, badIndex := good, good, good, good, good, good
	noCode.Code = 0
	noKey.DedupeKey = ""
	badHash.TxHash = "abcd"
	noTime.Time = time.Time{}
	noHeight.Height = 0
	badIndex.TxIndex = -1
	for name, line := range map[string]string{
		"undecodable":    `{"dedupe_key":"h100:1","tx_hash":` + "\n",
		"code 0":         jsonLine(t, noCode),
		"no key":         jsonLine(t, noKey),
		"a bad hash":     jsonLine(t, badHash),
		"a zero time":    jsonLine(t, noTime),
		"no height":      jsonLine(t, noHeight),
		"a bad tx index": jsonLine(t, badIndex),
	} {
		path := filepath.Join(t.TempDir(), failedtx.FileName)
		if err := os.WriteFile(path, []byte(line+jsonLine(t, good)), 0o644); err != nil {
			t.Fatal(err)
		}
		r, err := ingest.FailedTxs(st, path, time.Now())
		if err != nil {
			t.Fatalf("%s: a bad line stopped the pass: %v", name, err)
		}
		if r.Skipped != 1 || r.Line != 2 || !strings.Contains(r.LastSkipped, "line 1: bad record") {
			t.Fatalf("%s: %+v", name, r)
		}
		if r, err := ingest.FailedTxs(st, path, time.Now()); err != nil || r.Read != 0 {
			t.Fatalf("%s: the cursor did not move past the bad line: %+v %v", name, r, err)
		}
	}
	var n int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM failed_txs`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("%d rows, want the good line once: %v", n, err)
	}
}

// No file yet (the scanner writes it at its first failure): nothing is read
// and no cursor is written, so the health check has no row to watch.
func TestAMissingFailedTxsFileWritesNoCursor(t *testing.T) {
	st := openStore(t)
	path := filepath.Join(t.TempDir(), failedtx.FileName)
	r, err := ingest.FailedTxs(st, path, time.Now())
	if err != nil || r.Read != 0 || r.Offset != 0 {
		t.Fatalf("%+v %v", r, err)
	}
	var n int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM ingest_cursors`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d cursor row(s) for a file that is not there: %v", n, err)
	}
}

// A re-ingest from the first byte (a rebuild, or a cursor reset) reads every
// line again and inserts nothing new: one row per key, the first line's.
func TestReIngestingFailedTxsInsertsNothingNew(t *testing.T) {
	st := openStore(t)
	path := filepath.Join(t.TempDir(), failedtx.FileName)
	var body string
	for i := 0; i < 3; i++ {
		body += jsonLine(t, failedTx(200+int64(i), i))
	}
	// a re-scan wrote the first one again, later
	again := failedTx(200, 0)
	again.RecordedAt = again.RecordedAt.Add(time.Hour)
	body += jsonLine(t, again)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	r, err := ingest.FailedTxs(st, path, now)
	if err != nil || r.Inserted != 3 || r.Read != 4 || r.Skipped != 0 {
		t.Fatalf("first pass: %+v %v", r, err)
	}
	if err := st.SetCursor(path, 0, 0, now); err != nil {
		t.Fatal(err)
	}
	r, err = ingest.FailedTxs(st, path, now)
	if err != nil || r.Inserted != 0 || r.Read != 4 {
		t.Fatalf("the re-ingest from 0: %+v %v", r, err)
	}
	var raw string
	if err := st.DB().QueryRow(`SELECT raw_json FROM failed_txs WHERE dedupe_key = ?`, failedtx.Key(200, 0)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw+"\n" != jsonLine(t, failedTx(200, 0)) {
		t.Fatalf("the row is not the first line:\n%s", raw)
	}
}
