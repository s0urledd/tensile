package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// addFailedTxs gives the fixture a failed transaction a day in
// failed_txs.jsonl and in the store, as the scanner and the collector leave
// them, and builds its exports again as newRetireData built them, each now
// carrying the day's failure. It returns the file's bytes.
func addFailedTxs(t *testing.T, d *retireData) []byte {
	t.Helper()
	st, err := store.Open(d.db)
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	for day := 0; day < 4; day++ {
		at := day0.AddDate(0, 0, day).Add(13 * time.Hour)
		h := int64(2000 + day)
		r := failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(h, 0), Height: h, Time: at, AppVersion: 10,
			TxHash: strings.Repeat(fmt.Sprintf("%02x", 0xf0+day), 32), Code: 5, Codespace: "sdk",
			Log: "failed to execute message; message index: 0: insufficient funds", GasWanted: 200000, GasUsed: 90000, AntePassed: true, Fee: "2000utia",
			Messages: []failedtx.Msg{{Index: 0, TypeURL: "/celestia.fibre.v1.MsgDepositToEscrow", Signer: "celestia1pub",
				Detail: &failedtx.MsgDetail{Publisher: "celestia1pub", Amount: "1000utia"}}},
			RecordedAt: at.Add(time.Second)}
		l, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := st.InsertFailedTx(r, l); err != nil || !ok {
			t.Fatalf("failed tx: %v %v", ok, err)
		}
		body = append(append(body, l...), '\n')
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d.dir, failedtx.FileName), body, 0o644); err != nil {
		t.Fatal(err)
	}
	d.files[failedtx.FileName] = body
	// newRetireData's exports, built again: the scanner's start was recorded after them
	for _, p := range []string{d.expDir(), filepath.Join(d.dir, status.RunsFile)} {
		if err := os.RemoveAll(p); err != nil {
			t.Fatal(err)
		}
	}
	d.exports = nil
	b := &export.Builder{DataDir: d.dir, Dir: d.expDir(), Vantage: "ut-1", Build: "t", Hour: 3}
	for day := 1; day <= 4; day++ {
		built, err := b.Run(day0.AddDate(0, 0, day).Add(4 * time.Hour))
		if err != nil || len(built) != 1 {
			t.Fatalf("export of day %d: %v %v", day-1, built, err)
		}
		d.exports = append(d.exports, built[0])
	}
	d.remoteOK(t, 0)
	d.remoteOK(t, 1)
	d.copied(t, copiedAt, remoteFP)
	scannerStarted(t, d.dir, true)
	return body
}

// failedTxsMember is the day's failed_txs.jsonl member as its export's index
// entry lists it.
func failedTxsMember(t *testing.T, d *retireData, day int) export.Member {
	t.Helper()
	for _, m := range d.entry(t, day).Files {
		if m.Name == failedtx.FileName {
			return m
		}
	}
	t.Fatalf("day %d: no %s in the export", day, failedtx.FileName)
	return export.Member{}
}

// Failed transactions change nothing -retire decides: the fixture with a
// failure a day, in every export and in the store, has the same segments
// retired and kept, for the same reasons, the same days checked and the same
// bytes freed as the fixture without them. failed_txs.jsonl is in no
// archive and no report, and reads back as it was written.
func TestFailedTxsChangeNoRetirement(t *testing.T) {
	retire := retireByHand
	if rotates(t) {
		retire = record.Retire
	}
	retireSegment = retire
	t.Cleanup(func() { retireSegment = record.Retire })
	run := func(failed bool) (*retireData, retirement) {
		t.Helper()
		d := newRetireData(t)
		if failed {
			addFailedTxs(t, d)
			if m := failedTxsMember(t, d, 1); m.Lines != 1 || m.TimeField != "time" {
				t.Fatalf("day 1's export: %s member %+v", failedtx.FileName, m)
			}
		}
		specs, err := archived(d.dir)
		if err != nil {
			t.Fatal(err)
		}
		for day := 1; day <= 3; day++ {
			for _, f := range specs {
				archiveByHand(t, filePath(d.dir, f.Name), f.TimeField, day0.AddDate(0, 0, day))
			}
		}
		d.copied(t, "2026-10-05T04:45:00Z", remoteFP)
		r := d.retire(t, retireAt)
		d.readsWhole(t)
		return d, r
	}
	_, base := run(false)
	d, with := run(true)

	if base.code != 0 || base.rep.RetiredNow == 0 || base.rep.Kept == 0 {
		t.Fatalf("the baseline retires or keeps nothing: %d %+v\n%s%s", base.code, base.rep, base.out, base.errs)
	}
	if with.code != base.code || with.errs != base.errs {
		t.Fatalf("exit %d %q with failed transactions, %d %q without", with.code, with.errs, base.code, base.errs)
	}
	if !reflect.DeepEqual(with.rep.Checked, base.rep.Checked) || with.rep.RetiredNow != base.rep.RetiredNow || with.rep.BytesFreed != base.rep.BytesFreed ||
		with.rep.Kept != base.rep.Kept || with.rep.Errors != base.rep.Errors {
		t.Fatalf("with failed transactions: %+v\nwithout: %+v", with.rep, base.rep)
	}
	if !reflect.DeepEqual(with.rep.Files, base.rep.Files) {
		t.Fatalf("the files' reports differ:\n%+v\n%+v", with.rep.Files, base.rep.Files)
	}
	if fmt.Sprint(with.kept()) != fmt.Sprint(base.kept()) {
		t.Fatalf("kept for other reasons:\n%v\n%v", with.kept(), base.kept())
	}
	for _, f := range with.rep.Files {
		if f.Name == failedtx.FileName {
			t.Fatalf("failed_txs.jsonl is in the report: %+v", f)
		}
	}
	if _, err := os.Stat(record.ArchiveDir(filePath(d.dir, failedtx.FileName))); !os.IsNotExist(err) {
		t.Fatalf("failed_txs.jsonl has an archive: %v", err)
	}
	if got, err := os.ReadFile(filePath(d.dir, failedtx.FileName)); err != nil || !bytes.Equal(got, d.files[failedtx.FileName]) {
		t.Fatalf("failed_txs.jsonl changed: %v", err)
	}
}

// failed_txs.jsonl is not a file this command archives, so it is never
// rotated, retired or verified as an archive: named with -files it is
// refused in every mode, and an archive run over a data dir that holds it
// leaves it as it is.
func TestFailedTxsAreNeverArchived(t *testing.T) {
	for _, f := range Files {
		if f.Name == failedtx.FileName {
			t.Fatalf("%s is an archived file", failedtx.FileName)
		}
	}
	d := newRetireData(t)
	body := addFailedTxs(t, d)
	for _, mode := range [][]string{nil, {"-dry-run"}, {"-retire"}, {"-status"}, {"-verify"}} {
		args := append([]string{"-data-dir", d.dir, "-files", failedtx.FileName}, mode...)
		code, out, errs := runAt(t, retireAt, args...)
		if code != 2 || !strings.Contains(errs, `"failed_txs.jsonl" is not an archived file`) {
			t.Fatalf("observer-archive %v: %d\n%s%s", args, code, out, errs)
		}
	}
	if !rotates(t) {
		return // the archive run needs flock; TestFailedTxsChangeNoRetirement archives by hand
	}
	code, out, errs := runAt(t, day0.AddDate(0, 0, 3).Add(4*time.Hour+40*time.Minute), "-data-dir", d.dir, "-keep", "24h")
	if code != 0 || errs != "" || strings.Contains(out, failedtx.FileName) {
		t.Fatalf("archive: %d\n%s%s", code, out, errs)
	}
	if got, err := os.ReadFile(filePath(d.dir, failedtx.FileName)); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("failed_txs.jsonl changed: %v", err)
	}
	if _, err := os.Stat(record.ArchiveDir(filePath(d.dir, failedtx.FileName))); !os.IsNotExist(err) {
		t.Fatalf("failed_txs.jsonl has an archive: %v", err)
	}
}
