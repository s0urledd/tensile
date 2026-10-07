package store_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// backfillSummary is what TestSlimBackfillOfARealStore writes to TENSILE_BACKFILL_OUT.
type backfillSummary struct {
	DB string `json:"db"`
	// the store file and its write-ahead log: as given, once migrated to this build's schema, and once converted with
	// every page freed given back
	SizeBefore         int64 `json:"size_before"`
	SizeAfterMigration int64 `json:"size_after_migration"`
	SizeAfter          int64 `json:"size_after"`
	AutoVacuum         int64 `json:"auto_vacuum"`
	PageSize           int64 `json:"page_size"`
	// FreedByBatches is the bytes the batches gave back as they went (2,000 pages at most after each, as in the
	// collector); FreePagesLeft the pages still free when the last batch committed, all given back after
	FreedByBatches int64 `json:"freed_by_batches"`
	FreePagesLeft  int64 `json:"free_pages_left"`
	Batches        int   `json:"batches"`
	// Checked is the converted values read back, each as soon as its batch committed; Mismatches those that did not
	// give back exactly the value they replaced
	Checked         int64                  `json:"checked"`
	Mismatches      int64                  `json:"mismatches"`
	FirstMismatches []string               `json:"first_mismatches,omitempty"`
	MigrateS        float64                `json:"migrate_s"`
	BackfillS       float64                `json:"backfill_s"`
	VacuumS         float64                `json:"vacuum_s"`
	Tables          []backfillTableSummary `json:"tables"`
}

type backfillTableSummary struct {
	Table     string                           `json:"table"`
	RowsSeen  int64                            `json:"rows_seen"`
	LastRowid int64                            `json:"last_rowid"`
	Columns   map[string]backfillColumnSummary `json:"columns"`
}

type backfillColumnSummary struct {
	Converted int64            `json:"converted"`
	KeptAsIs  int64            `json:"kept_as_is"`
	Reasons   map[string]int64 `json:"kept_why,omitempty"`
	Examples  []string         `json:"kept_first,omitempty"`
}

// readBack is a converted value as a reader reads it now: the row's column read again, through rd, and decoded or
// resolved as every reader does. converted is false when the column does not hold a new form.
func readBack(ctx context.Context, rd *store.Store, v store.BackfillValue) (got []byte, converted bool, err error) {
	var col sql.NullString
	var raw []byte
	q := `SELECT ` + v.Column + ` FROM ` + v.Table + ` WHERE rowid = ?`
	if v.Column == "raw_json" {
		err = rd.DB().QueryRowContext(ctx, q, v.Rowid).Scan(&raw)
	} else {
		err = rd.DB().QueryRowContext(ctx, q, v.Rowid).Scan(&col)
	}
	if err != nil {
		return nil, false, err
	}
	switch v.Table + "." + v.Column {
	case "publications.raw_json":
		got, err = rd.Record(ctx, rd.DB(), raw)
	case "probes.raw_json":
		got, err = rd.ProbeRecord(ctx, rd.DB(), v.Hash, raw)
	case "reachability.raw_json":
		got, err = rd.ReachRecord(ctx, rd.DB(), raw)
	case "assignments.rows_json":
		var ns sql.NullString
		ns, err = rd.AssignedRows(ctx, rd.DB(), v.Hash, v.Validator, col)
		got = []byte(ns.String)
	case "probes.row_indices":
		var s string
		s, err = rd.RowIndices(ctx, rd.DB(), v.Hash, v.Validator, col.String)
		got = []byte(s)
	default:
		return nil, false, fmt.Errorf("no reader for %s.%s", v.Table, v.Column)
	}
	if v.Column == "raw_json" {
		converted = len(raw) > 0 && raw[0] != '{'
	} else {
		converted = col.Valid && col.String == store.RowsAssigned
	}
	return got, converted, err
}

// TestSlimBackfillOfARealStore converts in place a store a build before the slim record wrote from the real record
// (TENSILE_BACKFILL_DB; skipped without it), as the collector does but at once: no budget, no pacing. It is opened as
// the collector opens it, which migrates it to this build's schema. Every value converted is read back as soon as its
// batch commits, by a reader that loads the slim tables from the store as the API does, and must give back exactly the
// value it replaced: every one, not a sample. The pages freed then go back to the filesystem, all of them, and a
// summary goes to TENSILE_BACKFILL_OUT: per table the rows read and the values converted and kept as they are (with
// why, and the first few rows), and the file's size before and after. Any mismatch fails it.
func TestSlimBackfillOfARealStore(t *testing.T) {
	path := os.Getenv("TENSILE_BACKFILL_DB")
	if path == "" {
		t.Skip("TENSILE_BACKFILL_DB names no store")
	}
	ctx := context.Background()
	size := func() int64 {
		var n int64
		for _, p := range []string{path, path + "-wal"} {
			if fi, err := os.Stat(p); err == nil {
				n += fi.Size()
			}
		}
		return n
	}
	sum := backfillSummary{DB: path, SizeBefore: size()}
	t.Logf("%s: %d bytes", path, sum.SizeBefore)

	start := time.Now()
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sum.MigrateS = time.Since(start).Seconds()
	sum.SizeAfterMigration = size()
	if err := st.DB().QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&sum.AutoVacuum); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `PRAGMA page_size`).Scan(&sum.PageSize); err != nil {
		t.Fatal(err)
	}
	t.Logf("migrated in %.0fs: %d bytes, auto_vacuum %d, page size %d", sum.MigrateS, sum.SizeAfterMigration, sum.AutoVacuum, sum.PageSize)

	rd, err := store.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	b := st.NewBackfill()
	lastSaid := time.Now()
	b.OnCommit(func(vals []store.BackfillValue) error {
		for _, v := range vals {
			got, converted, err := readBack(ctx, rd, v)
			sum.Checked++
			if err == nil && converted && bytes.Equal(got, v.Old) {
				continue
			}
			sum.Mismatches++
			if len(sum.FirstMismatches) < 20 {
				sum.FirstMismatches = append(sum.FirstMismatches, fmt.Sprintf("%s.%s rowid %d (promise %s, validator %s): converted %v, err %v, %d bytes back of %d\n got %.300s\nwant %.300s",
					v.Table, v.Column, v.Rowid, v.Hash, v.Validator, converted, err, len(got), len(v.Old), got, v.Old))
			}
		}
		if time.Since(lastSaid) >= 3*time.Minute {
			lastSaid = time.Now()
			var where []string
			for _, tb := range b.Tables() {
				where = append(where, fmt.Sprintf("%s %d/%d", tb.Table, tb.Done, tb.Total))
			}
			t.Logf("%.0fs: rowids done %s; %d values checked, %d mismatches; %d bytes given back", time.Since(start).Seconds(),
				strings.Join(where, ", "), sum.Checked, sum.Mismatches, b.Freed())
		}
		return nil
	})
	start = time.Now()
	turn, err := b.Run(ctx, 0, nil)
	sum.BackfillS = time.Since(start).Seconds()
	rd.Close()
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if !turn.Done {
		t.Fatalf("the backfill ran to no end: %+v", turn)
	}
	sum.Batches, sum.FreedByBatches = turn.Batches, b.Freed()
	for _, tb := range b.Tables() {
		ts := backfillTableSummary{Table: tb.Table, RowsSeen: tb.Seen, LastRowid: tb.Total, Columns: map[string]backfillColumnSummary{}}
		for c, col := range tb.Columns {
			ts.Columns[c] = backfillColumnSummary{Converted: col.Converted, KeptAsIs: col.KeptAsIs, Reasons: col.Reasons, Examples: col.Examples}
		}
		sum.Tables = append(sum.Tables, ts)
	}
	t.Logf("converted in %.0fs, %d batches; %d values checked, %d mismatches", sum.BackfillS, sum.Batches, sum.Checked, sum.Mismatches)

	// every page freed back to the filesystem, a few hundred megabytes at a time, the log emptied after each
	start = time.Now()
	if err := st.DB().QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&sum.FreePagesLeft); err != nil {
		t.Fatal(err)
	}
	for {
		var free int64
		if err := st.DB().QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&free); err != nil {
			t.Fatal(err)
		}
		if free == 0 {
			break
		}
		if _, err := st.DB().ExecContext(ctx, `PRAGMA incremental_vacuum(100000)`); err != nil {
			t.Fatal(err)
		}
		var after int64
		if err := st.DB().QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if _, err := st.DB().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			t.Fatal(err)
		}
		if after >= free {
			t.Logf("%d pages free that the store does not give back (auto_vacuum %d)", after, sum.AutoVacuum)
			break
		}
	}
	if _, err := st.DB().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	sum.VacuumS = time.Since(start).Seconds()
	sum.SizeAfter = size()
	t.Logf("pages given back in %.0fs: %d bytes, from %d", sum.VacuumS, sum.SizeAfter, sum.SizeBefore)

	js, _ := json.MarshalIndent(sum, "", "  ")
	if out := os.Getenv("TENSILE_BACKFILL_OUT"); out != "" {
		if err := os.WriteFile(out, append(js, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%s", js)
	if sum.Mismatches > 0 {
		t.Fatalf("%d of %d converted values do not read back to what they replaced; the first:\n%s", sum.Mismatches, sum.Checked, strings.Join(sum.FirstMismatches, "\n"))
	}
}

// The run over the real record, on a small store of the earlier forms: what the integration run does with the
// pre-slim store (.github/workflows/integration.yml), checked on every push, summary and all.
func TestTheRealStoreRunOnASmallStore(t *testing.T) {
	path, _, _, _ := earlierStore(t)
	out := filepath.Join(t.TempDir(), "backfill.json")
	t.Setenv("TENSILE_BACKFILL_DB", path)
	t.Setenv("TENSILE_BACKFILL_OUT", out)
	TestSlimBackfillOfARealStore(t)
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var sum backfillSummary
	if err := json.Unmarshal(raw, &sum); err != nil {
		t.Fatal(err)
	}
	// every converted value read back: 2 + 17 + 14 + 14 + 4 (TestTheBackfillWritesWhatAnInsertWrites)
	if sum.Checked != 51 || sum.Mismatches != 0 || len(sum.Tables) != 4 || sum.SizeBefore == 0 || sum.SizeAfter == 0 {
		t.Fatalf("summary %s", raw)
	}
	if c := sum.Tables[2].Columns["row_indices"]; sum.Tables[2].Table != "probes" || c.Converted != 14 || c.KeptAsIs != 3 || len(c.Examples) != 3 {
		t.Fatalf("probes in the summary: %+v", sum.Tables[2])
	}
}
