package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/slim"
)

// The slim backfill (decided 2026-10-07): the rows stored before the slim record (migration 27, 2026-10-06; an
// endpoint check before migration 28) written in the forms a row this build inserts gets.
//
// Such a row keeps what an earlier build wrote: its record line in raw_json and its row lists in full. Every reader
// reads both forms (slim.go), so nothing had to be rewritten, but the store keeps the same row lists up to four times
// over: in the publication's line, in each assignment, in each reading's line and in its row_indices. The backfill
// writes each such value as an insert by this build writes it, with the insert path's own functions:
//
//   - publications.raw_json: the slim publication (publicationBody, UpsertPublication's);
//   - assignments.rows_json: RowsAssigned where the list is the validator's own assignment (assignmentRows);
//   - probes.raw_json: the slim reading against its publication (measurementBody, InsertProbe's);
//   - probes.row_indices: RowsAssigned where the list is the validator's own assignment (readingRows);
//   - reachability.raw_json: the slim endpoint check (reachabilityBody, InsertReachability's).
//
// A value is converted only where its new form reads back, through the store's own readers (Record, ProbeRecord,
// ReachRecord, AssignedRows, RowIndices), to the old value byte for byte; any other is left as it is and counted, with
// why. Nothing else in a row changes, its rowid included, and every reader decodes the values it decoded before, so
// what is kept computed from them (the API's caches of publications and verdicts, its endorsement ledger, its day
// partials) stays right without being told.
//
// It runs in the collector, the one process that adds to the slim tables (another adding entries would number them
// differently), a batch at a time between the collector's passes (Run). The tables go in BackfillTables' order, the
// publications first since the others are converted against them, each by rowid up to the highest rowid when this
// process first looked: a row stored after that is one this build wrote. A batch is one transaction: its rows are read
// whole and the statement closed before anything else is asked of the store (the collector's has one connection, and
// a read through it while rows were open would wait for itself); each value is converted and checked; each row is
// written back only where it still holds the values read (a compare and swap); the table entries the batch added are
// kept; and the table's progress is written to meta, so a collector that stops between batches goes on from where it
// was. A batch that fails leaves the rows, the tables and the progress as they were.

// BackfillTables are the tables the backfill converts, in its order.
var BackfillTables = [...]string{"publications", "assignments", "probes", "reachability"}

// backfillColumns are the columns of each table it converts.
var backfillColumns = [len(BackfillTables)][]string{{"raw_json"}, {"rows_json"}, {"raw_json", "row_indices"}, {"raw_json"}}

// backfillKeys are what a batch reads of each row beside them: its promise hash and its validator (an empty string
// where the table has none).
var backfillKeys = [len(BackfillTables)]string{"promise_hash, ''", "promise_hash, validator_address",
	"promise_hash, validator_address", "'', validator_address"}

// MetaBackfillPrefix followed by a table's name is the meta key of the last rowid of that table the backfill has done.
const MetaBackfillPrefix = "slim_backfill_"

// MetaBackfillDoneAt is when the backfill found every table done; from then on it reads nothing.
const MetaBackfillDoneAt = "slim_backfill_done_at"

// Bounds of one batch. It reads at most backfillRows rows, and none after the one that brings the values it read past
// backfillBytes (a publication's line with its row lists is a few hundred kilobytes), so its transaction stays well
// under a second and what it holds in memory a few megabytes. After it, at most backfillReclaimPages free pages go
// back to the filesystem: a few megabytes written, never a long write.
const (
	backfillRows         = 300
	backfillBytes        = 8 << 20
	backfillReclaimPages = 2000
)

// backfillExamples is how many of the values a column keeps as they are it names, with their rows.
const backfillExamples = 5

// Why a value of an earlier form is left as it is.
const (
	whyEncode         = "the slim encoder refuses it"
	whyLine           = "the insert path keeps it as its line"
	whyReadBack       = "its new form does not read back to it"
	whyPubUndecodable = "its publication does not decode"
	whyNoPub          = "its publication is not on record"
	whyPin            = "its publication's assignment is from a pin this build does not reproduce"
	whyNotList        = "not a list of rows"
	whyEmpty          = "an empty list"
	whyNotInSet       = "the validator is not in its publication's assignment"
	whyNotOwn         = "not the validator's own assignment in its order"
	whyChanged        = "the row changed while it was converted"
)

// Backfill is the slim backfill of one store in one process: where each table stands, and what was done since the
// process started. One goroutine uses it (the collector's loop).
type Backfill struct {
	s      *Store
	loaded bool
	// done: every table is done, and meta says so since doneAt; doneBefore: it said so when this process first looked
	done, doneBefore bool
	doneAt           string
	// at is the table being converted, an index into BackfillTables (len when every one is done)
	at       int
	tables   [len(BackfillTables)]BackfillTable
	since    time.Time
	pageSize int64
	freed    int64
	// rows and bytes bound a batch (backfillRows, backfillBytes; the tests make them small)
	rows, bytes int
	// committed, when set (the tests), is handed every value a batch converted, with the value it replaced, once the
	// batch has committed; an error it returns ends the turn
	committed func([]BackfillValue) error
}

// BackfillTable is where the backfill of one table stands.
type BackfillTable struct {
	Table string
	// Done is the last rowid done; Total the last one to do, the table's highest rowid when this process first looked.
	Done, Total int64
	// Seen is the rows read since the process started.
	Seen int64
	// Columns is what was done to each column converted since the process started.
	Columns map[string]*BackfillColumn
}

// BackfillColumn is what the backfill did to one column: the values converted, and those of an earlier form left as
// they are, counted by why (Reasons), the first few named with their rows (Examples).
type BackfillColumn struct {
	Converted, KeptAsIs int64
	Reasons             map[string]int64
	Examples            []string
}

// BackfillValue is one value a committed batch converted, and the value it replaced.
type BackfillValue struct {
	Table, Column   string
	Rowid           int64
	Hash, Validator string
	Old             []byte
}

// BackfillTurn is what one Run did.
type BackfillTurn struct {
	Batches             int
	Converted, KeptAsIs int64
	// Freed is the bytes given back to the filesystem.
	Freed int64
	// Finished names the tables this turn finished.
	Finished []string
	// Done: every table is done. DoneNow: this turn found so, and wrote MetaBackfillDoneAt.
	Done, DoneNow bool
}

// NewBackfill is the backfill of this store. It reads where it stands on its first Run.
func (s *Store) NewBackfill() *Backfill {
	b := &Backfill{s: s, since: time.Now(), rows: backfillRows, bytes: backfillBytes}
	for i, name := range BackfillTables {
		b.tables[i] = BackfillTable{Table: name, Columns: map[string]*BackfillColumn{}}
		for _, c := range backfillColumns[i] {
			b.tables[i].Columns[c] = &BackfillColumn{Reasons: map[string]int64{}}
		}
	}
	return b
}

// Done reports whether every table is done, as of the last Run.
func (b *Backfill) Done() bool { return b.done }

// Run converts a batch after another until budget has passed (0: until every table is done), until stop says so
// (asked before each batch: an ingest is due, the disk is busy), or until every table is done, and then says so in
// meta. An error ends the turn, its batch leaving the store as it was; the next turn begins again at that batch.
// Nothing else is asked of the store while a batch has rows open, so it runs on the collector's one connection.
func (b *Backfill) Run(ctx context.Context, budget time.Duration, stop func() bool) (BackfillTurn, error) {
	var turn BackfillTurn
	if err := b.load(ctx); err != nil {
		return turn, err
	}
	start := time.Now()
	for !b.done {
		if b.at == len(BackfillTables) {
			now := time.Now()
			if err := b.s.SetMeta(MetaBackfillDoneAt, TS(now), now); err != nil {
				return turn, err
			}
			b.done, b.doneAt, turn.DoneNow = true, TS(now), true
			break
		}
		if budget > 0 && turn.Batches > 0 && time.Since(start) >= budget {
			break
		}
		if stop != nil && stop() {
			break
		}
		if err := ctx.Err(); err != nil {
			return turn, err
		}
		at := b.at
		conv, kept, err := b.batch(ctx)
		if err != nil {
			return turn, fmt.Errorf("%s after rowid %d: %w", BackfillTables[at], b.tables[at].Done, err)
		}
		turn.Batches++
		turn.Converted += conv
		turn.KeptAsIs += kept
		if b.at > at {
			// the table the batch finished; one after it with no row to do is passed over unsaid
			turn.Finished = append(turn.Finished, BackfillTables[at])
		}
		// the pages the batch freed go back a few megabytes at a time
		pages, err := b.s.ReclaimSpace(ctx, backfillReclaimPages)
		if err != nil {
			return turn, fmt.Errorf("giving back the pages freed: %w", err)
		}
		turn.Freed += pages * b.pageSize
		b.freed += pages * b.pageSize
	}
	turn.Done = b.done
	return turn, nil
}

// load reads, once, where the backfill stands: done, or each table's last rowid done and highest rowid.
func (b *Backfill) load(ctx context.Context) error {
	if b.loaded {
		return nil
	}
	s := b.s
	at, err := b.meta(ctx, MetaBackfillDoneAt)
	if err != nil {
		return err
	}
	if at != "" {
		b.loaded, b.done, b.doneBefore, b.doneAt, b.at = true, true, true, at, len(BackfillTables)
		return nil
	}
	if err := s.db.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&b.pageSize); err != nil {
		return err
	}
	for i := range b.tables {
		tb := &b.tables[i]
		v, err := b.meta(ctx, MetaBackfillPrefix+tb.Table)
		if err != nil {
			return err
		}
		// a value that does not read as a rowid is begun again: converting a row twice changes nothing
		done, _ := strconv.ParseInt(v, 10, 64)
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid), 0) FROM `+tb.Table).Scan(&tb.Total); err != nil {
			return err
		}
		tb.Done = max(done, 0)
	}
	b.next()
	b.loaded = true
	return nil
}

func (b *Backfill) meta(ctx context.Context, key string) (string, error) {
	var v string
	err := b.s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// next moves at past every table done.
func (b *Backfill) next() {
	for b.at < len(BackfillTables) && b.tables[b.at].Done >= b.tables[b.at].Total {
		b.at++
	}
}

// backfillRow is one row of a batch as read: its rowid, promise hash and validator, and the value of each column
// converted as it is stored (a string for a TEXT, []byte for a BLOB, nil for NULL), with what replaces it (nil: the
// value stays).
type backfillRow struct {
	rowid           int64
	hash, validator string
	old, new        []any
}

// batch converts the next batch of the table being converted, in one transaction, and returns how many values it
// converted and how many of an earlier form it left as they are.
func (b *Backfill) batch(ctx context.Context) (converted, keptAsIs int64, err error) {
	s, i := b.s, b.at
	tb, cols := &b.tables[i], backfillColumns[i]
	// the one encoding (slimState.enc), from the tables' count before it through the commit or the rollback that
	// keeps or forgets what it added
	s.slim.enc.Lock()
	defer s.slim.enc.Unlock()
	t, err := s.tables(ctx, s.db, false)
	if err != nil {
		return 0, 0, err
	}
	kept := t.Stored()
	committed := false
	defer func() {
		if !committed {
			t.Rollback(kept)
		}
	}()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	rows, last, err := b.read(ctx, tx, i)
	if err != nil {
		return 0, 0, err
	}

	// each value converted and checked, with no statement open; what the batch did is counted once it commits
	did := map[string]*BackfillColumn{}
	for _, c := range cols {
		did[c] = &BackfillColumn{Reasons: map[string]int64{}}
	}
	keep := func(r *backfillRow, c, why, detail string) {
		col := did[c]
		col.KeptAsIs++
		col.Reasons[why]++
		if len(col.Examples)+len(tb.Columns[c].Examples) < backfillExamples {
			ex := fmt.Sprintf("rowid %d%s: %s", r.rowid, rowNames(r), why)
			if detail != "" {
				ex += ": " + clip(detail)
			}
			col.Examples = append(col.Examples, ex)
		}
	}
	var es []slim.Entry
	for _, r := range rows {
		for j, c := range cols {
			var added []slim.Entry
			var why, detail string
			if c == "raw_json" {
				added, why, detail, err = b.record(ctx, tx, t, tb.Table, r, j)
			} else {
				why, detail, err = b.rowList(ctx, tx, tb.Table, r, j)
			}
			if err != nil {
				return 0, 0, fmt.Errorf("rowid %d, %s: %w", r.rowid, c, err)
			}
			es = append(es, added...)
			if why != "" {
				keep(r, c, why, detail)
			}
		}
	}

	// each row written back where it still holds what was read, its unchanged values as they were
	sets, conds := make([]string, len(cols)), make([]string, len(cols))
	for j, c := range cols {
		sets[j], conds[j] = c+" = ?", c+" IS ?"
	}
	upd, err := tx.PrepareContext(ctx, `UPDATE `+tb.Table+` SET `+strings.Join(sets, ", ")+` WHERE rowid = ? AND `+strings.Join(conds, " AND "))
	if err != nil {
		return 0, 0, err
	}
	var vals []BackfillValue
	for _, r := range rows {
		changed := false
		args := make([]any, 0, 2*len(cols)+1)
		for j := range cols {
			v := r.old[j]
			if r.new[j] != nil {
				v, changed = r.new[j], true
			}
			args = append(args, v)
		}
		if !changed {
			continue
		}
		args = append(append(args, r.rowid), r.old...)
		res, err := upd.ExecContext(ctx, args...)
		if err != nil {
			upd.Close()
			return 0, 0, fmt.Errorf("rowid %d: %w", r.rowid, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			upd.Close()
			return 0, 0, err
		}
		for j, c := range cols {
			if r.new[j] == nil {
				continue
			}
			if n == 0 {
				// the entries its new form added are kept all the same: unused, they cost their bytes and nothing else
				keep(r, c, whyChanged, "")
				continue
			}
			did[c].Converted++
			if b.committed != nil {
				old, _ := bytesOf(r.old[j])
				vals = append(vals, BackfillValue{Table: tb.Table, Column: c, Rowid: r.rowid, Hash: r.hash, Validator: r.validator, Old: old})
			}
		}
	}
	upd.Close()
	if err := keepEntries(tx, es); err != nil {
		return 0, 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta (key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
		MetaBackfillPrefix+tb.Table, strconv.FormatInt(last, 10), ts(time.Now())); err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	committed = true

	tb.Done, tb.Seen = last, tb.Seen+int64(len(rows))
	for c, d := range did {
		col := tb.Columns[c]
		col.Converted += d.Converted
		col.KeptAsIs += d.KeptAsIs
		for why, n := range d.Reasons {
			col.Reasons[why] += n
		}
		col.Examples = append(col.Examples, d.Examples...)
		converted, keptAsIs = converted+d.Converted, keptAsIs+d.KeptAsIs
	}
	b.next()
	if b.committed != nil && len(vals) > 0 {
		if err := b.committed(vals); err != nil {
			return converted, keptAsIs, err
		}
	}
	return converted, keptAsIs, nil
}

// read reads the next batch of table i whole, and closes the statement before anything else is asked of the store:
// the rows past its last done, b.rows of them at most, the last the one whose values bring the batch past b.bytes.
// last is the last rowid read, or the table's last to do when no row is left.
func (b *Backfill) read(ctx context.Context, tx *sql.Tx, i int) (out []*backfillRow, last int64, err error) {
	tb, cols := &b.tables[i], backfillColumns[i]
	rows, err := tx.QueryContext(ctx, `SELECT rowid, `+backfillKeys[i]+`, `+strings.Join(cols, ", ")+` FROM `+tb.Table+`
		WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`, tb.Done, tb.Total, b.rows)
	if err != nil {
		return nil, 0, err
	}
	size := 0
	for size < b.bytes && rows.Next() {
		r := &backfillRow{old: make([]any, len(cols)), new: make([]any, len(cols))}
		dst := []any{&r.rowid, &r.hash, &r.validator}
		for j := range cols {
			dst = append(dst, &r.old[j])
		}
		if err := rows.Scan(dst...); err != nil {
			rows.Close()
			return nil, 0, err
		}
		for _, v := range r.old {
			x, _ := bytesOf(v)
			size += len(x)
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if len(out) == 0 {
		return nil, tb.Total, nil
	}
	return out, out[len(out)-1].rowid, nil
}

// record converts raw_json, column j of r, a row of table: its line in the slim form the table's insert path gives
// it, where that reads back through the store to the line byte for byte. It returns the table entries the new form
// added, or why the value is left as it is (and which detail), or neither for a value that is not a line (the slim
// form already, or a record an earlier build's retention emptied). The error is the store's own.
func (b *Backfill) record(ctx context.Context, tx *sql.Tx, t *slim.Tables, table string, r *backfillRow, j int) ([]slim.Entry, string, string, error) {
	s := b.s
	line, ok := bytesOf(r.old[j])
	if !ok || !isLine(line) {
		return nil, "", "", nil
	}
	k := t.Stored()
	var body any
	var refused error // why the insert path keeps the line, where it says
	var back func([]byte) ([]byte, error)
	switch table {
	case "publications":
		var err error
		if body, _, err = publicationBody(t, line, k); err != nil {
			t.Rollback(k)
			return nil, whyEncode, err.Error(), nil
		}
		back = func(x []byte) ([]byte, error) { return s.Record(ctx, tx, x) }
	case "probes":
		pub, err := s.pub(ctx, tx, r.hash)
		if errors.Is(err, ErrUndecodable) {
			// as InsertProbe stores a reading of a publication that does not decode: as its line
			return nil, whyPubUndecodable, err.Error(), nil
		}
		if err != nil {
			return nil, "", "", err
		}
		if body, err = measurementBody(t, line, pub, s.lookup(ctx, tx), k); err != nil {
			t.Rollback(k)
			return nil, whyEncode, err.Error(), nil
		}
		back = func(x []byte) ([]byte, error) { return s.ProbeRecord(ctx, tx, r.hash, x) }
	case "reachability":
		body, refused = reachabilityBody(t, line, k)
		back = func(x []byte) ([]byte, error) { return s.ReachRecord(ctx, tx, x) }
	default:
		return nil, "", "", fmt.Errorf("no record to convert in %s", table)
	}
	nb, ok := body.([]byte)
	if !ok {
		// the insert path keeps it as its line, and has forgotten what the attempt added
		detail := "its slim form would not give the line back byte for byte"
		if refused != nil {
			detail = refused.Error()
		}
		return nil, whyLine, detail, nil
	}
	got, err := back(nb)
	if err != nil && !errors.Is(err, ErrUndecodable) {
		return nil, "", "", err
	}
	if err != nil || !bytes.Equal(got, line) {
		t.Rollback(k)
		detail := fmt.Sprintf("it reads back to %d other bytes", len(got))
		if err != nil {
			detail = err.Error()
		}
		return nil, whyReadBack, detail, nil
	}
	r.new[j] = nb
	return t.Pending(), "", "", nil
}

// rowList converts rows_json or row_indices, column j of r, a row of table: RowsAssigned where the list is the
// validator's own assignment in its order, as the insert path marks it, and the store resolves the mark to the list
// byte for byte. It returns why a list is left as it is (and which detail), or nothing for a value that is not a list
// (NULL, or marked already). The error is the store's own.
func (b *Backfill) rowList(ctx context.Context, tx *sql.Tx, table string, r *backfillRow, j int) (string, string, error) {
	s := b.s
	raw, ok := bytesOf(r.old[j])
	if !ok || string(raw) == RowsAssigned {
		return "", "", nil
	}
	list := string(raw)
	pub, err := s.pub(ctx, tx, r.hash)
	switch {
	case errors.Is(err, ErrUndecodable):
		return whyPubUndecodable, err.Error(), nil
	case err != nil:
		return "", "", err
	case pub == nil:
		return whyNoPub, "", nil
	case pub.PinErr() != nil:
		return whyPin, pub.PinErr().Error(), nil
	}
	var mark any
	var n int
	var resolve func() (string, error)
	switch table {
	case "assignments":
		var rows []int
		if json.Unmarshal(raw, &rows) != nil || rows == nil {
			return whyNotList, list, nil
		}
		if mark, err = assignmentRows(pub, r.validator, rows); err != nil {
			return "", "", err
		}
		n = len(rows)
		resolve = func() (string, error) {
			got, err := s.AssignedRows(ctx, tx, r.hash, r.validator, sql.NullString{String: RowsAssigned, Valid: true})
			return got.String, err
		}
	case "probes":
		var rows []uint32
		if json.Unmarshal(raw, &rows) != nil || rows == nil {
			return whyNotList, list, nil
		}
		mark, n = readingRows(pub, r.validator, rows), len(rows)
		resolve = func() (string, error) { return s.RowIndices(ctx, tx, r.hash, r.validator, RowsAssigned) }
	default:
		return "", "", fmt.Errorf("no row list to convert in %s", table)
	}
	if m, _ := mark.(string); m != RowsAssigned {
		switch _, inSet := pub.Rows(r.validator); {
		case n == 0:
			return whyEmpty, list, nil
		case !inSet:
			return whyNotInSet, "", nil
		}
		return whyNotOwn, list, nil
	}
	got, err := resolve()
	if err != nil && !errors.Is(err, ErrUndecodable) {
		return "", "", err
	}
	if err != nil || got != list {
		detail := "the mark resolves to " + got
		if err != nil {
			detail = err.Error()
		}
		return whyReadBack, detail, nil
	}
	r.new[j] = RowsAssigned
	return "", "", nil
}

// bytesOf is a value as read, as bytes: false for NULL (or a number, which no converted column holds).
func bytesOf(v any) ([]byte, bool) {
	switch x := v.(type) {
	case string:
		return []byte(x), true
	case []byte:
		return x, true
	}
	return nil, false
}

// rowNames names a row's promise and validator, where it has them, for the log.
func rowNames(r *backfillRow) string {
	var out []string
	if r.hash != "" {
		out = append(out, "promise "+r.hash[:min(12, len(r.hash))])
	}
	if r.validator != "" {
		out = append(out, "validator "+r.validator)
	}
	if len(out) == 0 {
		return ""
	}
	return " (" + strings.Join(out, ", ") + ")"
}

// clip shortens a detail for the log.
func clip(s string) string {
	if len(s) > 160 {
		return s[:160] + "..."
	}
	return s
}

// Tables is where each table stands, a copy.
func (b *Backfill) Tables() []BackfillTable {
	out := make([]BackfillTable, len(b.tables))
	for i, tb := range b.tables {
		cp := tb
		cp.Columns = map[string]*BackfillColumn{}
		for c, col := range tb.Columns {
			x := *col
			x.Reasons = map[string]int64{}
			for why, n := range col.Reasons {
				x.Reasons[why] = n
			}
			x.Examples = append([]string(nil), col.Examples...)
			cp.Columns[c] = &x
		}
		out[i] = cp
	}
	return out
}

// Freed is the bytes given back to the filesystem since the process started.
func (b *Backfill) Freed() int64 { return b.freed }

// Since is when the process started counting.
func (b *Backfill) Since() time.Time { return b.since }

// Status is where the backfill stands, for the collector's status file, a new value every call: each table's rowid
// done and the last to do, and the values of each column converted and kept as they are since the process started
// (with why, where any was kept); the bytes given back to the filesystem; and, once every table is done, since when.
func (b *Backfill) Status() map[string]any {
	if b.doneBefore {
		return map[string]any{"done_at": b.doneAt}
	}
	out := map[string]any{"since": b.since.UTC().Format(time.RFC3339), "freed_bytes": b.freed}
	if b.done {
		out["done_at"] = b.doneAt
	}
	for _, tb := range b.tables {
		m := map[string]any{"done": tb.Done, "total": tb.Total}
		for c, col := range tb.Columns {
			cm := map[string]any{"converted": col.Converted, "kept_as_is": col.KeptAsIs}
			if len(col.Reasons) > 0 {
				why := map[string]int64{}
				for w, n := range col.Reasons {
					why[w] = n
				}
				cm["kept_why"] = why
			}
			m[c] = cm
		}
		out[tb.Table] = m
	}
	return out
}

// ReasonsLine is a column's kept values by why, most first, as one line for the log.
func (c *BackfillColumn) ReasonsLine() string {
	type kv struct {
		why string
		n   int64
	}
	var all []kv
	for w, n := range c.Reasons {
		all = append(all, kv{w, n})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].n > all[j].n || all[i].n == all[j].n && all[i].why < all[j].why })
	parts := make([]string, len(all))
	for i, x := range all {
		parts[i] = fmt.Sprintf("%s: %d", x.why, x.n)
	}
	return strings.Join(parts, "; ")
}
