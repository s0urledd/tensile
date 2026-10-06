package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// recordTables are the tables whose rows carry the record; everything else in the store is the same in every design.
var recordTables = []string{"probes", "publications", "assignments"}

// servingSizes measures two slim stores that keep every column and index today's queries use:
//
//   - S1: today's schema and indexes as they are; the record copies (probes.raw_json, publications.raw_json,
//     probes.row_indices, assignments.rows_json) give way to the slim record, from which they are computed again;
//   - S2: S1 with each text column written compactly, in the tables and in the indexes: times as integer
//     nanoseconds, hashes and addresses as bytes or as the number of their row, words (class, phase, outcome, …) as
//     the number of their dictionary entry. The same indexes, on the same columns.
func servingSizes(r run, ep, em [][]byte, tables []byte, dir string) {
	today := filepath.Join(dir, "today.db")
	s1 := filepath.Join(dir, "s1.db")
	s2 := filepath.Join(dir, "s2.db")
	for _, p := range []string{s1, s2} {
		_ = os.Remove(p)
	}
	copyFile(today, s1)

	// S1
	db := open(s1)
	tx, _ := db.Begin()
	exec(tx, `UPDATE assignments SET rows_json = ''`)
	exec(tx, `UPDATE probes SET row_indices = NULL`)
	for i, l := range r.pubs {
		var p scan.Publication
		_ = json.Unmarshal(l, &p)
		exec(tx, `UPDATE publications SET raw_json = ? WHERE promise_hash = ?`, ep[i], p.PromiseHash)
	}
	for i, l := range r.meas {
		var m probe.Measurement
		_ = json.Unmarshal(l, &m)
		exec(tx, `UPDATE probes SET raw_json = ? WHERE dedupe_key = ?`, em[i], m.DedupeKey())
	}
	exec(tx, `CREATE TABLE slim_tables (body BLOB NOT NULL)`)
	exec(tx, `INSERT INTO slim_tables VALUES (?)`, tables)
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	vacuum(db)
	s1Bytes := recordBytes(db, "slim_tables")
	db.Close()

	// S2
	src := open(today)
	dst := open(s2)
	dict := newDict()
	pubID, valID := map[string]int64{}, map[string]int64{}
	bodies := map[string][]byte{} // probes by dedupe key, publications by promise hash
	for i, l := range r.pubs {
		var p scan.Publication
		_ = json.Unmarshal(l, &p)
		bodies["p:"+p.PromiseHash] = ep[i]
	}
	for i, l := range r.meas {
		var m probe.Measurement
		_ = json.Unmarshal(l, &m)
		bodies["m:"+m.DedupeKey()] = em[i]
	}
	// publications first, so their row numbers stand for their hash elsewhere
	for _, t := range []string{"publications", "assignments", "probes"} {
		compactTable(src, dst, t, dict, pubID, valID, bodies)
	}
	exec(dst, `CREATE TABLE slim_tables (body BLOB NOT NULL)`)
	exec(dst, `INSERT INTO slim_tables VALUES (?)`, tables)
	for a, id := range valID {
		b, _ := hex.DecodeString(a)
		exec(dst, `INSERT INTO validators VALUES (?, ?)`, id, b)
	}
	dict.save(dst)
	indexes(src, dst, dict)
	vacuum(dst)
	s2Bytes := recordBytes(dst, "slim_tables", "words", "validators")
	src.Close()
	dst.Close()

	tdb := open(today)
	todayBytes := recordBytes(tdb)
	tdb.Close()
	nb := float64(len(r.pubs))
	fmt.Printf("\n== serving stores: the record tables with every column and index today's queries use (SQLite, after VACUUM)\n")
	fmt.Printf("today: %8.1f KB per blob in the database, + %.1f KB of record lines\n", float64(todayBytes)/nb/1000, float64(rawLen(r))/nb/1000)
	fmt.Printf("S1:    %8.1f KB per blob (the slim record replaces the copies; schema, columns and indexes as today)\n", float64(s1Bytes)/nb/1000)
	fmt.Printf("S2:    %8.1f KB per blob (S1 with compact columns and the same indexes on them)\n", float64(s2Bytes)/nb/1000)
	for _, d := range []struct {
		name string
		path string
	}{{"today", today}, {"S1", s1}, {"S2", s2}} {
		db := open(d.path)
		fmt.Printf("  %s by table and index, KB per blob:", d.name)
		rows, err := db.Query(`SELECT name, SUM(pgsize) FROM dbstat WHERE name IN (SELECT name FROM sqlite_master WHERE tbl_name IN ('probes','publications','assignments','slim_tables','words','validators')) GROUP BY name ORDER BY 2 DESC LIMIT 8`)
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			var n string
			var b int64
			_ = rows.Scan(&n, &b)
			fmt.Printf(" %s %.1f;", n, float64(b)/nb/1000)
		}
		rows.Close()
		fmt.Println()
		db.Close()
	}
}

func open(p string) *sql.DB {
	db, err := sql.Open("sqlite", p)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

type execer interface {
	Exec(string, ...any) (sql.Result, error)
}

func exec(x execer, q string, args ...any) {
	if _, err := x.Exec(q, args...); err != nil {
		log.Fatalf("%v: %.200s", err, q)
	}
}

func vacuum(db *sql.DB) { exec(db, "VACUUM") }

func copyFile(from, to string) {
	in, err := os.Open(from)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		log.Fatal(err)
	}
	out.Close()
}

// recordBytes is the pages of the record tables, their indexes and the named extra tables.
func recordBytes(db *sql.DB, extra ...string) int64 {
	return tableBytes(db, append(append([]string(nil), recordTables...), extra...)...)
}

// dict numbers the words of the text columns written as words.
type dict struct {
	id  map[string]int64
	all []string
}

func newDict() *dict { return &dict{id: map[string]int64{}} }

func (d *dict) of(s string) int64 {
	if id, ok := d.id[s]; ok {
		return id
	}
	d.all = append(d.all, s)
	d.id[s] = int64(len(d.all))
	return d.id[s]
}

func (d *dict) save(db *sql.DB) {
	exec(db, `CREATE TABLE words (id INTEGER PRIMARY KEY, word TEXT NOT NULL)`)
	tx, _ := db.Begin()
	for i, s := range d.all {
		exec(tx, `INSERT INTO words VALUES (?, ?)`, i+1, s)
	}
	_ = tx.Commit()
}

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)
var hex40 = regexp.MustCompile(`^[0-9a-f]{40}$`)

// compactTable copies a record table into dst with every text column written compactly, dropping the copies the
// slim record replaces and putting the slim record in their place.
func compactTable(src, dst *sql.DB, table string, d *dict, pubID, valID map[string]int64, bodies map[string][]byte) {
	rows, err := src.Query(`SELECT * FROM ` + table)
	if err != nil {
		log.Fatal(err)
	}
	cols, _ := rows.Columns()
	// the copies the slim record replaces; and a reading's dedupe key, which is its vantage, promise, validator and
	// schedule written out as text: a unique index on those four columns stands in for it
	drop := map[string]bool{"raw_json": true, "row_indices": true, "rows_json": true, "dedupe_key": true}
	var keep []string
	for _, c := range cols {
		if !drop[c] {
			keep = append(keep, c)
		}
	}
	var defs []string
	for _, c := range keep {
		defs = append(defs, c)
	}
	if table == "probes" {
		defs = append(defs, "attempt") // the dedupe key's fifth part, which no column of today's holds
	}
	defs = append(defs, "body BLOB")
	exec(dst, `CREATE TABLE `+table+` (`+strings.Join(defs, ", ")+`)`)
	n := len(keep) + 1
	if table == "probes" {
		n++
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", n), ",")
	tx, _ := dst.Begin()
	ins, err := tx.Prepare(`INSERT INTO ` + table + ` VALUES (` + ph + `)`)
	if err != nil {
		log.Fatal(err)
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			log.Fatal(err)
		}
		row := map[string]any{}
		for i, c := range cols {
			row[c] = vals[i]
		}
		var out []any
		for _, c := range keep {
			out = append(out, compact(table, c, row[c], d, pubID, valID))
		}
		if table == "probes" {
			parts := strings.Split(fmt.Sprint(row["dedupe_key"]), "|")
			attempt := int64(0)
			if len(parts) == 5 {
				fmt.Sscan(parts[4], &attempt)
			}
			out = append(out, attempt)
		}
		var body []byte
		switch table {
		case "probes":
			body = bodies["m:"+fmt.Sprint(row["dedupe_key"])]
		case "publications":
			body = bodies["p:"+fmt.Sprint(row["promise_hash"])]
		}
		out = append(out, body)
		res, err := ins.Exec(out...)
		if err != nil {
			log.Fatal(err)
		}
		if table == "publications" {
			pubID[fmt.Sprint(row["promise_hash"])], _ = res.LastInsertId()
		}
	}
	rows.Close()
	_ = tx.Commit()
	if table == "assignments" || table == "probes" {
		exec(dst, `CREATE TABLE IF NOT EXISTS validators (id INTEGER PRIMARY KEY, address BLOB NOT NULL)`)
	}
}

// compact writes one value of a text column compactly.
func compact(table, col string, v any, d *dict, pubID, valID map[string]int64) any {
	var s string
	switch x := v.(type) {
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		return v
	}
	switch {
	case col == "promise_hash" && table != "publications":
		if id, ok := pubID[s]; ok {
			return id
		}
		b, _ := hex.DecodeString(s)
		return b
	case col == "validator_address":
		if id, ok := valID[s]; ok {
			return id
		}
		valID[s] = int64(len(valID) + 1)
		return valID[s]
	case hex64.MatchString(s) || hex40.MatchString(s):
		b, _ := hex.DecodeString(s)
		return b
	}
	if len(s) >= 20 && strings.HasSuffix(s, "Z") {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixNano()
		}
	}
	if s == "" {
		return int64(0)
	}
	return d.of(s)
}

var quoted = regexp.MustCompile(`'([^']*)'`)

// indexes builds in dst the record tables' indexes of src, on the compact columns: the same names, the same columns,
// a partial index's words turned into their numbers.
func indexes(src, dst *sql.DB, d *dict) {
	rows, err := src.Query(`SELECT sql FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL AND tbl_name IN ('probes','publications','assignments')`)
	if err != nil {
		log.Fatal(err)
	}
	var qs []string
	for rows.Next() {
		var q string
		_ = rows.Scan(&q)
		qs = append(qs, q)
	}
	rows.Close()
	// today's primary keys and unique constraints are indexes too
	qs = append(qs,
		`CREATE UNIQUE INDEX probes_key ON probes (vantage, promise_hash, validator_address, scheduled_at, attempt)`,
		`CREATE UNIQUE INDEX publications_key ON publications (promise_hash)`,
		`CREATE UNIQUE INDEX assignments_key ON assignments (promise_hash, validator_address)`)
	for _, q := range qs {
		q = strings.ReplaceAll(q, "substr(classification_reason, 1, 9) = 'budget:p='", "classification_reason > 0")
		q = quoted.ReplaceAllStringFunc(q, func(m string) string {
			w := m[1 : len(m)-1]
			if w == "" {
				return "0"
			}
			return fmt.Sprint(d.of(w))
		})
		exec(dst, q)
	}
}
