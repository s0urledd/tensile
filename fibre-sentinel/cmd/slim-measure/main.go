// Command slim-measure tests the slim record format (internal/slim) on exported records and measures it.
//
//	slim-measure -db-dir DIR export-day-dir...
//
// It encodes every publication and reading of the given days, writes the shared tables out, decodes everything again
// from those tables alone and compares each record with its original line byte for byte. It then runs constructed
// cases (another order, part of the rows, rows of another promise, rows of none, an exception in a derived field)
// the same way, and loads the same records into today's store and into a slim store to compare what each keeps on
// disk. A prototype: nothing in the observer uses it.
package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "modernc.org/sqlite"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/slim"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

func lines(path string) [][]byte {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var out [][]byte
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<20), 64<<20)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) > 0 {
			out = append(out, append([]byte(nil), sc.Bytes()...))
		}
	}
	return out
}

type run struct {
	pubs, meas [][]byte
}

// roundTrip encodes the records, writes the tables, decodes everything from the written tables, and compares.
func roundTrip(r run) (encPubs, encMeas [][]byte, pst, mst []slim.Stats, tables []byte, bad int, firstBad string) {
	t := slim.NewTables()
	for _, l := range append(append([][]byte(nil), r.pubs...), r.meas...) {
		if err := t.CountStrings(l); err != nil {
			log.Fatal(err)
		}
	}
	for _, l := range r.pubs {
		b, st, err := t.EncodePublication(l)
		if err != nil {
			log.Fatal(err)
		}
		encPubs, pst = append(encPubs, b), append(pst, st)
	}
	for _, l := range r.meas {
		b, st, err := t.EncodeMeasurement(l)
		if err != nil {
			log.Fatal(err)
		}
		encMeas, mst = append(encMeas, b), append(mst, st)
	}
	tables = t.MarshalTables()
	d, err := slim.LoadTables(tables)
	if err != nil {
		log.Fatal("tables: ", err)
	}
	check := func(kind string, i int, got []byte, err error, want []byte) {
		if err == nil && bytes.Equal(got, want) {
			return
		}
		bad++
		if firstBad == "" {
			if err != nil {
				firstBad = fmt.Sprintf("%s %d: %v", kind, i, err)
				return
			}
			n := 0
			for n < len(got) && n < len(want) && got[n] == want[n] {
				n++
			}
			lo := max(0, n-80)
			firstBad = fmt.Sprintf("%s %d differs at byte %d:\n  want …%s\n  got  …%s", kind, i, n, want[lo:min(len(want), n+80)], got[lo:min(len(got), n+80)])
		}
	}
	for i, b := range encPubs {
		got, err := d.DecodePublication(b)
		check("publication", i, got, err, r.pubs[i])
	}
	for i, b := range encMeas {
		got, err := d.DecodeMeasurement(b)
		check("reading", i, got, err, r.meas[i])
	}
	return
}

func sum(xs []slim.Stats) (total, exc int) {
	for _, s := range xs {
		total += s.Bytes
		exc += s.Exceptions
	}
	return
}

func main() {
	dbDir := flag.String("db-dir", "", "where to write today's store and the slim store for the on-disk comparison (empty: skip)")
	flag.Parse()
	var r run
	for _, dir := range flag.Args() {
		r.pubs = append(r.pubs, lines(filepath.Join(dir, "publications.jsonl"))...)
		r.meas = append(r.meas, lines(filepath.Join(dir, "measurements.jsonl"))...)
	}
	// publications first, in settlement order, as a store receives them before their readings
	sort.SliceStable(r.pubs, func(i, j int) bool { return height(r.pubs[i]) < height(r.pubs[j]) })
	fmt.Printf("records: %d publications, %d readings\n", len(r.pubs), len(r.meas))

	// 0. the tree writes every line back as it was: the comparison below is byte for byte
	for _, l := range append(append([][]byte(nil), r.pubs...), r.meas...) {
		v, err := slim.Parse(l)
		if err != nil || !bytes.Equal(slim.Emit(v), l) {
			log.Fatalf("a line does not write back as it was: %.200s", l)
		}
	}

	// 1. the real records
	ep, em, pst, mst, tables, bad, firstBad := roundTrip(r)
	fmt.Printf("\n== real records: %d of %d decode byte for byte\n", len(r.pubs)+len(r.meas)-bad, len(r.pubs)+len(r.meas))
	if bad > 0 {
		fmt.Println(firstBad)
	}
	rawP, rawM := 0, 0
	for _, l := range r.pubs {
		rawP += len(l)
	}
	for _, l := range r.meas {
		rawM += len(l)
	}
	sp, ep_ := sum(pst)
	sm, em_ := sum(mst)
	orphans := 0
	for _, s := range mst {
		if s.NoPublication {
			orphans++
		}
	}
	fmt.Printf("readings whose publication is not in these days: %d (kept whole: nothing to derive from)\n", orphans)
	nb := len(r.pubs)
	fmt.Printf("publications: today %d bytes, slim %d (exceptions %d)\n", rawP, sp, ep_)
	fmt.Printf("readings:     today %d bytes, slim %d (exceptions %d)\n", rawM, sm, em_)
	fmt.Printf("tables:       %d bytes\n", len(tables))
	fmt.Printf("per blob:     today %.1f KB, slim %.1f KB (records %.1f + tables %.1f)\n",
		float64(rawP+rawM)/float64(nb)/1000, float64(sp+sm+len(tables))/float64(nb)/1000, float64(sp+sm)/float64(nb)/1000, float64(len(tables))/float64(nb)/1000)
	fmt.Printf("per reading:  today %.0f bytes, slim %.0f\n", float64(rawM)/float64(len(r.meas)), float64(sm)/float64(len(r.meas)))
	fields := map[string]int{}
	for _, s := range mst {
		for k, n := range s.Fields {
			fields[k] += n
		}
	}
	fmt.Println("slim reading bytes by field:")
	printTop(fields, sm, 18)
	pf := map[string]int{}
	for _, s := range pst {
		for k, n := range s.Fields {
			pf[k] += n
		}
	}
	exc := map[string]int{}
	for _, s := range mst {
		for k, n := range s.ExcFields {
			exc[k] += n
		}
	}
	if em_ > 0 {
		fmt.Println("exception bytes by field:")
		printTop(exc, em_, 8)
	}
	fmt.Println("slim publication bytes by field:")
	printTop(pf, sp, 8)
	_ = ep
	_ = em

	// 2. constructed cases
	cases(r)

	// 3. on disk
	if *dbDir != "" {
		onDisk(r, ep, em, tables, *dbDir)
	}
}

func printTop(m map[string]int, total, n int) {
	type kv struct {
		k string
		v int
	}
	var xs []kv
	for k, v := range m {
		xs = append(xs, kv{k, v})
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i].v > xs[j].v })
	for i, x := range xs {
		if i == n {
			break
		}
		fmt.Printf("  %-24s %8d  %5.1f%%\n", x.k, x.v, float64(x.v)*100/float64(total))
	}
}

func height(l []byte) int64 {
	var p struct {
		H int64 `json:"settlement_height"`
	}
	_ = json.Unmarshal(l, &p)
	return p.H
}

// cases builds readings no export holds (or holds rarely) from a real one and runs each through the format: the
// rows in another order, part of them, the rows of another promise over the same blob (shadowing), rows of no
// promise, and a reading whose copy of a publication field differs. Each is labelled constructed.
func cases(r run) {
	// a real publication and one of its served readings
	var pubLine, measLine []byte
	var pub scan.Publication
	var m probe.Measurement
	for _, l := range r.meas {
		if err := json.Unmarshal(l, &m); err != nil || m.Classification != probe.ClassHealthy || len(m.Download.RowIndices) < 8 {
			continue
		}
		for _, p := range r.pubs {
			if bytes.Contains(p, []byte(`"promise_hash":"`+m.PromiseHash+`"`)) {
				pubLine = p
				break
			}
		}
		if pubLine != nil {
			measLine = l
			break
		}
	}
	if pubLine == nil {
		log.Fatal("no served reading with its publication")
	}
	if err := json.Unmarshal(pubLine, &pub); err != nil {
		log.Fatal(err)
	}

	// a second promise over the same blob, settled later, whose validator set lacks the set's last validator: its
	// assignment differs, so a validator can hand back that promise's rows instead of these
	other := shadowPublication(pub)
	otherLine, _ := json.Marshal(other)

	mutate := func(f func(*probe.Measurement)) []byte {
		var x probe.Measurement
		_ = json.Unmarshal(measLine, &x)
		f(&x)
		b, _ := json.Marshal(x)
		return b
	}
	own := append([]uint32(nil), m.Download.RowIndices...)
	var otherRows []uint32
	for _, v := range other.Assignment.Validators {
		if v.Address == m.ValidatorAddress {
			for _, x := range v.Rows {
				otherRows = append(otherRows, uint32(x))
			}
		}
	}
	type kase struct {
		name string
		line []byte
	}
	ks := []kase{
		{"served, rows as assigned (real)", measLine},
		{"another order: the same rows reversed", mutate(func(x *probe.Measurement) {
			for i, j := 0, len(own)-1; i < j; i, j = i+1, j-1 {
				x.Download.RowIndices[i], x.Download.RowIndices[j] = own[j], own[i]
			}
		})},
		{"part of the rows: the first half, in order", mutate(func(x *probe.Measurement) {
			x.Download.RowIndices = own[:len(own)/2]
			x.Download.RowsReturned = len(own) / 2
			x.Download.AssignmentVerified = false
			x.Download.RowsSubsetOfAssignment = true
		})},
		{"part of the rows, shuffled: every third, from the end", mutate(func(x *probe.Measurement) {
			var p []uint32
			for i := len(own) - 1; i >= 0; i -= 3 {
				p = append(p, own[i])
			}
			x.Download.RowIndices = p
			x.Download.RowsReturned = len(p)
			x.Download.AssignmentVerified = false
			x.Download.RowsSubsetOfAssignment = true
		})},
		{"shadowing: the other promise's rows", mutate(func(x *probe.Measurement) {
			x.Download.RowIndices = otherRows
			x.Download.RowsReturned = len(otherRows)
			x.Download.AssignmentVerified = false
			x.Download.ShadowedBy = other.PromiseHash
			x.Classification = "SHADOWED_SHARD"
		})},
		{"rows of no promise", mutate(func(x *probe.Measurement) {
			x.Download.RowIndices = []uint32{16383, 7, 9001, 42}
			x.Download.RowsReturned = 4
			x.Download.AssignmentVerified = false
			x.Classification = "UNMATCHED_GENUINE"
		})},
		{"a derived field that differs: host at settlement", mutate(func(x *probe.Measurement) {
			x.HostAtSettlement = "203.0.113.9:7980"
		})},
		{"retried: attempt 2 with the first answer's facts", mutate(func(x *probe.Measurement) {
			x.Attempt = 2
			x.Retry = &probe.RetryInfo{Attempts: 2, DelayMS: 90000, FirstStartedAt: x.StartedAt.Add(-90e9), FirstOutcome: "TCP_TIMEOUT", FirstError: "dial tcp: i/o timeout", FirstDurationMS: 15000}
		})},
	}
	same := len(otherRows) == len(own)
	for i := 0; same && i < len(own); i++ {
		same = otherRows[i] == own[i]
	}
	fmt.Printf("\n== constructed cases (from a real reading of %s…, validator %s…, %d rows; the other promise assigns it %d rows, the same list: %v)\n", m.PromiseHash[:12], m.ValidatorAddress[:8], len(own), len(otherRows), same)
	base := 0
	for i, k := range ks {
		rr := run{pubs: [][]byte{pubLine, otherLine}, meas: [][]byte{k.line}}
		_, em, _, mst, _, bad, firstBad := roundTrip(rr)
		ok := "lossless"
		if bad > 0 {
			ok = "LOST: " + firstBad
		}
		if i == 0 {
			base = len(em[0])
		}
		fmt.Printf("  %-55s %5d bytes (%+d vs served, exceptions %d)  %s\n", k.name, len(em[0]), len(em[0])-base, mst[0].Exceptions, ok)
	}
}

// shadowPublication is pub settled again later by another promise, over a validator set without its largest
// validator: the total voting power changes, so the other validators' rows move.
func shadowPublication(pub scan.Publication) scan.Publication {
	o := pub
	h, _ := hex.DecodeString(pub.PromiseHash)
	h[0] ^= 0xff
	o.PromiseHash = hex.EncodeToString(h)
	o.SettlementHeight += 10
	vals := append([]scan.ValidatorAssignment(nil), pub.Assignment.Validators[1:]...)
	var set []assign.Validator
	for _, v := range vals {
		b, _ := hex.DecodeString(v.Address)
		var a assign.Address
		copy(a[:], b)
		set = append(set, assign.Validator{Address: a, VotingPower: v.VotingPower})
	}
	var c [32]byte
	cb, _ := hex.DecodeString(pub.Promise.Commitment)
	copy(c[:], cb)
	sm, err := assign.Assign(c, set, assign.ParamsV10BlobV0)
	if err != nil {
		log.Fatal(err)
	}
	o.Assignment.Validators = nil
	var total int64
	sigma := 0
	for i, v := range vals {
		v.Rows = sm[set[i].Address]
		v.RowCount = len(v.Rows)
		sigma += v.RowCount
		total += v.VotingPower
		o.Assignment.Validators = append(o.Assignment.Validators, v)
	}
	o.Assignment.TotalVotingPower = total
	o.Assignment.Sigma = sigma
	return o
}

// onDisk loads the records into today's store and into a slim store, each with the indexes its queries use, and
// reports the bytes each keeps.
func onDisk(r run, ep, em [][]byte, tables []byte, dir string) {
	_ = os.MkdirAll(dir, 0o755)
	today := filepath.Join(dir, "today.db")
	slimDB := filepath.Join(dir, "slim.db")
	for _, p := range []string{today, today + "-wal", today + "-shm", slimDB} {
		_ = os.Remove(p)
	}
	st, err := store.Open(today)
	if err != nil {
		log.Fatal(err)
	}
	for _, l := range r.pubs {
		var p scan.Publication
		if err := json.Unmarshal(l, &p); err != nil {
			log.Fatal(err)
		}
		if _, err := st.UpsertPublication(p, l); err != nil {
			log.Fatal(err)
		}
	}
	for _, l := range r.meas {
		var m probe.Measurement
		if err := json.Unmarshal(l, &m); err != nil {
			log.Fatal(err)
		}
		if _, err := st.InsertProbe(m, l); err != nil {
			log.Fatal(err)
		}
	}
	if _, err := st.DB().Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		log.Fatal(err)
	}
	if _, err := st.DB().Exec("VACUUM"); err != nil {
		log.Fatal(err)
	}
	pubT, probeT := tableBytes(st.DB(), "publications", "assignments"), tableBytes(st.DB(), "probes")
	st.Close()

	db, err := sql.Open("sqlite", slimDB)
	if err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE tables (body BLOB NOT NULL)`,
		`CREATE TABLE validators (id INTEGER PRIMARY KEY, address BLOB UNIQUE NOT NULL)`,
		`CREATE TABLE publications (id INTEGER PRIMARY KEY, promise_hash BLOB UNIQUE NOT NULL, commitment BLOB NOT NULL, settlement_height INTEGER NOT NULL, body BLOB NOT NULL)`,
		`CREATE INDEX publications_commitment ON publications (commitment)`,
		`CREATE INDEX publications_height ON publications (settlement_height)`,
		`CREATE TABLE probes (id INTEGER PRIMARY KEY, publication INTEGER NOT NULL, validator INTEGER NOT NULL, started_ns INTEGER NOT NULL, class TEXT NOT NULL, body BLOB NOT NULL)`,
		`CREATE INDEX probes_validator_time ON probes (validator, started_ns)`,
		`CREATE INDEX probes_publication ON probes (publication)`,
		`CREATE INDEX probes_class_time ON probes (class, started_ns)`,
	} {
		if _, err := db.Exec(q); err != nil {
			log.Fatal(err)
		}
	}
	tx, _ := db.Begin()
	_, _ = tx.Exec(`INSERT INTO tables VALUES (?)`, tables)
	pubID := map[string]int64{}
	for i, l := range r.pubs {
		var p scan.Publication
		_ = json.Unmarshal(l, &p)
		h, _ := hex.DecodeString(p.PromiseHash)
		c, _ := hex.DecodeString(p.Promise.Commitment)
		res, err := tx.Exec(`INSERT INTO publications (promise_hash, commitment, settlement_height, body) VALUES (?,?,?,?)`, h, c, p.SettlementHeight, ep[i])
		if err != nil {
			log.Fatal(err)
		}
		pubID[p.PromiseHash], _ = res.LastInsertId()
	}
	valID := map[string]int64{}
	for i, l := range r.meas {
		var m probe.Measurement
		_ = json.Unmarshal(l, &m)
		id, ok := valID[m.ValidatorAddress]
		if !ok {
			a, _ := hex.DecodeString(m.ValidatorAddress)
			res, err := tx.Exec(`INSERT INTO validators (address) VALUES (?)`, a)
			if err != nil {
				log.Fatal(err)
			}
			id, _ = res.LastInsertId()
			valID[m.ValidatorAddress] = id
		}
		if _, err := tx.Exec(`INSERT INTO probes (publication, validator, started_ns, class, body) VALUES (?,?,?,?,?)`,
			pubID[m.PromiseHash], id, m.StartedAt.UnixNano(), string(m.Classification), em[i]); err != nil {
			log.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("VACUUM"); err != nil {
		log.Fatal(err)
	}
	sPub, sProbe := tableBytes(db, "publications", "validators", "tables"), tableBytes(db, "probes")
	db.Close()
	fi1, _ := os.Stat(today)
	fi2, _ := os.Stat(slimDB)
	nb := float64(len(r.pubs))
	fmt.Printf("\n== on disk (SQLite, after VACUUM, indexes included)\n")
	fmt.Printf("today's store, these tables: publications+assignments %d, probes %d (whole file %d, with tables these records do not fill)\n", pubT, probeT, fi1.Size())
	fmt.Printf("slim store:                  publications+validators+tables %d, probes %d (whole file %d)\n", sPub, sProbe, fi2.Size())
	fmt.Printf("per blob: today %.1f KB in the database + %.1f KB of record lines; slim %.1f KB in the database, which is the record\n",
		float64(pubT+probeT)/nb/1000, float64(rawLen(r))/nb/1000, float64(sPub+sProbe)/nb/1000)
}

func rawLen(r run) int {
	n := 0
	for _, l := range r.pubs {
		n += len(l)
	}
	for _, l := range r.meas {
		n += len(l)
	}
	return n
}

// tableBytes is the pages of the named tables and their indexes (dbstat).
func tableBytes(db *sql.DB, names ...string) int64 {
	var total int64
	for _, n := range names {
		var b sql.NullInt64
		q := `SELECT SUM(pgsize) FROM dbstat WHERE name = ? OR name IN (SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?)`
		if err := db.QueryRow(q, n, n).Scan(&b); err != nil {
			if strings.Contains(err.Error(), "dbstat") {
				log.Fatal("this sqlite build has no dbstat: ", err)
			}
			log.Fatal(err)
		}
		total += b.Int64
	}
	return total
}
