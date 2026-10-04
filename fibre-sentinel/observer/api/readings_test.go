package api

import (
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// A publisher's reading counts are the Blobs list's Tensile lane over its
// blobs: every blob it paid for, by the status the reference
// (reconstructable) gives it, available or unavailable, and otherwise in its
// retention window while must_serve_until is ahead and not read once it has
// passed. The memo behind them keeps each status and computes it again only
// when the store says something it rests on changed, so the test changes
// each of those things in turn and holds the counts to the reference after
// every one.
func TestPublisherReadingsAreTheBlobsLane(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{st: st}
	ctx := context.Background()
	db := st.DB()

	full := make([]int, 40)
	for i := range full {
		full[i] = i
	}
	cases := []blobCase{
		{name: "available, window open", needed: 40, total: 160, points: 1, complete: true, vals: []valRows{
			{addr: "a1", rows: full[:20], attested: 1, served: true}, {addr: "a2", rows: full[20:], attested: 1, served: true}}},
		{name: "available, window over", needed: 20, total: 160, points: 1, complete: true, over: true, vals: []valRows{
			{addr: "b1", rows: full[:20], attested: 1, served: true}, {addr: "b2", rows: full[20:], attested: 1, served: false}}},
		{name: "unavailable", needed: 35, total: 160, points: 1, complete: true, over: true, vals: []valRows{
			{addr: "c1", rows: full[:20], attested: 1, served: true}, {addr: "c2", rows: full[20:], attested: 1, served: false}}},
		{name: "in retention window: the prober missed a request", needed: 30, total: 160, points: 1, complete: true, vals: []valRows{
			{addr: "d1", rows: full[:20], attested: 1, served: true}, {addr: "d2", rows: full[20:], attested: 1, missed: true}}},
		{name: "not read: the same once the window closed", needed: 30, total: 160, points: 1, complete: true, over: true, vals: []valRows{
			{addr: "e1", rows: full[:20], attested: 1, served: true}, {addr: "e2", rows: full[20:], attested: 1, missed: true}}},
		{name: "in retention window: no reading yet", needed: 20, total: 160, points: 0, vals: []valRows{
			{addr: "f1", rows: full[:20], attested: 1}, {addr: "f2", rows: full[20:], attested: 1}}},
		{name: "not read: no protocol params, window over", needed: 0, points: 1, complete: true, over: true, vals: []valRows{
			{addr: "g1", rows: full[:20], attested: 1, served: true}}},
		{name: "signed by a key, no settlement on record", needed: 20, total: 160, points: 1, complete: true, over: true, vals: []valRows{
			{addr: "h1", rows: full[:20], attested: 1, served: true}}},
	}
	const pubA, pubB = "celestia1d3mmg652pxj776dyqwlsrc93y64088g6ux8deq", "celestia1zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3shxjgz"
	key := secp256k1.GenPrivKey().PubKey().(*secp256k1.PubKey)
	pubK, err := scan.PublisherOf(hex.EncodeToString(key.Key))
	if err != nil {
		t.Fatal(err)
	}
	hashes := make([]string, len(cases))
	for i, c := range cases {
		hashes[i] = writeBlob(t, st, i, c)
	}
	// The second blob was read three days ago, so the prune below has a day
	// of rows to take; the rest were read today.
	old := time.Now().UTC().Add(-72 * time.Hour)
	if _, err := db.Exec(`UPDATE probes SET scheduled_at = ?, started_at = ?, finished_at = ? WHERE promise_hash = ?`,
		store.TS(old), store.TS(old), store.TS(old), hashes[1]); err != nil {
		t.Fatal(err)
	}
	settle := func(i int, publisher string) {
		t.Helper()
		if _, err := st.UpsertPayment(scan.Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("settle-%d-%s", i, publisher), Kind: "settlement",
			Height: int64(i + 2), Time: time.Now(), Publisher: publisher, PromiseHash: hashes[i], Namespace: "ns", BlobSize: 1024, Denom: "utia", AmountUtia: 1}, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	for i := range cases[:len(cases)-1] {
		if i%2 == 0 {
			settle(i, pubA)
		} else {
			settle(i, pubB)
		}
	}
	// the last is counted for the key that signed its promise, as paidBy counts it
	if _, err := db.Exec(`UPDATE publications SET signer_public_key = ? WHERE promise_hash = ?`, hex.EncodeToString(key.Key), hashes[len(cases)-1]); err != nil {
		t.Fatal(err)
	}

	// What the Blobs list's lane says of every blob, from the reference, for
	// the account paidBy names.
	want := func() map[string]readingCounts {
		t.Helper()
		now := time.Now()
		out := map[string]readingCounts{}
		for _, h := range hashes {
			owner := ""
			for _, p := range []string{pubA, pubB, pubK} {
				where, args, err := s.paidBy(ctx, p)
				if err != nil {
					t.Fatal(err)
				}
				var n int
				if err := db.QueryRow(`SELECT COUNT(*) FROM publications WHERE promise_hash = ? AND (`+where+`)`, append([]any{h}, args...)...).Scan(&n); err != nil {
					t.Fatal(err)
				}
				if n > 0 {
					owner = p
				}
			}
			if owner == "" {
				continue
			}
			rc, err := s.reconstructable(ctx, h, asOfPin{now: now})
			if err != nil {
				t.Fatal(err)
			}
			var msu string
			if err := db.QueryRow(`SELECT must_serve_until FROM publications WHERE promise_hash = ?`, h).Scan(&msu); err != nil {
				t.Fatal(err)
			}
			c := out[owner]
			switch {
			case rc.Status == verdict.BlobAvailable:
				c.Available++
			case rc.Status == verdict.BlobUnavailable:
				c.Unavailable++
			case parseStoreTime(msu).After(now):
				c.InRetentionWindow++
			default:
				c.NotRead++
			}
			out[owner] = c
		}
		return out
	}
	check := func(step string) map[string]readingCounts {
		t.Helper()
		rows, err := s.publisherRows(ctx, windowFor("all", time.Now()), "")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.readings.update(ctx, s, 0); err != nil {
			t.Fatal(err)
		}
		s.attachReadings(rows)
		got := map[string]readingCounts{}
		for _, r := range rows {
			if r.Readings == nil {
				t.Fatalf("%s: %s has no readings", step, r.Publisher)
			}
			got[r.Publisher] = *r.Readings
		}
		// the key's account moved no escrow on record, so it has no row
		if c, ok := s.readings.counts(time.Now())[pubK]; ok {
			got[pubK] = c
		}
		if exp := want(); fmt.Sprint(got) != fmt.Sprint(exp) {
			t.Errorf("%s:\n got  %v\n want %v (the Blobs lane)", step, got, exp)
		}
		return got
	}

	c := check("first computation")
	if c[pubA].Available == 0 || c[pubA].Unavailable == 0 || c[pubA].NotRead == 0 || c[pubB].InRetentionWindow < 2 || c[pubK].Available != 1 {
		t.Fatalf("the fixture no longer covers every count: %+v", c)
	}

	// A deadline corrected into the past: the blob with no reading yet is not
	// read any more.
	if _, err := st.ApplyPublicationCorrection(store.Correction{PromiseHash: hashes[5], UncertaintyID: "u1",
		ToMustServeUntil: time.Now().Add(-time.Minute), ToBasis: "test", Reason: "test", JudgedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	check("a deadline correction")

	// Its reading lands after all: the rows come back.
	at := time.Now().Add(-2 * time.Minute)
	for _, v := range cases[5].vals {
		insertEndRow(t, st, hashes[5], v.addr, v.rows, at)
	}
	check("a reading added")

	// The prober's missed request is judged again (an amendment): the reading
	// happened, and the rows are short.
	if _, err := st.ApplyAmendment(store.Amendment{DedupeKey: hashes[3] + "-d2-0", PromiseHash: hashes[3], ValidatorAddress: "d2",
		From: "NOT_PROBED", To: "FAULT", Reason: "test", JudgedAt: time.Now(), ScannerFrontier: time.Now()}); err != nil {
		t.Fatal(err)
	}
	check("an amendment")

	// A settlement recorded for the blob counted by its signer's key moves it
	// to the account the chain charged.
	settle(len(cases)-1, pubB)
	check("a settlement recorded")

	// A prune deletes whole days of rows and moves raw_from past them. The
	// observer no longer prunes (2026-10-04), but the memo still has to
	// follow raw_from for a database pruned before then: the blob read three
	// days ago is not read any more, and it is the only publication computed
	// again, the one with a row before raw_from.
	from := time.Now().UTC().Add(-48 * time.Hour).Truncate(24 * time.Hour)
	if res, err := db.Exec(`DELETE FROM probes WHERE started_at < ?`, store.TS(from)); err != nil {
		t.Fatal(err)
	} else if n, _ := res.RowsAffected(); n != int64(len(cases[1].vals)) {
		t.Fatalf("the prune took %d rows, want the old blob's %d", n, len(cases[1].vals))
	}
	if err := st.SetMeta("raw_from", from.Format("2006-01-02"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if c := check("a prune"); c[pubB].NotRead == 0 {
		t.Fatalf("the pruned blob is still read: %+v", c)
	}
	if s.readings.computed != 1 {
		t.Errorf("a prune computed %d publications again, want the one with rows before raw_from", s.readings.computed)
	}
	// raw_from moved back (a store rebuilt from an export): every one.
	if err := st.SetMeta("raw_from", from.Add(-24*time.Hour).Format("2006-01-02"), time.Now()); err != nil {
		t.Fatal(err)
	}
	check("raw_from moved back")
	if s.readings.computed != len(cases) {
		t.Errorf("raw_from moved back computed %d publications again, want all %d", s.readings.computed, len(cases))
	}

	// The newest probe row deleted and its rowid used again by another row,
	// one that turns the blob with the missed request Unavailable: the
	// rowid did not move, the row under it did.
	var top int64
	if err := db.QueryRow(`SELECT MAX(rowid) FROM probes`).Scan(&top); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM probes WHERE rowid = ?`, top); err != nil {
		t.Fatal(err)
	}
	insertEndRow(t, st, hashes[4], "e2", full[20:], time.Now().Add(-3*time.Hour))
	var again int64
	if err := db.QueryRow(`SELECT MAX(rowid) FROM probes`).Scan(&again); err != nil {
		t.Fatal(err)
	}
	if again != top {
		t.Fatalf("the newest rowid was not used again (%d, was %d): this no longer tests a reused rowid", again, top)
	}
	check("a reused rowid")
}

// A request does no reading work: before the memo's first computation its
// rows go out with no readings, and after it with the memo's counts.
func TestRequestsDoNotWaitForTheFirstReadingCount(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{st: st}
	rows := []publisherRow{{Publisher: "celestia1d3mmg652pxj776dyqwlsrc93y64088g6ux8deq"}}
	if s.attachReadings(rows); rows[0].Readings != nil {
		t.Fatalf("a request before the first count: readings %+v", rows[0].Readings)
	}
	if err := s.readings.update(context.Background(), s, 0); err != nil {
		t.Fatal(err)
	}
	if s.attachReadings(rows); rows[0].Readings == nil || *rows[0].Readings != (readingCounts{}) {
		t.Fatalf("a request after it: readings %+v", rows[0].Readings)
	}
}

// A full computation of the memo never holds a market computation up for
// longer than its slice: it computes a batch at least, goes on at the next
// update, and the rows carry no reading until it has landed. A later one (a
// migration) leaves the counts the memo had while it runs.
func TestReadingsFullComputationGoesBySlices(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{st: st}
	s.readings.chunk = 2
	ctx := context.Background()
	db := st.DB()
	const pub = "celestia1d3mmg652pxj776dyqwlsrc93y64088g6ux8deq"
	full := make([]int, 20)
	for i := range full {
		full[i] = i
	}
	blob := func(i int) {
		t.Helper()
		h := writeBlob(t, st, i, blobCase{needed: 20, total: 80, points: 1, complete: true, over: true,
			vals: []valRows{{addr: fmt.Sprintf("v%d", i), rows: full, attested: 1, served: true}}})
		if _, err := st.UpsertPayment(scan.Payment{SchemaVersion: 1, DedupeKey: fmt.Sprintf("s%d", i), Kind: "settlement", Height: int64(i + 2),
			Time: time.Now(), Publisher: pub, PromiseHash: h, Namespace: "ns", BlobSize: 1024, Denom: "utia", AmountUtia: 1}, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		blob(i)
	}
	available := func() int64 {
		t.Helper()
		rows := []publisherRow{{Publisher: pub}}
		if s.attachReadings(rows); rows[0].Readings == nil {
			return -1
		}
		return rows[0].Readings.Available
	}
	step := func(want int64, computed int) {
		t.Helper()
		if err := s.readings.update(ctx, s, time.Nanosecond); err != nil {
			t.Fatal(err)
		}
		if got := available(); got != want || s.readings.computed != computed {
			t.Fatalf("available %d after computing %d, want %d after %d (-1: no reading)", got, s.readings.computed, want, computed)
		}
	}
	step(-1, 2)
	step(-1, 2)
	step(5, 1)
	step(5, 0)

	blob(5)
	if _, err := db.Exec(`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`, store.SchemaVersion+1, store.TS(time.Now())); err != nil {
		t.Fatal(err)
	}
	step(5, 2)
	step(5, 2)
	step(6, 2)
	step(6, 0)
}

// What the memo runs on every market computation must not walk a table
// that grows with every blob: the newest row of each table it watches is
// one step from the end of the rowid order, a mark is checked by rowid, the
// rows above a mark are a rowid range, and a batch's publications are
// sought by their hash. The batch's own statements are reconstructBatch's,
// joined against the list passed in.
func TestReadingMemoQueriesUseIndexes(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	plan := func(name, q string, args []any, scans []string, want ...string) {
		t.Helper()
		p, err := st.QueryPlan(ctx, q, args...)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		joined := strings.Join(p, "\n")
		if bad := store.FullScans(p, scans, nil); len(bad) > 0 {
			t.Errorf("%s walks a whole table or index: %v\nplan:\n%s", name, bad, joined)
		}
		for _, w := range want {
			if !strings.Contains(joined, w) {
				t.Errorf("%s: plan does not use %q\nplan:\n%s", name, w, joined)
			}
		}
		if strings.HasPrefix(name, "newest") && strings.Contains(joined, "TEMP B-TREE FOR ORDER BY") {
			t.Errorf("%s sorts the table instead of reading its end\nplan:\n%s", name, joined)
		}
	}
	for _, tb := range readingTables {
		// a scan in rowid order that stops at its first row
		plan("newest "+tb.name, tb.newestSQL(), nil, []string{tb.name}, "SCAN "+tb.name)
		plan("mark "+tb.name, tb.atSQL(), []any{1}, nil, "USING INTEGER PRIMARY KEY (rowid=?)")
		plan("changed "+tb.name, tb.changed, []any{1, 2}, nil, "USING INTEGER PRIMARY KEY (rowid>? AND rowid<?)")
	}
	plan("facts", readingFactsSQL, []any{"[]"}, []string{"json_each"},
		"SEARCH pub USING INDEX sqlite_autoindex_publications_1 (promise_hash=?)", "SEARCH pay USING INDEX payments_promise (promise_hash=?)")
	plan("oldest rows", readingOldestSQL, []any{"[]"}, []string{"json_each"}, "COVERING INDEX probes_promise (promise_hash=?)")
	plan("batch selection", blobSel(readingSel, readingChunk)+`SELECT p.promise_hash, COUNT(*) FROM probes p JOIN sel ON sel.promise_hash = p.promise_hash GROUP BY p.promise_hash`,
		[]any{"[]"}, []string{"json_each", "sel"},
		"SEARCH publications USING INDEX sqlite_autoindex_publications_1 (promise_hash=?)", "probes_promise (promise_hash=?)")
}

// insertEndRow writes one row of a blob's end-of-window reading at the
// scheduled time given, in which validator returned rows, verified.
func insertEndRow(t *testing.T, st *store.Store, hash, validator string, rows []int, at time.Time) {
	t.Helper()
	idx := make([]string, len(rows))
	for i, r := range rows {
		idx[i] = fmt.Sprint(r)
	}
	var msu string
	if err := st.DB().QueryRow(`SELECT must_serve_until FROM publications WHERE promise_hash = ?`, hash).Scan(&msu); err != nil {
		t.Fatal(err)
	}
	ts := store.TS(at)
	if _, err := st.DB().Exec(`INSERT INTO probes (
		dedupe_key, vantage, promise_hash, commitment, blob_version, must_serve_until,
		validator_set_height, validator_address, validator_host, assigned,
		assigned_row_count, schedule_label, scheduled_at, started_at, finished_at,
		lateness_ms, dns_ok, dns_ms, tcp_ok, tcp_ms, tls_ok, tls_ms, tls_version,
		peer_cert_sha256, identity_ok, identity_reason, download_ok, download_ms,
		rows_returned, rows_expected, commitment_verified, assignment_verified,
		phase, outcome, classification, classification_reason, raw_error,
		total_duration_ms, raw_json, attested, row_indices
	) VALUES (?, 'v1', ?, ?, 0, ?, 1, ?, 'h:1', 1, ?, ?, ?, ?, ?, 0,
		1,1,1,1,1,1,'TLS1.3','', 1,'', 1, 1, ?, ?, 1, 1,
		'in_window', 'SERVED_OK', 'HEALTHY', '', '', 1, '{}', 1, ?)`,
		hash+"-"+validator+"-"+probe.EndReadLabel+"-"+ts, hash, hash, msu, validator, len(rows), probe.EndReadLabel, ts, ts, ts,
		len(rows), len(rows), "["+strings.Join(idx, ",")+"]"); err != nil {
		t.Fatal(err)
	}
}
