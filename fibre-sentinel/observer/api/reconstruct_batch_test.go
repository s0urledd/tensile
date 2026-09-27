package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The batched reconstructability path exists for speed, so the only thing worth
// testing about it is that it did not buy speed with a different answer. Every
// case below is built to land on a particular branch and is compared field by
// field against the reference, except served_distinct_rows, which the batch
// deliberately does not compute: the summary is its only caller and does not
// publish it, and not computing it is exactly what lets the bounds replace the
// row lists.
//
// The case that matters most is "ambiguous". The bounded path decides from
// sigma_rows - distinct_rows, and when a blob's served rows land within that
// many of the threshold the bounds straddle it and the code must fall back to
// the real union rather than guess. If that fallback ever breaks, this is the
// test that says so.

type valRows struct {
	addr     string
	rows     []int
	attested any // 1, 0 or nil
	served   bool
	nullRows bool
	failLast bool // served at every point but the last, where it answers NOT_FOUND
}

type blobCase struct {
	name     string
	needed   int64 // original_rows; 0 means "no protocol params recorded"
	total    int64
	vals     []valRows
	points   int  // how many in-window schedule points to write
	complete bool // whether every assigned validator has a result at the last point
	want     string
}

func writeBlob(t *testing.T, st *store.Store, idx int, c blobCase) string {
	t.Helper()
	db := st.DB()
	hash := fmt.Sprintf("%064x", idx+1)
	now := time.Now().UTC()
	msu := store.TS(now.Add(2 * time.Hour))

	sigma, seen := 0, map[int]struct{}{}
	for _, v := range c.vals {
		sigma += len(v.rows)
		for _, r := range v.rows {
			seen[r] = struct{}{}
		}
	}
	raw := "{}"
	if c.needed > 0 {
		b, err := json.Marshal(map[string]any{"assignment": map[string]any{
			"protocol_params": map[string]any{"original_rows": c.needed, "total_rows": c.total}}})
		if err != nil {
			t.Fatal(err)
		}
		raw = string(b)
	}
	if _, err := db.Exec(`INSERT INTO publications (
		promise_hash, commitment, blob_version, blob_size, namespace, chain_id,
		promise_height, creation_timestamp, signer, signer_public_key,
		validator_signature_count, settlement_height, settlement_time,
		settlement_tx_hash, settlement_tx_index, settlement_tx_code,
		must_serve_until, must_serve_until_basis, shard_retention_s,
		payment_promise_timeout_s, assignment_error, protocol_params_fingerprint,
		pinned_celestia_app, validator_set_height, total_voting_power, sigma_rows,
		distinct_rows, wrap_overlaps, validators_with_rows, recorded_at, raw_json
	) VALUES (?,?,0,1024,'ns','test',?,?,'signer','pk',0,?,?,'tx',0,0,?,'shard_retention',7200,3600,'','','',?,0,?,?,0,?,?,?)`,
		hash, hash, idx+1, store.TS(now), idx+2, store.TS(now), msu, idx+2,
		sigma, len(seen), len(c.vals), store.TS(now), raw); err != nil {
		t.Fatal(err)
	}

	for _, v := range c.vals {
		var rj any
		if !v.nullRows {
			b, err := json.Marshal(v.rows)
			if err != nil {
				t.Fatal(err)
			}
			rj = string(b)
		}
		if _, err := db.Exec(`INSERT INTO assignments
			(promise_hash, validator_address, voting_power, row_count, rows_json, attested)
			VALUES (?,?,?,?,?,?)`,
			hash, v.addr, 100, len(v.rows), rj, v.attested); err != nil {
			t.Fatal(err)
		}
	}

	for p := 0; p < c.points; p++ {
		at := store.TS(now.Add(time.Duration(p) * time.Minute))
		last := p == c.points-1
		for i, v := range c.vals {
			// An incomplete blob is one where somebody has no result at the
			// last point, which is what "pending" means: a gap in observation,
			// not a validator that failed to serve.
			if last && !c.complete && i == len(c.vals)-1 {
				continue
			}
			outcome, class := "NOT_FOUND", "FAULT"
			if v.served && !(last && v.failLast) {
				outcome, class = "SERVED_OK", "HEALTHY"
			}
			key := fmt.Sprintf("%s-%s-%d", hash, v.addr, p)
			if _, err := db.Exec(`INSERT INTO probes (
				dedupe_key, vantage, promise_hash, commitment, blob_version, must_serve_until,
				validator_set_height, validator_address, validator_host, assigned,
				assigned_row_count, schedule_label, scheduled_at, started_at, finished_at,
				lateness_ms, dns_ok, dns_ms, tcp_ok, tcp_ms, tls_ok, tls_ms, tls_version,
				peer_cert_sha256, identity_ok, identity_reason, download_ok, download_ms,
				rows_returned, rows_expected, commitment_verified, assignment_verified,
				phase, outcome, classification, classification_reason, raw_error,
				total_duration_ms, raw_json, attested
			) VALUES (?, 'v1', ?, ?, 0, ?, 1, ?, 'h:1', 1, ?, ?, ?, ?, ?, 0,
				1,1,1,1,1,1,'TLS1.3','', 1,'', ?, 1, ?, ?, 1, 1,
				'in_window', ?, ?, '', '', 1, '{}', ?)`,
				key, hash, hash, msu, v.addr, len(v.rows), fmt.Sprintf("w%d", p+1),
				at, at, at, boolInt(v.served && !(last && v.failLast)), len(v.rows), len(v.rows),
				outcome, class, v.attested); err != nil {
				t.Fatal(err)
			}
		}
	}
	return hash
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestReconstructBatchMatchesReference(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{st: st}
	ctx := context.Background()

	// Disjoint rows: sigma == distinct, so excess is 0 and the bounds are exact.
	full := []int{}
	for i := 0; i < 40; i++ {
		full = append(full, i)
	}
	cases := []blobCase{{
		name:   "yes: every proven-obliged validator served, rows well over the threshold",
		needed: 40, total: 160, points: 2, complete: true, want: "yes",
		vals: []valRows{
			{addr: "a1", rows: full[:20], attested: 1, served: true},
			{addr: "a2", rows: full[20:], attested: 1, served: true},
		},
	}, {
		name:   "degraded: rows all came back, but a proven-obliged validator stayed quiet",
		needed: 20, total: 160, points: 2, complete: true, want: "degraded",
		vals: []valRows{
			{addr: "b1", rows: full[:20], attested: 1, served: true},
			{addr: "b2", rows: full[20:], attested: 1, served: false},
		},
	}, {
		name:   "no: fewer rows came back than the blob needs",
		needed: 35, total: 160, points: 2, complete: true, want: "no",
		vals: []valRows{
			{addr: "c1", rows: full[:20], attested: 1, served: true},
			{addr: "c2", rows: full[20:], attested: 1, served: false},
		},
	}, {
		name:   "pending: no point where every assigned validator has a result",
		needed: 20, total: 160, points: 1, complete: false, want: "pending",
		vals: []valRows{
			{addr: "d1", rows: full[:20], attested: 1, served: true},
			{addr: "d2", rows: full[20:], attested: 1, served: true},
		},
	}, {
		name:   "unknown: a served validator has no recorded row list",
		needed: 20, total: 160, points: 2, complete: true, want: "unknown",
		vals: []valRows{
			{addr: "e1", rows: full[:20], attested: 1, served: true, nullRows: true},
			{addr: "e2", rows: full[20:], attested: 1, served: true},
		},
	}, {
		name:   "unknown: the publication recorded no protocol params",
		needed: 0, total: 0, points: 2, complete: true, want: "unknown",
		vals: []valRows{
			{addr: "f1", rows: full[:20], attested: 1, served: true},
			{addr: "f2", rows: full[20:], attested: 1, served: true},
		},
	}, {
		// Attestation unknown for the whole assignment: the reference falls
		// back to the assigned set for "whole", and so must the batch.
		name:   "yes with attestation unrecorded: the assigned set is the denominator",
		needed: 20, total: 160, points: 2, complete: true, want: "yes",
		vals: []valRows{
			{addr: "g1", rows: full[:20], attested: nil, served: true},
			{addr: "g2", rows: full[20:], attested: nil, served: true},
		},
	}, {
		// A quiet validator that the promise does NOT prove was obliged must
		// not demote the verdict.
		name:   "yes: the only quiet validator was never proven to owe the blob",
		needed: 20, total: 160, points: 2, complete: true, want: "yes",
		vals: []valRows{
			{addr: "h1", rows: full[:20], attested: 1, served: true},
			{addr: "h2", rows: full[20:], attested: 0, served: false},
		},
	}}

	// Three of four validators failing at the newest point is a point the
	// correlated-failure guard calls suspect, and every rate leaves it out.
	// The verdict is drawn at the newest point that is complete and not
	// suspect; judged at the suspect one it read "no" (one validator's ten
	// rows against twenty needed) on what is as likely the observer's own
	// trouble.
	cases = append(cases, blobCase{
		name:   "suspect: the newest complete point is excluded, the one before it judges",
		needed: 20, total: 160, points: 2, complete: true, want: "yes",
		vals: []valRows{
			{addr: "j1", rows: full[:10], attested: 1, served: true},
			{addr: "j2", rows: full[10:20], attested: 1, served: true, failLast: true},
			{addr: "j3", rows: full[20:30], attested: 1, served: true, failLast: true},
			{addr: "j4", rows: full[30:40], attested: 1, served: true, failLast: true},
		},
	}, blobCase{
		name:   "suspect: the only point is excluded, nothing is judged",
		needed: 20, total: 160, points: 1, complete: true, want: "unknown",
		vals: []valRows{
			{addr: "k1", rows: full[:10], attested: 1, served: true},
			{addr: "k2", rows: full[10:20], attested: 1, served: true, failLast: true},
			{addr: "k3", rows: full[20:30], attested: 1, served: true, failLast: true},
			{addr: "k4", rows: full[30:40], attested: 1, served: true, failLast: true},
		},
	})

	// The ambiguous band. Both validators hold row 0..19, so sigma is 40 and
	// distinct is 20: excess is 20. One serves, so the bounds on the union are
	// [20-20, 20] = [0, 20] and needed is 20 — they straddle it exactly, and
	// only the real union (20) answers. This is the fallback's reason to exist.
	cases = append(cases, blobCase{
		name:   "ambiguous: overlapping assignments put the bounds either side of the threshold",
		needed: 20, total: 160, points: 2, complete: true, want: "degraded",
		vals: []valRows{
			{addr: "i1", rows: full[:20], attested: 1, served: true},
			{addr: "i2", rows: full[:20], attested: 1, served: false},
		},
	})

	// An end-of-window reading of the endorsed validators alone
	// (sentinel-probe -end-read) leaves the validators the promise does not
	// name as signers without a row. They owe the blob nothing, so the point
	// is complete once every endorsed validator has a result; a missing
	// endorsed validator still leaves it pending.
	cases = append(cases, blobCase{
		name:   "complete without the unendorsed: every endorsed validator was read",
		needed: 20, total: 160, points: 1, complete: false, want: "yes",
		vals: []valRows{
			{addr: "l1", rows: full[:20], attested: 1, served: true},
			{addr: "l2", rows: full[20:], attested: 0, served: false},
		},
	}, blobCase{
		name:   "pending: an endorsed validator has no result yet",
		needed: 20, total: 160, points: 1, complete: false, want: "pending",
		vals: []valRows{
			{addr: "m1", rows: full[:20], attested: 0, served: true},
			{addr: "m2", rows: full[20:], attested: 1, served: true},
		},
	})

	hashes := make([]string, len(cases))
	for i, c := range cases {
		hashes[i] = writeBlob(t, st, i, c)
	}

	got, err := s.reconstructBatch(ctx, "", len(cases), asOfPin{now: time.Now()})
	if err != nil {
		t.Fatalf("reconstructBatch: %v", err)
	}

	for i, c := range cases {
		h := hashes[i]
		ref, err := s.reconstructable(ctx, h, asOfPin{now: time.Now()})
		if err != nil {
			t.Fatalf("%s: reference: %v", c.name, err)
		}
		if ref.Status != c.want {
			t.Errorf("%s: reference says %q, the case expects %q — the case is wrong or the reference changed",
				c.name, ref.Status, c.want)
		}
		b := got[h]
		if b == nil {
			t.Errorf("%s: batch returned nothing", c.name)
			continue
		}
		// Field by field, because a status that agrees by luck while the counts
		// behind it disagree is still a defect. served_distinct_rows is outside
		// the batch's contract and is normalised away on both sides: the batch
		// leaves it at zero, except on a blob that fell into the ambiguous band
		// and was answered by the reference verbatim.
		want, mine := *ref, *b
		want.ServedRows, mine.ServedRows = 0, 0
		if mine != want {
			t.Errorf("%s: batch disagrees with the reference\n batch: %+v\n  ref: %+v", c.name, mine, want)
		}
	}
}

// The selection the batch queries join against must be the same rows, in the
// same order, that blobRows itself returns — otherwise a page could show a
// verdict computed for a different publication.
func TestReconstructBatchHonoursTheSelection(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{st: st}
	ctx := context.Background()

	simple := blobCase{needed: 20, total: 160, points: 2, complete: true,
		vals: []valRows{{addr: "x1", rows: []int{1, 2, 3}, attested: 1, served: true}}}
	var hashes []string
	for i := 0; i < 5; i++ {
		hashes = append(hashes, writeBlob(t, st, i, simple))
	}

	// settlement_height is idx+2, so the newest two are the last two written.
	got, err := s.reconstructBatch(ctx, "", 2, asOfPin{now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("limit 2 returned %d verdicts", len(got))
	}
	for _, h := range hashes[3:] {
		if got[h] == nil {
			t.Errorf("newest publication %s missing from a limit-2 selection", h[:8])
		}
	}
	for _, h := range hashes[:3] {
		if got[h] != nil {
			t.Errorf("older publication %s should not be in a limit-2 selection", h[:8])
		}
	}

	one, err := s.reconstructBatch(ctx, "promise_hash = ?", 1, asOfPin{now: time.Now()}, hashes[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[hashes[0]] == nil {
		t.Fatalf("a where clause did not select its own publication: %v", one)
	}
}

// A pinned window must be answered from what was known at the pin. The
// publication selection was bounded by as_of but the probe queries behind the
// reconstructability verdict were not, so a blob that was still in flight at
// the pinned moment was judged with evidence that arrived afterwards. With a
// four-hour retention window on mocha, every blob is in that state for four
// hours after it settles, and the answer given was a statement about how it
// was served, drawn from probes that had not happened yet.
func TestReconstructBatchHonoursTheAsOfPin(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{st: st}
	ctx := context.Background()
	full := make([]int, 40)
	for i := range full {
		full[i] = i
	}
	// Two validators, two points, complete and fully served: live, this is
	// "yes". The probes are written at now and now+1m.
	c := blobCase{needed: 20, total: 160, points: 2, complete: true,
		vals: []valRows{
			{addr: "p1", rows: full[:20], attested: 1, served: true},
			{addr: "p2", rows: full[20:], attested: 1, served: true},
		}}
	hash := writeBlob(t, st, 0, c)

	live, err := s.reconstructBatch(ctx, "", 1, asOfPin{now: time.Now()})
	if err != nil {
		t.Fatalf("live: %v", err)
	}
	if live[hash] == nil || live[hash].Status != "yes" {
		t.Fatalf("live status = %+v, want yes", live[hash])
	}

	// Pinned to a minute before the first probe: nothing had been observed,
	// so there is nothing to say.
	before := time.Now().UTC().Add(-time.Minute)
	pinned, err := s.reconstructBatch(ctx, "", 1, asOfPin{at: store.TS(before), now: before})
	if err != nil {
		t.Fatalf("pinned: %v", err)
	}
	if pinned[hash] != nil && pinned[hash].Status != "unknown" {
		t.Fatalf("pinned before any probe: status = %q, want unknown or absent — the verdict was drawn from rows that did not exist yet",
			pinned[hash].Status)
	}

	// The reference must agree with the batch at the same pin.
	ref, err := s.reconstructable(ctx, hash, asOfPin{at: store.TS(before), now: before})
	if err != nil {
		t.Fatalf("reference pinned: %v", err)
	}
	if ref.Status != "unknown" {
		t.Fatalf("reference pinned status = %q, want unknown", ref.Status)
	}
	// And window_over is asked at the pin, not at the clock: the deadline is
	// two hours out, so it had not passed then and has not passed now.
	if ref.WindowOver {
		t.Error("window_over is true at a pin two hours before the deadline")
	}
}

// The blob page names the points no verdict counts, tallied as the verdict
// tallies them, and a publication with nothing assigned is empty lists, not
// nulls a page has to guard against.
func TestBlobDetailListsItsSuspectPointsAndNoNullLists(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	srv := New(st, "test")
	ts := httptest.NewServer(srv)
	defer func() { ts.Close(); srv.Close() }()

	full := []int{}
	for i := 0; i < 40; i++ {
		full = append(full, i)
	}
	suspect := writeBlob(t, st, 0, blobCase{needed: 20, total: 160, points: 2, complete: true, vals: []valRows{
		{addr: "j1", rows: full[:10], attested: 1, served: true},
		{addr: "j2", rows: full[10:20], attested: 1, served: true, failLast: true},
		{addr: "j3", rows: full[20:30], attested: 1, served: true, failLast: true},
		{addr: "j4", rows: full[30:40], attested: 1, served: true, failLast: true},
	}})
	empty := writeBlob(t, st, 1, blobCase{needed: 20, total: 160})

	fetch := func(hash string) map[string]json.RawMessage {
		t.Helper()
		resp, err := http.Get(ts.URL + "/v1/blobs/" + hash)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("/v1/blobs/%s: %d", hash, resp.StatusCode)
		}
		var out map[string]json.RawMessage
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	var pts []struct {
		Label  string `json:"label"`
		Reason string `json:"reason"`
	}
	got := fetch(suspect)
	if err := json.Unmarshal(got["suspect_points"], &pts); err != nil || len(pts) != 1 || pts[0].Label != "w2" || pts[0].Reason != "fault" {
		t.Fatalf("suspect_points: %s", got["suspect_points"])
	}
	var blob struct {
		Reconstructable struct {
			Status string `json:"status"`
			Point  string `json:"point"`
		} `json:"reconstructable"`
	}
	if err := json.Unmarshal(got["blob"], &blob); err != nil || blob.Reconstructable.Status != "yes" || blob.Reconstructable.Point != "w1" {
		t.Fatalf("verdict beside the suspect point: %+v (%v)", blob.Reconstructable, err)
	}

	got = fetch(empty)
	for _, k := range []string{"assignments", "probes", "suspect_points"} {
		if string(got[k]) != "[]" {
			t.Errorf("%s: %s, want []", k, got[k])
		}
	}
}
