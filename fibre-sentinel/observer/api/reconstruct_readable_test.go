package api

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The network's blob status figure is drawn from publications with a reading
// on record. A blob is read near the end of its retention window, so under a
// burst of blobs the newest reconstructSample publications were all still
// waiting for theirs, or had closed without one while the prober was behind,
// and the figure read "none read" beside blobs read and found Available.
// Here more publications are waiting than the bound allows; the figure must
// still find the ones that were read, count the unread ones apart, and say
// truthfully how many it examined.
func TestReconstructableCoversPublicationsThatCanHaveAVerdict(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	db := st.DB()
	s := &Server{st: st}
	ctx := context.Background()
	now := time.Now().UTC()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	pub := func(i int, settled, msu time.Time) string {
		hash := fmt.Sprintf("%064x", i)
		if _, err := tx.Exec(`INSERT INTO publications (
			promise_hash, commitment, blob_version, blob_size, namespace, chain_id,
			promise_height, creation_timestamp, signer, signer_public_key,
			validator_signature_count, settlement_height, settlement_time,
			settlement_tx_hash, settlement_tx_index, settlement_tx_code,
			must_serve_until, must_serve_until_basis, shard_retention_s,
			payment_promise_timeout_s, assignment_error, protocol_params_fingerprint,
			pinned_celestia_app, validator_set_height, total_voting_power, sigma_rows,
			distinct_rows, wrap_overlaps, validators_with_rows, recorded_at, raw_json
		) VALUES (?,?,0,1024,'ns','test',?,?,'signer','pk',0,?,?,'tx',0,0,?,'shard_retention',14400,3600,'','','',?,0,40,40,0,2,?,
			'{"assignment":{"protocol_params":{"original_rows":20,"total_rows":80}}}')`,
			hash, hash, i, store.TS(settled), i+1, store.TS(settled), store.TS(msu), i, store.TS(settled)); err != nil {
			t.Fatal(err)
		}
		rows := [][]int{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, {20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39}}
		for j, v := range []string{"a1", "a2"} {
			rj := "["
			for k, r := range rows[j] {
				if k > 0 {
					rj += ","
				}
				rj += fmt.Sprint(r)
			}
			rj += "]"
			if _, err := tx.Exec(`INSERT INTO assignments (promise_hash, validator_address, voting_power, row_count, rows_json, attested, settlement_height)
				VALUES (?,?,100,20,?,1,?)`, hash, v, rj, i+1); err != nil {
				t.Fatal(err)
			}
		}
		return hash
	}
	// read writes one validator's end-of-window reading: its 20 rows came
	// back verified, or it answered "no such shard".
	read := func(hash, val string, at, msu time.Time, served bool) {
		outcome, class, returned, verified := "NOT_FOUND", "FAULT", 0, 0
		if served {
			outcome, class, returned, verified = "SERVED_OK", "HEALTHY", 20, 1
		}
		if _, err := tx.Exec(`INSERT INTO probes (
				dedupe_key, vantage, promise_hash, commitment, blob_version, must_serve_until,
				validator_set_height, validator_address, validator_host, assigned,
				assigned_row_count, schedule_label, scheduled_at, started_at, finished_at,
				lateness_ms, dns_ok, dns_ms, tcp_ok, tcp_ms, tls_ok, tls_ms, tls_version,
				peer_cert_sha256, identity_ok, identity_reason, download_ok, download_ms,
				rows_returned, rows_expected, commitment_verified, assignment_verified,
				phase, outcome, classification, classification_reason, raw_error,
				total_duration_ms, raw_json, attested
			) VALUES (?, 'v1', ?, ?, 0, ?, 1, ?, 'h:1', 1, 20, 'end', ?, ?, ?, 0,
				1,1,1,1,1,1,'TLS1.3','', 1,'', ?, 1, ?, 20, ?, ?,
				'in_window', ?, ?, '', '', 1, '{}', 1)`,
			hash+"-"+val, hash, hash, store.TS(msu), val, store.TS(at), store.TS(at), store.TS(at),
			verified, returned, verified, verified, outcome, class); err != nil {
			t.Fatal(err)
		}
	}

	// Three blobs read before their retention ended, and found whole.
	readAt, readMsu := now.Add(-90*time.Minute), now.Add(-time.Hour)
	for i := 1; i <= 3; i++ {
		h := pub(i, now.Add(-5*time.Hour), readMsu)
		read(h, "a1", readAt, readMsu, true)
		read(h, "a2", readAt, readMsu, true)
	}
	// Two whose retention ended with no reading: not read by Tensile.
	for i := 4; i <= 5; i++ {
		pub(i, now.Add(-5*time.Hour), readMsu)
	}
	// One whose reading has an answer but no verdict yet: a1 said "no such
	// shard" and a2's rows could still make it whole. Pending.
	half := pub(6, now.Add(-30*time.Minute), now.Add(3*time.Hour))
	read(half, "a1", now.Add(-10*time.Minute), now.Add(3*time.Hour), false)
	// And more still waiting for their reading than the sample bound holds,
	// all newer than every one above.
	waiting := reconstructSample + 100
	for i := 0; i < waiting; i++ {
		pub(100+i, now.Add(-time.Duration(waiting-i)*time.Second), now.Add(3*time.Hour))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := s.reconstructableCount(ctx, windowFor("24h", now))
	if err != nil {
		t.Fatal(err)
	}
	if got.PublicationsInWindow != int64(6+waiting) {
		t.Errorf("publications_in_window = %d, want %d", got.PublicationsInWindow, 6+waiting)
	}
	if got.NotYetRead != int64(waiting) {
		t.Errorf("not_yet_read = %d, want %d", got.NotYetRead, waiting)
	}
	if got.Examined != 4 || got.SampleLimit != reconstructSample {
		t.Errorf("examined %d of limit %d, want 4 of %d: the three read, and the one read without a verdict yet", got.Examined, got.SampleLimit, reconstructSample)
	}
	if got.Yes != 3 || got.NotRead != 2 || got.Pending != 1 || got.No != 0 || got.Unknown != 0 {
		t.Errorf("statuses yes %d not_read %d pending %d no %d unknown %d, want 3/2/1/0/0", got.Yes, got.NotRead, got.Pending, got.No, got.Unknown)
	}
	if got.Recoverable.Num != 3 || got.Recoverable.Den != 3 {
		t.Errorf("recoverable = %d of %d, want 3 of 3: the blobs read are not hidden behind the ones waiting", got.Recoverable.Num, got.Recoverable.Den)
	}

	// Pinned before the readings and before those retention windows ended,
	// the same publications were all still waiting.
	pin := now.Add(-2 * time.Hour)
	pinned := Window{Name: "24h", Span: 24 * time.Hour, Start: pin.Add(-24 * time.Hour), End: pin, AsOf: true}
	got, err = s.reconstructableCount(ctx, pinned)
	if err != nil {
		t.Fatal(err)
	}
	if got.PublicationsInWindow != 5 || got.NotYetRead != 5 || got.NotRead != 0 || got.Examined != 0 || got.Recoverable.Den != 0 {
		t.Errorf("pinned before the readings: in window %d, not yet read %d, examined %d, recoverable den %d; want 5, 5, 0, 0",
			got.PublicationsInWindow, got.NotYetRead, got.Examined, got.Recoverable.Den)
	}
}
