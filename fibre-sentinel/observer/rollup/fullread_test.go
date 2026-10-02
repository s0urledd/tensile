package rollup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// answer is one of a validator's answers at a full reading.
type answer struct {
	out probe.Outcome
	cls probe.Classification // "" is probe.Classify's
	got []uint32             // rows that came back verified
}

// fullVal is one endorser of a full reading and its answers, in order.
type fullVal struct {
	name       string
	holds      []int
	answers    []answer
	unendorsed bool
}

func ans(out probe.Outcome) answer { return answer{out: out} }

// asked is a validator asked as often as outs says.
func asked(name string, holds []int, outs ...probe.Outcome) fullVal {
	v := fullVal{name: name, holds: holds}
	for _, o := range outs {
		a := ans(o)
		if o == probe.OutcomeServedOK {
			a.got = indices(holds)
		}
		v.answers = append(v.answers, a)
	}
	return v
}

func indices(rows []int) []uint32 {
	out := make([]uint32, len(rows))
	for i, r := range rows {
		out[i] = uint32(r)
	}
	return out
}

// fullBlob records one publication and its full reading under label: each
// validator's answers one after another, 90 s apart, every one a row of its
// own (Measurement.Attempt). It returns the promise hash.
func (r *readings) fullBlob(needed, total int, label string, vals ...fullVal) string {
	r.t.Helper()
	r.n++
	hash := fmt.Sprintf("%064x", r.n)
	settled := r.settled.Add(time.Duration(r.n) * time.Minute)
	msu := settled.Add(4 * time.Hour)
	at := msu.Add(-10 * time.Minute)
	p := scan.Publication{SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash, SettlementHeight: int64(100 + r.n), SettlementTime: settled,
		MustServeUntil: msu, Promise: scan.PromiseFields{Commitment: hash, Height: int64(99 + r.n), CreationTimestamp: settled}}
	p.Assignment.ProtocolParams.OriginalRows, p.Assignment.ProtocolParams.TotalRows = needed, total
	p.Assignment.ProtocolParams.Fingerprint = fmt.Sprintf("fp-%d-%d", needed, total)
	distinct := map[int]bool{}
	for _, v := range vals {
		p.Assignment.Validators = append(p.Assignment.Validators, scan.ValidatorAssignment{Address: v.name, VotingPower: 10, RowCount: len(v.holds), Rows: v.holds, Attested: !v.unendorsed})
		p.Assignment.Sigma += len(v.holds)
		for _, x := range v.holds {
			distinct[x] = true
		}
	}
	p.Assignment.Distinct, p.Assignment.ValidatorsWithRows = len(distinct), len(p.Assignment.Validators)
	raw, _ := json.Marshal(p)
	if _, err := r.st.UpsertPublication(p, raw); err != nil {
		r.t.Fatal(err)
	}
	r.pubs = append(r.pubs, p)
	for i, v := range vals {
		for k, a := range v.answers {
			start := at.Add(time.Duration(i)*time.Second + time.Duration(k)*90*time.Second)
			cls := a.cls
			if cls == "" {
				cls, _ = probe.Classify(probe.Evidence{Assigned: true, Attested: !v.unendorsed, Phase: probe.PhaseInWindow, Outcome: a.out,
					CommitmentVerified: len(a.got) > 0})
			}
			m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: "ut-1", PromiseHash: hash, Commitment: hash,
				MustServeUntil: msu, ValidatorAddress: v.name, ValidatorHost: v.name + ":7980", Assigned: true, Attested: !v.unendorsed,
				AssignedRowCount: len(v.holds), ScheduleLabel: label, ScheduledAt: at, StartedAt: start, FinishedAt: start.Add(time.Second),
				Attempt: k, Phase: probe.PhaseInWindow, Outcome: a.out, Classification: cls}
			m.TCP.OK = connected(a.out)
			m.TLS.OK = m.TCP.OK && a.out != probe.OutcomeTLSFail
			m.Download.RowsExpected = len(v.holds)
			if len(a.got) > 0 {
				m.Download.RowIndices, m.Download.RowsReturned, m.Download.CommitmentVerified = a.got, len(a.got), true
			}
			raw, _ := json.Marshal(m)
			if _, err := r.st.InsertProbe(m, raw); err != nil {
				r.t.Fatal(err)
			}
			r.ms = append(r.ms, m)
		}
	}
	return hash
}

// fullFixture is every shape of full reading the rule tells apart, read
// after probe.FullReadSince.
func fullFixture(t *testing.T) (*readings, map[string]string) {
	r := newReadings(t)
	r.settled = probe.FullReadSince.Add(24 * time.Hour)
	hashes := map[string]string{}
	nf, ok := probe.OutcomeNotFound, probe.OutcomeServedOK
	short := fullVal{name: "short", holds: rowsFrom(20, 4)}
	for i := 0; i < 3; i++ {
		short.answers = append(short.answers, answer{out: probe.OutcomePartial, cls: probe.ClassUnmatchedGenuine, got: []uint32{20}})
	}
	shadowed := fullVal{name: "shadowed", holds: rowsFrom(24, 4),
		answers: []answer{{out: probe.OutcomePartial, cls: probe.ClassShadowedShard, got: []uint32{24, 25}}}}
	deferred := fullVal{name: "deferred", holds: rowsFrom(28, 2),
		answers: []answer{{out: probe.OutcomePartial, cls: probe.ClassProbeError, got: []uint32{28}}}}
	other := asked("other", rowsFrom(30, 2), nf)
	other.unendorsed = true
	// Available: two validators rebuild it; every other endorser is judged
	// on its own answers all the same.
	hashes["available"] = r.fullBlob(8, 64, probe.FullReadLabel,
		asked("whole1", rowsFrom(0, 4), ok), asked("whole2", rowsFrom(4, 4), ok),
		asked("later", rowsFrom(8, 2), nf, ok),
		asked("never", rowsFrom(10, 2), nf, nf, nf),
		asked("busy", rowsFrom(12, 2), probe.OutcomeThrottled, probe.OutcomeThrottled, ok),
		asked("toolate", rowsFrom(14, 2), nf, probe.OutcomeMissed),
		asked("ourdns", rowsFrom(16, 2), probe.OutcomeTCPTimeout, probe.OutcomeProbeError, probe.OutcomeTCPTimeout),
		asked("mixed", rowsFrom(18, 2), probe.OutcomeServerError, probe.OutcomeRPCTimeout, nf),
		short, shadowed, deferred,
		asked("corrupt", rowsFrom(32, 2), probe.OutcomeInvalidRows, probe.OutcomeInvalidRows, probe.OutcomeInvalidRows),
		asked("badcert", rowsFrom(34, 2), probe.OutcomeIdentityFail, probe.OutcomeIdentityFail, probe.OutcomeIdentityFail),
		asked("nohost", rowsFrom(36, 2), probe.OutcomeNoHost, probe.OutcomeNoHost, probe.OutcomeNoHost),
		asked("refused", rowsFrom(38, 2), probe.OutcomeTCPRefused, probe.OutcomeTCPRefused, probe.OutcomeTCPRefused),
		other)
	// Unavailable: the rule is the same, whatever the blob came to.
	hashes["unavailable"] = r.fullBlob(8, 32, probe.FullReadLabel,
		asked("u-some", rowsFrom(0, 2), ok), asked("u-gone", rowsFrom(2, 4), nf, nf, nf),
		asked("u-late", rowsFrom(6, 2), probe.OutcomeRPCTimeout, probe.OutcomeRPCTimeout, ok),
		asked("u-ours", rowsFrom(8, 2), probe.OutcomeProbeError, probe.OutcomeProbeError, probe.OutcomeProbeError),
		asked("u-notmade", rowsFrom(10, 2), nf, probe.OutcomeMissed))
	// Not read: not one request reached a server, attempts included.
	hashes["noserver"] = r.fullBlob(8, 32, probe.FullReadLabel,
		asked("n-down", rowsFrom(0, 4), probe.OutcomeTCPTimeout, probe.OutcomeTCPTimeout, probe.OutcomeTCPTimeout),
		asked("n-nohost", rowsFrom(4, 2), probe.OutcomeNoHost, probe.OutcomeNoHost, probe.OutcomeNoHost),
		asked("n-ours", rowsFrom(6, 2), probe.OutcomeProbeError),
		asked("n-dns", rowsFrom(8, 2), probe.OutcomeDNSFail, probe.OutcomeTCPUnreachable, probe.OutcomeTCPTimeout))
	// The end label of the readings made between the full-reading deploy
	// and the attempts: one answer each, a full reading all the same.
	hashes["endafter"] = r.fullBlob(4, 16, probe.EndReadLabel,
		asked("e-whole", rowsFrom(0, 4), ok), asked("e-gone", rowsFrom(4, 2), nf), asked("e-slow", rowsFrom(6, 2), probe.OutcomeRPCTimeout),
		asked("e-ours", rowsFrom(8, 2), probe.OutcomeProbeError))
	return r, hashes
}

// sqlCounted is CountedClass and NotServedSQL over every stored row, by
// dedupe key.
func sqlCounted(t *testing.T, r *readings) map[string]string {
	t.Helper()
	q, err := r.st.DB().QueryContext(context.Background(), `SELECT dedupe_key, `+rollup.CountedClass("probes")+`, `+
		rollup.NotServedSQL("probes")+` FROM probes`)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	out := map[string]string{}
	for q.Next() {
		var k, c string
		var notServed bool
		if err := q.Scan(&k, &c, &notServed); err != nil {
			t.Fatal(err)
		}
		if notServed != (c == "FAULT") {
			t.Errorf("%s: CountedClass %q, not served %v", k, c, notServed)
		}
		out[k] = c
	}
	return out
}

// Every answer of a full reading counts the same in SQL (rollup.CountedClass
// and NotServedSQL) and in the Go twin, and as the rule says: served by an
// answer that served, not served by the last answer when every one failed
// its own way, nothing counted where one was this observer's gap or where
// the reading reached no server, whatever the blob came to.
func TestTheSQLAndTheGoTwinCountFullReadingsTheSame(t *testing.T) {
	r, hashes := fullFixture(t)
	rows, _, blobs := r.twin()
	sqlCls := sqlCounted(t, r)
	byPoint := map[string][]verdict.Row{}
	for _, row := range rows {
		byPoint[row.PromiseHash] = append(byPoint[row.PromiseHash], row)
	}
	byVal := map[string][]string{}
	for i, row := range rows {
		rd := verdict.ReadingOf(byPoint[row.PromiseHash], blobs[row.PromiseHash])
		want := string(row.CountedClass(rd))
		key := r.ms[i].DedupeKey()
		if got := sqlCls[key]; got != want {
			t.Errorf("%s %s attempt %d: SQL %q, Go %q", row.PromiseHash[60:], row.Validator, r.ms[i].Attempt, got, want)
		}
		byVal[row.Validator] = append(byVal[row.Validator], sqlCls[key])
	}
	nc, h, f, pe := string(verdict.NotCounted), "HEALTHY", "FAULT", "PROBE_ERROR"
	for v, want := range map[string][]string{
		"whole1": {h}, "later": {nc, h}, "never": {nc, nc, f}, "busy": {nc, nc, h}, "toolate": {nc, "NOT_PROBED"},
		"ourdns": {nc, pe, nc}, "mixed": {nc, nc, f}, "short": {nc, nc, f}, "shadowed": {h}, "deferred": {pe},
		"corrupt": {nc, nc, f}, "badcert": {nc, nc, f}, "nohost": {nc, nc, f}, "refused": {nc, nc, f}, "other": {"UNATTESTED"},
		"u-some": {h}, "u-gone": {nc, nc, f}, "u-late": {nc, nc, h}, "u-ours": {pe, pe, pe}, "u-notmade": {nc, "NOT_PROBED"},
		"n-down": {nc, nc, nc}, "n-nohost": {nc, nc, nc}, "n-ours": {pe}, "n-dns": {nc, nc, nc},
		"e-whole": {h}, "e-gone": {f}, "e-slow": {f}, "e-ours": {pe},
	} {
		if got := fmt.Sprint(byVal[v]); got != fmt.Sprint(want) {
			t.Errorf("%s: %s, want %s", v, got, fmt.Sprint(want))
		}
	}
	for blob, want := range map[string]string{
		"available": verdict.BlobAvailable, "unavailable": verdict.BlobUnavailable, "noserver": verdict.BlobNotRead, "endafter": verdict.BlobAvailable,
	} {
		h := hashes[blob]
		if got := verdict.BlobReading(byPoint[h], blobs[h], false); got.Status != want {
			t.Errorf("%s: %s, want %s", blob, got.Status, want)
		}
	}
}

// The obligations the day's rollup draws from full readings agree with the
// Go twin's, validator by validator, and say what the rule says.
func TestTheSQLAndTheGoTwinAgreeOnFullReadingObligations(t *testing.T) {
	r, _ := fullFixture(t)
	ctx := context.Background()
	rows, settled, blobs := r.twin()
	now := r.settled.Add(20 * 24 * time.Hour)
	if _, err := rollup.Run(ctx, r.st, now, rollup.Config{RollupAfter: 14 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	rolled, err := rollup.Load(ctx, r.st.DB(), now, "")
	if err != nil {
		t.Fatal(err)
	}
	_, byVal := verdict.ComputeObligations(rows, settled, verdict.Window{All: true, End: now}, blobs)
	names := map[string]bool{}
	for a := range byVal {
		names[a] = true
	}
	for a := range rolled.ObligationsByVal {
		names[a] = true
	}
	var sorted []string
	for a := range names {
		sorted = append(sorted, a)
	}
	sort.Strings(sorted)
	for _, a := range sorted {
		o := rolled.ObligationsByVal[a]
		got := verdict.Obligations{Total: o.Total, Served: o.Served, Broken: o.Broken, HeldParamUnverified: o.HeldParamUnverified,
			NotCounted: o.NotCounted(), Pending: o.Pending}
		if got != byVal[a] {
			t.Errorf("%s: rolled %+v, Go %+v", a, got, byVal[a])
		}
	}
	want := map[string]string{}
	for _, v := range []string{"whole1", "whole2", "later", "busy", "shadowed", "u-some", "u-late", "e-whole"} {
		want[v] = "served"
	}
	for _, v := range []string{"never", "mixed", "short", "corrupt", "badcert", "nohost", "refused", "u-gone", "e-gone", "e-slow"} {
		want[v] = "not served"
	}
	for _, v := range []string{"toolate", "ourdns", "deferred", "u-ours", "u-notmade", "n-down", "n-nohost", "n-ours", "n-dns", "e-ours"} {
		want[v] = "counted neither way"
	}
	for v, w := range want {
		b := byVal[v]
		got := "counted neither way"
		switch {
		case b.Total != 1:
			got = fmt.Sprintf("%d obligations", b.Total)
		case b.Served == 1:
			got = "served"
		case b.Broken == 1:
			got = "not served"
		case b.NotCounted != 1:
			got = fmt.Sprintf("%+v", b)
		}
		if got != w {
			t.Errorf("%s: %s, want %s", v, got, w)
		}
	}
	if _, ok := byVal["other"]; ok {
		t.Error("a validator that did not endorse has an obligation")
	}
}

// Every cell of (classification, outcome, held, verified, endorsed, phase)
// counts the same in SQL and in the Go twin at a full reading, under its
// own label and under the end label from probe.FullReadSince on: each row
// a reading of its own, so the reading reached a server exactly when that
// row did.
func TestTheSQLAndTheGoTwinCountEveryFullReadingCellTheSame(t *testing.T) {
	st := openStore(t)
	db := st.DB()
	ctx := context.Background()
	type cell struct {
		key                string
		label              string
		cls                probe.Classification
		out                probe.Outcome
		held, verified     bool
		assigned, attested bool
		phase              probe.Phase
	}
	var cells []cell
	i := 0
	for _, label := range []string{probe.FullReadLabel, probe.EndReadLabel} {
		for _, phase := range []probe.Phase{probe.PhaseInWindow, probe.PhaseGrace} {
			for _, who := range [][2]bool{{true, true}, {true, false}} {
				for _, c := range probe.AllClassifications {
					for _, o := range probe.AllOutcomes {
						for _, held := range []bool{false, true} {
							for _, verified := range []bool{false, true} {
								i++
								cells = append(cells, cell{key: "f" + strconv.Itoa(i), label: label, cls: c, out: o, held: held, verified: verified,
									assigned: who[0], attested: who[1], phase: phase})
							}
						}
					}
				}
			}
		}
	}
	b := func(v bool) int {
		if v {
			return 1
		}
		return 0
	}
	start := probe.FullReadSince.Add(time.Hour)
	ts := start.UTC().Format("2006-01-02T15:04:05.000000000Z")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cells {
		if _, err := tx.ExecContext(ctx, `INSERT INTO probes
			(dedupe_key, vantage, promise_hash, commitment, blob_version, must_serve_until, validator_set_height,
			 validator_address, validator_host, assigned, attested, assigned_row_count, schedule_label, scheduled_at, started_at,
			 finished_at, lateness_ms, dns_ok, dns_ms, tcp_ok, tcp_ms, tls_ok, tls_ms, identity_ok, download_ok,
			 download_ms, rows_returned, rows_expected, commitment_verified, assignment_verified, phase, outcome,
			 classification, total_duration_ms, raw_json, retention_unverified)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			c.key, "t", c.key, "c", 0, ts, 1,
			"v", "h", b(c.assigned), b(c.attested), 2, c.label, ts, ts,
			ts, 0, 1, 0, b(connected(c.out)), 0, 1, 0, 1, 1,
			0, 2, 2, b(c.verified), b(c.verified), string(c.phase), string(c.out),
			string(c.cls), 10, "{}", b(c.held)); err != nil {
			t.Fatalf("insert %s: %v", c.key, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `SELECT dedupe_key, `+rollup.CountedClass("probes")+`, `+rollup.NotServedSQL("probes")+` FROM probes`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var k, c string
		var notServed bool
		if err := rows.Scan(&k, &c, &notServed); err != nil {
			t.Fatal(err)
		}
		if notServed != (c == "FAULT") {
			t.Errorf("%s: CountedClass %q, not served %v", k, c, notServed)
		}
		got[k] = c
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(cells) {
		t.Fatalf("read %d rows back, inserted %d", len(got), len(cells))
	}
	for _, c := range cells {
		r := verdict.Row{PromiseHash: c.key, Validator: "v", ScheduleLabel: c.label, ScheduledAt: start, StartedAt: start, Classification: c.cls,
			Outcome: c.out, RetentionUnverified: c.held, CommitmentVerified: c.verified, Assigned: c.assigned, Attested: c.attested, Phase: c.phase,
			TCPOK: connected(c.out), RowsReturned: 2, AssignedRowCount: 2}
		want := r.CountedClass(verdict.ReadingOf([]verdict.Row{r}, verdict.BlobFacts{}))
		if got[c.key] != string(want) {
			t.Errorf("%s (%s, %s, held=%v, verified=%v, attested=%v, %s): SQL says %q, the Go twin says %q",
				c.label, c.cls, c.out, c.held, c.verified, c.attested, c.phase, got[c.key], want)
		}
	}
}
