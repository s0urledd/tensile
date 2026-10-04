package api_test

import (
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// fullVal is one endorser of a full reading insertFullReading writes, and
// its answers in order, one row each (Measurement.Attempt), 90 s apart.
type fullVal struct {
	addr  string
	rows  int
	tries []wire
	// owedLast: its last answer on record still owed it another attempt,
	// which is not on record (the prober stopped and never wrote it).
	owedLast bool
}

// insertFullReading writes one publication read once, as a full reading,
// needing needed distinct rows.
func insertFullReading(t *testing.T, st *store.Store, hash string, created, msu time.Time, needed int, at time.Time, vals []fullVal) {
	t.Helper()
	var assigned []scan.ValidatorAssignment
	held := map[string][]uint32{}
	next := 0
	for _, v := range vals {
		var rows []int
		for i := 0; i < v.rows; i++ {
			rows = append(rows, next)
			held[v.addr] = append(held[v.addr], uint32(next))
			next++
		}
		assigned = append(assigned, scan.ValidatorAssignment{Address: v.addr, VotingPower: 10, RowCount: v.rows, Rows: rows, Attested: true})
	}
	pub := scan.Publication{
		SchemaVersion: scan.AttestationSchemaVersion, PromiseHash: hash,
		SettlementHeight: 300, SettlementTime: created, MustServeUntil: msu, RecordedAt: created,
		SettlementTxHash: "tx" + hash, Signer: "celestia1pub",
		Promise:                 scan.PromiseFields{ChainID: "t", Height: 299, Commitment: "cc" + hash, CreationTimestamp: created, BlobSize: 4096},
		ValidatorSignatureCount: len(vals),
		Assignment: scan.AssignmentTable{
			ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: needed, TotalRows: 4 * needed},
			ValidatorSetHeight: 299, TotalVotingPower: int64(10 * len(vals)), Sigma: next, Distinct: next,
			ValidatorsWithRows: len(vals), AttestedWithRows: len(vals), SignatureEntries: len(vals), SignaturesVerified: len(vals),
			AttestedVotingPower: int64(10 * len(vals)), Validators: assigned,
		},
	}
	raw, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertPublication(pub, raw); err != nil {
		t.Fatal(err)
	}
	for i, v := range vals {
		for k, w := range v.tries {
			start := at.Add(time.Duration(i)*time.Second + time.Duration(k)*90*time.Second)
			// WRONG_ROWS here is genuine rows of the blob that are not the
			// validator's own: verified, and no settled promise's.
			verified := w.outcome == probe.OutcomeServedOK || w.outcome == probe.OutcomeWrongRows
			class, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: w.outcome,
				CommitmentVerified: verified})
			m := probe.Measurement{
				SchemaVersion: probe.AttestationSchemaVersion, Vantage: "test",
				PromiseHash: hash, Commitment: "cc" + hash, MustServeUntil: msu, ValidatorSetHeight: 299,
				ValidatorAddress: v.addr, ValidatorHost: v.addr + ":443",
				Assigned: true, Attested: true, AssignedRowCount: v.rows,
				ScheduleLabel: probe.FullReadLabel, ScheduledAt: at, StartedAt: start, FinishedAt: start.Add(time.Second), Attempt: k,
				Phase: probe.PhaseInWindow, Outcome: w.outcome,
				Classification: class, ClassificationReason: reason, TotalDurationMS: 10,
			}
			m.TCP.OK, m.TLS.OK, m.Identity.OK = w.tls, w.tls, w.tls
			if w.outcome == probe.OutcomeServedOK {
				m.Download.OK, m.Download.RowsReturned, m.Download.RowsExpected = true, v.rows, v.rows
				m.Download.CommitmentVerified, m.Download.AssignmentVerified = true, true
				m.Download.RowIndices = held[v.addr]
			}
			if w.outcome == probe.OutcomeWrongRows {
				m.Download.RowsReturned, m.Download.RowsExpected, m.Download.CommitmentVerified = v.rows, v.rows, true
				for r := 0; r < v.rows; r++ {
					m.Download.RowIndices = append(m.Download.RowIndices, uint32(1000+r))
				}
			}
			// As the prober writes it: every answer that did not serve and
			// was made, but the last, says when the next attempt is due.
			if (k < len(v.tries)-1 || v.owedLast) && w.outcome != probe.OutcomeMissed && !verified {
				due := m.FinishedAt.Add(90 * time.Second)
				m.NextAttemptDue = &due
			}
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.InsertProbe(m, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// The API judges a full reading as the rule does, everywhere it says
// served or not served: each endorser on its own answers on a blob that is
// Available all the same. Not served is the last answer of a validator that
// never served (on /v1/probes?served=no, a blob's assignments, its readings,
// and the obligations); a validator that served when asked again is
// served; one whose answers include this observer's gap counts neither way;
// the answers a later one replaced count neither way, and each row says
// which attempt it is.
func TestTheAPIJudgesAFullReadingOnEachEndorsersAnswers(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	created, msu := now.Add(-5*time.Hour), now.Add(-time.Hour)
	missed := wire{probe.OutcomeMissed, false}
	insertFullReading(t, st, "fr1", created, msu, 4, msu.Add(-10*time.Minute), []fullVal{
		{addr: "whole", rows: 4, tries: []wire{ok}},
		{addr: "later", rows: 2, tries: []wire{gone, ok}},
		{addr: "never", rows: 2, tries: []wire{gone, refused, err500}},
		{addr: "toolate", rows: 2, tries: []wire{gone, missed}},
		{addr: "ourside", rows: 2, tries: []wire{err500, local, err500}},
		{addr: "foreign", rows: 2, tries: []wire{gone, {probe.OutcomeWrongRows, true}}},
		{addr: "owedgone", rows: 2, tries: []wire{gone}, owedLast: true},
	})
	ts := httptestServer(t, st)

	var notServed struct {
		Probes []struct {
			ValidatorAddress string `json:"validator_address"`
			Attempt          int    `json:"attempt"`
			Reason           string `json:"classification_reason"`
		} `json:"probes"`
	}
	if code := get(t, ts, "/v1/probes?served=no&limit=100", &notServed); code != 200 {
		t.Fatalf("served=no: %d", code)
	}
	if len(notServed.Probes) != 1 || notServed.Probes[0].ValidatorAddress != "never" || notServed.Probes[0].Attempt != 2 ||
		!strings.HasPrefix(notServed.Probes[0].Reason, "not served: this validator's own rows did not come back") {
		t.Fatalf("served=no: %+v, want the last answer of the validator that never served", notServed.Probes)
	}

	var blob struct {
		Blob struct {
			Reconstructable struct {
				Status string `json:"status"`
			} `json:"reconstructable"`
		} `json:"blob"`
		Assignments []struct {
			ValidatorAddress string `json:"validator_address"`
			Service          string `json:"service"`
		} `json:"assignments"`
		Probes []struct {
			ValidatorAddress string `json:"validator_address"`
			Attempt          int    `json:"attempt"`
			Service          string `json:"service"`
			NextAttemptDue   string `json:"next_attempt_due"`
		} `json:"probes"`
	}
	if code := get(t, ts, "/v1/blobs/fr1", &blob); code != 200 {
		t.Fatalf("blob: %d", code)
	}
	// A row after which an attempt is still owed says when it is due; the
	// last answer of a validator asked three times owes none.
	for _, p := range blob.Probes {
		owes := (p.ValidatorAddress == "never" && p.Attempt < 2) || p.ValidatorAddress == "owedgone" ||
			(p.Attempt == 0 && (p.ValidatorAddress == "later" || p.ValidatorAddress == "toolate" || p.ValidatorAddress == "foreign")) ||
			(p.ValidatorAddress == "ourside" && p.Attempt < 2)
		if (p.NextAttemptDue != "") != owes {
			t.Errorf("reading of %s, attempt %d: next_attempt_due %q, want one: %v", p.ValidatorAddress, p.Attempt, p.NextAttemptDue, owes)
		}
	}
	if blob.Blob.Reconstructable.Status != "yes" {
		t.Errorf("blob status %q, want yes", blob.Blob.Reconstructable.Status)
	}
	want := map[string]string{"whole": "served", "later": "served", "never": "not_served", "toolate": "", "ourside": "", "foreign": "", "owedgone": ""}
	for _, a := range blob.Assignments {
		if a.Service != want[a.ValidatorAddress] {
			t.Errorf("assignment %s: service %q, want %q", a.ValidatorAddress, a.Service, want[a.ValidatorAddress])
		}
	}
	readings := map[string][]string{}
	for _, p := range blob.Probes {
		readings[p.ValidatorAddress] = append(readings[p.ValidatorAddress], p.Service)
	}
	attempts := map[string][]int{}
	for _, p := range blob.Probes {
		attempts[p.ValidatorAddress] = append(attempts[p.ValidatorAddress], p.Attempt)
	}
	for v, w := range map[string][]string{
		"whole": {"served"}, "later": {"", "served"}, "never": {"", "", "not_served"}, "toolate": {"", ""}, "ourside": {"", "", ""},
		"foreign": {"", ""}, "owedgone": {""},
	} {
		got := readings[v]
		// newest first on the page; put them back in attempt order
		as := attempts[v]
		order := make([]int, len(got))
		for i := range order {
			order[i] = i
		}
		sort.Slice(order, func(i, j int) bool { return as[order[i]] < as[order[j]] })
		var inOrder []string
		for _, i := range order {
			inOrder = append(inOrder, got[i])
		}
		if strings.Join(inOrder, ",") != strings.Join(w, ",") {
			t.Errorf("readings of %s by attempt: %q, want %q", v, inOrder, w)
		}
		sort.Ints(as)
		for i, a := range as {
			if a != i {
				t.Errorf("readings of %s: attempts %v", v, as)
				break
			}
		}
	}

	var vals struct {
		Validators []struct {
			Address     string          `json:"address"`
			Obligations obligationsJSON `json:"obligations"`
		} `json:"validators"`
	}
	if code := get(t, ts, "/v1/validators?window=all", &vals); code != 200 {
		t.Fatalf("validators: %d", code)
	}
	wantObl := map[string]obligationsJSON{
		"whole": {Total: 1, Served: 1}, "later": {Total: 1, Served: 1}, "never": {Total: 1, Broken: 1},
		"toolate": {Total: 1, NotCounted: 1}, "ourside": {Total: 1, NotCounted: 1},
		"foreign": {Total: 1, NotCounted: 1}, "owedgone": {Total: 1, NotCounted: 1},
	}
	for _, v := range vals.Validators {
		g := v.Obligations
		g.Rate = struct{ Num, Den int64 }{}
		if w, ok := wantObl[v.Address]; ok && g != w {
			t.Errorf("%s: %+v, want %+v", v.Address, g, w)
		}
	}
	partsAfter(t, st, "test", time.Time{})
}
