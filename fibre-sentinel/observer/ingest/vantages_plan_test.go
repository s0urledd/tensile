package ingest

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The chain guard's validator test is a few index seeks however many rows
// this observer's heartbeat has written. A validator the chain does not
// know, the case the guard exists for, must not walk the own vantage's
// rows: the file stops at that row and the same test runs on every pass.
func TestTheChainGuardSeeksTheValidatorNotTheVantage(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "o.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows, err := st.DB().Query(`EXPLAIN QUERY PLAN `+knownValidatorSQL, "aa", "", "aa", "ut-1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	var reach string
	for _, d := range plan {
		if strings.HasPrefix(d, "SCAN ") && d != "SCAN CONSTANT ROW" {
			t.Errorf("the guard's plan scans a table: %q", d)
		}
		if strings.Contains(d, "reachability") {
			reach = d
		}
	}
	if !strings.Contains(reach, "reachability_validator_time (validator_address=?)") {
		t.Fatalf("the guard's plan: %q, want a search of reachability_validator_time by validator", plan)
	}
}
