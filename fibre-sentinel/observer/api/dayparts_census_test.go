package api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The write census: a sealed day is exact only while every write that can
// change what it summed is noticed (dayparts_catchup.go), so every write
// the collector's code makes to the store is listed here with how it is
// noticed, or as a write no partial reads, and each way of noticing names
// the scenario of the equality harness that exercises it
// (dayparts_equiv_test.go, simWitnesses). A write added to the collector
// without a line here fails the test, which is the point: it has to be
// reasoned about before it ships.

// writeMechanism is how the partials notice one kind of write.
type writeMechanism struct {
	how      string
	scenario string // "" for a write no partial reads
}

const notRead = "not read by any partial"

// writeCensus maps every write signature ("INSERT t", "UPDATE t SET a,b",
// "DELETE t", "UPSERT t SET a,b" for ON CONFLICT DO UPDATE, "SCHEMA" for
// the migrations' DDL) to its mechanism.
var writeCensus = map[string]writeMechanism{
	// the rows the row days sum, and the obligations and readable counts of
	// the settlement days
	"INSERT probes": {"probes past the mark: the row's day, its promise's settlement day (span widened)", "a restarted prober's row"},
	"UPDATE probes SET amended_at,classification,classification_at_probe,classification_reason,shadowed_by":                                                       {"probe_amendments past the mark (written in the same transaction)", "an amendment"},
	"UPDATE probes SET classification,classification_at_probe,classification_reason,corrected_at,must_serve_until,must_serve_until_at_probe,phase,phase_at_probe": {"probe_corrections past the mark (written in the same transaction); applied again under its range, which adds no line: every row a correction wrote, fingerprinted every catch-up", "a row correction applied again under its range"},
	"UPDATE probes SET retention_unverified": {"an aggregate of the held rows every catch-up, per promise once it moves", "a held publication"},
	// The prune (rows, heartbeats, decisions, by the day they started) was
	// retired on 2026-10-04 and nothing writes it any more, but a database a
	// build before then pruned carries raw_from and the gap behind it: the
	// partials still follow raw_from and the anchors, and the harness prunes
	// as that build did (pruneLikeBefore, "a prune").
	"DELETE probes":                                  {"the collapse: its decision past the mark (collapsible sets)", "a collapse"},
	"INSERT sampling_decisions":                      {"sampling_decisions past the mark: the collapsible days, the settlement day", "a collapse"},
	"UPDATE sampling_decisions SET must_serve_until": {"the fingerprint of every decision a correction can reach", "a sampled-out point corrected"},
	"INSERT sampling_decision_points":                {"sampling_decision_points past the mark: the points' row days, the settlement day", "a collapse"},
	"UPDATE sampling_decision_points SET phase":      {"the fingerprint of every decision a correction can reach", "a sampled-out point corrected"},
	"INSERT reachability":                            {"reachability past the mark (this observer's own vantage): the row day", "a second vantage's row"},
	"INSERT probe_amendments":                        {"probe_amendments past the mark: the row's day and settlement day", "an amendment"},
	"INSERT probe_corrections":                       {"probe_corrections past the mark: the row's day and settlement day", "a probe verdict corrected"},
	// the ledger, and the settlement days' latest deadlines
	"INSERT publications":                          {"publications past the mark: the ledger, the settlement day's span from its rows, its points' row days", "a publication recorded after its deadline"},
	"INSERT assignments":                           {"written with its publication in one transaction: folded with it", "a publication recorded after its deadline"},
	"UPDATE publications SET retention_unverified": {"an aggregate of the held publications every catch-up, the set once it moves", "a held publication"},
	"UPDATE publications SET corrected_at,must_serve_until,must_serve_until_at_scan,must_serve_until_basis,must_serve_until_basis_at_scan": {"publication_corrections past the mark: the day's latest deadline read again; applied again under its range, which adds no line: the corrected publications fingerprinted", "a publication correction applied again under its range"},
	"INSERT publication_corrections": {"publication_corrections past the mark", "a publication deadline corrected"},
	// the ranges: they move the holds and the fingerprints' reach
	"INSERT param_uncertainty": {"the held rows and publications it raises (diffed); a verified range widens the fingerprints", "a range still holding"},
	"UPSERT param_uncertainty SET heights_read,holds,raw_json,resolution,resolve_error,resolve_method,resolved_at": {"the same", "a range corrected"},
	"UPDATE param_uncertainty SET corrected_at,holds":                                                              {"the holds it lifts, diffed", "a range corrected"},
	// meta: raw_from is read by every catch-up (a database pruned before
	// 2026-10-04 carries it); nothing else a partial reads
	"UPSERT meta SET updated_at,value": {"raw_from, read by every catch-up; no other key is read by a partial", "a prune"},
	"INSERT meta":                      {"the same", "a prune"},
	// the daily rollup: read raw beside the partials (rolledFor), unchanged
	"INSERT obligation_daily": {notRead + " (the rollup, folded in raw by rolledFor)", ""},
	"DELETE obligation_daily": {notRead + " (the rollup, folded in raw by rolledFor)", ""},
	"INSERT probe_daily":      {notRead + " (the rollup, folded in raw by rolledFor)", ""},
	"DELETE probe_daily":      {notRead + " (the rollup, folded in raw by rolledFor)", ""},
	// the migrations: the store's identity names the schema, and a new one
	// begins the partials again
	"SCHEMA": {"the store's identity (schema version): the partials begin again", ""},
	// the migrations' backfills, the same way
	"UPDATE assignments SET settlement_height": {"a migration's backfill: the store's identity (schema version)", ""},
	"UPDATE param_uncertainty SET holds":       {"a migration's backfill: the store's identity (schema version)", ""},
	"UPDATE probes SET clock_offset_ms,retry_first_outcome,sampling_binding,sampling_commitment,sampling_p": {"a migration's backfill: the store's identity (schema version)", ""},
	"UPDATE probes SET shadow_gap":                       {"a migration's backfill: the store's identity (schema version)", ""},
	"UPDATE probes SET rows_subset_of_assignment":        {"a migration's backfill: the store's identity (schema version)", ""},
	"UPDATE publications SET must_serve_until_ambiguous": {"a migration's backfill: the store's identity (schema version)", ""},
}

// notReadTables are tables no partial reads at all: every write to them is
// in the census by table.
var notReadTables = map[string]bool{
	"schema_migrations": true, "observer_runs": true, "ingest_cursors": true, "params_history": true, "endpoints": true,
	"validator_identities": true, "validator_avatars": true, "payments": true, "escrow_accounts": true, "withdrawal_queue": true,
	"host_events": true, "sampling_secrets": true, "probe_confirmations": true, "endpoint_hosting": true, "hosting_sources": true,
}

var (
	reInsert   = regexp.MustCompile(`(?is)\bINSERT\s+(?:OR\s+\w+\s+)?INTO\s+([\w.?]+)`)
	reUpdate   = regexp.MustCompile(`(?is)\bUPDATE\s+([\w.?]+)\s+SET\s+(.*?)(?:\bWHERE\b|$)`)
	reDelete   = regexp.MustCompile(`(?is)\bDELETE\s+FROM\s+([\w.?]+)`)
	reUpsert   = regexp.MustCompile(`(?is)ON\s+CONFLICT\b.*?DO\s+UPDATE\s+SET\s+(.*?)(?:\bWHERE\b|$)`)
	reDDL      = regexp.MustCompile(`(?is)^\s*(ALTER\s+TABLE|CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|VIEW))\b`)
	reSetCol   = regexp.MustCompile(`(?s)(?:^|,)\s*(\w+)\s*=`)
	reTempName = regexp.MustCompile(`(?i)^temp\.`)
)

// flatten is a string expression's text, a non-literal operand standing
// as "?".
func flatten(e ast.Expr) (string, bool) {
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind != token.STRING {
			return "", false
		}
		v, err := strconv.Unquote(x.Value)
		return v, err == nil
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return "", false
		}
		l, lok := flatten(x.X)
		r, rok := flatten(x.Y)
		if !lok && !rok {
			return "", false
		}
		if !lok {
			l = "?"
		}
		if !rok {
			r = "?"
		}
		return l + r, true
	case *ast.ParenExpr:
		return flatten(x.X)
	}
	return "", false
}

func setColumns(set string) string {
	var cols []string
	for _, m := range reSetCol.FindAllStringSubmatch(set, -1) {
		cols = append(cols, m[1])
	}
	sort.Strings(cols)
	return strings.Join(cols, ",")
}

// writeSignatures are the write statements in the Go files of dir.
func writeSignatures(t *testing.T, dir string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	fset := token.NewFileSet()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			e, ok := n.(ast.Expr)
			if !ok {
				return true
			}
			text, ok := flatten(e)
			if !ok {
				return true
			}
			at := filepath.Base(path) + ":" + strconv.Itoa(fset.Position(e.Pos()).Line)
			add := func(sig string) { out[sig] = append(out[sig], at) }
			if reDDL.MatchString(text) {
				add("SCHEMA")
				return false
			}
			table := func(v string) (string, bool) {
				if reTempName.MatchString(v) {
					return "", false
				}
				if strings.Contains(v, "?") {
					return "?", true
				}
				return v, true
			}
			for _, m := range reInsert.FindAllStringSubmatch(text, -1) {
				tb, ok := table(m[1])
				if !ok {
					continue
				}
				add("INSERT " + tb)
				if u := reUpsert.FindStringSubmatch(text); u != nil {
					add("UPSERT " + tb + " SET " + setColumns(u[1]))
				}
			}
			for _, m := range reUpdate.FindAllStringSubmatch(text, -1) {
				if tb, ok := table(m[1]); ok {
					add("UPDATE " + tb + " SET " + setColumns(m[2]))
				}
			}
			for _, m := range reDelete.FindAllStringSubmatch(text, -1) {
				if tb, ok := table(m[1]); ok {
					add("DELETE " + tb)
				}
			}
			return false // an inner literal is part of this one
		})
	}
	return out
}

// TestEveryWriteHasAnInvalidation reads every write the collector's code
// makes to the store (store, rollup, ingest, correct, collect, hosting and
// the collector's own main) and requires each to be in the census.
func TestEveryWriteHasAnInvalidation(t *testing.T) {
	dirs := []string{"../store", "../rollup", "../ingest", "../correct", "../collect", "../hosting", "../../cmd/observer-collector"}
	found := map[string][]string{}
	for _, d := range dirs {
		if _, err := os.Stat(d); err != nil {
			t.Fatal(err)
		}
		for sig, at := range writeSignatures(t, d) {
			found[sig] = append(found[sig], at...)
		}
	}
	scenarios := map[string]bool{}
	for _, w := range simWitnesses {
		scenarios[w.what] = true
	}
	var sigs []string
	for sig := range found {
		sigs = append(sigs, sig)
	}
	sort.Strings(sigs)
	for _, sig := range sigs {
		m, ok := writeCensus[sig]
		if !ok {
			f := strings.Fields(sig)
			if len(f) >= 2 && notReadTables[f[1]] {
				continue
			}
			t.Errorf("%s (%s) has no invalidation entry: say how the day partials notice it, or that no partial reads it", sig, strings.Join(found[sig], ", "))
			continue
		}
		if m.scenario != "" && !scenarios[m.scenario] {
			t.Errorf("%s: its scenario %q is not one the equality harness reaches", sig, m.scenario)
		}
		t.Logf("%-70s %s", sig, m.how)
	}
	// And nothing in the census that the code no longer writes, beyond the
	// few kept for writes the text above cannot see (none today).
	for sig := range writeCensus {
		if _, ok := found[sig]; !ok {
			t.Errorf("the census lists %s, which the code does not write", sig)
		}
	}
	// The writes SQLite makes on its own: cascades, and the rows the views
	// derive. A decision's points go with it, an amendment with its row, and
	// the rows a decision stands for follow its points and the assignments.
	for _, c := range []string{
		"sampling_decision_points: ON DELETE CASCADE from sampling_decisions (the prune of a build before 2026-10-04): raw_from and the anchors (a prune)",
		"probe_amendments: ON DELETE CASCADE from probes (the prune of a build before 2026-10-04): the ladder steps back past it (a prune)",
		"probe_rows / obligation_rows / sampled_out_rows: points × decisions × assignments; each base is followed above (a collapse)",
	} {
		t.Log(c)
	}
}
