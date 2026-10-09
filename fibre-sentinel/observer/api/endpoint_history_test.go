package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// ehStep is a host_events row of the walk.
func ehStep(h int64, fromTx int, host, source, hash string) hostStep {
	return hostStep{FromHeight: h, FromTxIndex: fromTx, Host: host, Source: source, Time: time.Unix(1_700_000_000+h, 0).UTC(), TxHash: hash}
}

// ehRow writes a row in one line: its outcome, block/tx, host<-previous,
// then first, attempted and #hash where it has them.
func ehRow(e endpointEvent) string {
	s := e.Outcome
	if e.Height != 0 {
		s += fmt.Sprintf(" %d", e.Height)
	}
	if e.TxIndex != nil {
		s += fmt.Sprintf("/%d", *e.TxIndex)
	}
	s += " " + e.Host
	if e.PreviousHost != "" {
		s += "<-" + e.PreviousHost
	}
	if e.First {
		s += " first"
	}
	if e.Attempted != "" {
		s += " " + e.Attempted
	}
	if e.TxHash != "" {
		s += " #" + e.TxHash
	}
	return s
}

func ehRows(rows []endpointEvent) string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = ehRow(r)
	}
	return strings.Join(out, " | ")
}

// The walk: a registration, a change and the same address again after the
// endpoint the record began with; a first registration after an explicit
// none; a change a scan gap hid, a first registration it hid, and a gap
// that hid nothing; failures merged in by block and index, a change or a
// registration by what was in force at their block.
func TestEndpointRowsWalkTheRegistrations(t *testing.T) {
	ev, seed, none, reseed := scan.HostFromEvent, scan.HostFromSeed, scan.HostNone, scan.HostFromReseed
	for _, c := range []struct {
		name  string
		steps []hostStep
		fails []failedSetHost
		want  string
	}{
		{"seed then events", []hostStep{ehStep(1, -1, "h1", seed, ""), ehStep(10, 1, "h2", ev, "a"), ehStep(12, 1, "h2", ev, "b")}, nil,
			"same 12/0 h2 #b | changed 10/0 h2<-h1 #a | before_record h1"},
		{"seed none then an event", []hostStep{ehStep(1, -1, "", none, ""), ehStep(10, 1, "h1", ev, "")}, nil,
			"registered 10/0 h1"},
		{"no seed, an event first", []hostStep{ehStep(10, 3, "h1", ev, "")}, nil,
			"registered 10/2 h1"},
		{"a gap that hid a change", []hostStep{ehStep(1, -1, "h1", seed, ""), ehStep(20, -1, "h2", reseed, "")}, nil,
			"after_gap 20 h2<-h1 | before_record h1"},
		{"a gap that hid nothing", []hostStep{ehStep(1, -1, "h1", seed, ""), ehStep(20, -1, "h1", reseed, "")}, nil,
			"before_record h1"},
		{"a gap that hid the first registration, after a seed none", []hostStep{ehStep(1, -1, "", none, ""), ehStep(20, -1, "h1", reseed, ""),
			ehStep(25, 1, "h2", ev, "c")}, nil,
			"changed 25/0 h2<-h1 #c | after_gap 20 h1 first"},
		{"a gap that hid the first registration, with no seed", []hostStep{ehStep(20, -1, "h1", reseed, "")}, nil,
			"after_gap 20 h1 first"},
		{"a gap after which none is registered", []hostStep{ehStep(1, -1, "h1", seed, ""), ehStep(20, -1, "", reseed, ""),
			ehStep(30, 1, "h2", ev, "")}, nil,
			"registered 30/0 h2 | before_record h1"},
		{"a seed read again is no row", []hostStep{ehStep(1, -1, "h1", seed, ""), ehStep(5, -1, "h1", scan.HostFromSeedCurrent, "")}, nil,
			"before_record h1"},
		{"a gap's row before the transactions of its block", []hostStep{ehStep(1, -1, "h1", seed, ""), ehStep(20, -1, "h2", reseed, ""),
			ehStep(20, 1, "h3", ev, "d")}, nil,
			"changed 20/0 h3<-h2 #d | after_gap 20 h2<-h1 | before_record h1"},
		{"failures merged", []hostStep{ehStep(1, -1, "", none, ""), ehStep(10, 1, "h2", ev, "e")},
			[]failedSetHost{{Height: 12, TxIndex: 0, Host: "h9", TxHash: "f1"}, {Height: 10, TxIndex: 1, Host: "h8", TxHash: "f2"},
				{Height: 5, TxIndex: 0, Host: "h7", TxHash: "f3"}},
			"failed 12/0 h9 change #f1 | failed 10/1 h8 change #f2 | registered 10/0 h2 #e | failed 5/0 h7 registration #f3"},
	} {
		rows, more := endpointRows(c.steps, c.fails)
		if got := ehRows(rows); got != c.want || more {
			t.Errorf("%s:\n got %s (more %v)\nwant %s", c.name, got, more, c.want)
		}
	}
}

// At most 50 rows other than the endpoint the record began with, which is
// always kept after them.
func TestEndpointRowsCap(t *testing.T) {
	for _, c := range []struct {
		events, fails int
		more          bool
	}{{60, 0, true}, {50, 0, false}, {45, 10, true}, {40, 10, false}} {
		steps := []hostStep{ehStep(1, -1, "h0", scan.HostFromSeed, "")}
		for i := 1; i <= c.events; i++ {
			steps = append(steps, ehStep(int64(100+i), 1, fmt.Sprintf("h%d", i), scan.HostFromEvent, ""))
		}
		var fails []failedSetHost
		for i := 1; i <= c.fails; i++ {
			fails = append(fails, failedSetHost{Height: int64(100 + 2*i), TxIndex: 3, Host: "x"})
		}
		rows, more := endpointRows(steps, fails)
		n := c.events + c.fails
		if n > endpointHistoryMax {
			n = endpointHistoryMax
		}
		if len(rows) != n+1 || more != c.more {
			t.Errorf("%d events and %d failures: %d rows (more %v), want %d (more %v)", c.events, c.fails, len(rows), more, n+1, c.more)
			continue
		}
		if last := rows[len(rows)-1]; last.Outcome != endpointBeforeRecord || last.Host != "h0" {
			t.Errorf("%d events and %d failures: the last row is %s", c.events, c.fails, ehRow(last))
		}
		top := int64(100 + c.events)
		if f := int64(100 + 2*c.fails); f > top {
			top = f
		}
		if rows[0].Height != top {
			t.Errorf("%d events and %d failures: the first row is %s, want the newest, at %d", c.events, c.fails, ehRow(rows[0]), top)
		}
		for i := 1; i < len(rows)-1; i++ {
			a, b := rows[i-1], rows[i]
			if a.Height < b.Height || (a.Height == b.Height && *a.TxIndex < *b.TxIndex) {
				t.Errorf("%d events and %d failures: %s before %s", c.events, c.fails, ehRow(a), ehRow(b))
			}
		}
	}
}

// ehHistory is the endpoint_history of a validator answer, its rows
// written as ehRow writes them; ok false when the answer has none.
func ehHistory(t *testing.T, g tqGot) (string, []endpointEvent, bool) {
	t.Helper()
	if g.status != 200 {
		t.Fatalf("%d %s", g.status, g.body)
	}
	raw, ok := g.keys["endpoint_history"]
	if !ok {
		return "", nil, false
	}
	var rows []endpointEvent
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	return ehRows(rows), rows, true
}

// The validator answer's endpoint_history, over the transaction page's
// fixture: each validator's registrations with the hash of every
// transaction whose cost is on record, and the final failed registrations
// its current operator signed at the top level, one row per transaction.
func TestEndpointHistoryOnTheValidatorAnswer(t *testing.T) {
	f := tqFixture(t)
	ask := func(addr string) tqGot { return tqAsk(t, f.srv, "/v1/validators/"+addr+"?window=24h") }
	for _, c := range []struct {
		name, addr, want string
	}{
		// not the failure inside a MsgExec (87), nor the one stopped before
		// the ante (91); one row for the transaction with two of A's
		// registrations (92), its first message's host
		{"A", tqValA, "failed 92/0 10.9.8.1:7980 change #" + tqFHostTwice + " | failed 90/0 10.9.9.1:7980 change #" + tqFTwoHosts +
			" | failed 86/0 10.9.9.9:7980 change #" + tqFHost + " | changed 64/0 " + tqHostA3 + "<-" + tqHostA2 + " #" + tqTwoHosts +
			" | same 58/0 " + tqHostA2 + " #" + tqSame + " | changed 55/2 " + tqHostA2 + "<-" + tqHostA1 + " #" + tqChg +
			" | registered 50/0 " + tqHostA1 + " #" + tqReg},
		// the registration at 68 has no cost line on record: no hash
		{"B", tqValB, "failed 90/0 10.9.9.2:7980 change #" + tqFTwoHosts + " | changed 68/0 " + tqHostB4 + "<-" + tqHostB3 +
			" | same 66/0 " + tqHostB3 + " #" + tqMixed + " | changed 64/0 " + tqHostB3 + "<-" + tqHostB2 + " #" + tqTwoHosts +
			" | changed 62/0 " + tqHostB2 + "<-" + tqHostB1 + " #" + tqExecHost + " | before_record " + tqHostB1},
		// superseded: its operator's failure is its newer key's
		{"the superseded key", tqValOld, "before_record " + tqHostO1},
		{"the current key", tqValNew, "failed 89/0 10.9.9.6:7980 change #" + tqFHostSuper + " | before_record " + tqHostN1},
	} {
		g := ask(c.addr)
		got, rows, ok := ehHistory(t, g)
		if !ok || got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.name, got, c.want)
		}
		if _, ok := g.keys["endpoint_history_truncated"]; ok {
			t.Errorf("%s: endpoint_history_truncated with %d rows", c.name, len(rows))
		}
		for _, r := range rows {
			if r.Outcome != endpointFailed {
				continue
			}
			rec := tqRecord(t, r.TxHash)
			if r.Reason != failedtx.Explain(rec).Reason || r.Time == nil || !r.Time.Equal(rec.Time) {
				t.Errorf("%s: the failed row %s: reason %q at %v, want %q at %s", c.name, r.TxHash, r.Reason, r.Time, failedtx.Explain(rec).Reason, rec.Time)
			}
		}
	}

	// No registration and no failure on record: no key.
	if _, _, ok := ehHistory(t, ask(tqValNone)); ok {
		t.Errorf("a validator with nothing registered carries endpoint_history")
	}
	// A pinned window is a rewound answer the page does not ask: no key.
	if _, _, ok := ehHistory(t, ask(ctValidator)); !ok {
		t.Fatalf("%s carries no endpoint_history", ctValidator)
	}
	pin := f.now.Add(-time.Hour).Format(time.RFC3339)
	if _, _, ok := ehHistory(t, tqAsk(t, f.srv, "/v1/validators/"+ctValidator+"?window=24h&as_of="+url.QueryEscape(pin))); ok {
		t.Errorf("a pinned window carries endpoint_history")
	}
}

// Both ways a validator answer is built carry the same endpoint_history:
// from the validator snapshots, and computed where the snapshots do not
// list the validator yet. The 30 s detail cache serves it with the rest.
func TestEndpointHistoryOnBothPathsAndCached(t *testing.T) {
	f := tqFixture(t)
	ctx := context.Background()
	win := windowFor("24h", f.now)
	for _, name := range detailSpans {
		// asked until the snapshot is in
		if g := tqAsk(t, f.srv, "/v1/validators?window="+name); g.status != 200 {
			t.Fatalf("the %s snapshot: %d %s", name, g.status, g.body)
		}
	}
	snap, ok, err := f.srv.detailFromSnapshots(ctx, tqValB, win, f.now)
	if err != nil || !ok {
		t.Fatalf("from the snapshots: ok %v, %v", ok, err)
	}
	// A server whose snapshots are not computed: the answer is computed.
	other := newServer(f.st, VantageInfo{Name: "ut-1"}, nil, withClock(func() time.Time { return f.now }), WithDayParts(false))
	t.Cleanup(func() {
		other.vals.stop()
		other.vals.wait()
	})
	status, out, err := other.validatorDetailIn(ctx, tqValB, win, f.now)
	if err != nil || status != 200 {
		t.Fatalf("computed: %d %v", status, err)
	}
	computed, _ := out.(map[string]any)
	if _, ok := computed["computed_at"]; ok {
		t.Fatalf("the second server answered from snapshots")
	}
	a, b := tqJSON(t, snap["endpoint_history"]), tqJSON(t, computed["endpoint_history"])
	if a != b || !strings.Contains(a, tqExecHost) {
		t.Errorf("endpoint_history from the snapshots:\n%s\ncomputed:\n%s", a, b)
	}

	// Cached: a registration stored after the first asking is not in the
	// second, within the 30 s, though the history now holds it.
	path := "/v1/validators/" + tqValB + "?window=7d"
	first, _, ok := ehHistory(t, tqAsk(t, f.srv, path))
	if !ok {
		t.Fatalf("%s: no endpoint_history", path)
	}
	tqExec(t, f.st, `INSERT INTO host_events (from_height, from_tx_index, cons_address, host, source, time) VALUES (70, 1, ?, '10.0.1.9:7980', 'event', ?)`,
		tqValB, store.TS(ctBlockTime(70)))
	if again, _, _ := ehHistory(t, tqAsk(t, f.srv, path)); again != first {
		t.Errorf("the second asking within the cache's 30 s:\n%s\nthe first:\n%s", again, first)
	}
	rows, _, err := f.srv.endpointHistory(ctx, tqValB)
	if err != nil {
		t.Fatal(err)
	}
	if got := ehRows(rows); !strings.Contains(got, "changed 70/0 10.0.1.9:7980<-"+tqHostB4) {
		t.Errorf("the history after the new registration: %s", got)
	}
}

// A registration whose transaction's cost was not on record has no hash; a
// backfill (cmd/sentinel-txbackfill) puts its cost line on record later, of
// a height below lines stored before it, and the walk's join gives the row
// the transaction's hash from then on, on the next answer computed.
func TestEndpointHistoryGivesTheHashOnceTheCostLineIsOnRecord(t *testing.T) {
	f := tqFixture(t)
	ctx := context.Background()
	before := "changed 68/0 " + tqHostB4 + "<-" + tqHostB3
	rows, _, err := f.srv.endpointHistory(ctx, tqValB)
	if err != nil {
		t.Fatal(err)
	}
	if got := ehRows(rows); !strings.Contains(got, before+" | ") {
		t.Fatalf("before the cost line: %s", got)
	}
	hash := tqHash("B's registration at 68, its cost backfilled")
	r := tqCost(hash, 68, 0, tqBech("celestia", 0xb1), tqMsg(0, failedtx.KindSetHost, tqOpB))
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := f.st.InsertTxCost(r, raw); err != nil || !ok {
		t.Fatalf("the backfilled cost line: %v %v", ok, err)
	}
	rows, _, err = f.srv.endpointHistory(ctx, tqValB)
	if err != nil {
		t.Fatal(err)
	}
	if got := ehRows(rows); !strings.Contains(got, before+" #"+hash+" | ") {
		t.Fatalf("after the cost line: %s", got)
	}
}
