package api

import (
	"context"
	"encoding/json"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// auditSim is a record of five days sealed as far as it is due.
func auditSim(t *testing.T, seed uint64) (*sim, *Server) {
	t.Helper()
	cfg := defaultSimConfig(seed)
	cfg.perDay, cfg.days = 10, 5
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(4*24*time.Hour + 6*time.Hour)); at = at.Add(6 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	if _, err := srv.sealDue(context.Background(), math.MaxInt); err != nil {
		t.Fatal(err)
	}
	return s, srv
}

// TestTheAuditDropsASealTheStoreDoesNotHold holds the checks of the
// partials against the store to what they are for. On partials that are
// what the store holds, the hourly audit and the comparison of a window
// find nothing. With every sealed day changed behind the store's back, the
// audit drops the day of each kind it reads and /v1/health says so; a
// window that differs puts the process on raw reads, as with the partials
// off, leaves the sealed days and the files as they are, and /v1/health
// fails; a ledger day that differs does the same; and a file holding
// changed days is refused when it is loaded, the partials then built from
// the store exact again.
func TestTheAuditDropsASealTheStoreDoesNotHold(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	s, srv := auditSim(t, 33)
	ctx := context.Background()
	seal := func(srv *Server) {
		if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
			t.Fatal(err)
		}
	}
	count := func(srv *Server) (int, int) {
		srv.parts.mu.Lock()
		defer srv.parts.mu.Unlock()
		return len(srv.parts.cur.rows), sealedSettle(srv.parts.cur)
	}
	rows, settles := count(srv)
	if rows < 2 || settles < 2 {
		t.Fatalf("too little sealed to audit: %d row days, %d settlement days", rows, settles)
	}
	// The days alone, eight times over, then with a window.
	for i := 0; i < 8; i++ {
		res, err := srv.auditOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.days) != 3 || len(res.diffs) != 0 {
			t.Fatalf("an audit of exact partials read %v and found %v", res.days, res.diffs)
		}
	}
	for _, list := range []bool{false, true} {
		if _, err := srv.auditWindow(ctx, list); err != nil {
			t.Fatal(err)
		}
	}
	if r, st := count(srv); r != rows || st != settles || srv.parts.rawFallback() != "" {
		t.Fatalf("the audit dropped days the store holds: %d row days and %d settlement days left of %d and %d; fallback %q",
			r, st, rows, settles, srv.parts.rawFallback())
	}

	// Every sealed day one more than the store holds, as a new generation.
	corrupt := func(srv *Server) {
		srv.parts.publish(func(e *epoch, _ []journalEntry) bool {
			for d, rd := range e.rows {
				c := *rd
				c.Vals = map[string]*rowPart{}
				for a, p := range rd.Vals {
					q := *p
					q.Probes++
					c.Vals[a] = &q
				}
				c.Gen = srv.parts.nextGen("row:" + d)
				e.rows[d] = &c
			}
			for d, sd := range e.settle {
				if sd.Seal == nil {
					continue
				}
				c := *sd.Seal
				c.Readable++
				c.Gen = srv.parts.nextGen("settle:" + d)
				e.settleMut(d).Seal = &c
			}
			e.content++
			return true
		}, 0)
	}
	corrupt(srv)
	res, err := srv.auditOnce(ctx)
	srv.parts.noteAudit(res, err, s.now)
	if err != nil {
		t.Fatal(err)
	}
	if r, st := count(srv); r != rows-1 || st != settles-1 || len(res.diffs) != 2 || res.fallback {
		t.Errorf("the audit of one day of each kind left %d row days and %d settlement days of %d and %d, found %v", r, st, rows, settles, res.diffs)
	}
	if _, c := srv.parts.health(s.now); c.OK || !strings.Contains(c.Detail, "last audit") {
		t.Errorf("/v1/health after an audit that differed: %+v", c)
	}
	if _, err := srv.auditWindow(ctx, rand.IntN(2) == 0); err != nil {
		t.Fatal(err)
	}
	if srv.parts.rawFallback() == "" {
		t.Fatal("a window that differed left the process on the partials")
	}
	if r, st := count(srv); r != rows-1 || st != settles-1 {
		t.Errorf("a window that differed dropped sealed days: %d row days and %d settlement days left of %d and %d", r, st, rows-1, settles-1)
	}
	if h, c := srv.parts.health(s.now); c.OK || h.State != "raw-fallback" {
		t.Errorf("/v1/health on raw reads: %+v, %+v", h, c)
	}
	// Every window is read raw: no epoch in a computation, and nothing
	// sealed or written.
	if err := srv.readTx(ctx, func(ctx context.Context) error {
		if epochOf(ctx) != nil {
			t.Errorf("a computation on raw reads was handed an epoch")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.networkSnapshot(ctx, windowFor("all", s.now), excludeSet{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}

	// Sealed again in another process, changed, and written out: the next
	// start refuses the file.
	other := s.openAPI()
	seal(other)
	corrupt(other)
	if err := other.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	next := s.openAPI()
	rng := rand.New(rand.NewPCG(33, 1))
	tally := s.compare(next, rng, 4, "after the refused file")
	if !strings.Contains(next.parts.origin, "refused") || !strings.Contains(next.parts.origin, "is not what the store holds") {
		t.Errorf("a file of changed days was not refused: %s", next.parts.origin)
	}
	t.Logf("%s\n%s", next.parts.origin, tally)
}

// TestALedgerDayThatDiffersPutsTheProcessOnRawReads: a ledger day is added
// to and kept forever, so one that is not what the store holds is not a day
// the audit can drop: the process reads every window raw from then on.
func TestALedgerDayThatDiffersPutsTheProcessOnRawReads(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	s, srv := auditSim(t, 34)
	ctx := context.Background()
	srv.parts.publish(func(e *epoch, _ []journalEntry) bool {
		for d := range e.settle {
			e.settleMut(d).Bytes++
		}
		return true
	}, 0)
	res, err := srv.auditOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !res.fallback || srv.parts.rawFallback() == "" {
		t.Fatalf("a ledger day one byte off: audit %+v, fallback %q", res, srv.parts.rawFallback())
	}
	if h, c := srv.parts.health(s.now); c.OK || h.State != "raw-fallback" {
		t.Errorf("/v1/health on raw reads: %+v, %+v", h, c)
	}
}

// TestTheAuditSweepsEverySealedDay: the audit reads the sealed days of each
// kind in turn, in day order, and comes back to the first after the last,
// so every sealed day is read within as many audits as there are, its
// cursor kept across a restart with the partials.
func TestTheAuditSweepsEverySealedDay(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	s, srv := auditSim(t, 35)
	ctx := context.Background()
	srv.parts.mu.Lock()
	e := srv.parts.cur
	want := map[string][]string{}
	for d := range e.rows {
		want["row day"] = append(want["row day"], d)
	}
	for d, sd := range e.settle {
		if sd.Seal != nil {
			want["settlement day"] = append(want["settlement day"], d)
		}
		if sd.Pubs > 0 {
			want["ledger day"] = append(want["ledger day"], d)
		}
	}
	srv.parts.mu.Unlock()
	n := 0
	for _, days := range want {
		sort.Strings(days)
		n = max(n, len(days))
	}
	got := map[string][]string{}
	read := func(srv *Server, times int) {
		for i := 0; i < times; i++ {
			res, err := srv.auditOnce(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.diffs) > 0 {
				t.Fatalf("exact partials: %v", res.diffs)
			}
			for _, x := range res.days {
				i := strings.LastIndexByte(x, ' ')
				got[x[:i]] = append(got[x[:i]], x[i+1:])
			}
		}
	}
	// Half the sweep, a restart from the files, the rest and once more.
	half := n / 2
	read(srv, half)
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	next := s.openAPI()
	read(next, n-half+1)
	for kind, days := range want {
		g := got[kind]
		if len(g) < len(days) {
			t.Fatalf("%s: %d audit(s) read %v of %v", kind, n+1, g, days)
		}
		seen := map[string]bool{}
		for _, d := range g[:len(days)] {
			seen[d] = true
		}
		if len(seen) != len(days) || !sort.StringsAreSorted(g[:len(days)]) {
			t.Errorf("%s: the audits read %v, not each of %v once in order", kind, g, days)
		}
		if len(g) > len(days) && g[len(days)] != days[0] {
			t.Errorf("%s: after the last, the audit read %s, not the first, %s", kind, g[len(days)], days[0])
		}
	}
}

// sealedSettle counts the settlement days with a seal.
func sealedSettle(e *epoch) int {
	n := 0
	for _, sd := range e.settle {
		if sd.Seal != nil {
			n++
		}
	}
	return n
}

// TestAPublicationStoredMidAuditIsNotADifference: the endorsement ledger is a
// cache of the whole record, read outside the comparison's transaction. On
// mocha (2026-10-06 15:09) a blob was stored between the shipped computation
// of the audit's validator list and the partials', so the second one alone
// had it as every endorser's newest endorsement, and the audit put the
// process on raw reads for partials that were what the store holds. Both ways
// now get one reading of the ledger: the same publication stored at the same
// moment is no difference, while the ledger outside the comparison has it.
func TestAPublicationStoredMidAuditIsNotADifference(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	s, srv := auditSim(t, 37)
	ctx := context.Background()
	srv.parts.mu.Lock()
	win, ok := srv.parts.cur.auditWindowOf()
	srv.parts.mu.Unlock()
	if !ok {
		t.Fatal("nothing sealed to audit")
	}

	// a publication every validator endorsed, newer than every one stored
	var newest string
	if err := s.st.DB().QueryRowContext(ctx, `SELECT MAX(settlement_time) FROM publications`).Scan(&newest); err != nil {
		t.Fatal(err)
	}
	at, err := time.Parse(time.RFC3339Nano, newest)
	if err != nil {
		t.Fatal(err)
	}
	var p scan.Publication
	for _, sp := range s.pubs {
		if sp.emitAt.After(s.now) || sp.pub.Assignment.Error != "" || len(sp.pub.Assignment.Validators) == 0 {
			continue
		}
		p = sp.pub
	}
	p.PromiseHash, p.SettlementTxHash = s.hash("mid-audit", 0), s.hash("mid-audit-tx", 0)[:40]
	p.SettlementHeight, p.SettlementTime, p.SettlementTxCode = s.height+1000, at.Add(time.Millisecond), 0
	p.Assignment.Validators = append([]scan.ValidatorAssignment(nil), p.Assignment.Validators...)
	for i := range p.Assignment.Validators {
		p.Assignment.Validators[i].Attested = true
	}
	raw, _ := json.Marshal(p)

	c := validatorsCase(srv, win, "")
	calls := 0
	mid := pathCase{c.name, func(ctx context.Context) (any, error) {
		out, err := c.run(ctx)
		if calls++; calls == 1 { // the shipped way is done; the partials' is next
			if ok, err := s.st.UpsertPublication(p, raw); err != nil || !ok {
				t.Fatalf("storing the publication: %v %v", ok, err)
			}
		}
		return out, err
	}}
	n, diffs, err := srv.comparePaths(ctx, []pathCase{mid})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(diffs) > 0 {
		t.Fatalf("a publication stored between the two ways is a difference (%d compared):\n%s", n, strings.Join(diffs, "\n"))
	}

	// the ledger outside the comparison has it: the race was run, not missed
	rows, err := srv.validatorRows(ctx, win, "")
	if err != nil {
		t.Fatal(err)
	}
	want, moved := store.TS(p.SettlementTime), 0
	for _, r := range rows {
		if r.Signing.LastEndorsedAt != nil && *r.Signing.LastEndorsedAt == want {
			moved++
		}
	}
	if moved == 0 {
		t.Fatalf("no validator's newest endorsement is the publication stored mid-comparison (%s)", want)
	}
	t.Logf("%d validators endorsed the publication stored mid-comparison; the comparison saw one ledger", moved)
}
