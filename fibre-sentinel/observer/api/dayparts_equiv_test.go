package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
)

// The equality proof of the day partials: after every collector pass,
// every figure computed from the partials is compared, byte for byte, with
// the same figure computed with the shipped statements over the whole
// window, both in one read transaction of the store as the API reads it.

// equivCase is one figure: a window of the network summary, of the
// validator list or of a validator's page, unpinned or pinned, with or
// without an exclusion.
type equivCase struct {
	name string
	kind string // for the tally
	run  func(ctx context.Context) (any, error)
}

// equivTally counts the comparisons made and the differences found, per
// kind of case.
type equivTally struct {
	compared map[string]int
	differ   map[string]int
	used     map[string]int
}

func newTally() *equivTally {
	return &equivTally{compared: map[string]int{}, differ: map[string]int{}, used: map[string]int{}}
}

func (t *equivTally) add(o *equivTally) {
	for k, n := range o.compared {
		t.compared[k] += n
	}
	for k, n := range o.differ {
		t.differ[k] += n
	}
	for k, n := range o.used {
		t.used[k] += n
	}
}

func (t *equivTally) String() string {
	var keys []string
	for k := range t.compared {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "  %-28s %6d compared, %d differ\n", k, t.compared[k], t.differ[k])
	}
	keys = keys[:0]
	for k := range t.used {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "  %-40s %6d over every comparison\n", k, t.used[k])
	}
	return b.String()
}

// pinned is the window a request with ?as_of=at would get.
func pinned(name string, at time.Time) Window {
	w := windowFor(name, at)
	w.AsOf = true
	return w
}

// equivCases is every case at the sim's clock: the unpinned windows of the
// network (with no exclusion, one and eight validators excluded) and of the
// validator list, one validator's rows and page, and the pins.
func (s *sim) equivCases(srv *Server, pins []time.Time) []equivCase {
	now := s.now
	var cases []equivCase
	addrs := make([]string, len(s.vals))
	for i, v := range s.vals {
		addrs[i] = v.addr
	}
	sort.Strings(addrs)
	excl := map[int]excludeSet{0: {}}
	for _, n := range []int{1, 8} {
		var ex excludeSet
		for _, a := range addrs[:n] {
			ex.addrs = append(ex.addrs, a)
		}
		excl[n] = ex
	}
	network := func(win Window, n int) equivCase {
		ex := excl[n]
		var names []string
		for _, a := range ex.addrs {
			names = append(names, a.(string))
		}
		name := "network/" + win.Name
		if win.AsOf {
			name += "@" + win.End.Format(time.RFC3339Nano)
		}
		if n > 0 {
			name += "/exclude-" + strconv.Itoa(n)
		}
		return equivCase{name, "network", func(ctx context.Context) (any, error) {
			resp, err := srv.computeNetwork(ctx, win, ex, names)
			if err != nil {
				return nil, err
			}
			resp.RecordThrough = srv.recordThrough(ctx)
			return map[string]any{"snapshot": resp, "published": networkOutOf(resp)}, nil
		}}
	}
	validators := func(win Window, only string) equivCase {
		name := "validators/" + win.Name
		kind := "validators"
		if win.AsOf {
			name += "@" + win.End.Format(time.RFC3339Nano)
		}
		if only != "" {
			name += "/" + only[:8]
			kind = "validator"
		}
		return equivCase{name, kind, func(ctx context.Context) (any, error) {
			rows, err := srv.validatorRows(ctx, win, only)
			if err != nil {
				return nil, err
			}
			return map[string]any{"rows": rows, "published": listOfRows(rows)}, nil
		}}
	}
	for _, name := range warmWindows {
		win := windowFor(name, now)
		for _, n := range []int{0, 1, 8} {
			cases = append(cases, network(win, n))
		}
		cases = append(cases, validators(win, ""))
		for _, a := range []string{s.vals[0].addr, s.vals[3].addr, s.vals[len(s.vals)-1].addr} {
			if name != "24h" {
				cases = append(cases, validators(win, a))
			}
		}
	}
	// A validator's page, as a pinned request computes it: its row and the
	// obligations of the four spans beside it.
	for _, a := range []string{s.vals[1].addr, s.vals[4].addr, s.vals[len(s.vals)-2].addr} {
		win := pinned("7d", now)
		cases = append(cases, equivCase{"detail/" + a[:8], "detail", func(ctx context.Context) (any, error) {
			status, out, err := srv.validatorDetailIn(ctx, a, win, now)
			if err != nil {
				return nil, err
			}
			return map[string]any{"status": status, "out": out}, nil
		}})
	}
	for _, at := range pins {
		for _, name := range []string{"7d", "30d", "all"} {
			cases = append(cases, network(pinned(name, at), 0))
		}
		for _, name := range []string{"7d", "all"} {
			cases = append(cases, validators(pinned(name, at), ""))
		}
	}
	return cases
}

// pinsAt is the pins the harness asks about at the clock: day and hour
// boundaries and a nanosecond either side, the last nanosecond of a day,
// random instants, a pin before raw_from, one inside a day's closure (its
// promises still under obligation), and one older than 62 days.
func (s *sim) pinsAt(rng *rand.Rand) []time.Time {
	now := s.now
	span := now.Sub(s.t0)
	if span <= 0 {
		return nil
	}
	rnd := func() time.Time { return s.t0.Add(time.Duration(rng.Int64N(int64(span)))) }
	day := rnd().Truncate(24 * time.Hour)
	hour := rnd().Truncate(time.Hour)
	pins := []time.Time{
		day.Add(-time.Nanosecond), day, day.Add(time.Nanosecond),
		hour.Add(-time.Nanosecond), hour, hour.Add(time.Nanosecond),
		day.Add(24*time.Hour - time.Nanosecond),
		rnd(), rnd(),
		now.Add(-63 * 24 * time.Hour),
		now.Truncate(24 * time.Hour).Add(2 * time.Hour),
	}
	if from, ok := rawFromOf(s); ok && from.After(s.t0) {
		pins = append(pins, s.t0.Add(time.Duration(rng.Int64N(int64(from.Sub(s.t0))))))
	}
	var out []time.Time
	for _, p := range pins {
		if !p.After(now) && p.After(s.t0.Add(-70*24*time.Hour)) {
			out = append(out, p)
		}
	}
	return out
}

func rawFromOf(s *sim) (time.Time, bool) {
	v, err := s.st.Meta("raw_from")
	if err != nil || v == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(dayLayout, v)
	return t, err == nil
}

// compare computes every case both ways in one read transaction and
// reports each difference. sample, when above zero, compares one unpinned
// window of the network and that many other cases, chosen at random.
func (s *sim) compare(srv *Server, rng *rand.Rand, sample int, label string) *equivTally {
	s.t.Helper()
	tally := newTally()
	cases := s.equivCases(srv, s.pinsAt(rng))
	if sample > 0 && sample < len(cases) {
		var keep []equivCase
		var rest []equivCase
		for _, c := range cases {
			if c.kind == "network" && !strings.Contains(c.name, "@") && !strings.Contains(c.name, "exclude") {
				keep = append(keep, c)
			} else {
				rest = append(rest, c)
			}
		}
		// one unpinned window of the network, and a sample of the rest
		rng.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
		cases = append([]equivCase{keep[rng.IntN(len(keep))]}, rest[:min(sample, len(rest))]...)
	}
	ctx := context.Background()
	err := srv.readTx(ctx, func(ctx context.Context) error {
		e := epochOf(ctx)
		if e == nil {
			return fmt.Errorf("no epoch: the partials did not catch up")
		}
		// How much of the "all" window the partials served: the sealed row
		// days summed and the settlement days whose seal was usable.
		all := windowFor("all", s.now)
		sealed, _ := e.rowSpans(all.startArg(), all.endArg())
		tally.used["row days summed (all)"] += len(sealed)
		start := all.startArg()
		if e.rawFrom != "" {
			start = dayLo(e.rawFrom)
		}
		for _, d := range e.settleDays(start, all.endArg()) {
			if sealUsable(e.settle[d].Seal, rollup.RowLowerBound(start), all.endArg(), all.endArg(), provisionalCutoff(s.now)) {
				tally.used["settlement days summed (all)"]++
			}
		}
		ref := withEpoch(ctx, nil)
		for _, c := range cases {
			want, err := c.run(ref)
			if err != nil {
				return fmt.Errorf("%s (shipped): %w", c.name, err)
			}
			got, err := c.run(ctx)
			if err != nil {
				return fmt.Errorf("%s (partials): %w", c.name, err)
			}
			a, _ := json.Marshal(want)
			b, _ := json.Marshal(got)
			tally.compared[c.kind]++
			if !bytes.Equal(scrubComputed(a), scrubComputed(b)) {
				tally.differ[c.kind]++
				s.t.Errorf("%s at %s: %s differs:\n%s", label, s.now.Format(time.RFC3339), c.name, jsonDiff(a, b, 12))
			}
		}
		return nil
	})
	if err != nil {
		s.t.Fatalf("%s at %s: %v", label, s.now.Format(time.RFC3339), err)
	}
	return tally
}

// equivSeeds is how many seeds the harness runs: four by default, which
// keeps the package inside its time; TENSILE_DAYPARTS_SEEDS sets more.
// equivFull is whether it runs at full scale.
func equivSeeds() int {
	if v, err := strconv.Atoi(os.Getenv("TENSILE_DAYPARTS_SEEDS")); err == nil && v > 0 {
		return v
	}
	return 4
}

// skipUnderRace skips a run of one goroutine under the race detector, which
// has nothing to find in it and slows it past the package's time;
// TestDayPartsUnderConcurrency is the partials' race test.
func skipUnderRace(t *testing.T) {
	if raceOn {
		t.Skip("one goroutine: nothing for the race detector (TestDayPartsUnderConcurrency)")
	}
}

func equivFull() bool { return os.Getenv("TENSILE_DAYPARTS_FULL") != "" }

// equivRun is one seed's run: the record written pass by pass with every
// scenario in it, the sealer run for none, one or every due day after each
// pass, and the API restarted from its files before every comparison.
// sample is how many cases each pass compares besides the unpinned network
// windows (0: every case).
func equivRun(t *testing.T, cfg simConfig, sample int, restart bool, step [2]int) *equivTally {
	t.Helper()
	s := newSim(t, cfg)
	// The windows ranked in bounded memory (dayparts_rank.go) in turn with
	// those summed whole: every window but the day's, those of more than
	// three sealed days, or those of more than a month (none here).
	s.apiOpts = []Option{withMergeDays([]int{0, 3, mergeDaysDefault}[cfg.seed%3])}
	s.plan()
	srv := s.openAPI()
	rng := rand.New(rand.NewPCG(cfg.seed, 99))
	tally := newTally()
	s.afterPrune = func(step, day string) {
		// between the statements of the prune, each its own commit
		tally.add(s.compare(srv, rng, 2, "prune "+step+" "+day))
	}
	// The fast tick's moments draw from an order of their own, so the passes
	// and the pins fall where they did before it existed.
	frng := rand.New(rand.NewPCG(cfg.seed, 0xfa57))
	end := s.t0.Add(time.Duration(cfg.days)*24*time.Hour + 8*time.Hour)
	sc := s.scen
	amended, failed, weird, reapplied, swapped := false, false, false, false, false
	ctx := context.Background()
	prev := s.now
	for at := s.t0.Add(time.Hour); !at.After(end); at = at.Add(time.Duration(step[0]+rng.IntN(step[1]-step[0])) * time.Minute) {
		down := func(t time.Time) bool { return !t.Before(sc.backlog[0]) && t.Before(sc.backlog[1]) }
		// Between two passes the collector's fast tick stores what the
		// scanner wrote since (state.json, publications, payments), and the
		// API computes before the pass brings in the rest.
		if fast := prev.Add(time.Duration(float64(at.Sub(prev)) * (0.2 + 0.7*frng.Float64()))); fast.After(prev) && !down(fast) {
			s.advance(fast)
			s.fastTick()
			tally.add(s.compare(srv, frng, 3, "fast tick"))
		}
		prev = at
		s.advance(at)
		if down(at) {
			continue // the collector is down
		}
		if !amended && !at.Before(sc.amendAt) {
			s.amendReplay()
			amended = true
		}
		failing := !failed && !at.Before(sc.heldVerified)
		if failing {
			s.failCorrector(sc.failAfter)
		}
		s.pass()
		if failing {
			s.healCorrector()
			failed = true
		}
		if !reapplied && !at.Before(sc.reapplyAt) {
			s.reapply()
			reapplied = true
		}
		switch {
		case s.inDark(at):
			// While the prober's lines are held back, the API seals what is
			// due: a day over meanwhile has heartbeats and no reading.
			if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
				t.Fatal(err)
			}
			s.noteDark(srv)
		case rng.IntN(3) == 1:
			if _, err := srv.sealDue(ctx, 1); err != nil {
				t.Fatal(err)
			}
		case rng.IntN(2) == 1:
			if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
				t.Fatal(err)
			}
		}
		if s.again != nil && at.After(sc.reapplyAt.Add(3*time.Hour)) {
			// After the sealer's turn too: the second apply of a correction,
			// which no log records.
			s.reapplyAgain()
		}
		if !swapped && !at.Before(sc.swapAt) {
			// A hold raised on two readings of sealed days, those days sealed
			// again with it, then moved in one transaction to two readings
			// of another validator whose rowids add up alike: the count of
			// the held rows and their sums stay as they were. The next pass
			// releases them, no range covering them.
			if q := s.holdQuad(srv); q != nil {
				moveHolds(t, s, nil, []int64{q[0], q[3]})
				if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
					t.Fatal(err)
				}
				moveHolds(t, s, []int64{q[0], q[3]}, []int64{q[1], q[2]})
				tally.add(s.compare(srv, rand.New(rand.NewPCG(cfg.seed, 0x5a9)), 0, "a hold moved between readings of equal sums"))
				s.swapped = true
			}
			swapped = true
		}
		if !weird && !at.Before(sc.weirdAt) {
			// After the sealer's turn, so that the comparison below is the
			// first to meet them, before a seal of the day beside them would
			// find them itself.
			s.weirdRow(s.t0.Add(5*24*time.Hour), false)
			s.weirdRow(s.t0.Add(3*24*time.Hour), true)
			weird = true
		}
		if restart {
			// The API stops and starts again from what it kept.
			if err := srv.keepDerived(ctx); err != nil {
				t.Fatal(err)
			}
			s.dropped += srv.parts.dropped
			srv = s.openAPI()
		}
		tally.add(s.compare(srv, rng, sample, "pass"))
		s.observe()
		if restart && s.saved && !strings.HasPrefix(srv.parts.origin, "loaded") {
			t.Errorf("at %s the restarted API did not begin from its files: %s", at.Format(time.RFC3339), srv.parts.origin)
		}
		s.saved = s.saved || restart
		if t.Failed() {
			return tally
		}
	}
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	tally.add(s.compare(srv, rng, 0, "end"))
	s.observe()
	s.checkScenarios(srv)
	return tally
}

// checkScenarios asserts that the run reached what it was written to reach:
// holds raised and lifted, every kind of correction, a collapse, an
// amendment, the prune, days sealed and then dropped by what came after.
func (s *sim) checkScenarios(srv *Server) {
	s.t.Helper()
	for _, c := range simWitnesses {
		if !s.seen[c.what] {
			s.t.Errorf("the run was to reach %s and did not", c.what)
		}
	}
	injected := false
	for _, l := range s.logs {
		injected = injected || strings.Contains(l, "injected")
	}
	if !injected {
		s.t.Errorf("the corrector never failed part way")
	}
	if s.dropped+srv.parts.dropped == 0 {
		s.t.Errorf("no sealed day was ever dropped by what came after it")
	}
	if !s.sealedDark {
		s.t.Errorf("no day was sealed without a reading while the prober's lines were held back")
	}
	if s.fastStored == 0 {
		s.t.Errorf("the fast tick never stored a publication between passes")
	}
	if !s.swapped {
		s.t.Errorf("no hold was moved between readings of equal rowid sums")
	}
	e := srv.parts.cur
	sealed := 0
	for _, sd := range e.settle {
		if sd.Seal != nil {
			sealed++
		}
	}
	if len(e.rows) == 0 || sealed == 0 {
		s.t.Errorf("nothing sealed: %d row days, %d settlement days", len(e.rows), sealed)
	}
	// No settlement day whose obligation rows tie is sealed with them.
	tied := 0
	for d, sd := range e.settle {
		var n int64
		lo, hi := sd.MinStart, sd.MaxStart
		if sd.Seal != nil {
			lo, hi = sd.Seal.MinStart, sd.Seal.MaxStart
		}
		if lo == "" {
			continue
		}
		if err := s.st.DB().QueryRow(dayTiesSQL, dayLo(d), dayHi(d), hi, lo).Scan(&n); err != nil {
			s.t.Fatal(err)
		}
		if n == 0 {
			continue
		}
		tied++
		if sd.Seal != nil && sd.Seal.Obl != nil {
			s.t.Errorf("settlement day %s is sealed with %d obligation row(s) tied on their newest reading", d, n)
		}
	}
	s.t.Logf("%d settlement day(s) with tied obligation rows, read raw", tied)
}

// noteDark records whether a row day over while the prober's lines were held
// back has been sealed with no probe row to anchor it: the day the backlog
// lands on, and the prune later deletes.
func (s *sim) noteDark(srv *Server) {
	srv.parts.mu.Lock()
	defer srv.parts.mu.Unlock()
	e := srv.parts.cur
	if e == nil {
		return
	}
	for d := dayOfTime(s.scen.dark[0].Add(24 * time.Hour)); d < dayOfTime(s.scen.dark[1]); d = dayAdd(d, 1) {
		if a := e.anchors[d]; e.rows[d] != nil && (a == nil || a.Probe == nil) {
			s.sealedDark = true
		}
	}
}

// observe records which of the scenarios' witnesses the store holds now:
// what the prune takes away later still counts as reached.
func (s *sim) observe() {
	s.t.Helper()
	if s.seen == nil {
		s.seen = map[string]bool{}
	}
	for _, c := range simWitnesses {
		if s.seen[c.what] {
			continue
		}
		var n int64
		if err := s.st.DB().QueryRow(c.q).Scan(&n); err != nil {
			s.t.Fatalf("%s: %v", c.what, err)
		}
		s.seen[c.what] = n > 0
	}
}

// simWitnesses are what the store holds once each scenario has happened.
var simWitnesses = []struct {
	what, q string
}{
	{"a publication deadline corrected", `SELECT COUNT(*) FROM publication_corrections`},
	{"a probe verdict corrected", `SELECT COUNT(*) FROM probe_corrections`},
	// what every apply writes is corrected_at; one no line of the log was
	// judged at is an apply the log kept no line of
	{"a publication correction applied again under its range", `SELECT COUNT(*) FROM publications p WHERE p.corrected_at IS NOT NULL
		AND NOT EXISTS (SELECT 1 FROM publication_corrections k WHERE k.promise_hash = p.promise_hash AND k.judged_at = p.corrected_at)`},
	{"a row correction applied again under its range", `SELECT COUNT(*) FROM probes r WHERE r.corrected_at IS NOT NULL
		AND NOT EXISTS (SELECT 1 FROM probe_corrections k WHERE k.dedupe_key = r.dedupe_key AND k.judged_at = r.corrected_at)`},
	{"a sampled-out point corrected", `SELECT COUNT(*) FROM sampling_decisions d JOIN publications p ON p.promise_hash = d.promise_hash WHERE d.must_serve_until <> p.must_serve_until OR p.corrected_at IS NOT NULL`},
	{"a collapse", `SELECT COUNT(*) FROM sampling_decisions WHERE source = 'rows'`},
	{"an amendment", `SELECT COUNT(*) FROM probe_amendments`},
	{"a held publication", `SELECT COUNT(*) FROM publications WHERE retention_unverified = 1`},
	{"a range still holding", `SELECT COUNT(*) FROM param_uncertainty WHERE holds = 1`},
	{"a range corrected", `SELECT COUNT(*) FROM param_uncertainty WHERE corrected_at IS NOT NULL`},
	{"a prune", `SELECT COUNT(*) FROM meta WHERE key = 'raw_from'`},
	{"a rolled day", `SELECT COUNT(*) FROM obligation_daily`},
	{"a second vantage's row", `SELECT COUNT(*) FROM probes WHERE vantage = 'v2'`},
	{"a restarted prober's row", `SELECT COUNT(*) FROM probes WHERE outcome = 'MISSED' AND started_at > must_serve_until`},
	{"a deferred shadow verdict", `SELECT COUNT(*) FROM probes WHERE shadow_gap IS NOT NULL`},
	{"a row long before its settlement", `SELECT COUNT(*) FROM probes r JOIN publications p ON p.promise_hash = r.promise_hash WHERE r.started_at < strftime('%Y-%m-%dT%H:%M:%f', p.settlement_time, '-2 hours')`},
	{"a row start that is not a store timestamp", `SELECT COUNT(*) FROM probes WHERE length(started_at) <> 30`},
	{"a row start of the right shape that is no time", `SELECT COUNT(*) FROM probes WHERE started_at GLOB '*:60.*'`},
	{"a publication recorded after its deadline", `SELECT COUNT(*) FROM publications WHERE recorded_at > must_serve_until`},
	{"original_rows not a power of two", `SELECT COUNT(*) FROM publications WHERE json_extract(raw_json, '$.assignment.protocol_params.original_rows') = 4000`},
	// full readings: a later attempt moves how the validator's answers
	// before it count, which are of the same promise
	{"a full reading's later attempt", `SELECT COUNT(*) FROM probes WHERE schedule_label = 'full' AND dedupe_key GLOB '*|[12]'`},
	{"a full reading's attempt started the day after its reading", `SELECT COUNT(*) FROM probes a JOIN probes b
		ON b.promise_hash = a.promise_hash AND b.scheduled_at = a.scheduled_at AND b.validator_address = a.validator_address AND b.vantage = a.vantage
		WHERE a.schedule_label = 'full' AND b.schedule_label = 'full' AND substr(b.started_at, 1, 10) > substr(a.started_at, 1, 10)`},
	{"a full reading's attempt owed and not on record", `SELECT COUNT(*) FROM probes a WHERE a.next_attempt_due IS NOT NULL
		AND NOT EXISTS (SELECT 1 FROM probes b WHERE b.promise_hash = a.promise_hash AND b.scheduled_at = a.scheduled_at
			AND b.validator_address = a.validator_address AND b.vantage = a.vantage AND b.started_at > a.started_at)`},
	{"a short answer of the validator's own rows", `SELECT COUNT(*) FROM probes WHERE schedule_label = 'full' AND outcome = 'PARTIAL' AND rows_subset_of_assignment = 1`},
	{"a short answer of rows not the validator's own", `SELECT COUNT(*) FROM probes WHERE schedule_label = 'full' AND outcome = 'PARTIAL' AND rows_subset_of_assignment = 0`},
}

// TestDayPartsEquivalence runs the harness over many seeds: every figure
// from the partials equal, byte for byte, to the shipped statements', after
// every pass, after the fast tick between two passes, and between the
// statements of every prune (pruneLikeBefore).
// TENSILE_DAYPARTS_FULL runs every case at every pass at the design's
// scale (200 publications a day); TENSILE_DAYPARTS_SEEDS sets the seeds.
func TestDayPartsEquivalence(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	seeds := equivSeeds()
	total := newTally()
	results := make([]*equivTally, seeds)
	for i := 0; i < seeds; i++ {
		t.Run("seed="+strconv.Itoa(i+1), func(t *testing.T) {
			t.Parallel()
			cfg := defaultSimConfig(uint64(i + 1))
			// At the design's scale every case is compared after every pass
			// of one to three hours; by default, at a seventh of the volume,
			// a sample after every pass of two to five hours, so the seeds
			// fit the package's time.
			sample, step := 0, [2]int{60, 180}
			if !equivFull() {
				cfg.perDay, sample, step = 30, 2, [2]int{120, 300}
			}
			results[i] = equivRun(t, cfg, sample, true, step)
		})
	}
	t.Cleanup(func() {
		for _, r := range results {
			if r != nil {
				total.add(r)
			}
		}
		t.Logf("%d seeds:\n%s", seeds, total)
	})
}

// TestDayPartsRollback runs the API with the partials, then as the build
// before them (partials off, no partials file written) while the collector
// goes on (holds verified and corrected, a collapse, an older build's
// prune), then with the partials again from the files written before:
// every change made meanwhile is found by the catch-up, and every figure is
// still exact.
func TestDayPartsRollback(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(101)
	cfg.perDay = 30
	s := newSim(t, cfg)
	s.apiOpts = []Option{withMergeDays(2)}
	s.plan()
	srv := s.openAPI()
	rng := rand.New(rand.NewPCG(101, 7))
	ctx := context.Background()
	tally := newTally()
	step := func(srv *Server, at time.Time, compare bool) {
		s.advance(at)
		if !at.Before(s.scen.backlog[0]) && at.Before(s.scen.backlog[1]) {
			return
		}
		s.pass()
		if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
			t.Fatal(err)
		}
		if compare {
			tally.add(s.compare(srv, rng, 3, "rollback"))
		}
	}
	at := s.t0.Add(time.Hour)
	for ; at.Before(s.t0.Add(4*24*time.Hour + 12*time.Hour)); at = at.Add(3 * time.Hour) {
		step(srv, at, true)
	}
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	// N-1: the partials off, nothing of them written, the collector going
	// on through the verified range, its corrections, the collapse and the
	// prune.
	old := s.openAPI(WithDayParts(false))
	for ; at.Before(s.t0.Add(9 * 24 * time.Hour)); at = at.Add(3 * time.Hour) {
		step(old, at, false)
		if _, err := old.networkSnapshot(ctx, windowFor("30d", at), excludeSet{}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := old.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	// N again, from the files written before the rollback.
	back := s.openAPI()
	tally.add(s.compare(back, rng, 0, "back"))
	if !strings.HasPrefix(back.parts.origin, "loaded") {
		t.Errorf("the API back from the rollback did not begin from its files: %s", back.parts.origin)
	}
	if back.parts.dropped == 0 {
		t.Errorf("nothing the rollback's leg changed was dropped from the partials kept before it")
	}
	for ; !at.After(s.t0.Add(time.Duration(cfg.days)*24*time.Hour + 6*time.Hour)); at = at.Add(3 * time.Hour) {
		step(back, at, true)
	}
	t.Logf("rollback N -> N-1 -> N:\n%s", tally)
}
