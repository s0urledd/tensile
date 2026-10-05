package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// histSim is a record of five days sealed as far as it is due, its
// partials written, the API's histogram cache at mb megabytes and every
// window of more than merge sealed days ranked in bounded memory.
func histSim(t *testing.T, seed uint64, mb, merge int) (*sim, *Server) {
	t.Helper()
	cfg := defaultSimConfig(seed)
	cfg.perDay, cfg.days = 10, 5
	s := newSim(t, cfg)
	s.apiOpts = []Option{withMergeDays(merge)}
	s.plan()
	srv := s.openAPI(WithHistCache(mb))
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(4*24*time.Hour + 6*time.Hour)); at = at.Add(6 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	ctx := context.Background()
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	return s, srv
}

// sealedRowDays is the newest epoch's sealed row days, in order.
func sealedRowDays(srv *Server) []*rowDay {
	srv.parts.mu.Lock()
	defer srv.parts.mu.Unlock()
	var out []*rowDay
	for _, rd := range srv.parts.cur.rows {
		out = append(out, rd)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Day < out[j].Day })
	return out
}

// TestRowDaysKeepNoHistogramInMemory: a sealed row day keeps in memory
// everything a window sums of it but its histograms, which its seal file
// holds, from the moment it is sealed and after a start from the files;
// the index holds none either. Every figure is still the shipped
// statements', with the histograms kept in memory within the cache's bound
// and with none kept.
func TestRowDaysKeepNoHistogramInMemory(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	s, srv := histSim(t, 41, DefaultHistCacheMB, mergeDaysDefault)
	check := func(srv *Server, when string) {
		t.Helper()
		days := sealedRowDays(srv)
		if len(days) < 3 {
			t.Fatalf("%s: %d row days sealed", when, len(days))
		}
		bins := 0
		for _, rd := range days {
			if rd.file == "" || rd.digest == "" {
				t.Errorf("%s: row day %s names no seal file", when, rd.Day)
			}
			for addr, p := range rd.Vals {
				if p.Lat != nil || p.Tput != nil {
					t.Errorf("%s: row day %s holds %s's histograms in memory", when, rd.Day, addr[:8])
				}
			}
			srv.parts.mu.Lock()
			e := srv.parts.cur
			srv.parts.mu.Unlock()
			whole, err := srv.parts.readRowSeal(e, rd)
			if err != nil {
				t.Fatalf("%s: row day %s: %v", when, rd.Day, err)
			}
			for addr, p := range whole.Vals {
				bins += len(p.Lat) + len(p.Tput)
				q := *p
				q.Lat, q.Tput = nil, nil
				if !sameJSON(&q, rd.Vals[addr]) {
					t.Errorf("%s: row day %s: %s's counts in memory are not its seal file's", when, rd.Day, addr[:8])
				}
			}
		}
		if bins == 0 {
			t.Errorf("%s: the seal files hold no histogram", when)
		}
	}
	check(srv, "sealed")
	b, err := os.ReadFile(filepath.Join(s.snaps, dayPartsFile))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte(`"lat":`)) || bytes.Contains(b, []byte(`"tput":`)) {
		t.Errorf("the index holds histograms")
	}
	rng := rand.New(rand.NewPCG(41, 1))
	tally := s.compare(srv, rng, 0, "sealed")
	for _, mb := range []int{DefaultHistCacheMB, 0} {
		next := s.openAPI(WithHistCache(mb))
		tally.add(s.compare(next, rng, 0, "loaded"))
		if !strings.HasPrefix(next.parts.origin, "loaded") {
			t.Fatalf("not loaded: %s", next.parts.origin)
		}
		check(next, "loaded")
		held, n := next.parts.hists.held()
		if mb == 0 && (held != 0 || n != 0) {
			t.Errorf("with no cache, %d bytes of %d days kept", held, n)
		}
		if mb > 0 && n == 0 {
			t.Errorf("with a cache, no day kept")
		}
	}
	t.Logf("%s", tally)
}

// TestAWindowOverAnUnusableSealFileReadsItRaw: a seal file removed,
// replaced by another day's or damaged while the API runs is met by the
// first window that sums its day: that window reads the day raw, exactly
// what the seal held, the day is dropped and logged, and the sealer seals
// it again under a new name. No figure differs at any point, with the
// windows summed whole and ranked in bounded memory.
func TestAWindowOverAnUnusableSealFileReadsItRaw(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	for _, merge := range []int{mergeDaysDefault, 0} {
		t.Run(fmt.Sprintf("merge=%d", merge), func(t *testing.T) {
			t.Parallel()
			unusableRun(t, merge)
		})
	}
}

func unusableRun(t *testing.T, merge int) {
	s, srv := histSim(t, 42, 0, merge)
	var logs logLines
	srv.parts.log = logs.logf
	days := sealedRowDays(srv)
	if len(days) < 3 {
		t.Fatalf("%d row days sealed", len(days))
	}
	dir := srv.parts.sealDir()
	gone, swapped, damaged := days[0], days[1], days[2]
	if err := os.Remove(filepath.Join(dir, gone.file)); err != nil {
		t.Fatal(err)
	}
	other, err := os.ReadFile(filepath.Join(dir, damaged.file))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, swapped.file), other, 0o644); err != nil {
		t.Fatal(err)
	}
	b := append([]byte(nil), other...)
	b[len(b)/2] ^= 1
	if err := os.WriteFile(filepath.Join(dir, damaged.file), b, 0o644); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(42, 1))
	tally := s.compare(srv, rng, 0, "seal files unusable")
	for _, rd := range []*rowDay{gone, swapped, damaged} {
		if sealedRow(srv, rd.Day) {
			t.Errorf("row day %s, its seal file unusable, is still sealed", rd.Day)
		}
		if len(logs.with("row day "+rd.Day+": its seal file cannot be used")) != 1 {
			t.Errorf("row day %s: not logged once:\n%s", rd.Day, strings.Join(logs.lines, "\n"))
		}
	}
	if srv.parts.unusable == 0 {
		t.Fatal("no window met an unusable seal file")
	}
	ctx := context.Background()
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	for _, rd := range []*rowDay{gone, swapped, damaged} {
		srv.parts.mu.Lock()
		again := srv.parts.cur.rows[rd.Day]
		srv.parts.mu.Unlock()
		if again == nil || again.file == rd.file || again.Gen <= rd.Gen {
			t.Errorf("row day %s was not sealed again under a new name: %+v", rd.Day, again)
		}
	}
	before := srv.parts.unusable
	tally.add(s.compare(srv, rng, 0, "sealed again"))
	if srv.parts.unusable != before {
		t.Errorf("the days sealed again met unusable seal files")
	}
	t.Logf("%s", tally)
}

// TestAReadRacingASealAgainSumsTheSealItsEpochNames: a computation holds an
// epoch in which a day is sealed, and before it reads the day's seal file
// rows of the day arrive, a catch-up drops it, the sealer seals it again
// under a new name and a write of the partials removes the old file; or
// the old file's name comes to hold the new file's bytes. The computation
// never sums the new seal, which holds rows its snapshot does not: it
// reads the day raw, in its own snapshot, and every figure is the shipped
// statements' in that snapshot. The newest epoch keeps the new seal. With
// the windows summed whole and ranked in bounded memory.
func TestAReadRacingASealAgainSumsTheSealItsEpochNames(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	for _, merge := range []int{mergeDaysDefault, 0} {
		t.Run(fmt.Sprintf("merge=%d", merge), func(t *testing.T) {
			t.Parallel()
			raceRun(t, merge)
		})
	}
}

func raceRun(t *testing.T, merge int) {
	for _, replace := range []bool{false, true} {
		s, srv := histSim(t, 43, 0, merge)
		days := sealedRowDays(srv)
		if len(days) < 3 {
			t.Fatalf("%d row days sealed", len(days))
		}
		d := days[len(days)/2]
		ctx := context.Background()
		raced := false
		var newer *rowDay
		srv.parts.beforeRead = func(day string) {
			if day != d.Day || raced {
				return
			}
			raced = true
			// Rows of the day arrive: copies of a validator's served readings,
			// each a hundred seconds slower, so the day sealed again ranks
			// otherwise.
			db := s.st.DB()
			for _, q := range []string{
				`DROP TABLE IF EXISTS temp.sim_race`,
				`CREATE TEMP TABLE sim_race AS SELECT * FROM probes WHERE started_at >= '` + dayLo(d.Day) + `' AND started_at <= '` + dayHi(d.Day) +
					`' AND classification = 'HEALTHY' AND total_duration_ms > 0 AND validator_address = '` + s.vals[0].addr + `'`,
				`UPDATE temp.sim_race SET dedupe_key = 'race|' || dedupe_key, total_duration_ms = total_duration_ms + 100000, download_ms = download_ms * 7`,
				`INSERT INTO probes SELECT * FROM temp.sim_race`,
				`DROP TABLE temp.sim_race`,
			} {
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
			catchUpNow(t, srv)
			if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
				t.Fatal(err)
			}
			if err := srv.keepDerived(ctx); err != nil {
				t.Fatal(err)
			}
			srv.parts.mu.Lock()
			newer = srv.parts.cur.rows[d.Day]
			srv.parts.mu.Unlock()
			if newer == nil || newer.file == d.file {
				t.Fatalf("row day %s was not sealed again: %+v", d.Day, newer)
			}
			old := filepath.Join(srv.parts.sealDir(), d.file)
			if _, err := os.Stat(old); !os.IsNotExist(err) {
				t.Errorf("the old seal file of row day %s is still there: %v", d.Day, err)
			}
			if replace {
				b, err := os.ReadFile(filepath.Join(srv.parts.sealDir(), newer.file))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(old, b, 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}
		rng := rand.New(rand.NewPCG(43, 1))
		tally := s.compare(srv, rng, 0, "a read racing a seal again")
		if !raced {
			t.Fatalf("no computation read row day %s's seal file", d.Day)
		}
		if srv.parts.unusable == 0 {
			t.Errorf("the computation did not meet the old seal file as unusable")
		}
		srv.parts.mu.Lock()
		kept := srv.parts.cur.rows[d.Day]
		srv.parts.mu.Unlock()
		if kept == nil || kept.digest != newer.digest {
			t.Errorf("the newest epoch lost the day sealed again: %+v", kept)
		}
		srv.parts.beforeRead = nil
		tally.add(s.compare(srv, rng, 0, "after the race"))
		t.Logf("replace %v:\n%s", replace, tally)
	}
}

// TestASealFileIsWrittenWhenSealedAndRemovedWhenRefused: the sealer writes
// a row day's seal file before it publishes the day, so a write of the
// partials from an epoch taken before the seal leaves it; and a seal a
// catch-up refused leaves no file behind.
func TestASealFileIsWrittenWhenSealedAndRemovedWhenRefused(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(44)
	cfg.perDay, cfg.days = 10, 5
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(4*24*time.Hour + 6*time.Hour)); at = at.Add(6 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	ctx := context.Background()
	if _, err := srv.sealDue(ctx, 4); err != nil {
		t.Fatal(err)
	}
	srv.parts.mu.Lock()
	early := srv.parts.cur
	srv.parts.mu.Unlock()
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if err := srv.parts.write(ctx, srv, early); err != nil {
		t.Fatal(err)
	}
	days := sealedRowDays(srv)
	if len(days) <= len(early.rows) {
		t.Fatalf("nothing sealed after the early epoch: %d row days, %d before", len(days), len(early.rows))
	}
	for _, rd := range days {
		if _, err := os.Stat(filepath.Join(srv.parts.sealDir(), rd.file)); err != nil {
			t.Errorf("row day %s: its seal file is gone after a write of an older epoch: %v", rd.Day, err)
		}
	}
	tally := s.compare(srv, rand.New(rand.NewPCG(44, 1)), 0, "after a write of an older epoch")

	// A seal refused: a row of the day lands, and a catch-up drops it,
	// between its read and its publish.
	d := days[1].Day
	var promises []string
	rows, err := s.st.DB().Query(`SELECT DISTINCT promise_hash FROM probes WHERE started_at >= ? AND started_at <= ?
		AND classification IN ('HEALTHY', 'FAULT') ORDER BY promise_hash LIMIT 2`, dayLo(d), dayHi(d))
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			t.Fatal(err)
		}
		promises = append(promises, h)
	}
	rows.Close()
	if len(promises) < 2 || !s.lateRow(d, promises[0]) {
		t.Fatalf("no rows to write on %s: %v", d, promises)
	}
	catchUpNow(t, srv)
	refused := false
	srv.parts.beforePublish = func(kind, day string) {
		if kind == "row" && day == d && !refused {
			refused = true
			if !s.lateRow(d, promises[1]) {
				t.Errorf("no row to write on %s", d)
			}
			catchUpNow(t, srv)
		}
	}
	files := func() map[string]bool {
		out := map[string]bool{}
		ents, err := os.ReadDir(srv.parts.sealDir())
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			out[e.Name()] = true
		}
		return out
	}
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if !refused {
		t.Fatalf("row day %s was not sealed again", d)
	}
	srv.parts.mu.Lock()
	pending, gen := len(srv.parts.pending), srv.parts.gens["row:"+d]
	srv.parts.mu.Unlock()
	if sealedRow(srv, d) {
		t.Errorf("row day %s's refused seal was published", d)
	}
	if pending != 0 {
		t.Errorf("%d seal file(s) still pending", pending)
	}
	if files()[sealName("row", d, gen)] {
		t.Errorf("the refused seal of row day %s left its file %s", d, sealName("row", d, gen))
	}
	// Tried again a second after the refusal (sealRow).
	srv.parts.beforePublish = nil
	s.now = s.now.Add(2 * time.Second)
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if !sealedRow(srv, d) {
		t.Errorf("row day %s was not sealed again", d)
	}
	tally.add(s.compare(srv, rand.New(rand.NewPCG(44, 2)), 0, "sealed again"))
	t.Logf("%s", tally)
}

// TestHistCacheKeepsTheNewestDays: the cache keeps within its bound, the
// newest days first: an older day does not push a newer one out, a day
// sealed again replaces its older seal, and a day dropped goes.
func TestHistCacheKeepsTheNewestDays(t *testing.T) {
	h := func(bins int) dayHists {
		return dayHists{"a": {lat: make([]hbin, bins)}}
	}
	one := h(100).size()
	c := newHistCache(3 * one)
	for _, d := range []string{"2026-03-05", "2026-03-03", "2026-03-04"} {
		c.put(d, "x"+d, h(100))
	}
	c.put("2026-03-02", "x2026-03-02", h(100))
	if c.get("2026-03-02", "x2026-03-02") != nil {
		t.Errorf("an older day pushed a newer one out")
	}
	c.put("2026-03-06", "x2026-03-06", h(100))
	if c.get("2026-03-03", "x2026-03-03") != nil || c.get("2026-03-06", "x2026-03-06") == nil {
		t.Errorf("a newer day did not push the oldest out")
	}
	c.put("2026-03-05", "y", h(100))
	if c.get("2026-03-05", "x2026-03-05") != nil || c.get("2026-03-05", "y") == nil {
		t.Errorf("a day sealed again did not replace its older seal")
	}
	c.retain(map[string]bool{"y": true, "x2026-03-06": true})
	if held, n := c.held(); n != 2 || held != 2*one {
		t.Errorf("after a day dropped: %d bytes of %d days", held, n)
	}
	c.put("2026-03-07", "z", h(1000))
	if c.get("2026-03-07", "z") != nil {
		t.Errorf("a day larger than the bound was kept")
	}
	off := newHistCache(0)
	off.put("2026-03-07", "z", h(1))
	if off.get("2026-03-07", "z") != nil {
		t.Errorf("a cache of no bytes kept a day")
	}
}

// TestTheWarmUpWritesTheSealFilesTheNextAPILoads: -warm-only seals every
// day due into its own directory, the row days' files written as they are
// sealed and the index without their histograms, and an API started on
// that directory loads them and sums the windows from them exactly.
func TestTheWarmUpWritesTheSealFilesTheNextAPILoads(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(45)
	cfg.perDay, cfg.days = 10, 5
	s := newSim(t, cfg)
	s.plan()
	s.openAPI()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(4*24*time.Hour + 6*time.Hour)); at = at.Add(6 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	warm := filepath.Join(t.TempDir(), "snapshots.next")
	clock := func(srv *Server) { srv.clock = func() time.Time { return s.now } }
	if err := WarmSnapshots(context.Background(), s.ro, VantageInfo{Name: simVantage}, nil, WithSnapshotDir(warm), clock); err != nil {
		t.Fatal(err)
	}
	var f partsFile
	if ok, why := readDerived(filepath.Join(warm, dayPartsFile), &f); !ok {
		t.Fatalf("the warm-up wrote no index: %s", why)
	}
	if len(f.Rows) < 3 || len(f.Rows) != countKind(f.Seals, "row") {
		t.Fatalf("the warm-up's index: %d row days, %d row seal files", len(f.Rows), countKind(f.Seals, "row"))
	}
	for key, ref := range f.Seals {
		if gone, why := checkSealFile(filepath.Join(warm, dayPartsDir, ref.File), ref.Digest); gone || why != "" {
			t.Errorf("%s: %s missing %v, %s", key, ref.File, gone, why)
		}
	}
	next := s.openAPI(WithSnapshotDir(warm))
	tally := s.compare(next, rand.New(rand.NewPCG(45, 1)), 0, "after the warm-up")
	if !strings.HasPrefix(next.parts.origin, "loaded") || len(sealedRowDays(next)) != len(f.Rows) {
		t.Errorf("the API after the warm-up: %s, %d row days of %d", next.parts.origin, len(sealedRowDays(next)), len(f.Rows))
	}
	t.Logf("%s", tally)
}

// countKind counts the seals of a kind in an index.
func countKind(seals map[string]sealRef, kind string) int {
	n := 0
	for key := range seals {
		if strings.HasPrefix(key, kind+":") {
			n++
		}
	}
	return n
}

// TestBinsReadAsTheDecoderReadsThem: the bins' own reader gives what the
// JSON decoder gives for every array Marshal writes, and for anything else
// it gives what the decoder gives, error or not.
func TestBinsReadAsTheDecoderReadsThem(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	cases := []string{`null`, `[]`, ` [ ] `, `[[1,2]]`, ` [ [ -5 , 0 ] , [7,1] ] `, `[[9223372036854775807,1],[-9223372036854775808,2]]`,
		`[[1,2,3]]`, `[[1]]`, `[[1.5,2]]`, `[[1e3,2]]`, `[[01,2]]`, `[[-0,2]]`, `[[9223372036854775808,1]]`, `[[1,2],]`, `[[1,2]`, `[1,2]`,
		`{}`, `"x"`, `[[1,"2"]]`, `[[1,2]] x`, `[null]`, `[[null,1]]`}
	for i := 0; i < 300; i++ {
		var bins []hbin
		for k := rng.IntN(50); k > 0; k-- {
			bins = append(bins, hbin{rng.Int64() - rng.Int64(), rng.Int64N(1 << uint(rng.IntN(62)+1))})
		}
		b, err := json.Marshal(hbins(bins))
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, string(b))
	}
	for _, c := range cases {
		var want []hbin
		werr := json.Unmarshal([]byte(c), &want)
		var got hbins
		gerr := json.Unmarshal([]byte(c), &got)
		if (werr == nil) != (gerr == nil) {
			t.Errorf("%s: error %v, the decoder's %v", c, gerr, werr)
			continue
		}
		if werr == nil && !sameJSON([]hbin(got), want) {
			t.Errorf("%s: read %v, the decoder %v", c, got, want)
		}
	}
}

// TestTheAuditDropsADayWhoseSealFileIsGone: the hourly audit reads a row
// day whole, its histograms from its seal file; with the file gone it
// drops the day, as a window does, and reports no difference: the file is
// gone, the day was not wrong.
func TestTheAuditDropsADayWhoseSealFileIsGone(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	s, srv := histSim(t, 46, 0, mergeDaysDefault)
	days := sealedRowDays(srv)
	if len(days) < 2 {
		t.Fatalf("%d row days sealed", len(days))
	}
	for _, rd := range days {
		if err := os.Remove(filepath.Join(srv.parts.sealDir(), rd.file)); err != nil {
			t.Fatal(err)
		}
	}
	res, err := srv.auditOnce(context.Background())
	srv.parts.noteAudit(res, err, s.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.diffs) > 0 || res.fallback {
		t.Errorf("the audit found a difference in a day whose file is gone: %v", res.diffs)
	}
	if n := len(sealedRowDays(srv)); n != len(days)-1 {
		t.Errorf("after the audit %d row days are sealed, want %d", n, len(days)-1)
	}
	if _, c := srv.parts.health(s.now); !c.OK {
		t.Errorf("/v1/health after the audit: %+v", c)
	}
	t.Logf("%s", s.compare(srv, rand.New(rand.NewPCG(46, 1)), 0, "seal files gone"))
}
