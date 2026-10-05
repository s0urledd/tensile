package api

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
)

// TestBucketsAreInTheOrderOfTheirValues: every value is in its bucket's
// bounds, and the buckets follow the values.
func TestBucketsAreInTheOrderOfTheirValues(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	vals := []int64{math.MinInt64, -1, 0, 1, 2, 63, 64, 65, 127, 128, 129, 255, 256, 1 << 40, 1<<62 - 1, 1 << 62, math.MaxInt64}
	for i := 0; i < 20000; i++ {
		vals = append(vals, rng.Int64N(1<<uint(1+rng.IntN(62))))
	}
	for _, v := range vals {
		b := bucketOf(v)
		lo, hi := bucketBounds(b)
		if v < lo || v > hi {
			t.Fatalf("%d is in bucket %d, whose bounds are %d..%d", v, b, lo, hi)
		}
		if b > 0 && bucketOf(lo) != b || bucketOf(hi) != b {
			t.Fatalf("bucket %d's bounds %d..%d are not its own", b, lo, hi)
		}
		if hi < math.MaxInt64 && bucketOf(hi+1) != b+1 {
			t.Fatalf("after bucket %d (to %d) comes %d", b, hi, bucketOf(hi+1))
		}
	}
}

// rankWorld is a record of days of random histograms for the selection.
type rankWorld struct {
	rng   *rand.Rand
	addrs []string
	files map[string]dayHists
	next  int
	// shape draws a value: a narrow range with many repeats (service
	// times), a wide one with almost none (transfer rates), the odd value
	// at or below zero and the odd huge one.
	shape func(kind histKind) int64
}

func newRankWorld(seed uint64, vals int) *rankWorld {
	w := &rankWorld{rng: rand.New(rand.NewPCG(seed, 5)), files: map[string]dayHists{}}
	for i := 0; i < vals; i++ {
		w.addrs = append(w.addrs, fmt.Sprintf("v%02d", i))
	}
	drift := 1 + w.rng.Float64()
	w.shape = func(kind histKind) int64 {
		switch x := w.rng.IntN(200); {
		case x == 0:
			return -w.rng.Int64N(5)
		case x == 1:
			return 1<<62 + w.rng.Int64N(1<<40)
		}
		if kind == kindLat {
			return 20 + int64(float64(w.rng.IntN(900))*drift)
		}
		return 100_000 + int64(w.rng.Float64()*drift*50_000_000)
	}
	return w
}

// hists draws one day's histograms, n readings a validator at most.
func (w *rankWorld) hists(n int) dayHists {
	h := dayHists{}
	for _, a := range w.addrs {
		if w.rng.IntN(6) == 0 {
			continue // no reading of this validator this day
		}
		v := &valHists{}
		for _, k := range []histKind{kindLat, kindTput} {
			m := map[int64]int64{}
			for i := w.rng.IntN(n + 1); i > 0; i-- {
				m[w.shape(k)]++
			}
			bins := histOf(m).segs[0].bins
			if k == kindLat {
				v.lat = bins
			} else {
				v.tput = bins
			}
		}
		h[a] = v
	}
	return h
}

// day is a sealed day of fresh histograms, its file kept by digest.
func (w *rankWorld) day(name string, n int) *rowDay {
	w.next++
	digest := fmt.Sprintf("d%06d", w.next)
	w.files[digest] = w.hists(n)
	return &rowDay{Day: name, digest: digest, file: name + ".json"}
}

func (w *rankWorld) read(rd *rowDay) (dayHists, error) { return w.files[rd.digest], nil }

// keys is every rank of every validator and the network's.
func (w *rankWorld) keys() []rankKey {
	var out []rankKey
	for _, a := range w.addrs {
		out = append(out, rankKey{scopeKey{a, kindLat}, q50}, rankKey{scopeKey{a, kindLat}, q95}, rankKey{scopeKey{a, kindTput}, q50})
	}
	return append(out, rankKey{scopeKey{"", kindLat}, q50}, rankKey{scopeKey{"", kindLat}, q95})
}

// brute is every rank merged whole, as a short window sums them.
func (w *rankWorld) brute(days []*rowDay, raw dayHists, keys []rankKey, ex excludeSet) rankResult {
	res := rankResult{n: map[scopeKey]int64{}, at: map[rankKey]rankAnswer{}}
	for _, k := range keys {
		m := map[int64]int64{}
		add := func(h dayHists) {
			binsOf(h, k.scope, ex, func(l []hbin) {
				for _, b := range l {
					m[b[0]] += b[1]
				}
			})
		}
		for _, rd := range days {
			add(w.files[rd.digest])
		}
		add(raw)
		h := histOf(m)
		res.n[k.scope] = h.n
		v, ok := h.at(k.q.rank(h.n))
		res.at[k] = rankAnswer{v, ok}
	}
	return res
}

func sameRanks(t *testing.T, label string, keys []rankKey, want, got rankResult) {
	t.Helper()
	for _, k := range keys {
		if want.n[k.scope] != got.n[k.scope] {
			t.Fatalf("%s: %+v: %d values, want %d", label, k.scope, got.n[k.scope], want.n[k.scope])
		}
		if w, g := want.at[k], got.at[k]; w != g {
			t.Fatalf("%s: %+v: %+v, want %+v", label, k, g, w)
		}
		h := got.histFor(k.scope)
		v, ok := h.at(k.q.rank(h.n))
		if w := want.at[k]; v != w.v || ok != w.ok {
			t.Fatalf("%s: %+v as a hist: %d %v, want %+v", label, k, v, ok, w)
		}
	}
}

// TestRankSelectionIsExact: the ranks a long window reads, selected in two
// reads of its days or from the bands kept across its refreshes, are the
// ranks of everything merged whole, as the days come and go, are sealed
// again, the raw part grows and shrinks and the record drifts: the bands
// built again when a rank has left them, and only then.
func TestRankSelectionIsExact(t *testing.T) {
	ctx := context.Background()
	rebuilt := 0
	defer func() {
		if rebuilt == 0 {
			t.Errorf("no band was ever built again: a rank never left its band")
		}
		t.Logf("%d band(s) built again over every seed", rebuilt)
	}()
	for seed := uint64(1); seed <= 60; seed++ {
		w := newRankWorld(seed, 1+int(seed%6))
		perDay := 1 + w.rng.IntN(60)
		var days []*rowDay
		for d := 0; d < 3+w.rng.IntN(20); d++ {
			days = append(days, w.day(fmt.Sprintf("2026-01-%02d", d+1), perDay))
		}
		set := &bandSet{bands: map[rankKey]*rankBand{}}
		keys := w.keys()
		built := 0
		for round := 0; round < 12; round++ {
			label := fmt.Sprintf("seed %d round %d", seed, round)
			raw := w.hists(w.rng.IntN(perDay + 1))
			var ex excludeSet
			if w.rng.IntN(3) == 0 {
				ex.addrs = []any{w.addrs[w.rng.IntN(len(w.addrs))]}
			}
			sel := &rankSel{read: w.read, days: days, raw: raw, keys: keys, ex: ex}
			got, err := sel.run(ctx)
			if err != nil {
				t.Fatal(err)
			}
			sameRanks(t, label+" (two reads)", keys, w.brute(days, raw, keys, ex), got)
			// The bands are kept for the network as a whole.
			sel.ex = excludeSet{}
			got, err = sel.runBands(ctx, set)
			if err != nil {
				t.Fatal(err)
			}
			sameRanks(t, label+" (bands)", keys, w.brute(days, raw, keys, excludeSet{}), got)
			if round == 0 && set.built == 0 {
				t.Fatalf("%s: no band built", label)
			}
			// Once more, nothing moved: no band is built again.
			before := set.built
			again, err := sel.runBands(ctx, set)
			if err != nil {
				t.Fatal(err)
			}
			sameRanks(t, label+" (bands again)", keys, w.brute(days, raw, keys, excludeSet{}), again)
			if set.built != before {
				t.Fatalf("%s: %d band(s) built again with nothing moved", label, set.built-before)
			}
			if round > 0 {
				rebuilt += set.built - built
			}
			built = set.built
			// The record moves: a day sealed, one sealed again, one dropped.
			switch w.rng.IntN(4) {
			case 0:
				days = append(days, w.day(fmt.Sprintf("2026-02-%02d", round+1), perDay))
			case 1:
				i := w.rng.IntN(len(days))
				days[i] = w.day(days[i].Day, perDay)
			case 2:
				if len(days) > 1 {
					i := w.rng.IntN(len(days))
					days = append(days[:i], days[i+1:]...)
				}
			default:
				days = append(days, w.day(fmt.Sprintf("2026-03-%02d", round+1), 3*perDay))
			}
		}
	}
}

// TestBandsHoldAFewDaysOfAWindow: a long window's bands hold a few days'
// bins of each rank, not the window's, and a refresh with one day more
// reads that day alone.
func TestBandsHoldAFewDaysOfAWindow(t *testing.T) {
	ctx := context.Background()
	w := newRankWorld(99, 4)
	w.shape = func(kind histKind) int64 {
		if kind == kindLat {
			return 20 + w.rng.Int64N(900)
		}
		return 100_000 + w.rng.Int64N(50_000_000)
	}
	var days []*rowDay
	for d := 0; d < 120; d++ {
		days = append(days, w.day(fmt.Sprintf("d%03d", d), 400))
	}
	keys := w.keys()
	set := &bandSet{bands: map[rankKey]*rankBand{}}
	reads := 0
	read := func(rd *rowDay) (dayHists, error) { reads++; return w.read(rd) }
	raw := w.hists(200)
	sel := &rankSel{read: read, days: days, raw: raw, keys: keys}
	got, err := sel.runBands(ctx, set)
	if err != nil {
		t.Fatal(err)
	}
	sameRanks(t, "built", keys, w.brute(days, raw, keys, excludeSet{}), got)
	var all int
	for _, rd := range days {
		for _, v := range w.files[rd.digest] {
			all += len(v.lat) + len(v.tput)
		}
	}
	bins, _ := set.held()
	if bins*10 > all {
		t.Errorf("the bands hold %d bins of the window's %d", bins, all)
	}
	t.Logf("built with %d reads; the bands hold %d bins of the window's %d", reads, bins, all)
	reads = 0
	days = append(days, w.day("d120", 400))
	sel.days = days
	got, err = sel.runBands(ctx, set)
	if err != nil {
		t.Fatal(err)
	}
	sameRanks(t, "a day more", keys, w.brute(days, raw, keys, excludeSet{}), got)
	if reads != 1 {
		t.Errorf("a refresh with one day more read %d days", reads)
	}
}
