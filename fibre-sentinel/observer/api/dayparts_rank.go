package api

import (
	"context"
	"math"
	"math/bits"
	"sort"
	"sync"
)

// The ranks of a long window, exact, in bounded memory.
//
// A window's service time and transfer rate are values at ranks of the
// histograms of every day it covers. Summed whole, as a short window is
// (mergeDays and fewer sealed days), that is a map of every distinct value
// of the window, and a transfer rate is nearly always a value of its own:
// for "all", one bin per reading ever made. So a window of more days reads
// its days twice instead, and keeps only what the ranks need:
//
//   - first the counts of every value in buckets of 1/64 of an octave
//     (bucketOf), from which the bucket each rank falls in, and the rank
//     within it, are known exactly;
//   - then the bins of those buckets alone, from which the value is.
//
// Every count is exact and the value is the one at the rank, so the
// figure is the one a merge of everything would give, which is the one the
// shipped statement gives (the histograms' own proof, rowsql.go).
//
// Reading every day twice on every refresh is still work that grows with
// the record, and "all" is refreshed every few minutes. So the live "all"
// window keeps, for each rank, a band of values around it (rankBand): for
// each day, how many of its values fall below the band, how many above,
// and its bins inside. A refresh sums those, adds the raw rows of the days
// not sealed, and finds the rank inside the band, reading no file but a
// newly sealed day's; a rank that has moved out of its band (the raw part
// grew past it, or the record drifted) has the band built again by the two
// reads above, centred on the rank, wide enough for twice as many values
// as a day or the raw part holds on either side. A band's day is kept by
// the digest of its seal file, so a day sealed again is another day to it,
// and a day dropped is let go at the next refresh. What a band holds is a
// few days' bins of each rank, whatever the record's length.

// mergeDaysDefault is how many sealed days a window may cover and still be
// summed whole (a 30d window and the one before it).
const mergeDaysDefault = 31

// histKind is which of a validator's histograms.
type histKind uint8

const (
	kindLat histKind = iota
	kindTput
)

// quantile is a rank the figures read: the median (c+1)/2 or the 95th
// percentile (95c+99)/100 (valLatencySQL, hist.percentiles).
type quantile uint8

const (
	q50 quantile = iota
	q95
)

func (q quantile) rank(n int64) int64 {
	if q == q50 {
		return (n + 1) / 2
	}
	return (n*95 + 99) / 100
}

// scopeKey is one population ranked: a validator's histogram of a kind, or,
// with no address, the network's service times (every validator the
// selection keeps).
type scopeKey struct {
	addr string
	kind histKind
}

// rankKey is a rank of a population.
type rankKey struct {
	scope scopeKey
	q     quantile
}

// binsOf calls f with every bin list of scope in h: a validator's own, or
// for the network every validator's service times but those ex names.
func binsOf(h dayHists, sc scopeKey, ex excludeSet, f func([]hbin)) {
	if sc.addr == "" {
		for addr, v := range h {
			if !ex.has(addr) && len(v.lat) > 0 {
				f(v.lat)
			}
		}
		return
	}
	v := h[sc.addr]
	if v == nil {
		return
	}
	if sc.kind == kindLat {
		f(v.lat)
	} else {
		f(v.tput)
	}
}

// bucketOf is v's bucket: 0 for v <= 0, v itself below 64, and above it
// 1/64 of an octave: the leading bit and the six after it. Buckets are in
// the order of their values.
func bucketOf(v int64) int {
	switch {
	case v <= 0:
		return 0
	case v < 64:
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 1
	return 64 + (e-6)*64 + int((uint64(v)>>(e-6))&63)
}

// bucketBounds are the first and last value of bucket b.
func bucketBounds(b int) (lo, hi int64) {
	switch {
	case b <= 0:
		return math.MinInt64, 0
	case b < 64:
		return int64(b), int64(b)
	}
	e, m := (b-64)/64+6, uint64((b-64)%64)
	return int64((64 + m) << (e - 6)), int64(((64 + m + 1) << (e - 6)) - 1)
}

// rankAnswer is a rank's value; ok is false for a rank no value has.
type rankAnswer struct {
	v  int64
	ok bool
}

// rankResult is what a selection found: each population's count and the
// value at each rank asked.
type rankResult struct {
	n  map[scopeKey]int64
	at map[rankKey]rankAnswer
}

// histFor is the hist of scope the figures read: its count and the values
// at its ranks.
func (r rankResult) histFor(sc scopeKey) hist {
	h := hist{n: r.n[sc]}
	for _, q := range []quantile{q50, q95} {
		if a, ok := r.at[rankKey{sc, q}]; ok && a.ok {
			h.segs = append(h.segs, histSeg{base: q.rank(h.n) - 1, bins: []hbin{{a.v, 1}}})
		}
	}
	return h
}

// daySource reads a sealed day's histograms; an error wraps errUnusable.
type daySource func(rd *rowDay) (dayHists, error)

// errDayUnusable carries the sealed day a selection could not read.
type errDayUnusable struct {
	rd  *rowDay
	err error
}

func (e *errDayUnusable) Error() string { return e.err.Error() }
func (e *errDayUnusable) Unwrap() error { return e.err }

// rankSel is one selection: the sealed days' histograms read through read,
// the raw part's, the ranks asked, and who the network is.
type rankSel struct {
	read daySource
	days []*rowDay
	raw  dayHists
	keys []rankKey
	ex   excludeSet
}

// scopes are the populations the keys rank, each once.
func (s *rankSel) scopes() []scopeKey {
	seen := map[scopeKey]bool{}
	var out []scopeKey
	for _, k := range s.keys {
		if !seen[k.scope] {
			seen[k.scope] = true
			out = append(out, k.scope)
		}
	}
	return out
}

// candidate is the buckets one rank is looked for in, and what pass 1 knew
// of them.
type candidate struct {
	key    rankKey
	n, r   int64
	lo, hi int64 // values, inclusive
	below  int64 // values below lo, sealed and raw
}

// passOne counts every population's values by bucket, the sealed days' and
// the raw part's, and places each rank's candidate: its bucket, widened on
// either side until it holds margin(key, n) values beside the rank, or the
// values run out.
func (s *rankSel) passOne(ctx context.Context, margin func(rankKey, int64) int64) ([]candidate, map[scopeKey]int64, error) {
	scopes := s.scopes()
	counts := make(map[scopeKey]map[int]int64, len(scopes))
	for _, sc := range scopes {
		counts[sc] = map[int]int64{}
	}
	add := func(h dayHists) {
		for _, sc := range scopes {
			c := counts[sc]
			binsOf(h, sc, s.ex, func(bins []hbin) {
				for _, b := range bins {
					c[bucketOf(b[0])] += b[1]
				}
			})
		}
	}
	for _, rd := range s.days {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		h, err := s.read(rd)
		if err != nil {
			return nil, nil, &errDayUnusable{rd, err}
		}
		add(h)
	}
	add(s.raw)
	ns := map[scopeKey]int64{}
	order := map[scopeKey][]int{}
	for sc, c := range counts {
		bs := make([]int, 0, len(c))
		for b, k := range c {
			if k > 0 {
				bs = append(bs, b)
				ns[sc] += k
			}
		}
		sort.Ints(bs)
		order[sc] = bs
	}
	var out []candidate
	for _, k := range s.keys {
		n := ns[k.scope]
		r := k.q.rank(n)
		if n == 0 || r < 1 {
			continue
		}
		bs, c := order[k.scope], counts[k.scope]
		var before int64
		i := 0
		for ; i < len(bs); i++ {
			if before+c[bs[i]] >= r {
				break
			}
			before += c[bs[i]]
		}
		m := margin(k, n)
		lo, hi := i, i
		for got := r - before - 1; got < m && lo > 0; got += c[bs[lo]] {
			lo--
		}
		for got := before + c[bs[i]] - r; got < m && hi < len(bs)-1; got += c[bs[hi]] {
			hi++
		}
		var below int64
		for j := 0; j < lo; j++ {
			below += c[bs[j]]
		}
		vlo, _ := bucketBounds(bs[lo])
		_, vhi := bucketBounds(bs[hi])
		out = append(out, candidate{key: k, n: n, r: r, lo: vlo, hi: vhi, below: below})
	}
	return out, ns, nil
}

// inside appends to dst the bins of bins in [lo, hi], and counts those
// below and above.
func inside(dst []hbin, bins []hbin, lo, hi int64) (_ []hbin, below, above int64) {
	for _, b := range bins {
		switch {
		case b[0] < lo:
			below += b[1]
		case b[0] > hi:
			above += b[1]
		default:
			dst = append(dst, b)
		}
	}
	return dst, below, above
}

// mergeBins merges bin lists into one, ascending, each value once.
func mergeBins(lists ...[]hbin) []hbin {
	m := map[int64]int64{}
	for _, l := range lists {
		for _, b := range l {
			m[b[0]] += b[1]
		}
	}
	out := make([]hbin, 0, len(m))
	for v, n := range m {
		if n > 0 {
			out = append(out, hbin{v, n})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// pick is the value at rank r of bins, base values being below them.
func pick(bins []hbin, base, r int64) rankAnswer {
	r -= base
	if r < 1 {
		return rankAnswer{}
	}
	for _, b := range bins {
		if r <= b[1] {
			return rankAnswer{v: b[0], ok: true}
		}
		r -= b[1]
	}
	return rankAnswer{}
}

// run selects every rank with two reads of every sealed day.
func (s *rankSel) run(ctx context.Context) (rankResult, error) {
	cands, ns, err := s.passOne(ctx, func(rankKey, int64) int64 { return 0 })
	if err != nil {
		return rankResult{}, err
	}
	// Each rank's bucket, its values counted as they come: what is held is
	// the bucket's distinct values.
	counts := make([]map[int64]int64, len(cands))
	for i := range counts {
		counts[i] = map[int64]int64{}
	}
	collect := func(h dayHists) {
		for i, c := range cands {
			m := counts[i]
			binsOf(h, c.key.scope, s.ex, func(l []hbin) {
				for _, b := range l {
					if b[0] >= c.lo && b[0] <= c.hi {
						m[b[0]] += b[1]
					}
				}
			})
		}
	}
	for _, rd := range s.days {
		if err := ctx.Err(); err != nil {
			return rankResult{}, err
		}
		h, err := s.read(rd)
		if err != nil {
			return rankResult{}, &errDayUnusable{rd, err}
		}
		collect(h)
	}
	collect(s.raw)
	res := rankResult{n: ns, at: map[rankKey]rankAnswer{}}
	for i, c := range cands {
		res.at[c.key] = pick(histOf(counts[i]).segs[0].bins, c.below, c.r)
	}
	return res, nil
}

// ---- the bands ----

// rankBand is a band of values [lo, hi] around a rank, and each sealed
// day's part of it by the digest of its seal file.
type rankBand struct {
	lo, hi int64
	days   map[string]*bandDay
}

// bandDay is a sealed day's values below the band, above it, and its bins
// inside it.
type bandDay struct {
	below, above int64
	bins         []hbin
}

// bandSet is the bands kept for a window across its computations
// (keptBands).
type bandSet struct {
	mu    sync.Mutex
	bands map[rankKey]*rankBand
	// built counts the bands built, for tests.
	built int
}

// keptBands is the band set of a window name, made on first use.
func (dp *dayParts) keptBands(name string) *bandSet {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	if dp.bands == nil {
		dp.bands = map[string]*bandSet{}
	}
	b := dp.bands[name]
	if b == nil {
		b = &bandSet{bands: map[rankKey]*rankBand{}}
		dp.bands[name] = b
	}
	return b
}

// bandMargin is how many values a band holds on either side of its rank
// when it is built: twice as many as the raw part holds, or as a day holds
// on average, whichever is more.
func (s *rankSel) bandMargin(k rankKey, n int64) int64 {
	var raw int64
	binsOf(s.raw, k.scope, s.ex, func(l []hbin) {
		for _, b := range l {
			raw += b[1]
		}
	})
	day := n / int64(max(1, len(s.days)))
	return 2*max(raw, day) + 16
}

// runBands selects every rank from the bands kept in set, building again
// those whose rank has left them, or that are not there yet.
func (s *rankSel) runBands(ctx context.Context, set *bandSet) (rankResult, error) {
	set.mu.Lock()
	defer set.mu.Unlock()
	res := rankResult{n: map[scopeKey]int64{}, at: map[rankKey]rankAnswer{}}
	digests := make(map[string]bool, len(s.days))
	for _, rd := range s.days {
		digests[rd.digest] = true
	}
	// The days no band has a part of yet: newly sealed, or sealed again.
	var stale []rankKey
	missing := map[string]*rowDay{}
	for _, k := range s.keys {
		b := set.bands[k]
		if b == nil {
			stale = append(stale, k)
			continue
		}
		for d := range b.days {
			if !digests[d] {
				delete(b.days, d)
			}
		}
		for _, rd := range s.days {
			if b.days[rd.digest] == nil {
				missing[rd.digest] = rd
			}
		}
	}
	for _, rd := range missing {
		if err := ctx.Err(); err != nil {
			return rankResult{}, err
		}
		h, err := s.read(rd)
		if err != nil {
			return rankResult{}, &errDayUnusable{rd, err}
		}
		for _, k := range s.keys {
			b := set.bands[k]
			if b == nil || b.days[rd.digest] != nil {
				continue
			}
			b.days[rd.digest] = partOf(h, k.scope, s.ex, b.lo, b.hi)
		}
	}
	// Each rank from its band, if it is in it.
	for _, k := range s.keys {
		b := set.bands[k]
		if b == nil {
			continue
		}
		var below, above int64
		lists := make([][]hbin, 0, len(s.days)+1)
		for _, rd := range s.days {
			bd := b.days[rd.digest]
			below += bd.below
			above += bd.above
			lists = append(lists, bd.bins)
		}
		var raw []hbin
		binsOf(s.raw, k.scope, s.ex, func(l []hbin) {
			var lo, hi int64
			raw, lo, hi = inside(raw, l, b.lo, b.hi)
			below += lo
			above += hi
		})
		in := mergeBins(append(lists, raw)...)
		var within int64
		for _, x := range in {
			within += x[1]
		}
		n := below + within + above
		res.n[k.scope] = n
		r := k.q.rank(n)
		if n == 0 || r < 1 {
			res.at[k] = rankAnswer{}
			continue
		}
		if r <= below || r > below+within {
			stale = append(stale, k)
			continue
		}
		res.at[k] = pick(in, below, r)
	}
	if len(stale) == 0 {
		return res, nil
	}
	// The bands to build: two reads of every day for their ranks alone.
	sub := *s
	sub.keys = stale
	cands, ns, err := sub.passOne(ctx, s.bandMargin)
	if err != nil {
		return rankResult{}, err
	}
	for sc, n := range ns {
		res.n[sc] = n
	}
	perDay := make([]map[string]*bandDay, len(cands))
	for i := range cands {
		perDay[i] = make(map[string]*bandDay, len(s.days))
	}
	split := func(h dayHists, i int) *bandDay {
		c := cands[i]
		return partOf(h, c.key.scope, s.ex, c.lo, c.hi)
	}
	for _, rd := range s.days {
		if err := ctx.Err(); err != nil {
			return rankResult{}, err
		}
		h, err := s.read(rd)
		if err != nil {
			return rankResult{}, &errDayUnusable{rd, err}
		}
		for i := range cands {
			perDay[i][rd.digest] = split(h, i)
		}
	}
	for i, c := range cands {
		raw := split(s.raw, i)
		lists := make([][]hbin, 0, len(s.days)+1)
		for _, bd := range perDay[i] {
			lists = append(lists, bd.bins)
		}
		in := mergeBins(append(lists, raw.bins)...)
		res.at[c.key] = pick(in, c.below, c.r)
		// The band: the values margin ranks either side of the rank, within
		// the candidate; each day's part of it.
		m := s.bandMargin(c.key, c.n)
		lo, hi := in[0][0], in[len(in)-1][0]
		if a := pick(in, c.below, c.r-m); a.ok {
			lo = a.v
		}
		if a := pick(in, c.below, c.r+m); a.ok {
			hi = a.v
		}
		b := &rankBand{lo: lo, hi: hi, days: make(map[string]*bandDay, len(s.days))}
		for d, bd := range perDay[i] {
			nb := &bandDay{below: bd.below, above: bd.above}
			var below, above int64
			nb.bins, below, above = inside(nil, bd.bins, lo, hi)
			nb.below += below
			nb.above += above
			b.days[d] = nb
		}
		set.bands[c.key] = b
		set.built++
	}
	// A rank whose population is empty has no band.
	for _, k := range stale {
		if _, ok := res.at[k]; !ok {
			res.at[k] = rankAnswer{}
			delete(set.bands, k)
		}
	}
	return res, nil
}

// partOf is a day's part of a band [lo, hi] of scope: a validator's own
// bins are in order already, the network's are merged.
func partOf(h dayHists, sc scopeKey, ex excludeSet, lo, hi int64) *bandDay {
	var bd bandDay
	lists := 0
	binsOf(h, sc, ex, func(l []hbin) {
		var below, above int64
		bd.bins, below, above = inside(bd.bins, l, lo, hi)
		bd.below += below
		bd.above += above
		lists++
	})
	if lists > 1 {
		bd.bins = mergeBins(bd.bins)
	}
	return &bd
}

// held is how many bins and days' parts the bands of a set hold.
func (set *bandSet) held() (bins, parts int) {
	set.mu.Lock()
	defer set.mu.Unlock()
	for _, b := range set.bands {
		for _, bd := range b.days {
			bins += len(bd.bins)
			parts++
		}
	}
	return bins, parts
}
