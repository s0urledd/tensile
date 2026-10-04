package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// How a window is assembled from the partials: the days it covers whole
// that are sealed and usable, summed in memory, and the rest of it read
// raw with the shipped statements over the bounds that are left.

// partsFor reports whether win is computed from the partials. The day's
// window is not: it is mostly the two days no partial is sealed for yet,
// so it is read raw whole, as it always was.
func partsFor(win Window) bool {
	return win.Span == 0 || win.Span > 24*time.Hour
}

// ---- row figures ----

// rowAcc is one validator's row figures over a window, accumulated from
// days and raw spans.
type rowAcc struct {
	classes    map[string]int64
	probes     int64
	lastSeen   string
	lastServed string
	gaps       map[string]int64
	lat, tput  map[int64]int64
	beats, up  int64
	identUp    int64
	lastDown   string
	lastUp     string
}

func newRowAcc() *rowAcc {
	return &rowAcc{classes: map[string]int64{}, gaps: map[string]int64{}, lat: map[int64]int64{}, tput: map[int64]int64{}}
}

func maxString(a, b string) string {
	if b > a {
		return b
	}
	return a
}

// add folds one partial in.
func (a *rowAcc) add(p *rowPart) {
	for c, n := range p.Classes {
		a.classes[c] += n
	}
	a.probes += p.Probes
	a.lastSeen = maxString(a.lastSeen, p.LastSeen)
	a.lastServed = maxString(a.lastServed, p.LastServed)
	for o, n := range p.Gaps {
		a.gaps[o] += n
	}
	for _, b := range p.Lat {
		a.lat[b[0]] += b[1]
	}
	for _, b := range p.Tput {
		a.tput[b[0]] += b[1]
	}
	a.beats += p.Beats
	a.up += p.Up
	a.identUp += p.IdentUp
	a.lastDown = maxString(a.lastDown, p.LastDown)
	a.lastUp = maxString(a.lastUp, p.LastUp)
}

// hist is a histogram's bins in ascending order and its count.
type hist struct {
	bins []hbin
	n    int64
}

func histOf(m map[int64]int64) hist {
	h := hist{bins: make([]hbin, 0, len(m))}
	for v, n := range m {
		if n > 0 {
			h.bins = append(h.bins, hbin{v, n})
			h.n += n
		}
	}
	sort.Slice(h.bins, func(i, j int) bool { return h.bins[i][0] < h.bins[j][0] })
	return h
}

// at is the value at rank r (1-based) in ascending order, as ROW_NUMBER over
// the rows reads it; ok is false for a rank no row has.
func (h hist) at(r int64) (int64, bool) {
	if r < 1 {
		return 0, false
	}
	for _, b := range h.bins {
		if r <= b[1] {
			return b[0], true
		}
		r -= b[1]
	}
	return 0, false
}

// percentiles are the ranks latencyWhere and valLatencySQL read: the median
// (c+1)/2 and the 95th percentile (95c+99)/100.
func (h hist) percentiles() (p50, p95 *int64) {
	if v, ok := h.at((h.n + 1) / 2); ok {
		p50 = &v
	}
	if v, ok := h.at((h.n*95 + 99) / 100); ok {
		p95 = &v
	}
	return p50, p95
}

// rowSpans splits [lo, hi] into the sealed row days it covers whole and
// can use, and the raw spans left between them, in order.
func (e *epoch) rowSpans(lo, hi string) (sealed []*rowDay, raw [][2]string) {
	if lo > hi {
		return nil, nil
	}
	days := make([]string, 0, len(e.rows))
	for d := range e.rows {
		days = append(days, d)
	}
	sort.Strings(days)
	cur := lo
	for _, d := range days {
		if dayLo(d) < lo || dayHi(d) > hi || (e.rawFrom != "" && d < e.rawFrom) || !e.gapClean(d) {
			continue
		}
		if dayLo(d) > cur {
			// the raw span stops at the end of the day before; gapClean says
			// nothing lies between that and d
			raw = append(raw, [2]string{cur, dayHi(dayAdd(d, -1))})
		}
		sealed = append(sealed, e.rows[d])
		cur = dayLo(dayAdd(d, 1))
	}
	if cur <= hi {
		raw = append(raw, [2]string{cur, hi})
	}
	return sealed, raw
}

// rowWindow is every validator's row figures over the rows started in
// [lo, hi]: sealed days and raw spans. only, when set, is the one validator
// asked for.
func (s *Server) rowWindow(ctx context.Context, e *epoch, lo, hi, only string) (map[string]*rowAcc, error) {
	out := map[string]*rowAcc{}
	get := func(addr string) *rowAcc {
		a, ok := out[addr]
		if !ok {
			a = newRowAcc()
			out[addr] = a
		}
		return a
	}
	sealed, raw := e.rowSpans(lo, hi)
	for _, d := range sealed {
		for addr, p := range d.Vals {
			if only != "" && addr != only {
				continue
			}
			get(addr).add(p)
		}
	}
	for _, sp := range raw {
		parts, err := s.rowSpanParts(ctx, s.q(ctx), sp[0], sp[1], only)
		if err != nil {
			return nil, err
		}
		for addr, p := range parts {
			get(addr).add(p)
		}
	}
	return out, nil
}

// rowSpanParts reads every validator's rows started in [lo, hi] with the
// row statements (rowsql.go), for one validator when only is set: a day
// being sealed, or a raw span of a window.
func (s *Server) rowSpanParts(ctx context.Context, q store.Querier, lo, hi, only string) (map[string]*rowPart, error) {
	out := map[string]*rowPart{}
	get := func(addr string) *rowPart {
		p, ok := out[addr]
		if !ok {
			p = &rowPart{}
			out[addr] = p
		}
		return p
	}
	filter, args := "", func(base ...any) []any { return base }
	if only != "" {
		filter = " AND validator_address = ?"
		args = func(base ...any) []any { return append(base, only) }
	}
	each := func(query string, qargs []any, scan func(*sql.Rows) error) error {
		rows, err := q.QueryContext(ctx, query, qargs...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if err := scan(rows); err != nil {
				return err
			}
		}
		return rows.Err()
	}
	if err := each(valClassesSQL(filter), args(lo, hi), func(r *sql.Rows) error {
		var addr, c string
		var n int64
		if err := r.Scan(&addr, &c, &n); err != nil {
			return err
		}
		p := get(addr)
		if p.Classes == nil {
			p.Classes = map[string]int64{}
		}
		p.Classes[c] += n
		return nil
	}); err != nil {
		return nil, err
	}
	if err := each(valSeenSQL(filter), args(lo, hi), func(r *sql.Rows) error {
		var addr, last string
		var served sql.NullString
		var n int64
		if err := r.Scan(&addr, &n, &last, &served); err != nil {
			return err
		}
		p := get(addr)
		p.Probes, p.LastSeen, p.LastServed = n, last, served.String
		return nil
	}); err != nil {
		return nil, err
	}
	if err := each(valGapsSQL(filter), args(lo, hi), func(r *sql.Rows) error {
		var addr, o string
		var n int64
		if err := r.Scan(&addr, &o, &n); err != nil {
			return err
		}
		p := get(addr)
		if p.Gaps == nil {
			p.Gaps = map[string]int64{}
		}
		p.Gaps[o] += n
		return nil
	}); err != nil {
		return nil, err
	}
	for _, h := range []struct {
		q   string
		dst func(*rowPart) *[]hbin
	}{
		{valLatencyHistSQL(filter), func(p *rowPart) *[]hbin { return &p.Lat }},
		{valThroughputHistSQL(filter), func(p *rowPart) *[]hbin { return &p.Tput }},
	} {
		if err := each(h.q, args(lo, hi), func(r *sql.Rows) error {
			var addr string
			var v, n int64
			if err := r.Scan(&addr, &v, &n); err != nil {
				return err
			}
			b := h.dst(get(addr))
			*b = append(*b, hbin{v, n})
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if err := each(valBeatsSQL(filter), args(lo, hi, s.vantage), func(r *sql.Rows) error {
		var addr string
		var seen, up, ident int64
		var lastDown, lastUp sql.NullString
		if err := r.Scan(&addr, &seen, &up, &ident, &lastDown, &lastUp); err != nil {
			return err
		}
		p := get(addr)
		p.Beats, p.Up, p.IdentUp, p.LastDown, p.LastUp = seen, up, ident, lastDown.String, lastUp.String
		return nil
	}); err != nil {
		return nil, err
	}
	for _, p := range out {
		sort.Slice(p.Lat, func(i, j int) bool { return p.Lat[i][0] < p.Lat[j][0] })
		sort.Slice(p.Tput, func(i, j int) bool { return p.Tput[i][0] < p.Tput[j][0] })
	}
	return out, nil
}

// netRows is the network's row figures from the validators' own: the
// figures computeNetwork reads, over every validator ex does not name.
type netRows struct {
	probed     int64
	classes    classCounts
	probes     int64
	gaps       int64
	byOutcome  map[string]int64
	lat        hist
	beats, up  int64
	validators map[string]bool
}

func netRowsOf(m map[string]*rowAcc, ex excludeSet) netRows {
	n := netRows{classes: classCounts{}, byOutcome: map[string]int64{}, validators: map[string]bool{}}
	lat := map[int64]int64{}
	for addr, a := range m {
		if ex.has(addr) {
			continue
		}
		if a.probes > 0 {
			n.probed++
			n.validators[addr] = true
		}
		for c, k := range a.classes {
			n.classes[c] += k
		}
		n.probes += a.probes
		for o, k := range a.gaps {
			n.byOutcome[o] += k
			n.gaps += k
		}
		for v, k := range a.lat {
			lat[v] += k
		}
		n.beats += a.beats
		n.up += a.up
	}
	n.lat = histOf(lat)
	return n
}

// ---- obligations ----

// oblSelect is who an obligation figure is over: one validator (only), or
// every validator ex does not name.
type oblSelect struct {
	only string
	ex   excludeSet
}

func (v oblSelect) keeps(addr string) bool {
	if v.only != "" {
		return addr == v.only
	}
	return !v.ex.has(addr)
}

// clause is the filter the raw statements take, and its arguments.
func (v oblSelect) clause() (string, []any) {
	if v.only != "" {
		return ` AND pr.validator_address = ?`, []any{v.only}
	}
	return v.ex.clause("pr.validator_address"), v.ex.addrs
}

// errNoParts is returned by a window assembly that cannot use the
// partials for a figure (the ledger is still being built, a publication's
// time is not a store timestamp): the caller reads the shipped statement.
var errNoParts = errors.New("day partials not usable for this figure")

// oblWindow is readObligations over win, assembled from sealed settlement
// days and raw spans: the settlement range is the window's (from raw_from
// on for a pruned "all", as obligationArgs sets it), pending is cut at
// pendCut, and a fault is provisional after cutoff ("" asks for no
// provisional figure).
func (s *Server) oblWindow(ctx context.Context, e *epoch, win Window, pendCut, cutoff string, sel oblSelect) (obligationPass, error) {
	if !e.ledgerBuilt || e.pubsOdd {
		return obligationPass{}, errNoParts
	}
	start := win.startArg()
	if win.Span == 0 && e.rawFrom != "" {
		start = dayLo(e.rawFrom)
	}
	end := win.endArg()
	rowFloor := rollup.RowLowerBound(start)
	p := obligationPass{byVal: map[string]rollup.Obligations{}, prov: map[string]*provisionalFaults{}}
	settling := map[string]int64{}
	youngest := map[string]string{}
	days := e.settleDays(start, end)

	type span struct {
		a, b       string
		lo, hi     string
		known      bool
		any        bool
		firstD, lD string
	}
	var groups []span
	var cur *span
	flush := func() {
		if cur != nil && cur.any {
			groups = append(groups, *cur)
		}
		cur = nil
	}
	for _, d := range days {
		sd := e.settle[d]
		whole := dayLo(d) >= start && dayHi(d) <= end
		if whole && sealUsable(sd.Seal, rowFloor, end, pendCut, cutoff) {
			flush()
			for addr, o := range sd.Seal.Obl {
				if !sel.keeps(addr) {
					continue
				}
				r := p.byVal[addr]
				r.Add(o)
				p.byVal[addr] = r
			}
			continue
		}
		a, b := maxString(start, dayLo(d)), dayHi(d)
		if end < b {
			b = end
		}
		if cur == nil {
			cur = &span{a: a, known: true}
		}
		cur.b = b
		if !sd.SpanKnown {
			cur.known = false
		}
		if !sd.SpanKnown || sd.hasRows() {
			cur.any = true
			if sd.hasRows() {
				if cur.lo == "" || sd.MinStart < cur.lo {
					cur.lo = sd.MinStart
				}
				cur.hi = maxString(cur.hi, sd.MaxStart)
			}
		}
	}
	flush()
	extra, extraArgs := sel.clause()
	for _, g := range groups {
		rowLo, rowHi := rowFloor, end
		if g.known {
			// Every row of the span's promises starts inside [g.lo, g.hi], so
			// no row the window's bounds admit is left out.
			rowLo, rowHi = maxString(rowFloor, g.lo), g.hi
			if end < rowHi {
				rowHi = end
			}
			if rowLo > rowHi {
				continue
			}
		}
		args := append([]any{cutoff, cutoff, pendCut, g.a, g.b, rowHi, rowLo}, extraArgs...)
		rows, err := s.q(ctx).QueryContext(ctx, obligationPassSQL(extra), args...)
		if err != nil {
			return obligationPass{}, err
		}
		for rows.Next() {
			var addr string
			var r rollup.Obligations
			var n int64
			var young sql.NullString
			if err := rows.Scan(append(append([]any{&addr}, scanObligations(&r)...), &n, &young)...); err != nil {
				rows.Close()
				return obligationPass{}, err
			}
			o := p.byVal[addr]
			o.Add(r)
			p.byVal[addr] = o
			if n > 0 {
				settling[addr] += n
				youngest[addr] = maxString(youngest[addr], young.String)
			}
		}
		if err := rows.Close(); err != nil {
			return obligationPass{}, err
		}
		if err := rows.Err(); err != nil {
			return obligationPass{}, err
		}
	}
	if cutoff != "" {
		for addr, n := range settling {
			p.prov[addr] = newProvisional(n, youngest[addr])
		}
	}
	return p, nil
}

// sealUsable reports whether a sealed settlement day is what the window's
// statement would read over it: every row of its promises inside the
// window's row bounds, so the window admits exactly the rows the seal read
// (store.ObligationRowsVerified, the day's exact span); no deadline after
// the pending cut, so nothing is pending (nothing was when it was sealed);
// and no fault after the provisional cutoff, so nothing is provisional.
func sealUsable(seal *settleSeal, rowFloor, end, pendCut, cutoff string) bool {
	if seal == nil || seal.Obl == nil {
		return false
	}
	if seal.MinStart != "" && (seal.MinStart < rowFloor || seal.MaxStart > end) {
		return false
	}
	return seal.MaxRowMSU <= pendCut && (cutoff == "" || seal.MaxFirstFault <= cutoff)
}

// settleDays is the ledger days with a publication settled in [lo, hi], in
// order.
func (e *epoch) settleDays(lo, hi string) []string {
	var out []string
	for d, sd := range e.settle {
		if sd.Pubs == 0 || dayHi(d) < lo || dayLo(d) > hi {
			continue
		}
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// ---- publications ----

// pubSpan is a run of settlement days read raw: its bounds and the height
// range of its publications.
type pubSpan struct {
	a, b     string
	hlo, hhi int64
}

// pubWindow splits the publications settled in [lo, hi] into the ledger
// days it covers whole and the partial days at its ends, read raw.
func (e *epoch) pubWindow(lo, hi string) (whole []string, raw []pubSpan) {
	for _, d := range e.settleDays(lo, hi) {
		sd := e.settle[d]
		if dayLo(d) >= lo && dayHi(d) <= hi {
			whole = append(whole, d)
			continue
		}
		raw = append(raw, pubSpan{a: maxString(lo, dayLo(d)), b: minString(hi, dayHi(d)), hlo: sd.HLo, hhi: sd.HHi})
	}
	return whole, raw
}

func minString(a, b string) string {
	if b < a {
		return b
	}
	return a
}

// pubCounts is the network's publication count and bytes over win.
func (s *Server) pubCounts(ctx context.Context, e *epoch, win Window) (n, bytes int64, err error) {
	if !e.ledgerBuilt || e.pubsOdd {
		return 0, 0, errNoParts
	}
	whole, raw := e.pubWindow(win.startArg(), win.endArg())
	for _, d := range whole {
		n += e.settle[d].Pubs
		bytes += e.settle[d].Bytes
	}
	for _, sp := range raw {
		var k, b int64
		if err := s.q(ctx).QueryRowContext(ctx, pubCountSQL, sp.a, sp.b, sp.hlo, sp.hhi).Scan(&k, &b); err != nil {
			return 0, 0, err
		}
		n, bytes = n+k, bytes+b
	}
	return n, bytes, nil
}

// readableCounts is reconstructableCount's count statement over win: every
// publication, those still waiting for a reading, and those whose window
// closed without one. A sealed whole day adds its count and its unread
// ones once every deadline of it had passed at the moment asked about (and
// every row of it had started by a pinned end); any other day is read raw.
func (s *Server) readableCounts(ctx context.Context, e *epoch, win Window, pin asOfPin) (total, waiting, closedUnread int64, err error) {
	if !e.ledgerBuilt || e.pubsOdd {
		return 0, 0, 0, errNoParts
	}
	at := store.TS(pin.now)
	whole, raw := e.pubWindow(win.startArg(), win.endArg())
	var runs []pubSpan
	var cur *pubSpan
	for _, d := range whole {
		sd := e.settle[d]
		if sd.Seal != nil && sd.MaxPubMSU < at && (pin.at == "" || sd.Seal.MaxStart <= pin.at) {
			total += sd.Pubs
			closedUnread += sd.Pubs - sd.Seal.Readable
			if cur != nil {
				runs = append(runs, *cur)
				cur = nil
			}
			continue
		}
		if cur == nil {
			cur = &pubSpan{a: dayLo(d), hlo: sd.HLo, hhi: sd.HHi}
		}
		cur.b = dayHi(d)
		if sd.HLo < cur.hlo {
			cur.hlo = sd.HLo
		}
		if sd.HHi > cur.hhi {
			cur.hhi = sd.HHi
		}
	}
	if cur != nil {
		runs = append(runs, *cur)
	}
	runs = append(runs, raw...)
	bound, pargs := pin.bound("r", nil)
	query := readableCountSQL(bound)
	for _, sp := range runs {
		args := append(append(append(append([]any{}, pargs...), at), pargs...), at, sp.a, sp.b, sp.hlo, sp.hhi)
		var n, w, c int64
		if err := s.q(ctx).QueryRowContext(ctx, query, args...).Scan(&n, &w, &c); err != nil {
			return 0, 0, 0, err
		}
		total, waiting, closedUnread = total+n, waiting+w, closedUnread+c
	}
	return total, waiting, closedUnread, nil
}

// signingWindow is signingByValidator's counts over win from the ledger and
// the partial days, for one validator when only is set.
func (s *Server) signingWindow(ctx context.Context, e *epoch, win Window, only string) (map[string]signingStats, error) {
	if !e.ledgerBuilt || e.pubsOdd {
		return nil, errNoParts
	}
	sums := map[string]sigPart{}
	whole, raw := e.pubWindow(win.startArg(), win.endArg())
	for _, d := range whole {
		for addr, sp := range e.settle[d].Signing {
			if only != "" && addr != only {
				continue
			}
			t := sums[addr]
			for i := range t {
				t[i] += sp[i]
			}
			sums[addr] = t
		}
	}
	filter := heightsSQL("p.")
	if only != "" {
		filter += ` AND a.validator_address = ?`
	}
	for _, sp := range raw {
		args := []any{sp.a, sp.b, sp.hlo, sp.hhi}
		if only != "" {
			args = append(args, only)
		}
		rows, err := s.q(ctx).QueryContext(ctx, signingByValidatorSQL(filter), args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var addr string
			var t sigPart
			if err := rows.Scan(&addr, &t[0], &t[1], &t[2], &t[3]); err != nil {
				rows.Close()
				return nil, err
			}
			c := sums[addr]
			for i := range c {
				c[i] += t[i]
			}
			sums[addr] = c
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	out := make(map[string]signingStats, len(sums))
	for addr, t := range sums {
		st := signingStats{Assigned: t[0], Signed: t[1], Unknown: t[2], NoHost: t[3]}
		st.Rate = rate(st.Signed, st.Assigned)
		out[addr] = st
	}
	return out, nil
}

// loadWindow is the in-window half of loadByValidator (promises, rows and
// row data) from the ledger and the partial days. bytesOK is false when a
// term is outside the class whose sum is exact (loadBytes): the caller then
// reads the bytes with the shipped statement.
func (s *Server) loadWindow(ctx context.Context, e *epoch, win Window, only string) (map[string]*loadPart, error) {
	if !e.ledgerBuilt || e.pubsOdd {
		return nil, errNoParts
	}
	out := map[string]*loadPart{}
	get := func(addr string) *loadPart {
		l, ok := out[addr]
		if !ok {
			l = &loadPart{Bytes: map[int]*big.Int{}}
			out[addr] = l
		}
		return l
	}
	whole, raw := e.pubWindow(win.startArg(), win.endArg())
	for _, d := range whole {
		for addr, l := range e.settle[d].Load {
			if only != "" && addr != only {
				continue
			}
			get(addr).add(l)
		}
	}
	filter := ""
	if only != "" {
		filter = ` AND a.validator_address = ?6`
	}
	for _, sp := range raw {
		where := `p.settlement_time >= ? AND p.settlement_time <= ? AND p.settlement_height >= ? AND p.settlement_height <= ? AND p.settlement_tx_code = 0 AND p.assignment_error = ''`
		doc, err := s.origRows.docWhere(ctx, s.st.DB(), where, sp.a, sp.b, sp.hlo, sp.hhi)
		if err != nil {
			return nil, err
		}
		args := []any{sp.a, sp.b, sp.hlo, sp.hhi, doc}
		if only != "" {
			args = append(args, only)
		}
		rows, err := s.q(ctx).QueryContext(ctx, loadSpanSQL(filter), args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var addr string
			var orig any
			var t loadTerms
			if err := rows.Scan(&addr, &orig, &t.n, &t.rows, &t.hi, &t.lo, &t.maxTerm, &t.maxSize); err != nil {
				rows.Close()
				return nil, err
			}
			get(addr).add(t.part(orig))
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// loadTerms is one group of loadSpanSQL or ledgerLoadSQL: the endorsed
// assignments of one validator on publications of one original_rows, their
// rows, their row data split at bit twenty, and the largest term and blob.
type loadTerms struct {
	n, rows, hi, lo  int64
	maxTerm, maxSize int64
}

// exactTerm is the bound under which row_count * blob_size, and blob_size
// itself, are integers a double holds exactly, so that each term the
// statement adds is exactly that integer scaled by 2^-k.
const exactTerm = int64(1) << 53

// part is the group as a partial. A group whose original_rows is NULL or
// zero adds no term (the statement's term is NULL and SUM skips it) but
// its assignments and rows all the same; one whose original_rows is not
// a power of two, or whose terms are too large to be exact, is Odd.
func (t loadTerms) part(orig any) *loadPart {
	l := &loadPart{Promises: t.n, Rows: t.rows, Bytes: map[int]*big.Int{}}
	k, ok, none := origExponent(orig)
	switch {
	case none:
	case !ok || t.maxTerm >= exactTerm || t.maxSize >= exactTerm:
		l.Odd = true
	default:
		l.Terms = t.n
		v := new(big.Int).Lsh(big.NewInt(t.hi), 20)
		v.Add(v, big.NewInt(t.lo))
		l.Bytes[k] = v
	}
	return l
}

// origExponent reads original_rows as the statement does: k for 2^k, none
// for a value whose term is NULL (NULL, zero), ok false for anything else.
func origExponent(v any) (k int, ok, none bool) {
	switch x := v.(type) {
	case nil:
		return 0, false, true
	case int64:
		if x == 0 {
			return 0, false, true
		}
		if x < 0 || x&(x-1) != 0 {
			return 0, false, false
		}
		for x > 1 {
			x >>= 1
			k++
		}
		return k, true, false
	case float64:
		if x == 0 {
			return 0, false, true
		}
		if x < 1 || x != float64(int64(x)) || x >= 1<<62 {
			return 0, false, false
		}
		return origExponent(int64(x))
	}
	return 0, false, false
}

// loadBytes is loadSQL's in-window load bytes from a partial:
// CAST(SUM(terms) AS INTEGER), where SUM is SQLite's
// Kahan-Babuska-Neumaier sum. Every term is an integer times 2^-k, so each
// is a multiple of 2^-K for the largest k, and so is every partial sum,
// every rounded sum and every rounding error the summation forms; the
// errors are carried exactly while n·S < 2^(106-K), and the result is then
// the exact sum rounded to the nearest double, ties to even, whatever
// order the terms came in (TestLoadSumIsExactlyRounded holds the bundled
// SQLite to that). That rounded value, truncated, is computed here from
// the exact integers. ok is false when the partial is outside that class
// (Odd, or past the bound), and the statement itself is read instead.
func (l *loadPart) loadBytes() (int64, bool) {
	if l.Odd {
		return 0, false
	}
	if l.Terms == 0 {
		return 0, true
	}
	K := 0
	for k := range l.Bytes {
		if k > K {
			K = k
		}
	}
	m := new(big.Int)
	for k, v := range l.Bytes {
		m.Add(m, new(big.Int).Lsh(v, uint(K-k)))
	}
	// n·M < 2^106, M being the sum in units of 2^-K, and S < 2^62 so the
	// cast cannot clamp.
	if new(big.Int).Mul(m, big.NewInt(l.Terms)).BitLen() > 106 || m.BitLen()-K > 62 {
		return 0, false
	}
	f := new(big.Float).SetPrec(uint(m.BitLen() + 1)).SetInt(m)
	f.SetMantExp(f, -K)
	x, _ := f.Float64()
	return int64(x), true
}

// heldLoad is what each validator holds at now (loadStats.StoredBytes),
// read raw over the held publications (loadHeldSQL).
func (s *Server) heldLoad(ctx context.Context, now time.Time, only string) (map[string]int64, error) {
	nowArg := store.TS(now.UTC())
	doc, err := s.origRows.docWhere(ctx, s.st.DB(), `p.settlement_tx_code = 0 AND p.assignment_error = '' AND (p.must_serve_until > ? AND p.settlement_time <= ?)`, nowArg, nowArg)
	if err != nil {
		return nil, err
	}
	filter, args := "", []any{nil, nil, nowArg, doc}
	if only != "" {
		filter = ` AND a.validator_address = ?5`
		args = append(args, only)
	}
	rows, err := s.q(ctx).QueryContext(ctx, loadHeldSQL(filter), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var addr string
		var n int64
		if err := rows.Scan(&addr, &n); err != nil {
			return nil, err
		}
		out[addr] = n
	}
	return out, rows.Err()
}

// describe is a short account of which days a window read sealed, for the
// shadow audit's log.
func (e *epoch) describe(lo, hi string) string {
	sealed, raw := e.rowSpans(lo, hi)
	return fmt.Sprintf("%d sealed row days, %d raw spans", len(sealed), len(raw))
}

// fill sets on a validator's row the row figures validatorRows reads with
// its statements, as those statements set them: only where the validator
// has rows of that statement's population.
func (a *rowAcc) fill(v *validatorRow) {
	for c, n := range a.classes {
		v.Classes[c] = n
	}
	if lat := histOf(a.lat); lat.n > 0 {
		v.LatencySample = lat.n
		v.LatencyP50, v.LatencyP95 = lat.percentiles()
		tput := histOf(a.tput)
		v.ThroughputSample = tput.n
		if b, ok := tput.at((tput.n + 1) / 2); ok && tput.n >= throughputMinSample {
			v.BytesPerSecond = &b
		}
	}
	if a.probes > 0 {
		v.ProbeCount = a.probes
		last := a.lastSeen
		v.LastSeenAt = &last
		if a.lastServed != "" {
			served := a.lastServed
			v.LastServedAt = &served
		}
	}
	if a.beats > 0 {
		v.Reachability = rate(a.up, a.beats)
		v.IdentityValid = rate(a.identUp, a.up)
		if a.lastDown != "" {
			at := a.lastDown
			v.LastUnreachableAt = &at
		}
		if a.lastUp != "" {
			at := a.lastUp
			v.LastReachableAt = &at
		}
	}
}
