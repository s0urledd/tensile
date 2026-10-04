package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The shadow audit: the partials checked against the store while they
// serve. Every hour the next sealed row day, the next sealed settlement day
// and the next day of the ledger, each in turn in day order and back to the
// first after the last (auditNext, its cursor kept with the partials), are
// read again raw in one read transaction and compared with what is kept for
// them, every field a window reads: a row day's sums; a settlement day's
// readable count, its exact row span and latest row deadline, the latest
// deadline of its publications, its obligations and the newest first fault
// among them, whether it holds rows or none; a ledger day's count, bytes,
// height range, latest deadline, signing and load. Every sealed day is so
// audited within as many hours as there are sealed days of its kind. For
// the first few hours after a start a window is also computed both ways,
// from the partials and with the shipped statements over the whole window,
// in one read: the network summary or the validator list of a pinned window
// as short as still sums sealed days, the two consecutive sealed days with
// the fewest rows and half a day either side (auditWindowOf), so that its
// raw half costs two quiet days and not a week of busy ones. It runs beside
// the sealer, not in it, and a comparison that took longer than
// auditWindowBudget ends them for the process.
//
// A sealed day that differs is logged with what differs and dropped, read
// raw until it is sealed again. A ledger day or a window that differs is
// not a day the partials can drop: the process reads every window raw from
// then on, as with -day-partials=false, says so in /v1/health, and leaves
// the files as they are for whoever looks into it (rawFallback). Nothing a
// reader is served waits for the audit.

const (
	auditEvery = time.Hour
	// auditWindowRuns is how many windows a process compares after it
	// starts, one an hour; auditWindowBudget how long one may take before
	// the rest are left out.
	auditWindowRuns   = 3
	auditWindowBudget = 2 * time.Minute
)

// checkRowSeal reads row day d again and says how it differs from what the
// seal holds, "" when it does not.
func (s *Server) checkRowSeal(ctx context.Context, q store.Querier, rd *rowDay) (string, error) {
	got, err := s.rowSpanParts(ctx, q, dayLo(rd.Day), dayHi(rd.Day), "")
	if err != nil {
		return "", err
	}
	if !sameJSON(got, rd.Vals) {
		a, _ := json.Marshal(rd.Vals)
		b, _ := json.Marshal(got)
		return "the validators' rows:\n" + jsonDiff(b, a, 6), nil
	}
	return "", nil
}

// checkSettleSeal reads settlement day d again, as it was sealed (the
// pending cut at the moment it was sealed, its span), and says how it
// differs from what the seal and the day hold, "" when it does not: every
// field a window reads of it (readableCounts, oblWindow, sealUsable),
// whether the day has rows or none.
func (s *Server) checkSettleSeal(ctx context.Context, q store.Querier, d string, sd *settleDay) (string, error) {
	seal := sd.Seal
	var diffs []string
	differ := func(what string, kept, store any) {
		diffs = append(diffs, fmt.Sprintf("%s: kept %v, the store %v", what, kept, store))
	}
	var readable int64
	if err := q.QueryRowContext(ctx, dayReadableSQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&readable); err != nil {
		return "", err
	}
	if readable != seal.Readable {
		differ("readable", seal.Readable, readable)
	}
	lo, hi, msu, err := s.daySpan(ctx, d, sd)
	if err != nil {
		return "", err
	}
	if lo != seal.MinStart || hi != seal.MaxStart {
		differ("row span", seal.MinStart+".."+seal.MaxStart, lo+".."+hi)
	}
	if msu != seal.MaxRowMSU {
		differ("latest row deadline", seal.MaxRowMSU, msu)
	}
	var pubMSU sql.NullString
	if err := q.QueryRowContext(ctx, dayMaxPubMSUSQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&pubMSU); err != nil {
		return "", err
	}
	if pubMSU.String != sd.MaxPubMSU {
		differ("latest publication deadline", sd.MaxPubMSU, pubMSU.String)
	}
	if seal.Obl != nil {
		got := map[string]rollup.Obligations{}
		fault := ""
		if seal.MinStart != "" {
			rows, err := q.QueryContext(ctx, obligationPassSQL(""), "", "", seal.SealedAt, dayLo(d), dayHi(d), seal.MaxStart, seal.MinStart)
			if err != nil {
				return "", err
			}
			for rows.Next() {
				var addr string
				var r rollup.Obligations
				var n int64
				var young sql.NullString
				if err := rows.Scan(append(append([]any{&addr}, scanObligations(&r)...), &n, &young)...); err != nil {
					rows.Close()
					return "", err
				}
				got[addr] = r
				fault = maxString(fault, young.String)
			}
			if err := rows.Close(); err != nil {
				return "", err
			}
			if err := rows.Err(); err != nil {
				return "", err
			}
		}
		if !reflect.DeepEqual(got, seal.Obl) {
			a, _ := json.Marshal(seal.Obl)
			b, _ := json.Marshal(got)
			diffs = append(diffs, "obligations:\n"+jsonDiff(b, a, 6))
		}
		if fault != seal.MaxFirstFault {
			differ("newest first fault", seal.MaxFirstFault, fault)
		}
	}
	return strings.Join(diffs, "; "), nil
}

// ledgerDaySQL is a ledger day's publications read whole: the shipped
// count statement's (pubCountSQL) count and bytes over the day, with their
// height range and latest deadline.
const ledgerDaySQL = `SELECT COUNT(*), COALESCE(SUM(blob_size), 0), COALESCE(MIN(settlement_height), 0), COALESCE(MAX(settlement_height), 0),
		COALESCE(MAX(must_serve_until), '')
	FROM publications WHERE settlement_time >= ? AND settlement_time <= ? AND settlement_height >= ? AND settlement_height <= ?`

// checkLedgerDay reads ledger day d again with the statements a window
// reads a partial day with (pubCountSQL, signingByValidatorSQL,
// loadSpanSQL), over the day and its height range, and says how it differs
// from what the ledger holds, "" when it does not.
func (s *Server) checkLedgerDay(ctx context.Context, q store.Querier, d string, sd *settleDay) (string, error) {
	var diffs []string
	var n, bytes, hlo, hhi int64
	var msu string
	if err := q.QueryRowContext(ctx, ledgerDaySQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&n, &bytes, &hlo, &hhi, &msu); err != nil {
		return "", err
	}
	if n != sd.Pubs || bytes != sd.Bytes || hlo != sd.HLo || hhi != sd.HHi || msu != sd.MaxPubMSU {
		diffs = append(diffs, fmt.Sprintf("publications: kept %d, %d bytes, heights %d..%d, latest deadline %s; the store %d, %d bytes, heights %d..%d, latest deadline %s",
			sd.Pubs, sd.Bytes, sd.HLo, sd.HHi, sd.MaxPubMSU, n, bytes, hlo, hhi, msu))
	}
	signing := map[string]sigPart{}
	rows, err := q.QueryContext(ctx, signingByValidatorSQL(heightsSQL("p.")), dayLo(d), dayHi(d), sd.HLo, sd.HHi)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var addr string
		var t sigPart
		if err := rows.Scan(&addr, &t[0], &t[1], &t[2], &t[3]); err != nil {
			rows.Close()
			return "", err
		}
		signing[addr] = t
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	if !sameJSON(signing, sd.Signing) {
		a, _ := json.Marshal(sd.Signing)
		b, _ := json.Marshal(signing)
		diffs = append(diffs, "signing:\n"+jsonDiff(b, a, 6))
	}
	where := `p.settlement_time >= ? AND p.settlement_time <= ? AND p.settlement_height >= ? AND p.settlement_height <= ? AND p.settlement_tx_code = 0 AND p.assignment_error = ''`
	doc, err := s.origRows.docWhere(ctx, s.st.DB(), where, dayLo(d), dayHi(d), sd.HLo, sd.HHi)
	if err != nil {
		return "", err
	}
	load := map[string]*loadPart{}
	rows, err = q.QueryContext(ctx, loadSpanSQL(""), dayLo(d), dayHi(d), sd.HLo, sd.HHi, doc)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var addr string
		var orig any
		var t loadTerms
		if err := rows.Scan(&addr, &orig, &t.n, &t.rows, &t.hi, &t.lo, &t.maxTerm, &t.maxSize); err != nil {
			rows.Close()
			return "", err
		}
		l, ok := load[addr]
		if !ok {
			l = &loadPart{Bytes: map[int]*big.Int{}}
			load[addr] = l
		}
		l.add(t.part(orig))
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	if !sameJSON(load, sd.Load) {
		a, _ := json.Marshal(sd.Load)
		b, _ := json.Marshal(load)
		diffs = append(diffs, "load:\n"+jsonDiff(b, a, 6))
	}
	return strings.Join(diffs, "; "), nil
}

// verifyLoaded recomputes, after the first catch-up of a loaded epoch, the
// newest sealed day of each kind and one other of each at random, and
// compares them with what the file held. A difference refuses the file.
func (s *Server) verifyLoaded(ctx context.Context, q store.Querier, e *epoch) (refusal, error) {
	pick := func(days []string) []string {
		if len(days) == 0 {
			return nil
		}
		sort.Strings(days)
		out := []string{days[len(days)-1]}
		if len(days) > 1 {
			out = append(out, days[rand.IntN(len(days)-1)])
		}
		return out
	}
	var rowDays, settleDays []string
	for d := range e.rows {
		rowDays = append(rowDays, d)
	}
	for d, sd := range e.settle {
		if sd.Seal != nil {
			settleDays = append(settleDays, d)
		}
	}
	for _, d := range pick(rowDays) {
		diff, err := s.checkRowSeal(ctx, q, e.rows[d])
		if err != nil {
			return "", err
		}
		if diff != "" {
			return refusal("row day " + d + " is not what the store holds: " + diff), nil
		}
	}
	for _, d := range pick(settleDays) {
		diff, err := s.checkSettleSeal(ctx, q, d, e.settle[d])
		if err != nil {
			return "", err
		}
		if diff != "" {
			return refusal("settlement day " + d + " is not what the store holds: " + diff), nil
		}
	}
	return "", nil
}

// sameJSON reports whether two values encode alike.
func sameJSON(a, b any) bool {
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && bytes.Equal(x, y)
}

// The kinds of day the audit sweeps, each with a cursor of its own.
const (
	auditRow    = "row"
	auditSettle = "settle"
	auditLedger = "ledger"
)

// auditNext is the day of a kind the audit reads next: the first of days
// after the cursor, or the first of all once it has passed the last. days
// need not be sorted.
func auditNext(days []string, cursor string) string {
	next, first := "", ""
	for _, d := range days {
		if first == "" || d < first {
			first = d
		}
		if d > cursor && (next == "" || d < next) {
			next = d
		}
	}
	if next == "" {
		return first
	}
	return next
}

// auditResult is what one audit read and found.
type auditResult struct {
	// days are the days read, "kind day" each; diffs what differed, each
	// "kind day: what".
	days  []string
	diffs []string
	// fallback is set when a ledger day differed: no day to drop.
	fallback bool
	took     time.Duration
}

// auditOnce is one hour's audit: the next sealed row day, sealed
// settlement day and ledger day, read again. A sealed day that differs is
// dropped; a ledger day that differs puts the process on raw reads.
func (s *Server) auditOnce(ctx context.Context) (auditResult, error) {
	var res auditResult
	t0 := time.Now()
	dp := s.parts
	dp.mu.Lock()
	// Every hold read again and diffed by the catch-up below, whatever the
	// holds' counter says: a flag moved by a build that does not keep it is
	// found within the hour.
	dp.fullHolds = true
	dp.mu.Unlock()
	var badRow, badSettle string
	cursors := map[string]string{}
	err := s.partsTx(ctx, func(ctx context.Context, e *epoch) error {
		// The cursor as the partials were loaded with it, now that they are.
		dp.mu.Lock()
		for k, v := range dp.auditAt {
			cursors[k] = v
		}
		dp.mu.Unlock()
		q := s.q(ctx)
		var rowDays, settleDays, ledgerDays []string
		for d := range e.rows {
			rowDays = append(rowDays, d)
		}
		for d, sd := range e.settle {
			if sd.Seal != nil {
				settleDays = append(settleDays, d)
			}
			if sd.Pubs > 0 && e.ledgerBuilt && !e.pubsOdd {
				ledgerDays = append(ledgerDays, d)
			}
		}
		if d := auditNext(rowDays, cursors[auditRow]); d != "" {
			diff, err := s.checkRowSeal(ctx, q, e.rows[d])
			if err != nil {
				return err
			}
			res.days = append(res.days, "row day "+d)
			if diff != "" {
				res.diffs = append(res.diffs, "row day "+d+": "+diff)
				badRow = d
			}
			cursors[auditRow] = d
		}
		if d := auditNext(settleDays, cursors[auditSettle]); d != "" {
			diff, err := s.checkSettleSeal(ctx, q, d, e.settle[d])
			if err != nil {
				return err
			}
			res.days = append(res.days, "settlement day "+d)
			if diff != "" {
				res.diffs = append(res.diffs, "settlement day "+d+": "+diff)
				badSettle = d
			}
			cursors[auditSettle] = d
		}
		if d := auditNext(ledgerDays, cursors[auditLedger]); d != "" {
			diff, err := s.checkLedgerDay(ctx, q, d, e.settle[d])
			if err != nil {
				return err
			}
			res.days = append(res.days, "ledger day "+d)
			if diff != "" {
				res.diffs = append(res.diffs, "ledger day "+d+": "+diff)
				res.fallback = true
			}
			cursors[auditLedger] = d
		}
		return nil
	})
	res.took = time.Since(t0)
	if err != nil && err != errNoParts {
		return res, err
	}
	dp.mu.Lock()
	dp.auditAt = cursors
	dp.mu.Unlock()
	if badRow != "" || badSettle != "" {
		dp.publish(func(cur *epoch, _ []journalEntry) bool {
			if badRow != "" && cur.rows[badRow] != nil {
				delete(cur.rows, badRow)
				cur.content++
			}
			if badSettle != "" && cur.settle[badSettle] != nil && cur.settle[badSettle].Seal != nil {
				cur.settleMut(badSettle).Seal = nil
				cur.content++
			}
			return true
		}, 0, journalEntry{rows: map[string]bool{badRow: badRow != ""}, settle: map[string]bool{badSettle: badSettle != ""}})
	}
	if res.fallback {
		dp.fallBack("a ledger day differs from the store: " + strings.Join(res.diffs, "; "))
	}
	return res, nil
}

// auditWindow computes one window both ways, auditWindowOf's: the network
// summary, or with list the validator list. A difference puts the process
// on raw reads (rawFallback). It reports how long the comparison took.
func (s *Server) auditWindow(ctx context.Context, list bool) (time.Duration, error) {
	var win Window
	ok := false
	s.parts.mu.Lock()
	if s.parts.cur != nil {
		win, ok = s.parts.cur.auditWindowOf()
	}
	s.parts.mu.Unlock()
	if !ok {
		return 0, nil
	}
	c := networkCase(s, win, excludeSet{}, nil)
	if list {
		c = validatorsCase(s, win, "")
	}
	t0 := time.Now()
	n, diffs, err := s.comparePaths(ctx, []pathCase{c})
	took := time.Since(t0)
	if err != nil {
		return took, err
	}
	if len(diffs) > 0 {
		s.parts.fallBack(fmt.Sprintf("%s differs from the shipped statements (%d compared):\n%s", c.name, n, strings.Join(diffs, "\n")))
	}
	return took, nil
}

// auditWindowOf is the window the audit compares: pinned, from noon of the
// day before to noon of the day after the two consecutive sealed row days
// with the fewest rows (one day where no two are consecutive), so that it
// sums sealed days and reads raw spans at both ends while its raw half
// costs two quiet days. false when nothing is sealed.
func (e *epoch) auditWindowOf() (Window, bool) {
	size := func(d string) (int64, bool) {
		rd := e.rows[d]
		if rd == nil {
			return 0, false
		}
		var n int64
		for _, p := range rd.Vals {
			n += p.Probes + p.Beats
		}
		return n, true
	}
	best, bestN, bestDays := "", int64(-1), 0
	for d := range e.rows {
		n, _ := size(d)
		days := 1
		if m, ok := size(dayAdd(d, 1)); ok {
			n, days = n+m, 2
		}
		if days > bestDays || (days == bestDays && (bestN < 0 || n < bestN || (n == bestN && d < best))) {
			best, bestN, bestDays = d, n, days
		}
	}
	if best == "" {
		return Window{}, false
	}
	lo, err := time.Parse(dayLayout, best)
	if err != nil {
		return Window{}, false
	}
	start, end := lo.Add(-12*time.Hour), lo.Add(time.Duration(bestDays)*24*time.Hour+12*time.Hour)
	return Window{Name: "audit", Span: end.Sub(start), Start: start, End: end, AsOf: true}, true
}

// computedFields are the two fields that say when and how fast, not what.
var computedFields = regexp.MustCompile(`"(computed_at|compute_ms)":("[^"]*"|[0-9]+),?`)

// scrubComputed drops them, for a comparison of what.
func scrubComputed(b []byte) []byte { return computedFields.ReplaceAll(b, nil) }

// pathCase is one figure the comparison computes both ways.
type pathCase struct {
	name string
	run  func(ctx context.Context) (any, error)
}

func networkCase(s *Server, win Window, ex excludeSet, names []string) pathCase {
	name := "network " + win.Name
	if win.AsOf {
		name += " as of " + win.End.Format(time.RFC3339Nano)
	}
	if ex.on() {
		name += " excluding " + strconv.Itoa(len(ex.addrs))
	}
	return pathCase{name, func(ctx context.Context) (any, error) {
		resp, err := s.computeNetwork(ctx, win, ex, names)
		if err != nil {
			return nil, err
		}
		resp.RecordThrough = s.recordThrough(ctx)
		return map[string]any{"snapshot": resp, "published": networkOutOf(resp)}, nil
	}}
}

func validatorsCase(s *Server, win Window, only string) pathCase {
	name := "validators " + win.Name
	if only != "" {
		name += " " + only
	}
	return pathCase{name, func(ctx context.Context) (any, error) {
		rows, err := s.validatorRows(ctx, win, only)
		if err != nil {
			return nil, err
		}
		return map[string]any{"rows": rows, "published": listOfRows(rows)}, nil
	}}
}

// comparePaths computes every case from the partials and with the shipped
// statements over the whole window, in one read transaction, and returns
// how many it compared and where each difference is.
func (s *Server) comparePaths(ctx context.Context, cases []pathCase) (int, []string, error) {
	var diffs []string
	n := 0
	err := s.readTx(ctx, func(ctx context.Context) error {
		if epochOf(ctx) == nil {
			return errNoParts
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
			n++
			if !bytes.Equal(scrubComputed(a), scrubComputed(b)) {
				diffs = append(diffs, c.name+":\n"+jsonDiff(a, b, 12))
			}
		}
		return nil
	})
	return n, diffs, err
}

// CompareDayParts computes at now, over st, the network summary of the 7d,
// 30d and "all" windows (as a live window, pinned at now, and without the
// lowest-addressed validator that has readings) and the validator list of
// each, from the day partials, after sealing every day due by then, and
// with the shipped statements over the whole window, in one read, and
// returns how many figures it compared and where each difference is. The
// parity tests of other packages hold their fixtures to the partials with
// it as the clock moves on past the rollup and the prune.
func CompareDayParts(ctx context.Context, st *store.Store, vantage string, now time.Time) (int, []string, error) {
	s := newServer(st, VantageInfo{Name: vantage}, nil)
	s.clock = func() time.Time { return now }
	if _, err := s.sealDue(ctx, math.MaxInt); err != nil {
		return 0, nil, err
	}
	var first string
	_ = st.DB().QueryRowContext(ctx, `SELECT MIN(validator_address) FROM probes`).Scan(&first)
	var cases []pathCase
	for _, name := range []string{"7d", "30d", "all"} {
		win := windowFor(name, now)
		pin := win
		pin.AsOf = true
		cases = append(cases, networkCase(s, win, excludeSet{}, nil), networkCase(s, pin, excludeSet{}, nil),
			validatorsCase(s, win, ""), validatorsCase(s, pin, ""))
		if first != "" {
			cases = append(cases, networkCase(s, win, excludeSet{addrs: []any{first}}, []string{first}), validatorsCase(s, win, first))
		}
	}
	return s.comparePaths(ctx, cases)
}

// jsonDiff names up to n paths where two JSON documents differ.
func jsonDiff(a, b []byte, n int) string {
	var x, y any
	_ = json.Unmarshal(a, &x)
	_ = json.Unmarshal(b, &y)
	var out []string
	var walk func(path string, x, y any)
	walk = func(path string, x, y any) {
		if len(out) >= n {
			return
		}
		switch xv := x.(type) {
		case map[string]any:
			yv, ok := y.(map[string]any)
			if !ok {
				out = append(out, fmt.Sprintf("  %s: shipped %v, partials %v", path, x, y))
				return
			}
			keys := map[string]bool{}
			for k := range xv {
				keys[k] = true
			}
			for k := range yv {
				keys[k] = true
			}
			var ks []string
			for k := range keys {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			for _, k := range ks {
				walk(path+"."+k, xv[k], yv[k])
			}
		case []any:
			yv, ok := y.([]any)
			if !ok || len(xv) != len(yv) {
				out = append(out, fmt.Sprintf("  %s: shipped %d items, partials %v", path, len(xv), y))
				return
			}
			for i := range xv {
				walk(path+"["+strconv.Itoa(i)+"]", xv[i], yv[i])
			}
		default:
			if !reflect.DeepEqual(x, y) {
				out = append(out, fmt.Sprintf("  %s: shipped %v, partials %v", path, x, y))
			}
		}
	}
	walk("", x, y)
	return strings.Join(out, "\n")
}
