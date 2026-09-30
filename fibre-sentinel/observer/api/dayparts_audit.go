package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
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
// serve. Every hour one sealed row day and one sealed settlement day,
// chosen at random, are read again raw in a read transaction and compared
// with what is kept for them; for the first week after a start the
// network summary of one longer window is also computed both ways, from
// the partials and with the shipped statements over the whole window, in
// one read. A difference is logged with where it is, and what differs is
// dropped: the day, or every sealed day when a window differed. Nothing a
// reader is served waits for it.

const (
	auditEvery = time.Hour
	auditWeek  = 7 * 24 * time.Hour
)

// checkRowSeal reads row day d again and reports whether it is what the
// seal holds.
func (s *Server) checkRowSeal(ctx context.Context, q store.Querier, rd *rowDay) (bool, error) {
	got, err := s.rowSpanParts(ctx, q, dayLo(rd.Day), dayHi(rd.Day), "")
	if err != nil {
		return false, err
	}
	return sameJSON(got, rd.Vals), nil
}

// checkSettleSeal reads settlement day d again, as it was sealed (the
// pending cut at the moment it was sealed, its span), and reports whether
// it is what the seal holds.
func (s *Server) checkSettleSeal(ctx context.Context, q store.Querier, d string, sd *settleDay) (bool, error) {
	seal := sd.Seal
	var readable int64
	if err := q.QueryRowContext(ctx, dayReadableSQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&readable); err != nil {
		return false, err
	}
	if readable != seal.Readable {
		return false, nil
	}
	if seal.Obl == nil || seal.MinStart == "" {
		return true, nil
	}
	rows, err := q.QueryContext(ctx, obligationPassSQL(""), "", "", seal.SealedAt, dayLo(d), dayHi(d), seal.MaxStart, seal.MinStart)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	got := map[string]rollup.Obligations{}
	for rows.Next() {
		var addr string
		var r rollup.Obligations
		var n int64
		var young sql.NullString
		if err := rows.Scan(append(append([]any{&addr}, scanObligations(&r)...), &n, &young)...); err != nil {
			return false, err
		}
		got[addr] = r
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return reflect.DeepEqual(got, seal.Obl), nil
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
		ok, err := s.checkRowSeal(ctx, q, e.rows[d])
		if err != nil {
			return "", err
		}
		if !ok {
			return refusal("row day " + d + " is not what the store holds"), nil
		}
	}
	for _, d := range pick(settleDays) {
		ok, err := s.checkSettleSeal(ctx, q, d, e.settle[d])
		if err != nil {
			return "", err
		}
		if !ok {
			return refusal("settlement day " + d + " is not what the store holds"), nil
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

// auditOnce is one hour's audit.
func (s *Server) auditOnce(ctx context.Context, started time.Time) error {
	var badRow, badSettle string
	err := s.partsTx(ctx, func(ctx context.Context, e *epoch) error {
		q := s.q(ctx)
		var rowDays, settleDays []string
		for d := range e.rows {
			rowDays = append(rowDays, d)
		}
		for d, sd := range e.settle {
			if sd.Seal != nil {
				settleDays = append(settleDays, d)
			}
		}
		if len(rowDays) > 0 {
			d := rowDays[rand.IntN(len(rowDays))]
			ok, err := s.checkRowSeal(ctx, q, e.rows[d])
			if err != nil {
				return err
			}
			if !ok {
				badRow = d
			}
		}
		if len(settleDays) > 0 {
			d := settleDays[rand.IntN(len(settleDays))]
			ok, err := s.checkSettleSeal(ctx, q, d, e.settle[d])
			if err != nil {
				return err
			}
			if !ok {
				badSettle = d
			}
		}
		return nil
	})
	if err != nil && err != errNoParts {
		return err
	}
	if badRow != "" || badSettle != "" {
		if s.log != nil {
			s.log.Printf("day partials: audit: sealed row day %q / settlement day %q is not what the store holds; dropped", badRow, badSettle)
		}
		s.parts.publish(func(cur *epoch, _ []journalEntry) bool {
			if badRow != "" {
				delete(cur.rows, badRow)
			}
			if badSettle != "" && cur.settle[badSettle] != nil {
				cur.settleMut(badSettle).Seal = nil
			}
			return true
		}, 0)
	}
	if s.now().Sub(started) > auditWeek {
		return nil
	}
	names := []string{"7d", "30d", "all"}
	win := windowFor(names[rand.IntN(len(names))], s.now())
	n, diffs, err := s.comparePaths(ctx, []pathCase{networkCase(s, win, excludeSet{}, nil)})
	if err != nil {
		return err
	}
	if len(diffs) > 0 {
		if s.log != nil {
			s.log.Printf("day partials: audit: network %s differs from the shipped statements (%d compared); every sealed day dropped:\n%s", win.Name, n, strings.Join(diffs, "\n"))
		}
		s.parts.publish(func(cur *epoch, _ []journalEntry) bool {
			cur.rows = map[string]*rowDay{}
			for d, sd := range cur.settle {
				if sd.Seal != nil {
					cur.settleMut(d).Seal = nil
				}
			}
			return true
		}, 0)
	}
	return nil
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
