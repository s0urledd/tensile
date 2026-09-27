package api

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
)

// The validator page's day × schedule-point grid: for every UTC day of the
// window and every in-window schedule point, how many of the validator's
// rated probes at that point came back served. It is serve_rate_by_point
// spread over the calendar, in the manner of a custody-compliance heatmap,
// so a reader can tell a validator that failed late points for one bad day
// from one that has pruned early every day of the month; the pooled rate and
// the pooled per-point rate both hide that.
//
// The population is the serve rate's own: assigned probes in the in_window
// phase, by effective class, with the points this observer does not trust
// itself at left out. Each such probe is one attested obligation observed at
// that point. A cell carries:
//
//   - served:   HEALTHY
//   - faults:   FAULT
//   - held_out: every other class of that population (UNATTESTED,
//     UNREACHABLE, gaps...), which no rate speaks for.
//
// served / (served + faults) is the cell's ratio; faults / (served + faults)
// is the only thing that may be drawn in the fault colour. A cell with rows
// but nothing rated is "no verdict", and a cell with no rows is "no data":
// neither is ever a fault. Days are bucketed by started_at, the same column
// every probe-level window bound uses.
//
// Raw probe rows are pruned after the retention (observer/rollup), and the
// daily rollup keeps a validator's class counts per day but not per point,
// so a day before raw_from has a whole-day cell (point "day") from
// probe_daily and no per-point cells. The "day" row is published for raw days
// too, summed from the per-point cells, so the bottom row reads the same way
// across the boundary.

type heatCell struct {
	Day     string `json:"day"`
	Point   string `json:"point"`
	Served  int64  `json:"served"`
	Faults  int64  `json:"faults"`
	HeldOut int64  `json:"held_out"`
	// Rolled is set on a whole-day cell that comes from the daily rollup.
	Rolled bool `json:"rolled,omitempty"`
}

type heatmap struct {
	// Days is every UTC day of the window, oldest first, including days
	// without a row: an empty day is drawn as no data, not skipped. On the
	// "all" window it starts at the first day with a row.
	Days []string `json:"days"`
	// Points are the in-window schedule points, in schedule order.
	Points []string `json:"points"`
	// Cells holds only the (day, point) pairs that have rows, plus one
	// point "day" cell per day with rows.
	Cells []heatCell `json:"cells"`
	// RawFrom is set once raw rows have been pruned: days before it have a
	// whole-day cell only.
	RawFrom string `json:"raw_from,omitempty"`
}

// heatmapMaxDays bounds the grid on the "all" window to the newest days, so
// the page stays drawable. Three months at one column a day is already the
// most a phone can show without scrolling the grid itself.
const heatmapMaxDays = 92

func (s *Server) validatorHeatmap(ctx context.Context, addr string, win Window) (*heatmap, error) {
	db := s.st.DB()
	_, ss, err := s.suspectPoints(ctx, win)
	if err != nil {
		return nil, err
	}
	// ObligationClass, so an end-of-window reading is counted in its cell the
	// way the validator's obligations count it (no rows is a fault).
	cls := rollup.ObligationClass("")
	rows, err := db.QueryContext(ctx, `SELECT substr(started_at, 1, 10) AS day, schedule_label,
			COALESCE(SUM(CASE WHEN `+cls+` = 'HEALTHY' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN `+cls+` = 'FAULT' THEN 1 ELSE 0 END), 0),
			COUNT(*)
		FROM probe_rows WHERE validator_address = ? AND started_at >= ? AND started_at <= ?
			AND assigned = 1 AND phase = 'in_window'`+ss.clause("scheduled_at")+`
		GROUP BY day, schedule_label`,
		append([]any{addr, win.startArg(), win.endArg()}, ss.args...)...)
	if err != nil {
		return nil, err
	}
	out := &heatmap{Cells: []heatCell{}}
	dayTotal := map[string]*heatCell{}
	points := map[string]bool{}
	for rows.Next() {
		var c heatCell
		var all int64
		if err := rows.Scan(&c.Day, &c.Point, &c.Served, &c.Faults, &all); err != nil {
			rows.Close()
			return nil, err
		}
		c.HeldOut = all - c.Served - c.Faults
		out.Cells = append(out.Cells, c)
		points[c.Point] = true
		t := dayTotal[c.Day]
		if t == nil {
			t = &heatCell{Day: c.Day, Point: "day"}
			dayTotal[c.Day] = t
		}
		t.Served, t.Faults, t.HeldOut = t.Served+c.Served, t.Faults+c.Faults, t.HeldOut+c.HeldOut
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Days the raw record no longer holds: the rollup's whole-day classes,
	// which are the same population (rollup.rollDay counts in-window
	// assigned probes by effective class, suspect points excluded).
	if from, pruned := rollup.RawFrom(s.st); pruned {
		out.RawFrom = from.Format("2006-01-02")
		lo := "0000"
		if win.Span > 0 {
			lo = win.Start.UTC().Format("2006-01-02")
		}
		prow, err := db.QueryContext(ctx, `SELECT day, classes_json FROM probe_daily
			WHERE validator_address = ? AND day >= ? AND day < ? AND day <= ?`,
			addr, lo, out.RawFrom, win.End.UTC().Format("2006-01-02"))
		if err != nil {
			return nil, err
		}
		for prow.Next() {
			var day, cj string
			if err := prow.Scan(&day, &cj); err != nil {
				prow.Close()
				return nil, err
			}
			var classes map[string]int64
			if json.Unmarshal([]byte(cj), &classes) != nil {
				continue // a malformed rollup row is no data, never a zero
			}
			c := &heatCell{Day: day, Point: "day", Rolled: true}
			for k, n := range classes {
				switch k {
				case "HEALTHY":
					c.Served += n
				case "FAULT":
					c.Faults += n
				default:
					c.HeldOut += n
				}
			}
			dayTotal[day] = c
		}
		prow.Close()
		if err := prow.Err(); err != nil {
			return nil, err
		}
	}
	for _, t := range dayTotal {
		out.Cells = append(out.Cells, *t)
	}

	// Points: the four in-window points of the earlier schedule, in order,
	// so an empty column is shown as empty, whenever the window holds any of
	// them or nothing at all; then anything else on record, the end-of-window
	// reading among them. A window of end readings alone does not show four
	// empty columns for a schedule it was never read under.
	legacy := len(points) == 0
	for _, p := range []string{"w1", "w2", "w3", "w4"} {
		legacy = legacy || points[p]
	}
	for _, p := range []string{"w1", "w2", "w3", "w4"} {
		if legacy {
			out.Points = append(out.Points, p)
		}
		delete(points, p)
	}
	extra := make([]string, 0, len(points))
	for p := range points {
		extra = append(extra, p)
	}
	sort.Strings(extra)
	out.Points = append(out.Points, extra...)

	// Days: the window's calendar. The "all" window starts at the first day
	// with a row; with nothing on record it is just the last day.
	first := win.Start
	if win.Span == 0 {
		first = win.End
		for d := range dayTotal {
			if t, err := time.Parse("2006-01-02", d); err == nil && t.Before(first) {
				first = t
			}
		}
	}
	d0 := time.Date(first.UTC().Year(), first.UTC().Month(), first.UTC().Day(), 0, 0, 0, 0, time.UTC)
	end := win.End.UTC()
	for d := d0; !d.After(end); d = d.AddDate(0, 0, 1) {
		out.Days = append(out.Days, d.Format("2006-01-02"))
	}
	if len(out.Days) > heatmapMaxDays {
		out.Days = out.Days[len(out.Days)-heatmapMaxDays:]
		keep := map[string]bool{}
		for _, d := range out.Days {
			keep[d] = true
		}
		cells := out.Cells[:0]
		for _, c := range out.Cells {
			if keep[c.Day] {
				cells = append(cells, c)
			}
		}
		out.Cells = cells
	}
	// Deterministic order, so two reads of the same data serialise alike.
	sort.Slice(out.Cells, func(i, j int) bool {
		if out.Cells[i].Day != out.Cells[j].Day {
			return out.Cells[i].Day < out.Cells[j].Day
		}
		return out.Cells[i].Point < out.Cells[j].Point
	})
	return out, nil
}
