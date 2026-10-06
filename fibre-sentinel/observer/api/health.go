package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Liveness of the observer itself, read straight from the status files the
// four processes keep in the data directory (internal/status). It does not
// go through the database on purpose: the collector is one of the four, and
// "is the observer working" must not depend on the collector working.
//
// /v1/health is for machines: 200 when every component is alive and the
// disk has room, 503 with the reasons otherwise. Point any uptime monitor at
// it and the operator hears about a dead prober from the monitor, not from a
// reader.

// expectedComponents is every process a deployment runs. A missing file is
// reported, not ignored: a prober that never started looks exactly like one
// that died, and both are worth knowing.
var expectedComponents = []string{"scanner", "prober", "heartbeat", "collector"}

// failingAfter is how long a component may keep reporting errors before the
// health check counts it as failing rather than hiccuping.
const failingAfter = 10 * time.Minute

// scannerLagBlocks is how far the scanner may trail the chain tip the
// collector sees before health calls it behind. At six seconds a block this
// is twenty minutes.
const scannerLagBlocks = 200

// chainStaleAfter is how old the chain's newest block may be before health
// fails. Every other check here is drawn from the same RPC node the observer
// reads, so a node whose height stops advancing — a consensus halt, a node
// stuck mid-sync, a public endpoint that fell behind — looks exactly like a
// quiet network: the scanner is parked waiting for a height rather than
// failing, and the lag between it and the tip is zero because both are the
// same stopped number. The tip's own clock is the one thing that says which
// it is. Mocha's blocks are seconds apart; ten minutes is a long silence.
const chainStaleAfter = 10 * time.Minute

// unassignableWindow is how recently a publication must have settled, on
// the chain's clock, for a missing row assignment to fail health. A day is
// long enough that a monitor polling every few minutes cannot miss it and
// an operator who was asleep still finds it failing in the morning, and
// short enough that one bad blob does not mask every later failure.
const unassignableWindow = 24 * time.Hour

// minBlockSeconds is the shortest block interval the height prefilter in
// recentUnassignable assumes. It only has to be a floor: a height bound of
// unassignableWindow / minBlockSeconds blocks below the tip reaches back
// past the window as long as blocks are at least this far apart, and
// Celestia's are several seconds apart.
const minBlockSeconds = 1

// diskFloor is the free share of the data disk below which health fails, and
// the health watcher alerts. Nothing is ever deleted to make room (the prune
// was retired on 2026-10-04), and the disk is shared with other services, so
// the alarm comes while a sixth of it is still free.
const diskFloor = 0.15

// componentStatus is one process as /v1/health shows it.
type componentStatus struct {
	Component string    `json:"component"`
	Present   bool      `json:"present"`
	Alive     bool      `json:"alive"`
	OK        bool      `json:"ok"`
	StartedAt time.Time `json:"started_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	// Age is how long ago the file was refreshed, so a reader does not have
	// to compare clocks.
	AgeS        int64          `json:"age_s"`
	StoppedAt   *time.Time     `json:"stopped_at,omitempty"`
	StopReason  string         `json:"stop_reason,omitempty"`
	LastOKAt    *time.Time     `json:"last_ok_at,omitempty"`
	LastError   string         `json:"last_error,omitempty"`
	LastErrorAt *time.Time     `json:"last_error_at,omitempty"`
	Height      int64          `json:"height,omitempty"`
	Detail      map[string]any `json:"detail,omitempty"`
	Disk        *status.Disk   `json:"disk,omitempty"`
	Vantage     string         `json:"vantage,omitempty"`
	Version     string         `json:"version,omitempty"`
}

// chainTipTime is the block time of the chain's newest block, as the
// collector last saw it. Absent before the collector's first status poll.
func (s *Server) chainTipTime(ctx context.Context) (time.Time, bool) {
	var v string
	if err := s.st.DB().QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'chain_tip_time'`).Scan(&v); err != nil || v == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(store.TimeLayout, v)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// healthCheck is one row of /v1/health.
type healthCheck struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

type healthResponse struct {
	// Status is "ok", "degraded" (something is wrong but data still flows)
	// or "down" (no component is alive). Machines look at the HTTP code.
	Status     string            `json:"status"`
	Checks     []healthCheck     `json:"checks"`
	Components []componentStatus `json:"components"`
	ScanGaps   []scan.ScanGap    `json:"scan_gaps,omitempty"`
	PinStatus  string            `json:"pin_status"`
	ServerTime time.Time         `json:"server_time"`
	// DayPartials is the state of the day partials the longer windows are
	// summed from (dayparts_health.go).
	DayPartials *dayPartsHealth `json:"day_partials,omitempty"`
}

// components reads the status directory into the API's shape.
func (s *Server) components(now time.Time) []componentStatus {
	byName := map[string]status.Report{}
	if s.dataDir != "" {
		reports, err := status.ReadAll(s.dataDir)
		if err != nil && s.log != nil {
			s.log.Printf("api: status files: %v", err)
		}
		for _, r := range reports {
			byName[r.Component] = r
		}
	}
	names := append([]string(nil), expectedComponents...)
	for n := range byName {
		known := false
		for _, e := range expectedComponents {
			if e == n {
				known = true
			}
		}
		if !known {
			names = append(names, n)
		}
	}
	out := make([]componentStatus, 0, len(names))
	for _, n := range names {
		r, ok := byName[n]
		if !ok {
			out = append(out, componentStatus{Component: n})
			continue
		}
		out = append(out, componentStatus{
			Component: n, Present: true, Alive: r.Alive(now), OK: r.OK,
			StartedAt: r.StartedAt, UpdatedAt: r.UpdatedAt, AgeS: int64(now.Sub(r.UpdatedAt).Seconds()),
			StoppedAt: r.StoppedAt, StopReason: r.StopReason, LastOKAt: r.LastOKAt, LastError: r.LastError, LastErrorAt: r.LastErrorAt,
			Height: r.Height, Detail: r.Detail, Disk: r.Disk, Vantage: r.Vantage, Version: r.Version,
		})
	}
	return out
}

// scanGaps is what the scanner recorded in state.json, via the collector.
func (s *Server) scanGaps(ctx context.Context) []scan.ScanGap {
	var raw string
	if err := s.st.DB().QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'scan_gaps'`).Scan(&raw); err != nil || raw == "" {
		return nil
	}
	var gaps []scan.ScanGap
	if err := json.Unmarshal([]byte(raw), &gaps); err != nil {
		return nil
	}
	return gaps
}

// pinStatus compares the chain's app version with the celestia-app major
// this build's assignment constants are pinned to. "chain_ahead" means the
// chain has upgraded past the pin and the row assignments this observer
// computes may no longer be the ones validators actually hold; see the
// runbook in deploy/README.md for bumping the pin.
func pinStatus(appVersion string) string {
	if appVersion == "" {
		return "unknown"
	}
	chain, err := strconv.Atoi(appVersion)
	if err != nil {
		return "unknown"
	}
	pinned, err := strconv.Atoi(strings.TrimPrefix(assign.PinnedCelestiaAppVersion, "v"))
	if err != nil {
		return "unknown"
	}
	switch {
	case chain > pinned:
		return "chain_ahead"
	case chain < pinned:
		return "chain_behind"
	default:
		return "matches"
	}
}

// health evaluates the checks. It never errors: a check that cannot be
// evaluated is reported as failing with the reason.
func (s *Server) health(ctx context.Context, now time.Time) healthResponse {
	comps := s.components(now)
	var checks []healthCheck
	alive := 0
	var scannerH, chainH int64
	var disk *status.Disk
	for _, c := range comps {
		switch {
		case !c.Present:
			checks = append(checks, healthCheck{c.Component, false, "no status file: never started, or an older build"})
		case !c.Alive:
			why := "not running"
			if c.StoppedAt != nil {
				why = fmt.Sprintf("stopped %s ago (%s)", now.Sub(*c.StoppedAt).Round(time.Second), c.StopReason)
			} else {
				why = fmt.Sprintf("no update for %s; last error: %s", now.Sub(c.UpdatedAt).Round(time.Second), orNone(c.LastError))
			}
			checks = append(checks, healthCheck{c.Component, false, why})
		case !c.OK && c.LastErrorAt != nil && now.Sub(*c.LastErrorAt) < failingAfter && (c.LastOKAt == nil || now.Sub(*c.LastOKAt) > failingAfter):
			alive++
			checks = append(checks, healthCheck{c.Component, false, "alive but failing: " + c.LastError})
		default:
			alive++
			d := "alive"
			if !c.OK {
				d = "alive, last cycle failed: " + c.LastError
			}
			checks = append(checks, healthCheck{c.Component, true, d})
		}
		if c.Component == "scanner" {
			scannerH = c.Height
		}
		if c.Component == "collector" {
			chainH = c.Height
		}
		if c.Disk != nil && (disk == nil || c.Disk.FreeShare < disk.FreeShare) {
			disk = c.Disk
		}
	}
	if scannerH > 0 && chainH > 0 {
		lag := chainH - scannerH
		checks = append(checks, healthCheck{"scanner_lag", lag <= scannerLagBlocks,
			fmt.Sprintf("scanner at %d, chain at %d (%d blocks behind)", scannerH, chainH, lag)})
	}
	if tip, ok := s.chainTipTime(ctx); ok {
		age := now.Sub(tip)
		checks = append(checks, healthCheck{"chain_liveness", age <= chainStaleAfter,
			fmt.Sprintf("newest block %s old (%s)", age.Round(time.Second), tip.UTC().Format(time.RFC3339))})
	}
	if disk != nil {
		checks = append(checks, healthCheck{"disk", disk.FreeShare >= diskFloor,
			fmt.Sprintf("%.1f%% free (%.1f GiB of %.1f GiB)", disk.FreeShare*100, float64(disk.FreeBytes)/(1<<30), float64(disk.TotalBytes)/(1<<30))})
	}
	gaps := s.scanGaps(ctx)
	if n := len(gaps); n > 0 {
		missing := int64(0)
		for _, g := range gaps {
			missing += g.To - g.From + 1
		}
		checks = append(checks, healthCheck{"scan_gaps", false,
			fmt.Sprintf("%d height range(s), %d blocks not scanned (the RPC node could not serve them, or the operator skipped them with -skip-heights; each range gives its reason); publications in them are unknown to this observer", n, missing)})
	}
	var appVersion string
	_ = s.st.DB().QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'app_version'`).Scan(&appVersion)
	pin := pinStatus(appVersion)
	if pin == "chain_ahead" {
		checks = append(checks, healthCheck{"pin", false,
			fmt.Sprintf("chain app version %s is past this build's pin (%s): row assignments may be stale; bump the pin", appVersion, assign.PinnedCelestiaAppVersion)})
	}
	// Unassignable publications fail health only while they are recent. The
	// scanner does not re-scan a settled publication, and nothing else ever
	// clears assignment_error, so an all-time count is a latch: one blob of
	// an unknown version the day Fibre activated would hold /v1/health at 503
	// for the life of the database, and a monitor that sees "degraded" all
	// the time stops telling anyone when the prober dies next to it. What an
	// operator can act on is "this is still happening" — a build that cannot
	// assign what the chain is settling now — so the failing count is the
	// publications settled within unassignableWindow. The all-time count
	// stays in the detail (and in /v1/meta) so the history is not hidden,
	// and the check stays listed, passing, while any exist.
	if total := s.unassignablePublications(ctx); total > 0 {
		recent := s.recentUnassignable(ctx, now, max(scannerH, chainH))
		if recent > 0 {
			checks = append(checks, healthCheck{"unassignable_publications", false,
				fmt.Sprintf("%d publication(s) settled in the last %s with no row assignment (unknown blob version or params): never probed; %d all time",
					recent, fmtWindow(unassignableWindow), total)})
		} else {
			checks = append(checks, healthCheck{"unassignable_publications", true,
				fmt.Sprintf("none settled in the last %s; %d older publication(s) with no row assignment, never probed",
					fmtWindow(unassignableWindow), total)})
		}
	}

	var parts *dayPartsHealth
	if s.parts != nil {
		h, c := s.parts.health(now)
		parts = &h
		checks = append(checks, c)
	} else if s.noParts {
		parts = &dayPartsHealth{State: "off"}
	}

	st := "ok"
	for _, c := range checks {
		if !c.OK {
			st = "degraded"
		}
	}
	if alive == 0 {
		st = "down"
	}
	return healthResponse{Status: st, Checks: checks, Components: comps, ScanGaps: gaps, PinStatus: pin, ServerTime: now.UTC(), DayPartials: parts}
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	h := s.health(r.Context(), s.now())
	code := http.StatusOK
	if h.Status != "ok" {
		code = http.StatusServiceUnavailable
	}
	// Never cached: a monitor must see the state now, not fifteen seconds
	// ago, and a 503 held by a proxy would outlive the fix.
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, code, h)
}

// unassignablePublications is the count behind the health check of the same
// name, which /v1/health and /v1/meta run on every call. It used to read
// every publication to find the few with an assignment error;
// publications_unassignable
// (migration 22) is partial on exactly this test, so it now reads only
// those. The predicate must stay spelled so SQLite can match it to the
// index's WHERE (`<>` and `!=` are the same operator).
func (s *Server) unassignablePublications(ctx context.Context) int64 {
	var n sql.NullInt64
	_ = s.st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM publications WHERE assignment_error != ''`).Scan(&n)
	return n.Int64
}

// recentUnassignable counts publications with no row assignment that
// settled within unassignableWindow of now. settlement_time is the exact
// criterion, but it has no index; settlement_height does. So when a tip
// height is known (the collector's or the scanner's, from the status
// files) the query is also bounded below by a height that is certainly
// older than the window — tip minus the window at minBlockSeconds per
// block — and SQLite walks publications_settlement over roughly the last
// day instead of the whole table on every health poll. With no tip known
// the height bound is 0 and the time filter alone decides: correct, only
// slower. The time bound uses store.TS, the fixed-width layout
// settlement_time is written in, so the string comparison is a time one.
func (s *Server) recentUnassignable(ctx context.Context, now time.Time, tip int64) int64 {
	floor := tip - int64(unassignableWindow/time.Second)/minBlockSeconds
	if floor < 0 {
		floor = 0
	}
	var n sql.NullInt64
	_ = s.st.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM publications WHERE settlement_height >= ? AND settlement_time >= ? AND assignment_error != ''`,
		floor, store.TS(now.Add(-unassignableWindow))).Scan(&n)
	return n.Int64
}

// fmtWindow prints a whole-hour duration the way a reader says it: "24h",
// not time.Duration's "24h0m0s".
func fmtWindow(d time.Duration) string {
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", d/time.Hour)
	}
	return d.String()
}
