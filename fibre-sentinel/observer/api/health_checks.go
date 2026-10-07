package api

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/hosting"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The checks /v1/health runs beside "is each process alive": whether each
// one still completes its work, whether the record moves, whether the
// chain-side polls and the second vantages arrive. Each exists because a
// failure of its kind once kept /v1/health at 200: a collector deadlocked
// on its one store connection read "alive" for as long as it was stuck,
// because its status file is rewritten by a goroutine of its own whatever
// its loop does.

// stallCadences and stallFloor bound how long a component may go without a
// completed cycle (status.CadenceKey): three of its own cadences, and never
// less than three minutes, which the status file's refresh and a loop's
// ordinary jitter fit well inside.
const (
	stallCadences = 3
	stallFloor    = 3 * time.Minute
)

// healthState is what the checks remember between two calls (healthState
// on Server).
type healthState struct {
	// errs are the API's own 5xx answers (health_errors.go).
	errs routeErrors
	// ingest is each record file's cursor offset as the last call saw it,
	// and when it was first seen there (ingestCheck).
	ingestMu sync.Mutex
	ingest   map[string]ingestSeen
}

// errorStage is what a public answer may say of a component's error: the
// stage the message names before its first colon ("reading", "export",
// "gap at h=1234"), never the error itself, which can carry local paths,
// RPC addresses and SQL. The whole message is in the component's journal.
func errorStage(msg string) string {
	if msg == "" {
		return "none"
	}
	stage, _, found := strings.Cut(msg, ":")
	stage = strings.TrimSpace(stage)
	if !found || stage == "" || len(stage) > 40 || strings.ContainsAny(stage, `/\"'@`) {
		return "in its journal"
	}
	return stage
}

// tipSlack is how much newer than the scanner's last cycle the newest block
// may be while the scanner still counts as waiting at the tip: the block's
// time is the proposer's clock and the cycle's this host's, a few seconds
// apart at most, and a live chain moves past it within a few blocks.
const tipSlack = 30 * time.Second

// componentCheck is the check of one present, alive component.
//
// A component that states its cadence fails when its last completed cycle
// is older than stallCadences of it (at least stallFloor), however fresh
// its file: "no completed cycle for <d>", with the stage of an error it
// reported since its last cycle when there is one (its loop runs, and
// every cycle fails). The scanner completes a cycle per block, so while
// the newest block the observer knows of is no newer than its last cycle
// it is waiting at the tip, not stuck, and chain_liveness says why. A
// component that states none (an older build) is judged as before: failing
// while it keeps reporting errors with no success in failingAfter.
func componentCheck(c componentStatus, now, tip time.Time, tipKnown bool) healthCheck {
	if cadence, ok := (status.Report{Detail: c.Detail}).Cadence(); ok {
		bound := max(stallCadences*cadence, stallFloor)
		last := c.StartedAt
		if c.LastOKAt != nil && c.LastOKAt.After(last) {
			last = *c.LastOKAt
		}
		waiting := c.Component == "scanner" && tipKnown && !tip.After(last.Add(tipSlack))
		if since := now.Sub(last); since > bound && !waiting {
			d := fmt.Sprintf("no completed cycle for %s", since.Round(time.Second))
			if e := c.LastErrorAt; e != nil && e.After(last) && now.Sub(*e) <= bound {
				d += "; failing: " + errorStage(c.LastError)
			}
			return healthCheck{c.Component, false, d}
		}
	}
	if !c.OK && c.LastErrorAt != nil && now.Sub(*c.LastErrorAt) < failingAfter && (c.LastOKAt == nil || now.Sub(*c.LastOKAt) > failingAfter) {
		return healthCheck{c.Component, false, "alive but failing: " + errorStage(c.LastError)}
	}
	// The prober's readings run beside its planning loop, whose cycles
	// succeed whatever the readings do: every reading failing (a coder or
	// a client-order error after a params change) wrote each blob as this
	// observer's own gap while the check stayed green.
	if failed, made, ok := proberReadings(c); ok && failed > 0 && made == 0 {
		return healthCheck{c.Component, false, fmt.Sprintf("alive but failing: %d reading(s) failed in the last 15m and none was made", failed)}
	}
	d := "alive"
	if !c.OK {
		d = "alive, last cycle failed: " + errorStage(c.LastError)
	}
	return healthCheck{c.Component, true, d}
}

// proberReadings is the prober's count of the readings that failed in the
// last fifteen minutes and of those it made, false when its file carries
// neither (another component, or an older build).
func proberReadings(c componentStatus) (failed, made int64, ok bool) {
	f, okF := c.Detail["reading_errors_15m"].(float64)
	m, okM := c.Detail["readings_15m"].(float64)
	return int64(f), int64(m), okF && okM
}

// workFailingAfter is how long one of the collector's stages may keep
// failing before the work check fails.
const workFailingAfter = 10 * time.Minute

// workCheck is the collector's work beside its chain poll: the export, the
// registry, the corrections, the holds, retention, the store heartbeat and
// the hosting lookup. Each failing stage is in its file's work_errors,
// with since when, until it next succeeds. Before this, a stage's error
// was the status file's last error until the next poll's OK overwrote it
// a minute later, so an export that stopped for good never failed health.
func workCheck(c componentStatus, now time.Time) healthCheck {
	errs, _ := c.Detail["work_errors"].(map[string]any)
	stages := make([]string, 0, len(errs))
	for k := range errs {
		stages = append(stages, k)
	}
	sort.Strings(stages)
	var bad, young []string
	for _, stage := range stages {
		e, _ := errs[stage].(map[string]any)
		raw, _ := e["since"].(string)
		since, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			bad = append(bad, stage+" failing (since an unknown time)")
			continue
		}
		if d := now.Sub(since); d > workFailingAfter {
			bad = append(bad, fmt.Sprintf("%s failing for %s", stage, d.Round(time.Minute)))
		} else {
			young = append(young, fmt.Sprintf("%s (%s)", stage, d.Round(time.Second)))
		}
	}
	switch {
	case len(bad) > 0:
		return healthCheck{"work", false, strings.Join(bad, "; ")}
	case len(young) > 0:
		return healthCheck{"work", true, "failing for under " + workFailingAfter.String() + ": " + strings.Join(young, ", ")}
	default:
		return healthCheck{"work", true, "no collector stage failing"}
	}
}

// ingestStaleAfter is how long lines may wait in a record file with its
// cursor unmoved before the ingest check fails: a minute of the
// collector's passes many times over.
const ingestStaleAfter = 10 * time.Minute

// ingestSeen is one cursor's offset as a check saw it, and when it first
// saw it there.
type ingestSeen struct {
	offset int64
	since  time.Time
}

// ingestCheck says whether the collector keeps up with the record: for
// every file it ingests, lines past its cursor (the file's logical end
// beyond the offset ingested) that have waited ingestStaleAfter with the
// cursor unmoved. A collector whose loop is stuck kept every check green
// while it was, the chain's liveness included, and then blamed the chain;
// this one names the record, which is what stops moving. A file with
// nothing to ingest says nothing, so a quiet chain is not a stall.
//
// The cursor's updated_at is when its last pass began, so a single pass
// over a long backlog would look stuck; an offset that moved since the
// previous call is progress whatever updated_at says. Where no record file
// can be read from here, the check falls back on the cursors alone
// (ingestBlindAfter).
func (s *Server) ingestCheck(ctx context.Context, now time.Time) (healthCheck, bool) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT file, byte_offset, updated_at FROM ingest_cursors`)
	if err != nil {
		return healthCheck{}, false
	}
	type cursor struct {
		file    string
		offset  int64
		updated time.Time
	}
	var cs []cursor
	for rows.Next() {
		var c cursor
		var at string
		if rows.Scan(&c.file, &c.offset, &at) != nil {
			continue
		}
		if t, err := time.Parse(store.TimeLayout, at); err == nil {
			c.updated = t
		}
		cs = append(cs, c)
	}
	rows.Close()
	if len(cs) == 0 {
		return healthCheck{}, false
	}
	s.hs.ingestMu.Lock()
	if s.hs.ingest == nil {
		s.hs.ingest = map[string]ingestSeen{}
	}
	var stuck []string
	var latest, stuckLatest time.Time // the newest progress of any file, and of a stuck one
	readable := 0
	for _, c := range cs {
		progress := c.updated
		if seen, ok := s.hs.ingest[c.file]; ok && seen.offset == c.offset {
			if seen.since.After(progress) {
				progress = seen.since
			}
		} else {
			since := time.Time{}
			if ok {
				// moved since the last call: that is progress now
				since = now
				progress = now
			}
			s.hs.ingest[c.file] = ingestSeen{offset: c.offset, since: since}
		}
		if progress.After(latest) {
			latest = progress
		}
		end, err := record.LogicalEnd(c.file)
		if err != nil {
			continue // gone, or unreadable here
		}
		readable++
		if end <= c.offset {
			continue // nothing waiting
		}
		if now.Sub(progress) > ingestStaleAfter {
			stuck = append(stuck, filepath.Base(c.file))
			if progress.After(stuckLatest) {
				stuckLatest = progress
			}
		}
	}
	s.hs.ingestMu.Unlock()
	if readable == 0 {
		// Not one record file readable from here (another layout than the
		// collector's): what waits cannot be seen, so the cursors alone
		// say it, by how long none of them has moved.
		if d := now.Sub(latest); !latest.IsZero() && d > ingestBlindAfter {
			return healthCheck{"ingest", false, fmt.Sprintf("collector has ingested nothing for %s", d.Round(time.Second))}, true
		}
		return healthCheck{"ingest", true, fmt.Sprintf("a record file ingested within %s", ingestBlindAfter)}, true
	}
	if len(stuck) == 0 {
		return healthCheck{"ingest", true, fmt.Sprintf("no lines waiting longer than %s", ingestStaleAfter)}, true
	}
	sort.Strings(stuck)
	stuck = compactStrings(stuck)
	// How long nothing came: from any file when none moved within the
	// bound, else from the files named, as a line the collector cannot get
	// past would leave them while the others move on.
	if d := now.Sub(latest); d > ingestStaleAfter {
		return healthCheck{"ingest", false, fmt.Sprintf("collector has ingested nothing for %s; lines waiting in %s", d.Round(time.Second), strings.Join(stuck, ", "))}, true
	}
	return healthCheck{"ingest", false, fmt.Sprintf("collector has ingested nothing for %s from %s while lines wait there; other files are ingested",
		now.Sub(stuckLatest).Round(time.Second), strings.Join(stuck, ", "))}, true
}

// ingestBlindAfter is the ingest check's bound when it cannot read the
// record files: how long no cursor may stand still. Every file grows within
// it on a working observer, the heartbeat's every few minutes.
const ingestBlindAfter = 15 * time.Minute

// compactStrings drops adjacent duplicates from a sorted list: two
// vantages' files share a base name.
func compactStrings(in []string) []string {
	out := in[:0]
	for i, v := range in {
		if i == 0 || v != in[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// healthMetaKeys are the meta keys the checks read, in one query.
var healthMetaKeys = []string{
	"app_version", "fibre_active",
	"chain_status_polled_at", "endpoints_polled_at", "escrow_polled_at", "identities_polled_at",
	hosting.MetaEnabled, hosting.MetaASNDBModified,
}

// healthMeta reads healthMetaKeys; a key not stored is absent.
func (s *Server) healthMeta(ctx context.Context) map[string]string {
	out := map[string]string{}
	q := `SELECT key, value FROM meta WHERE key IN (?` + strings.Repeat(",?", len(healthMetaKeys)-1) + `)`
	args := make([]any, len(healthMetaKeys))
	for i, k := range healthMetaKeys {
		args[i] = k
	}
	rows, err := s.st.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if rows.Scan(&k, &v) == nil {
			out[k] = v
		}
	}
	return out
}

// chainPolls are the collector's chain-side polls the chain_polls check
// holds to an age: the meta key of the last successful one, and how old it
// may get. The escrow poll asks a module that exists only once Fibre is
// active, and is not judged before; the endpoint poll counts an answer
// that the module is not there yet as made.
var chainPolls = []struct {
	name, key string
	bound     time.Duration
	fibreOnly bool
}{
	{"chain_status", "chain_status_polled_at", 15 * time.Minute, false},
	{"endpoints", "endpoints_polled_at", 15 * time.Minute, false},
	{"escrow", "escrow_polled_at", 15 * time.Minute, true},
	{"identities", "identities_polled_at", 26 * time.Hour, false},
}

// chainPollsCheck fails while a chain-side poll has not succeeded within
// its bound. A failed poll only logs and keeps the last value, so escrow
// balances, endpoint lists and validator names could freeze with nothing
// saying so. A poll never recorded is judged only once the collector that
// should have made it (one stating its cadence, a build that records every
// poll) has run longer than the bound; before that the check leaves it
// out. Listed while any poll is on record.
func chainPollsCheck(meta map[string]string, collector *componentStatus, now time.Time) (healthCheck, bool) {
	var stale, fresh []string
	for _, p := range chainPolls {
		if p.fibreOnly && meta["fibre_active"] != "yes" {
			continue
		}
		raw := meta[p.key]
		if raw == "" {
			if collector == nil {
				continue
			}
			if _, ok := (status.Report{Detail: collector.Detail}).Cadence(); !ok {
				continue
			}
			if up := now.Sub(collector.StartedAt); up > p.bound {
				stale = append(stale, fmt.Sprintf("%s never polled in the collector's %s", p.name, up.Round(time.Minute)))
			}
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			stale = append(stale, p.name+" polled at an unreadable time")
			continue
		}
		age := now.Sub(at)
		if age > p.bound {
			stale = append(stale, fmt.Sprintf("%s last polled %s ago (bound %s)", p.name, roundAge(age), roundAge(p.bound)))
		} else {
			fresh = append(fresh, fmt.Sprintf("%s %s ago", p.name, roundAge(age)))
		}
	}
	switch {
	case len(stale) > 0:
		return healthCheck{"chain_polls", false, "stale: " + strings.Join(stale, "; ")}, true
	case len(fresh) > 0:
		return healthCheck{"chain_polls", true, strings.Join(fresh, ", ")}, true
	default:
		return healthCheck{}, false
	}
}

// roundAge rounds an age for a detail: seconds under a minute, minutes
// under an hour, hours past it.
func roundAge(d time.Duration) time.Duration {
	switch {
	case d < time.Minute:
		return d.Round(time.Second)
	case d < time.Hour:
		return d.Round(time.Minute)
	default:
		return d.Round(time.Hour)
	}
}

// Second vantages: seen if their newest row is under vantageSeenWithin
// old, and late once it is over vantageLateAfter (a five-minute heartbeat,
// a pull a minute, a collector pass every ten seconds).
const (
	vantageSeenWithin = 7 * 24 * time.Hour
	vantageLateAfter  = 20 * time.Minute
)

// vantagesCheck holds every other vantage seen in the last week to a
// newest row under vantageLateAfter old. A pull that fails on every run (a
// vantage whose file was replaced) and a remote heartbeat that died look
// the same from here, rows that stop arriving, and both used to be silent:
// confirmations from the second location just stopped, and an endpoint
// only this vantage could not reach was published without the check that
// exists for it. Listed while another vantage was seen.
func (s *Server) vantagesCheck(ctx context.Context, now time.Time) (healthCheck, bool) {
	rows, err := s.st.DB().QueryContext(ctx, recentVantagesSQL)
	if err != nil {
		return healthCheck{}, false
	}
	defer rows.Close()
	var late, ok []string
	for rows.Next() {
		var name string
		var at sql.NullString
		if rows.Scan(&name, &at) != nil || !at.Valid || name == s.vantage {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, at.String)
		if err != nil {
			continue
		}
		age := now.Sub(t)
		switch {
		case age > vantageSeenWithin:
		case age > vantageLateAfter:
			late = append(late, fmt.Sprintf("%s: newest row %s old", name, roundAge(age)))
		default:
			ok = append(ok, fmt.Sprintf("%s: newest row %s old", name, roundAge(age)))
		}
	}
	switch {
	case len(late) > 0:
		return healthCheck{"vantages", false, strings.Join(late, "; ") + fmt.Sprintf(" (bound %s)", vantageLateAfter)}, true
	case len(ok) > 0:
		return healthCheck{"vantages", true, strings.Join(ok, "; ")}, true
	default:
		return healthCheck{}, false
	}
}

// hostingDBStaleAfter is how old the IP-to-ASN file may get before the
// hosting_db check fails: the monthly refresh (fibre-hosting-db@.timer)
// missed once, and some.
const hostingDBStaleAfter = 45 * 24 * time.Hour

// hostingDBCheck fails while the hosting lookup reads an IP-to-ASN file
// older than hostingDBStaleAfter: hosts that moved since are attributed to
// their old network, and nothing else says how old the file is. Listed
// while the lookup is on.
func hostingDBCheck(meta map[string]string, now time.Time) (healthCheck, bool) {
	if meta[hosting.MetaEnabled] != "yes" {
		return healthCheck{}, false
	}
	at, err := time.Parse(time.RFC3339Nano, meta[hosting.MetaASNDBModified])
	if err != nil {
		return healthCheck{}, false
	}
	days := int(now.Sub(at) / (24 * time.Hour))
	if now.Sub(at) > hostingDBStaleAfter {
		return healthCheck{"hosting_db", false, fmt.Sprintf("the IP-to-ASN file is %d days old; fibre-hosting-db@.timer refreshes it monthly", days)}, true
	}
	return healthCheck{"hosting_db", true, fmt.Sprintf("the IP-to-ASN file is %d days old", days)}, true
}
