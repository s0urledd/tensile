package api

import (
	"fmt"
	"strings"
	"time"
)

// The snapshots check: whether every window's figures still refresh. A
// refresh that fails is logged and the previous snapshot served, which is
// right for a reader and was silent for everyone else: a window whose every
// computation failed (one bad row in the readings memo, a long window past
// its timeout) kept the site's figures frozen at their last computed_at,
// for hours, with /v1/health at 200.

// A window fails the check when its last successful computation is older
// than twice its refresh interval and at least snapshotStaleFloor, or when
// snapshotFailuresInRow of its refreshes in a row failed.
const (
	snapshotStaleFloor    = 15 * time.Minute
	snapshotFailuresInRow = 3
)

// snapshotState is one window of one cache as the check sees it.
type snapshotState struct {
	label, window string
	// since is how long ago its last computation succeeded, or the cache
	// was made when none has in this process (a snapshot loaded from disk
	// is served meanwhile, and the warm-up is under way); ever says which.
	since    time.Duration
	ever     bool
	bound    time.Duration
	failures int
}

// states is every window's state at now.
func (c *snapshotCache[T]) states(now time.Time) []snapshotState {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]snapshotState, 0, len(warmWindows))
	for _, name := range warmWindows {
		// The interval stale() refreshes the window at: its TTL, or twice
		// what it took to compute when that is longer.
		interval := c.ttl(name)
		if e := c.entries[name]; e != nil {
			interval = max(interval, 2*time.Duration(e.ms)*time.Millisecond)
		}
		st := snapshotState{label: c.label, window: name, bound: max(2*interval, snapshotStaleFloor), failures: c.failures[name]}
		last := c.started
		if t, ok := c.lastOK[name]; ok && !t.Before(last) {
			last, st.ever = t, true
		}
		st.since = now.Sub(last)
		out = append(out, st)
	}
	return out
}

// snapshotsCheck fails while any window of the three caches is stale or
// keeps failing. The detail names the window, not the error, which is in
// the API's journal.
func (s *Server) snapshotsCheck(now time.Time) healthCheck {
	var states []snapshotState
	if s.net != nil {
		states = append(states, s.net.states(now)...)
	}
	if s.vals != nil {
		states = append(states, s.vals.states(now)...)
	}
	if s.market != nil {
		states = append(states, s.market.states(now)...)
	}
	var bad []string
	for _, st := range states {
		var why []string
		if st.since > st.bound {
			if st.ever {
				why = append(why, fmt.Sprintf("last refreshed %s ago (bound %s)", roundAge(st.since), roundAge(st.bound)))
			} else {
				why = append(why, fmt.Sprintf("not computed in the %s since the API started (bound %s)", roundAge(st.since), roundAge(st.bound)))
			}
		}
		if st.failures >= snapshotFailuresInRow {
			why = append(why, fmt.Sprintf("%d refreshes in a row failed", st.failures))
		}
		if len(why) > 0 {
			bad = append(bad, st.label+" "+st.window+": "+strings.Join(why, ", "))
		}
	}
	if len(bad) > 0 {
		return healthCheck{"snapshots", false, strings.Join(bad, "; ") + " (the errors are in the API's journal)"}
	}
	return healthCheck{"snapshots", true, "every window refreshed within its bound"}
}
