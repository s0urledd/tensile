package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// The API's own failures, for /v1/health. A route that answers 500 only
// logged it (writeInternal): one undecodable row once made /v1/blobs answer
// 500 for the whole page, every request, while /v1/health stayed 200 and
// the health watch said nothing. Each route's 5xx answers are counted here
// by minute, and the api_errors check fails while any came recently.

// Route errors are kept for routeErrorsKept and fail the check while one
// is under routeErrorsFailFor old.
const (
	routeErrorsKept    = 15 * time.Minute
	routeErrorsFailFor = 10 * time.Minute
)

// routeErrorMinutes is how many one-minute buckets cover routeErrorsKept.
const routeErrorMinutes = int(routeErrorsKept / time.Minute)

// routeErrors counts each route's 5xx answers.
type routeErrors struct {
	mu sync.Mutex
	m  map[string]*routeErrorCount
}

// routeErrorCount is one route's 5xx answers: per minute over the last
// routeErrorsKept, the last one's time, and its status.
type routeErrorCount struct {
	minutes [routeErrorMinutes]struct {
		minute int64
		n      int
	}
	last   time.Time
	status int
}

// countsAsError is whether an answer with this status is a failure of the
// API's: a 5xx, but not 503, which the API answers on purpose (a window
// still being computed, asked again after Retry-After, and /v1/health's own
// verdict). A window that never finishes computing is the snapshots
// check's.
func countsAsError(status int) bool {
	return status >= 500 && status != http.StatusServiceUnavailable
}

// note counts one 5xx answer of route at t.
func (e *routeErrors) note(route string, status int, t time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.m == nil {
		e.m = map[string]*routeErrorCount{}
	}
	c := e.m[route]
	if c == nil {
		c = &routeErrorCount{}
		e.m[route] = c
	}
	minute := t.Unix() / 60
	b := &c.minutes[minute%int64(routeErrorMinutes)]
	if b.minute != minute {
		b.minute, b.n = minute, 0
	}
	b.n++
	if t.After(c.last) {
		c.last, c.status = t, status
	}
}

// routeErrorSummary is one route's recent 5xx answers.
type routeErrorSummary struct {
	route  string
	n      int // within routeErrorsKept
	last   time.Time
	status int
}

// recent is every route with a 5xx answer within routeErrorsKept of now,
// by route; a route quiet longer is forgotten.
func (e *routeErrors) recent(now time.Time) []routeErrorSummary {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []routeErrorSummary
	first := now.Add(-routeErrorsKept).Unix()/60 + 1
	for route, c := range e.m {
		if now.Sub(c.last) > routeErrorsKept {
			delete(e.m, route)
			continue
		}
		n := 0
		for _, b := range c.minutes {
			if b.minute >= first {
				n += b.n
			}
		}
		out = append(out, routeErrorSummary{route: route, n: n, last: c.last, status: c.status})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].route < out[j].route })
	return out
}

// routeOf names the route a request matched, for the counts: its pattern
// without the method ("/v1/blobs/{hash}"), so a route is one entry however
// many hashes are asked for. A request that matched none is "other".
func routeOf(r *http.Request) string {
	if r.Pattern == "" {
		return "other"
	}
	_, path, found := strings.Cut(r.Pattern, " ")
	if !found {
		return r.Pattern
	}
	return path
}

// apiErrorsCheck fails while any route answered a 5xx within
// routeErrorsFailFor. The error itself is in the API's journal
// (writeInternal logs it); the detail names the route, how many and when.
func (s *Server) apiErrorsCheck(now time.Time) healthCheck {
	var bad []string
	for _, r := range s.hs.errs.recent(now) {
		if now.Sub(r.last) > routeErrorsFailFor {
			continue
		}
		bad = append(bad, fmt.Sprintf("%s: %d answer(s) %d in the last %s, the last %s ago", r.route, r.n, r.status,
			fmtMinutes(routeErrorsKept), now.Sub(r.last).Round(time.Second)))
	}
	if len(bad) > 0 {
		return healthCheck{"api_errors", false, strings.Join(bad, "; ") + " (the errors are in the API's journal)"}
	}
	return healthCheck{"api_errors", true, fmt.Sprintf("no 5xx answer in the last %s", fmtMinutes(routeErrorsFailFor))}
}

// fmtMinutes prints a whole-minute duration as "15m".
func fmtMinutes(d time.Duration) string {
	return fmt.Sprintf("%dm", int(d/time.Minute))
}
