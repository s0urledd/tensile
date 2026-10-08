package api

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// The partials as an operator sees them: the one way out when they cannot
// be trusted (raw reads for the rest of the process), and the state
// /v1/health shows.

// fallBack puts the process on raw reads for good: every window read whole
// with the shipped statements, each statement on its own, as with
// -day-partials=false; nothing sealed, audited or written after, the files
// left as they are for whoever looks into them. It is for a difference the
// partials cannot drop day by day: a window or a ledger day that is not
// what the store holds. The first call says so loudly; /v1/health fails
// until the process is restarted.
func (dp *dayParts) fallBack(why string) {
	dp.mu.Lock()
	first := dp.fallback == ""
	if first {
		dp.fallback, dp.fallbackAt = why, time.Now()
	}
	dp.mu.Unlock()
	if first {
		dp.logf("day partials: AUDIT FOUND A DIFFERENCE: %s; every window is read raw from now on, as with -day-partials=false, "+
			"and the files are left as they are; /v1/health says so until the API is restarted", why)
	}
}

// rawFallback is why the process reads every window raw, "" while it does
// not.
func (dp *dayParts) rawFallback() string {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	return dp.fallback
}

// unsealedFor is how long a due day may stay unsealed before /v1/health
// fails: two days, many times what sealing one takes even at its pace and
// behind a backlog.
const unsealedFor = 48 * time.Hour

// dayPartsHealth is the partials in /v1/health.
type dayPartsHealth struct {
	// State is on, off (-day-partials=false), loading (before the kept
	// partials are loaded or built) or raw-fallback (an audit found a
	// difference: every window is read raw); Why says why it is the last,
	// or, when the load failed, how many times, in a fixed phrase: its
	// error names the files' path under the data directory and carries the
	// store's own text (the API's journal has it: "day partials: load
	// failed: ...").
	State string `json:"state"`
	Why   string `json:"why,omitempty"`
	// Origin is loaded (from the files kept) or built (from the store).
	Origin string `json:"origin,omitempty"`
	// The days sealed, and whether the publication ledger is built.
	SealedRowDays    int  `json:"sealed_row_days"`
	SealedSettleDays int  `json:"sealed_settlement_days"`
	LedgerBuilt      bool `json:"ledger_built"`
	// OldestDue is the oldest day due to be sealed and not sealed
	// ("row 2026-10-01", "settlement 2026-10-01"), and since when the
	// sealer has found it so; RawDays the days kept raw on purpose (tied
	// obligation rows, a start beside the day that is not a store
	// timestamp), which are not counted as due.
	OldestDue      string     `json:"oldest_unsealed_due,omitempty"`
	OldestDueSince *time.Time `json:"oldest_unsealed_due_since,omitempty"`
	RawDays        int        `json:"raw_days,omitempty"`
	// The last hourly audit, and what it found; an audit that failed is
	// said in a fixed phrase, as a failed load is (partsAuditFailed).
	LastAuditAt *time.Time `json:"last_audit_at,omitempty"`
	LastAudit   string     `json:"last_audit,omitempty"`
	// Rebuilds is how many times the partials were begun again since the
	// start, the first build included.
	Rebuilds int `json:"rebuilds"`
	// The last write of the files that failed, while the next has not
	// succeeded: when, and a fixed phrase in place of its error, which
	// names the files' path under the data directory (the API's journal
	// has it: "day partials: writing ...").
	LastSaveError   string     `json:"last_save_error,omitempty"`
	LastSaveErrorAt *time.Time `json:"last_save_error_at,omitempty"`
}

// partsSaveFailed is what /v1/health says of a write of the files that
// failed (dayPartsHealth.LastSaveError).
const partsSaveFailed = "writing the partials failed; the error is in the API's journal"

// partsLoadFailed is what /v1/health says of a load of the partials that
// failed, n times in a row (dayPartsHealth.Why): the error itself names
// the file it was loading and is the store's or the system's text.
func partsLoadFailed(n int) string {
	return fmt.Sprintf("the load failed %d time(s); the error is in the API's journal", n)
}

// partsAuditFailed is what /v1/health says of an hourly audit that failed
// (dayPartsHealth.LastAudit), for the same reason.
const partsAuditFailed = "failed; the error is in the API's journal"

// health is the partials' state at now, and the check /v1/health runs on
// it: failing on a difference an audit found, and on a day due and not
// sealed for longer than unsealedFor.
func (dp *dayParts) health(now time.Time) (dayPartsHealth, healthCheck) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	h := dayPartsHealth{State: "on", Rebuilds: dp.rebuilds}
	switch {
	case dp.fallback != "":
		h.State, h.Why = "raw-fallback", dp.fallback
	case dp.cur == nil:
		h.State = "loading"
		if f := dp.failures["load"]; f != nil {
			h.Why = partsLoadFailed(f.n)
		}
	}
	switch {
	case strings.HasPrefix(dp.origin, "loaded"):
		h.Origin = "loaded"
	case strings.HasPrefix(dp.origin, "built"):
		h.Origin = "built"
	}
	if e := dp.cur; e != nil {
		h.SealedRowDays, h.LedgerBuilt = len(e.rows), e.ledgerBuilt
		for _, sd := range e.settle {
			if sd.Seal != nil {
				h.SealedSettleDays++
			}
		}
	}
	var due []string
	for k := range dp.dueSince {
		if dp.raw[k] == nil {
			due = append(due, k)
		}
	}
	// The oldest by day, rows before settlements of the same day.
	sort.Slice(due, func(i, j int) bool {
		ki, di, _ := strings.Cut(due[i], ":")
		kj, dj, _ := strings.Cut(due[j], ":")
		if di != dj {
			return di < dj
		}
		return ki < kj
	})
	if len(due) > 0 {
		kind, day, _ := strings.Cut(due[0], ":")
		if kind == "settle" {
			kind = "settlement"
		}
		since := dp.dueSince[due[0]]
		h.OldestDue, h.OldestDueSince = kind+" "+day, &since
	}
	h.RawDays = len(dp.raw)
	if !dp.audited.IsZero() {
		at := dp.audited
		h.LastAuditAt, h.LastAudit = &at, dp.auditNote
		if strings.HasPrefix(dp.auditNote, "failed: ") {
			h.LastAudit = partsAuditFailed
		}
	}
	if dp.saveErr != "" {
		at := dp.saveErrAt
		h.LastSaveError, h.LastSaveErrorAt = partsSaveFailed, &at
	}
	c := healthCheck{Name: "day_partials", OK: true}
	var bad []string
	if dp.fallback != "" {
		bad = append(bad, "an audit found the partials not what the store holds; every window is read raw until the API is restarted: "+dp.fallback)
	}
	if strings.HasPrefix(dp.auditNote, "differs") {
		bad = append(bad, "the last audit found a sealed day not what the store holds (dropped, read raw until sealed again)")
	}
	if h.OldestDueSince != nil && now.Sub(*h.OldestDueSince) > unsealedFor {
		bad = append(bad, fmt.Sprintf("%s day is due and not sealed for %s", h.OldestDue, now.Sub(*h.OldestDueSince).Round(time.Hour)))
	}
	if len(bad) > 0 {
		c.OK, c.Detail = false, strings.Join(bad, "; ")
		return h, c
	}
	// What is checked, not the state, which day_partials holds: /v1/meta
	// carries the checks, and two processes over one store, one still
	// loading, must not differ there.
	c.Detail = "no audit found a difference, and no day due has stayed unsealed for two days"
	return h, c
}
