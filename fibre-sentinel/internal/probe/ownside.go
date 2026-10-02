package probe

// This observer's own side of a full reading's failures.
//
// At a full reading a validator's failure counts against it whatever the
// blob came to, so a failure that this observer caused must not reach the
// rule as the validator's. Classify already files what is plainly this
// observer's (its resolver failing, no route out of this machine, an
// identity check that ran out of time, a request a shutdown abandoned) as
// PROBE_ERROR. ownSide does the same for three failures that look like the
// validator's on the wire but are not shown to be:
//
//   - its network was down: a connect that timed out or found no route,
//     while no request of this observer reached any server from
//     OwnSideWindow before the request began until it ended, and none of
//     the endpoints this observer reached last answers a connect now
//     (networkUp). A whole reading that reached no server is already this
//     observer's gap (verdict Reading.Ran); this is the same for an answer
//     whose own minutes saw this observer reach no one, however its
//     reading went;
//   - its resolver was slow: the lookup took more than SlowLookup of the
//     request's time, and the request then ran out of time;
//   - its clock: a certificate read as outside its signed window, when the
//     window's edge lies within this observer's measured clock offset and
//     a minute of the request (the identity check reads this observer's
//     clock).
//
// Such an answer is rewritten to PROBE_ERROR, with what came over the wire
// kept in its raw error. Only answers of a full reading are looked at:
// every other reading keeps the rule of its time.

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"
)

// OwnSideWindow is how long before a request began this observer must have
// reached no server for a connect that timed out to be read as its own
// network's failure.
const OwnSideWindow = 30 * time.Second

// SlowLookup is how much of a request's time this observer's resolver may
// take before a timeout that follows is read as the resolver's.
const SlowLookup = 5 * time.Second

// reachKeep is how long reachLog keeps an event.
const reachKeep = 30 * time.Minute

// reachLog keeps when this observer's requests reached a server (their
// connection opened, or was refused) and where they connected, and the
// last network check.
type reachLog struct {
	mu sync.Mutex
	ev []reachEvent

	checkMu   sync.Mutex // one network check at a time
	checkedAt time.Time
	up        bool
}

type reachEvent struct {
	at       time.Time // when the connection opened, or was refused
	endpoint string    // the address it connected to; "" when refused
}

// note records a request that reached a server (Reached by its connection).
func (l *reachLog) note(m Measurement) {
	if !m.TCP.OK && m.Outcome != OutcomeTCPRefused {
		return
	}
	at := m.StartedAt.Add(time.Duration(m.DNS.DurationMS+m.TCP.DurationMS) * time.Millisecond)
	ep := ""
	if m.TCP.OK {
		if i := strings.LastIndex(m.TCP.Detail, "-> "); i >= 0 {
			ep = m.TCP.Detail[i+3:]
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ev = append(l.ev, reachEvent{at: at, endpoint: ep})
	cut := time.Now().Add(-reachKeep)
	i := 0
	for i < len(l.ev) && l.ev[i].at.Before(cut) {
		i++
	}
	if i > 0 {
		l.ev = append(l.ev[:0], l.ev[i:]...)
	}
}

// reachedBetween reports whether a request reached a server in [from, to].
func (l *reachLog) reachedBetween(from, to time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.ev {
		if !e.at.Before(from) && !e.at.After(to) {
			return true
		}
	}
	return false
}

// recentEndpoints is up to n of the addresses connected to last, newest
// first, each once.
func (l *reachLog) recentEndpoints(n int) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for i := len(l.ev) - 1; i >= 0 && len(out) < n; i-- {
		ep := l.ev[i].endpoint
		if ep == "" || seen[ep] {
			continue
		}
		seen[ep] = true
		out = append(out, ep)
	}
	return out
}

// networkCheckTTL is how long one network check answers for.
const networkCheckTTL = 10 * time.Second

// networkUp reports whether this observer can reach the servers it reached
// last: a TCP connect, closed at once, to up to three of the addresses it
// connected to most recently, three seconds each, at once. One that
// connects says the network is up. With none to try (nothing reached in
// half an hour) it is not shown up. One check answers for ten seconds.
func (p *Prober) networkUp(ctx context.Context) bool {
	l := &p.reach
	l.checkMu.Lock()
	defer l.checkMu.Unlock()
	if !l.checkedAt.IsZero() && time.Since(l.checkedAt) < networkCheckTTL {
		return l.up
	}
	eps := l.recentEndpoints(3)
	up := false
	if len(eps) > 0 {
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		res := make(chan bool, len(eps))
		for _, ep := range eps {
			go func(ep string) {
				var d net.Dialer
				c, err := d.DialContext(cctx, "tcp", ep)
				if err == nil {
					_ = c.Close()
				}
				res <- err == nil
			}(ep)
		}
		for range eps {
			if <-res {
				up = true
			}
		}
		cancel()
	}
	l.checkedAt, l.up = time.Now(), up
	return up
}

// ownSide rewrites an answer of a full reading whose failure this
// observer cannot pin on the validator into this observer's gap
// (PROBE_ERROR): see the comment at the top of this file.
func (p *Prober) ownSide(ctx context.Context, m *Measurement) {
	if m.ScheduleLabel != FullReadLabel || m.fullServed() || m.fullGap() {
		return
	}
	var why string
	switch {
	case identityAtTheEdge(*m):
		why = "this observer's own clock: the certificate's signed window ends or starts within this observer's measured clock offset and a minute of the request, so whether it had lapsed rests on this observer's clock"
	case m.DNS.Attempted && m.DNS.DurationMS > SlowLookup.Milliseconds() &&
		(m.Outcome == OutcomeTCPTimeout || m.Outcome == OutcomeRPCTimeout):
		why = "this observer's own resolver: the lookup took " + (time.Duration(m.DNS.DurationMS) * time.Millisecond).String() +
			" of the request's time, and the request then ran out of it"
	case (m.Outcome == OutcomeTCPTimeout || m.Outcome == OutcomeTCPUnreachable) &&
		!p.reach.reachedBetween(m.StartedAt.Add(-OwnSideWindow), m.FinishedAt) && !p.networkUp(ctx):
		why = "this observer's own network: no request of this observer reached any server from " + OwnSideWindow.String() +
			" before this one began until it ended, and none of the servers it reached last answered a connect after it"
	}
	if why == "" {
		return
	}
	raw := string(m.Outcome)
	if m.RawError != "" {
		raw += ": " + m.RawError
	}
	m.RawError = why + "; on the wire: " + raw
	m.Outcome = OutcomeProbeError
	classifyRow(m)
	m.ClassificationReason = "counted neither way: " + why
}

// identityAtTheEdge reports a certificate read as outside its signed
// window whose window edge lies within the row's clock offset and a minute
// of the request's start.
func identityAtTheEdge(m Measurement) bool {
	if m.Outcome != OutcomeIdentityFail || !m.Identity.Stale {
		return false
	}
	off := time.Duration(m.ClockOffsetMS) * time.Millisecond
	if off < 0 {
		off = -off
	}
	margin := off + time.Minute
	for _, s := range []string{m.Identity.ClaimedNotBefore, m.Identity.ClaimedNotAfter} {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			continue
		}
		d := t.Sub(m.StartedAt)
		if d < 0 {
			d = -d
		}
		if d <= margin {
			return true
		}
	}
	return false
}
