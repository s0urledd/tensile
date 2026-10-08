package probe

// This observer's own side of a full reading's failures.
//
// At a full reading a validator's failure counts against it whatever the
// blob came to, so a failure that this observer caused must not reach the
// rule as the validator's. Classify already files what is plainly this
// observer's (no route out of this machine, an identity check that ran out
// of time, a request a shutdown abandoned) as PROBE_ERROR. ownSide looks
// again at the failures that read the same on the wire whichever side
// caused them, and keeps one as the validator's only when this observer's
// own side is shown working in its minutes:
//
//   - a connect that timed out: this observer's gap when no request of its
//     reached any server from OwnSideWindow before the request began until
//     it ended, and none of the endpoints of other validators it reached
//     last answers a connect now (networkUp). A whole reading that reached
//     no server is already this observer's gap (verdict Reading.Ran); this
//     is the same for an answer whose own minutes saw this observer reach
//     no one, however its reading went;
//   - a connect an ICMP unreachable from the path turned away
//     (TCP_UNREACHABLE: "no route to host", "network is unreachable" from
//     beyond this machine, "host is down"): the validator's only when this
//     observer's connections over the same IP version reached other servers
//     in those minutes, or the servers it reached last over it answer a
//     connect now (pathWorking). A vantage whose IPv6 is broken must not
//     fault an IPv6-only validator;
//   - a lookup of the validator's host name that failed other than with "no
//     such host" (Run filed it as this observer's gap): the validator's
//     (DNS_FAIL) only when this observer's resolver answered for other
//     validators' host names in those minutes, its connections reached
//     servers in them, and it answers a name no cache can hold after the
//     request (resolverWorking): a lame or broken zone of the validator's
//     then fails its lookups and no one else's;
//   - a lookup that took more than SlowLookup of the request's time, after
//     which the request ran out of it: this observer's resolver's, unless
//     the resolver is shown working the same way;
//   - its clock: a certificate read as expired or not yet valid, when the
//     instant the check turns at (the signed window's edge widened by
//     tlsverify.ClockSkew) lies within this observer's measured clock
//     offset and a minute of the request (the identity check reads this
//     observer's clock).
//
// An answer not shown to be the validator's is rewritten to PROBE_ERROR,
// with what came over the wire kept in its raw error and why in its
// classification reason. An unreachable connect, a failed lookup or a slow
// one kept as the validator's says why in its raw error. Only answers of a
// full reading are looked at: every other reading keeps the rule of its
// time.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"time"

	tlsverify "github.com/plsgiveup/fibre/fibre-tlsverify"
)

// OwnSideWindow is how long before a request began this observer must have
// reached no server for a connect that timed out to be read as its own
// network's failure, and the start of the span in which its resolver and
// its connections must be shown working for a failed lookup or an
// unreachable connect to be the validator's.
const OwnSideWindow = 30 * time.Second

// SlowLookup is how much of a request's time this observer's resolver may
// take before a timeout that follows is read as the resolver's.
const SlowLookup = 5 * time.Second

// reachKeep is how long reachLog keeps an event.
const reachKeep = 30 * time.Minute

// reachLog keeps when this observer's requests reached a server (their
// connection opened, or was refused) and where they connected, when its
// resolver answered for a validator's host name, and its last checks of
// its own side.
type reachLog struct {
	mu  sync.Mutex
	ev  []reachEvent
	res []resolveEvent

	checkMu sync.Mutex          // one check at a time
	checks  map[string]ownCheck // the last check of each kind
}

type reachEvent struct {
	at        time.Time // when the connection opened, or was refused
	endpoint  string    // the address it connected to; "" when refused
	family    string    // the endpoint's IP version; "" when refused
	validator string    // whose endpoint it was
}

// resolveEvent is a lookup of a validator's host name that this observer's
// resolver answered within SlowLookup.
type resolveEvent struct {
	at        time.Time
	name      string
	validator string
}

// ownCheck is what a check of this observer's own side found, and when.
type ownCheck struct {
	at time.Time
	up bool
}

// note records a request that reached a server (Reached by its connection),
// and a lookup of its host name that the resolver answered in good time.
func (l *reachLog) note(m Measurement) {
	reached := m.TCP.OK || m.Outcome == OutcomeTCPRefused
	resolved := m.DNS.Attempted && m.DNS.OK && m.DNS.DurationMS <= SlowLookup.Milliseconds()
	if !reached && !resolved {
		return
	}
	cut := time.Now().Add(-reachKeep)
	l.mu.Lock()
	defer l.mu.Unlock()
	if name, _, err := net.SplitHostPort(m.ValidatorHost); resolved && err == nil {
		at := m.StartedAt.Add(time.Duration(m.DNS.DurationMS) * time.Millisecond)
		l.res = append(l.res, resolveEvent{at: at, name: name, validator: m.ValidatorAddress})
		l.res = keepSince(l.res, cut, func(e resolveEvent) time.Time { return e.at })
	}
	if !reached {
		return
	}
	at := m.StartedAt.Add(time.Duration(m.DNS.DurationMS+m.TCP.DurationMS) * time.Millisecond)
	ep, fam := "", ""
	if m.TCP.OK {
		if i := strings.LastIndex(m.TCP.Detail, "-> "); i >= 0 {
			ep = m.TCP.Detail[i+3:]
			if h, _, err := net.SplitHostPort(ep); err == nil {
				fam = family(h)
			}
		}
	}
	l.ev = append(l.ev, reachEvent{at: at, endpoint: ep, family: fam, validator: m.ValidatorAddress})
	l.ev = keepSince(l.ev, cut, func(e reachEvent) time.Time { return e.at })
}

// keepSince drops from the front of es, kept in the order noted, the events
// before cut.
func keepSince[E any](es []E, cut time.Time, at func(E) time.Time) []E {
	i := 0
	for i < len(es) && at(es[i]).Before(cut) {
		i++
	}
	if i == 0 {
		return es
	}
	return append(es[:0], es[i:]...)
}

// reachedBetween reports whether a request reached a server in [from, to]:
// over the IP version family, or by any route, refusals included, when
// family is "".
func (l *reachLog) reachedBetween(from, to time.Time, family string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.ev {
		if !e.at.Before(from) && !e.at.After(to) && (family == "" || e.family == family) {
			return true
		}
	}
	return false
}

// resolvedNames counts the host names of validators other than except that
// the resolver answered in [from, to].
func (l *reachLog) resolvedNames(from, to time.Time, except string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := map[string]bool{}
	for _, e := range l.res {
		if !e.at.Before(from) && !e.at.After(to) && e.validator != except {
			seen[e.name] = true
		}
	}
	return len(seen)
}

// recentEndpoints is up to n of the addresses connected to last over the IP
// version family ("" any), newest first, each once, none of them except's.
func (l *reachLog) recentEndpoints(n int, except, family string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for i := len(l.ev) - 1; i >= 0 && len(out) < n; i-- {
		e := l.ev[i]
		if e.endpoint == "" || seen[e.endpoint] || (except != "" && e.validator == except) || (family != "" && e.family != family) {
			continue
		}
		seen[e.endpoint] = true
		out = append(out, e.endpoint)
	}
	return out
}

// maxControlParent is the longest host name a control lookup (resolverUp)
// goes under: with its label and a dot it stays a valid name, which the
// resolver must be asked about rather than refuse here as malformed.
const maxControlParent = 253 - len("fibre-sentinel-check-0123456789abcdef.")

// recentNames is up to n of the host names of validators other than except
// that the resolver answered last, newest first, each once.
func (l *reachLog) recentNames(n int, except string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for i := len(l.res) - 1; i >= 0 && len(out) < n; i-- {
		e := l.res[i]
		name := strings.TrimSuffix(e.name, ".")
		if name == "" || len(name) > maxControlParent || seen[name] || e.validator == except {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}

// networkCheckTTL is how long one check of this observer's own side
// answers for.
const networkCheckTTL = 10 * time.Second

// check runs a check of this observer's own side of the kind named, unless
// one ran less than networkCheckTTL ago, and says what it found.
func (l *reachLog) check(kind string, run func() bool) bool {
	l.checkMu.Lock()
	defer l.checkMu.Unlock()
	if c, ok := l.checks[kind]; ok && time.Since(c.at) < networkCheckTTL {
		return c.up
	}
	up := run()
	if l.checks == nil {
		l.checks = map[string]ownCheck{}
	}
	l.checks[kind] = ownCheck{at: time.Now(), up: up}
	return up
}

// networkUp reports whether this observer can reach the servers it reached
// last over the IP version family ("" any): a TCP connect, closed at once,
// to up to three of the addresses it connected to most recently, other than
// the failing validator's (except), three seconds each, at once. One that
// connects says the network is up. With none to try (nothing reached in
// half an hour) it is not shown up. One check answers for ten seconds.
func (p *Prober) networkUp(ctx context.Context, except, family string) bool {
	return p.reach.check("tcp "+family, func() bool {
		eps := p.reach.recentEndpoints(3, except, family)
		if len(eps) == 0 {
			return false
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
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
		up := false
		for range eps {
			if <-res {
				up = true
			}
		}
		return up
	})
}

// resolverUp reports whether this observer's resolver answers a lookup no
// cache can answer for it: a fresh label (controlLabel) under up to three
// of the host names of validators other than except that it answered last,
// three seconds each, at once. An answer of either kind, addresses or "no
// such host", says the resolver works: it came from that zone's own
// servers, through the resolver. With no name to try it is not shown
// working. One check answers for ten seconds.
func (p *Prober) resolverUp(ctx context.Context, except string) bool {
	return p.reach.check("dns", func() bool {
		names := p.reach.recentNames(3, except)
		if len(names) == 0 {
			return false
		}
		lookup := p.lookupHost
		if lookup == nil {
			lookup = net.DefaultResolver.LookupHost
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		res := make(chan bool, len(names))
		for _, name := range names {
			go func(name string) {
				_, err := lookup(cctx, controlLabel()+"."+name)
				var dnsErr *net.DNSError
				res <- err == nil || (errors.As(err, &dnsErr) && dnsErr.IsNotFound)
			}(name)
		}
		up := false
		for range names {
			if <-res {
				up = true
			}
		}
		return up
	})
}

// controlLabel is a DNS label no resolver can hold an answer for, naming
// this observer to whoever reads the zone's query log.
func controlLabel() string {
	return fmt.Sprintf("fibre-sentinel-check-%016x", rand.Uint64())
}

// ownSide judges again, at a full reading, an answer whose failure this
// observer cannot pin on the validator without showing its own side
// working: see the comment at the top of this file.
func (p *Prober) ownSide(ctx context.Context, m *Measurement) {
	if m.ScheduleLabel != FullReadLabel || m.fullServed() {
		return
	}
	if m.resolverFailed {
		p.judgeLookup(ctx, m)
		return
	}
	if m.fullGap() {
		return
	}
	var why string
	switch {
	case identityAtTheEdge(*m):
		why = "this observer's own clock: the instant the identity check turns at, the certificate's signed window edge widened by its clock skew, lies within this observer's measured clock offset and a minute of the request, so whether it had lapsed rests on this observer's clock"
	case m.DNS.Attempted && m.DNS.DurationMS > SlowLookup.Milliseconds() &&
		(m.Outcome == OutcomeTCPTimeout || m.Outcome == OutcomeRPCTimeout):
		slow := "the lookup took " + (time.Duration(m.DNS.DurationMS) * time.Millisecond).String() +
			" of the request's time, and the request then ran out of it"
		ok, evidence := p.resolverWorking(ctx, *m)
		if ok {
			m.RawError = clip("the validator's host name: " + slow + ", while " + evidence + "; on the wire: " + m.RawError)
			return
		}
		why = "this observer's own resolver: " + slow + "; not shown working: " + evidence
	case m.Outcome == OutcomeTCPUnreachable:
		ok, evidence := p.pathWorking(ctx, *m)
		if ok {
			m.RawError = clip("the validator's host: turned away as unreachable while " + evidence + "; on the wire: " + m.RawError)
			return
		}
		why = "this observer's own network: " + evidence
	case m.Outcome == OutcomeTCPTimeout &&
		!p.reach.reachedBetween(m.StartedAt.Add(-OwnSideWindow), m.FinishedAt, "") && !p.networkUp(ctx, m.ValidatorAddress, ""):
		why = "this observer's own network: no request of this observer reached any server from " + OwnSideWindow.String() +
			" before this one began until it ended, and none of the other servers it reached last answered a connect after it"
	}
	if why == "" {
		return
	}
	raw := string(m.Outcome)
	if m.RawError != "" {
		raw += ": " + m.RawError
	}
	m.RawError = clip(why + "; on the wire: " + raw)
	m.Outcome = OutcomeProbeError
	classifyRow(m)
	m.ClassificationReason = "counted neither way: " + why
}

// judgeLookup judges again a lookup of the validator's host name that failed
// other than with "no such host", which Run filed as this observer's gap:
// it is the validator's (DNS_FAIL) when this observer's resolver is shown
// working in its minutes (resolverWorking), and stays this observer's gap
// otherwise. The row says which, and why.
func (p *Prober) judgeLookup(ctx context.Context, m *Measurement) {
	m.resolverFailed = false
	ok, evidence := p.resolverWorking(ctx, *m)
	if !ok {
		why := "this observer's own resolver, not shown working: " + evidence
		m.RawError = clip(why + "; on the wire: " + m.RawError)
		m.ClassificationReason = "counted neither way: " + why
		return
	}
	m.Outcome = OutcomeDNSFail
	m.RawError = clip("the validator's host name: its lookup failed while " + evidence + "; on the wire: " + m.RawError)
	classifyRow(m)
}

// resolverWorking reports whether this observer's resolver was shown
// working in the minutes of m, a request whose lookup failed or was slow:
// from OwnSideWindow before the request began until it ended it answered
// for other validators' host names within SlowLookup and this observer's
// connections reached servers, and after it, it answers a name no cache
// can hold (resolverUp). Answers from its cache alone would not do: a
// resolver whose upstream is down still answers the names it holds. It says
// what it found.
func (p *Prober) resolverWorking(ctx context.Context, m Measurement) (bool, string) {
	from, to := m.StartedAt.Add(-OwnSideWindow), m.FinishedAt
	span := "from " + OwnSideWindow.String() + " before this request began until it ended"
	names := p.reach.resolvedNames(from, to, m.ValidatorAddress)
	switch {
	case names == 0:
		return false, "the resolver answered for no other validator's host name " + span
	case !p.reach.reachedBetween(from, to, ""):
		return false, "this observer's connections reached no server " + span
	case !p.resolverUp(ctx, m.ValidatorAddress):
		return false, "the resolver did not answer a name no cache holds after the request"
	}
	return true, fmt.Sprintf("this observer's resolver answered for %d other validators' host names and its connections reached servers %s, and the resolver answered a name no cache holds after the request", names, span)
}

// pathWorking reports whether this observer's network was shown working
// over the IP versions m's connects were turned away on
// (Measurement.unreachFamilies; any, when it names none): a connection over
// one of them reached a server from OwnSideWindow before the request began
// until it ended, or a server reached over it last answers a connect now
// (networkUp). It says what it found.
func (p *Prober) pathWorking(ctx context.Context, m Measurement) (bool, string) {
	fams := m.unreachFamilies
	if len(fams) == 0 {
		fams = []string{""}
	}
	span := "from " + OwnSideWindow.String() + " before this request began until it ended"
	for _, f := range fams {
		if p.reach.reachedBetween(m.StartedAt.Add(-OwnSideWindow), m.FinishedAt, f) {
			return true, "this observer's connections" + over(f) + " reached other servers " + span
		}
		if p.networkUp(ctx, m.ValidatorAddress, f) {
			return true, "the servers this observer reached" + over(f) + " last answered a connect after it"
		}
	}
	return false, "no request of this observer" + over(fams...) + " reached any server " + span +
		", and none of the other servers it reached" + over(fams...) + " last answered a connect after it"
}

// over is " over IPv4" (IPv6, or both) for the IP versions named, "" when
// none is.
func over(fams ...string) string {
	var named []string
	for _, f := range fams {
		if f != "" {
			named = append(named, f)
		}
	}
	if len(named) == 0 {
		return ""
	}
	return " over " + strings.Join(named, " or ")
}

// identityAtTheEdge reports a certificate read as expired or not yet valid
// at an instant within the row's clock offset and a minute of the request's
// start from the instant the identity check turns at: the signed window's
// edge widened by tlsverify.ClockSkew (NotAfter+ClockSkew for an expiry,
// NotBefore-ClockSkew for a certificate not yet valid), as tlsverify judges
// it. An empty or overlong window is a property of the signed payload
// alone, never of this observer's clock, and is not exempted.
func identityAtTheEdge(m Measurement) bool {
	if m.Outcome != OutcomeIdentityFail || !m.Identity.Stale {
		return false
	}
	var edge string
	var skew time.Duration
	switch tlsverify.Reason(m.Identity.Reason) {
	case tlsverify.ReasonCertExpired:
		edge, skew = m.Identity.ClaimedNotAfter, tlsverify.ClockSkew
	case tlsverify.ReasonCertNotYetValid:
		edge, skew = m.Identity.ClaimedNotBefore, -tlsverify.ClockSkew
	default:
		return false
	}
	t, err := time.Parse(time.RFC3339, edge)
	if err != nil {
		return false
	}
	margin := abs(time.Duration(m.ClockOffsetMS)*time.Millisecond) + time.Minute
	return abs(t.Add(skew).Sub(m.StartedAt)) <= margin
}
