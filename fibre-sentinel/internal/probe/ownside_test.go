package probe

import (
	"context"
	"crypto/ed25519"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	tlsverify "github.com/plsgiveup/fibre/fibre-tlsverify"
)

// failedLookup is an answer of a full reading whose lookup of the
// validator's host name failed other than with "no such host", as Run
// files it: this observer's gap until ownSide judges it.
func failedLookup(now time.Time) Measurement {
	m := Measurement{ValidatorAddress: "v9", ValidatorHost: "fibre.lame.example:7980", ScheduleLabel: FullReadLabel,
		Phase: PhaseInWindow, Assigned: true, Attested: true, StartedAt: now.Add(-15 * time.Second), FinishedAt: now,
		Outcome: OutcomeProbeError, RawError: "resolver: lookup fibre.lame.example: server misbehaving", resolverFailed: true}
	m.DNS = StepResult{Attempted: true, DurationMS: 40, Error: "lookup fibre.lame.example: server misbehaving"}
	classifyRow(&m)
	return m
}

// answered is validator v's request at at, whose lookup of host the
// resolver answered and whose connection reached ep (reached), or did not.
func answered(v, host string, at time.Time, ep string, reached bool) Measurement {
	m := Measurement{ValidatorAddress: v, ValidatorHost: host, StartedAt: at,
		DNS: StepResult{Attempted: true, OK: true, DurationMS: 30}, TCP: StepResult{Attempted: true, DurationMS: 20}}
	if reached {
		m.TCP.OK, m.TCP.Detail = true, "-> "+ep
	} else {
		m.Outcome = OutcomeTCPTimeout
	}
	return m
}

// A lookup of the validator's host name that failed other than with "no
// such host" (a lame or DNSSEC-broken zone, an authoritative server that
// never answers) is the validator's only when this observer's resolver is
// shown working in the same minutes: it answered other validators' names,
// this observer's connections reached servers, and it answers a name no
// cache can hold. Answers its cache could give are not enough. Otherwise
// the lookup stays this observer's gap, as it was; the row says which and
// why.
func TestALookupThatFailedIsTheValidatorsOnlyWhileTheResolverWorks(t *testing.T) {
	now := time.Now().UTC()
	ok := func(_ context.Context, host string) ([]string, error) {
		if strings.HasSuffix(host, ".lame.example") {
			return nil, &net.DNSError{Err: "server misbehaving", Name: host}
		}
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	down := func(_ context.Context, host string) ([]string, error) {
		return nil, &net.DNSError{Err: "server misbehaving", Name: host}
	}
	for _, c := range []struct {
		name   string
		seen   []Measurement
		lookup func(context.Context, string) ([]string, error)
		theirs bool
	}{
		{"nothing else resolved", nil, ok, false},
		{"others resolved and reached, and a fresh name answers",
			[]Measurement{answered("v1", "fibre.ok.example:7980", now.Add(-10*time.Second), "192.0.2.50:7980", true)}, ok, true},
		{"others resolved and reached, but no fresh name answers (a cache answering for a dead upstream)",
			[]Measurement{answered("v1", "fibre.ok.example:7980", now.Add(-10*time.Second), "192.0.2.50:7980", true)}, down, false},
		{"others resolved, but no connection reached a server",
			[]Measurement{answered("v1", "fibre.ok.example:7980", now.Add(-10*time.Second), "", false)}, ok, false},
		{"only long before the request",
			[]Measurement{answered("v1", "fibre.ok.example:7980", now.Add(-10*time.Minute), "192.0.2.50:7980", true)}, ok, false},
		{"only the validator's own other requests",
			[]Measurement{answered("v9", "fibre.lame2.example:7980", now.Add(-10*time.Second), "192.0.2.50:7980", true)}, ok, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := testProber(t)
			var mu sync.Mutex
			var asked []string
			p.lookupHost = func(ctx context.Context, host string) ([]string, error) {
				mu.Lock()
				asked = append(asked, host)
				mu.Unlock()
				return c.lookup(ctx, host)
			}
			for _, m := range c.seen {
				p.reach.note(m)
			}
			m := failedLookup(now)
			p.ownSide(context.Background(), &m)
			if c.theirs {
				if m.Outcome != OutcomeDNSFail || m.Classification != ClassUnreachable || m.fullGap() ||
					!strings.HasPrefix(m.RawError, "the validator's host name: ") || !strings.Contains(m.RawError, "on the wire: resolver: ") {
					t.Fatalf("%s / %s %q, want the validator's DNS_FAIL", m.Outcome, m.Classification, m.RawError)
				}
				mu.Lock()
				defer mu.Unlock()
				if len(asked) != 1 || !strings.HasPrefix(asked[0], "fibre-sentinel-check-") || !strings.HasSuffix(asked[0], ".fibre.ok.example") {
					t.Fatalf("asked %q, want a fresh label under another validator's name", asked)
				}
				return
			}
			if m.Outcome != OutcomeProbeError || !m.fullGap() ||
				!strings.HasPrefix(m.ClassificationReason, "counted neither way: this observer's own resolver, not shown working: ") ||
				!strings.Contains(m.RawError, "on the wire: resolver: ") {
				t.Fatalf("%s / %s %q %q, want this observer's gap with why", m.Outcome, m.Classification, m.ClassificationReason, m.RawError)
			}
			// judged once: a second look leaves the row as it is
			before := m.RawError
			p.ownSide(context.Background(), &m)
			if m.RawError != before {
				t.Fatalf("judged twice: %q", m.RawError)
			}
		})
	}
}

// A lookup that failed on this machine's side of its resolver (no socket,
// no buffer, its resolver's own address refusing) stays this observer's
// gap at a full reading, whatever else the resolver answered in its
// minutes: Go's resolver keeps only the text of such an error, and that is
// what is read. A lame zone's failure in the same minutes is the
// validator's.
func TestALookupThatFailedOnThisMachineStaysItsGap(t *testing.T) {
	for _, c := range []struct {
		err    string
		theirs bool
	}{
		{"dial udp 127.0.0.53:53: socket: too many open files", false},
		{"dial udp 127.0.0.53:53: socket: no buffer space available", false},
		{"read udp 127.0.0.1:40000->127.0.0.53:53: read: connection refused", false},
		{"server misbehaving", true},
	} {
		t.Run(c.err, func(t *testing.T) {
			p := testProber(t)
			p.lookupHost = func(_ context.Context, host string) ([]string, error) {
				return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
			}
			p.reach.note(answered("v1", "fibre.ok.example:7980", time.Now().UTC().Add(-10*time.Second), "192.0.2.50:7980", true))
			in := probeInput("fibre.lame.example:7980", make(ed25519.PublicKey, ed25519.PublicKeySize))
			in.SchedulePoint.Label = FullReadLabel
			in.ClientRules, in.RequestTimeout = true, 5*time.Second
			in.hooks = &netHooks{lookup: func(_ context.Context, host string) ([]string, error) {
				return nil, &net.DNSError{Err: c.err, Name: host, Server: "127.0.0.53:53"}
			}}
			m := Run(context.Background(), in, mustCoder(t), StepTimeouts{})
			if m.Outcome != OutcomeProbeError || m.resolverFailed != c.theirs {
				t.Fatalf("%s (%s), judged again %v", m.Outcome, m.RawError, m.resolverFailed)
			}
			p.ownSide(context.Background(), &m)
			if c.theirs {
				if m.Outcome != OutcomeDNSFail || m.fullGap() {
					t.Fatalf("%s / %s %q, want the validator's DNS_FAIL", m.Outcome, m.Classification, m.RawError)
				}
				return
			}
			if m.Outcome != OutcomeProbeError || !m.fullGap() || !strings.HasPrefix(m.RawError, "resolver: ") {
				t.Fatalf("%s / %s %q, want this observer's gap", m.Outcome, m.Classification, m.RawError)
			}
		})
	}
}

// A slow lookup followed by a timeout is this observer's resolver's unless
// the resolver is shown working in the same minutes; then the timeout is
// the validator's, and the row says why.
func TestASlowLookupIsTheResolversUnlessItWorks(t *testing.T) {
	now := time.Now().UTC()
	p := testProber(t)
	p.lookupHost = func(_ context.Context, host string) ([]string, error) {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	p.reach.note(answered("v1", "fibre.ok.example:7980", now.Add(-10*time.Second), "192.0.2.50:7980", true))
	m := Measurement{ValidatorAddress: "v9", ValidatorHost: "fibre.slow.example:7980", ScheduleLabel: FullReadLabel,
		Phase: PhaseInWindow, Assigned: true, Attested: true, StartedAt: now.Add(-15 * time.Second), FinishedAt: now,
		Outcome: OutcomeRPCTimeout, RawError: "context deadline exceeded"}
	m.DNS, m.TCP = StepResult{Attempted: true, OK: true, DurationMS: 6000}, StepResult{Attempted: true, OK: true}
	classifyRow(&m)
	p.ownSide(context.Background(), &m)
	if m.Outcome != OutcomeRPCTimeout || m.fullGap() || !strings.HasPrefix(m.RawError, "the validator's host name: the lookup took 6s") {
		t.Fatalf("%s / %s %q, want the validator's timeout", m.Outcome, m.Classification, m.RawError)
	}
}

// A connect an ICMP unreachable turned away is the validator's only while
// this observer's connections over the same IP version reach servers: a
// vantage whose IPv6 is broken does not fault an IPv6-only validator on the
// strength of its IPv4.
func TestAnUnreachableConnectIsTheValidatorsOnlyOverAWorkingFamily(t *testing.T) {
	now := time.Now().UTC()
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	row := func(fams ...string) Measurement {
		m := Measurement{ValidatorAddress: "v9", ScheduleLabel: FullReadLabel, Phase: PhaseInWindow, Assigned: true, Attested: true,
			StartedAt: now.Add(-15 * time.Second), FinishedAt: now, Outcome: OutcomeTCPUnreachable,
			RawError: "dial tcp: connect: network is unreachable", unreachFamilies: fams}
		m.TCP = StepResult{Attempted: true}
		classifyRow(&m)
		return m
	}
	reached := func(at time.Time, ep string) Measurement {
		return Measurement{ValidatorAddress: "v1", StartedAt: at, TCP: StepResult{OK: true, Detail: "-> " + ep}}
	}
	for _, c := range []struct {
		name   string
		seen   []Measurement
		m      Measurement
		theirs bool
	}{
		{"over IPv6, only IPv4 reached", []Measurement{reached(now.Add(-10*time.Second), "192.0.2.50:7980")}, row("IPv6"), false},
		{"over IPv6, IPv6 reached", []Measurement{reached(now.Add(-10*time.Second), "[2001:db8::5]:7980")}, row("IPv6"), true},
		{"over IPv4, IPv4 reached", []Measurement{reached(now.Add(-10*time.Second), "192.0.2.50:7980")}, row("IPv4"), true},
		{"over IPv4, a server reached over it earlier answers now", []Measurement{reached(now.Add(-10*time.Minute), up.Addr().String())}, row("IPv4"), true},
		{"nothing reached", nil, row("IPv4"), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := testProber(t)
			for _, r := range c.seen {
				p.reach.note(r)
			}
			m := c.m
			p.ownSide(context.Background(), &m)
			if c.theirs {
				if m.Outcome != OutcomeTCPUnreachable || !strings.HasPrefix(m.RawError, "the validator's host: turned away as unreachable while ") {
					t.Fatalf("%s / %s %q, want the validator's", m.Outcome, m.Classification, m.RawError)
				}
				return
			}
			if m.Outcome != OutcomeProbeError || !strings.HasPrefix(m.ClassificationReason, "counted neither way: this observer's own network: ") ||
				!strings.Contains(m.RawError, "on the wire: TCP_UNREACHABLE") {
				t.Fatalf("%s / %s %q, want this observer's gap", m.Outcome, m.ClassificationReason, m.RawError)
			}
		})
	}
}

// The identity check reads this observer's clock only for an expiry or a
// start: it turns ClockSkew past the signed edge, and an answer whose
// turning point is within this observer's clock offset and a minute of the
// request rests on that clock. An empty or overlong signed window does not
// depend on any clock and stays the validator's.
func TestOnlyAWindowsEdgeRestsOnThisObserversClock(t *testing.T) {
	now := time.Now().UTC()
	row := func(reason tlsverify.Reason, notBefore, notAfter time.Time) Measurement {
		m := Measurement{ValidatorAddress: "v9", ScheduleLabel: FullReadLabel, Phase: PhaseInWindow, Assigned: true, Attested: true,
			StartedAt: now.Add(-15 * time.Second), FinishedAt: now, Outcome: OutcomeIdentityFail, ClockOffsetMS: 2000,
			RawError: "fibre tls identity [" + string(reason) + "]"}
		m.TCP.OK = true
		m.Identity = IdentityResult{Attempted: true, Stale: true, Reason: string(reason),
			ClaimedNotBefore: notBefore.Format(time.RFC3339), ClaimedNotAfter: notAfter.Format(time.RFC3339)}
		classifyRow(&m)
		return m
	}
	start := now.Add(-15 * time.Second)
	for _, c := range []struct {
		name string
		m    Measurement
		own  bool
	}{
		{"expired a few seconds past the check's turning point",
			row(tlsverify.ReasonCertExpired, start.Add(-24*time.Hour), start.Add(-tlsverify.ClockSkew-5*time.Second)), true},
		{"not yet valid a few seconds before it",
			row(tlsverify.ReasonCertNotYetValid, start.Add(tlsverify.ClockSkew+5*time.Second), start.Add(24*time.Hour)), true},
		{"expired long before",
			row(tlsverify.ReasonCertExpired, start.Add(-48*time.Hour), start.Add(-24*time.Hour)), false},
		{"an empty window signed at the request",
			row(tlsverify.ReasonWindowEmpty, start, start), false},
		{"an overlong window starting at the request",
			row(tlsverify.ReasonWindowTooLong, start, start.Add(10*365*24*time.Hour)), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := testProber(t)
			m := c.m
			p.ownSide(context.Background(), &m)
			if got := m.Outcome == OutcomeProbeError; got != c.own {
				t.Fatalf("%s / %s %q", m.Outcome, m.Classification, m.ClassificationReason)
			}
		})
	}
}
