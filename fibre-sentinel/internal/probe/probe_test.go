package probe

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

func pub(settle, msu time.Time) scan.Publication {
	return scan.Publication{
		PromiseHash:    "abcdef0123456789",
		SettlementTime: settle,
		MustServeUntil: msu,
		Promise:        scan.PromiseFields{Commitment: "00", BlobVersion: 0},
		Assignment:     scan.AssignmentTable{ValidatorSetHeight: 100},
	}
}

func TestPhaseAt(t *testing.T) {
	settle := time.Now().UTC()
	msu := settle.Add(10 * time.Minute)
	p := pub(settle, msu)
	cfg := DefaultScheduleConfig()
	tol := cfg.PruneTolerance

	cases := []struct {
		t    time.Time
		want Phase
	}{
		{msu.Add(-time.Minute), PhaseInWindow},
		{msu.Add(-time.Millisecond), PhaseInWindow},
		{msu.Add(time.Second), PhaseGrace},
		{msu.Add(tol), PhaseGrace},
		{msu.Add(tol + time.Second), PhasePost},
	}
	for _, c := range cases {
		if got := PhaseAt(c.t, p, cfg); got != c.want {
			t.Errorf("PhaseAt(%s) = %s, want %s", c.t.Sub(msu), got, c.want)
		}
	}
}

func TestClassify_Taxonomy(t *testing.T) {
	type row struct {
		assigned bool
		phase    Phase
		outcome  Outcome
		want     Classification
		// commitmentVerified and shadowed matter only for WRONG_ROWS and
		// PARTIAL: rows that verify against the commitment and are exactly
		// another settled promise's assignment mean that promise answered
		// in this one's place, which no validator can prevent. Verified
		// rows with no such promise are UNMATCHED_GENUINE: held out of the
		// rate, never a fault, because an upload whose promise never
		// settled can answer under hash-order serving.
		commitmentVerified bool
		shadowed           bool
	}
	// Every existing row describes an ATTESTED validator: one the settled
	// promise proves stored the shard. The unattested rows are a separate
	// table below, because the obligation itself differs.
	rows := []row{
		// assigned, in obligation
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeServedOK, want: ClassHealthy},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeNotFound, want: ClassFault},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeInvalidRows, want: ClassFault},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeServerError, want: ClassServerError},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeThrottled, want: ClassThrottled},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeIdentityFail, want: ClassIdentityMismatch},
		// unreachable is not a retention verdict: from one vantage it is not
		// distinguishable from a problem on the observer's own path.
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeRPCUnavailable, want: ClassUnreachable},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeTCPRefused, want: ClassUnreachable},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeTCPTimeout, want: ClassUnreachable},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeDNSFail, want: ClassUnreachable},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeTLSFail, want: ClassUnreachable},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeRPCError, want: ClassUnreachable},
		// rows that do not verify against the commitment are corrupt data
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeWrongRows, want: ClassFault},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomePartial, want: ClassFault},
		// rows that DO verify are another promise's shard for the same blob
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeWrongRows, want: ClassShadowedShard, commitmentVerified: true, shadowed: true},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomePartial, want: ClassShadowedShard, commitmentVerified: true, shadowed: true},
		{assigned: true, phase: PhaseGrace, outcome: OutcomeWrongRows, want: ClassShadowedShard, commitmentVerified: true, shadowed: true},
		{assigned: true, phase: PhasePost, outcome: OutcomeWrongRows, want: ClassShadowedShard, commitmentVerified: true, shadowed: true},
		// verified rows, no settled promise that assigns them: held out, not
		// a fault (an upload whose promise never settled can answer under
		// hash-order serving)
		{assigned: true, phase: PhaseInWindow, outcome: OutcomePartial, want: ClassUnmatchedGenuine, commitmentVerified: true},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeWrongRows, want: ClassUnmatchedGenuine, commitmentVerified: true},
		{assigned: true, phase: PhaseGrace, outcome: OutcomePartial, want: ClassUnmatchedGenuine, commitmentVerified: true},
		{assigned: true, phase: PhasePost, outcome: OutcomePartial, want: ClassServedPastWindow, commitmentVerified: true},
		// no registered host is a registry state, not a refusal to serve
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeNoHost, want: ClassNotRegistered},
		{assigned: true, phase: PhaseGrace, outcome: OutcomeNoHost, want: ClassNotRegistered},
		{assigned: true, phase: PhasePost, outcome: OutcomeNoHost, want: ClassNotRegistered},
		// assigned, grace
		{assigned: true, phase: PhaseGrace, outcome: OutcomeNotFound, want: ClassTolerated},
		{assigned: true, phase: PhaseGrace, outcome: OutcomeRPCUnavailable, want: ClassTolerated},
		{assigned: true, phase: PhaseGrace, outcome: OutcomeServedOK, want: ClassHealthy},
		{assigned: true, phase: PhaseGrace, outcome: OutcomeIdentityFail, want: ClassIdentityMismatch},
		{assigned: true, phase: PhaseGrace, outcome: OutcomePartial, want: ClassFault},
		{assigned: true, phase: PhaseGrace, outcome: OutcomeWrongRows, want: ClassFault},
		{assigned: true, phase: PhaseGrace, outcome: OutcomeServerError, want: ClassTolerated},
		{assigned: true, phase: PhaseGrace, outcome: OutcomeThrottled, want: ClassTolerated},
		// assigned, post
		{assigned: true, phase: PhasePost, outcome: OutcomeNotFound, want: ClassExpectedGone},
		{assigned: true, phase: PhasePost, outcome: OutcomeServedOK, want: ClassServedPastWindow},
		{assigned: true, phase: PhasePost, outcome: OutcomeTCPTimeout, want: ClassUnreachablePostWindow},
		{assigned: true, phase: PhasePost, outcome: OutcomeInvalidRows, want: ClassFault},
		// DownloadShard enforces no assignment, so serving rows outside it
		// after the obligation ended is not a rule the validator broke.
		{assigned: true, phase: PhasePost, outcome: OutcomeWrongRows, want: ClassServedPastWindow},
		{assigned: true, phase: PhasePost, outcome: OutcomePartial, want: ClassServedPastWindow},
		{assigned: true, phase: PhasePost, outcome: OutcomeServerError, want: ClassUnreachablePostWindow},
		{assigned: true, phase: PhasePost, outcome: OutcomeThrottled, want: ClassUnreachablePostWindow},
		// unassigned
		{assigned: false, phase: PhaseInWindow, outcome: OutcomeNotFound, want: ClassExpectedUnassigned},
		{assigned: false, phase: PhaseInWindow, outcome: OutcomeServedOK, want: ClassServingUnassigned},
		{assigned: false, phase: PhasePost, outcome: OutcomeNotFound, want: ClassExpectedUnassigned},
		{assigned: false, phase: PhaseGrace, outcome: OutcomeRPCUnavailable, want: ClassExpectedUnassigned},
		{assigned: false, phase: PhaseInWindow, outcome: OutcomeWrongRows, want: ClassServingUnassigned},
		{assigned: false, phase: PhaseInWindow, outcome: OutcomeInvalidRows, want: ClassServingUnassigned},
		{assigned: false, phase: PhaseInWindow, outcome: OutcomePartial, want: ClassServingUnassigned},
		{assigned: false, phase: PhaseInWindow, outcome: OutcomeIdentityFail, want: ClassIdentityMismatch},
		{assigned: false, phase: PhaseGrace, outcome: OutcomeNoHost, want: ClassExpectedUnassigned},
		{assigned: false, phase: PhaseInWindow, outcome: OutcomeServerError, want: ClassExpectedUnassigned},
		{assigned: false, phase: PhaseInWindow, outcome: OutcomeThrottled, want: ClassExpectedUnassigned},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeReachable, want: ClassNotProbed},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeRPCDeadline, want: ClassProbeError},
		{assigned: false, phase: PhasePost, outcome: OutcomeRPCDeadline, want: ClassProbeError},
		// probe-side
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeProbeError, want: ClassProbeError},
		{assigned: true, phase: PhaseInWindow, outcome: OutcomeMissed, want: ClassNotProbed},
	}
	for _, r := range rows {
		got, reason := Classify(Evidence{
			Assigned: r.assigned, Attested: true, Phase: r.phase,
			Outcome: r.outcome, CommitmentVerified: r.commitmentVerified, Shadowed: r.shadowed,
		})
		if got != r.want {
			t.Errorf("Classify(assigned=%v, attested, %s, %s, commitmentVerified=%v) = %s (%q), want %s",
				r.assigned, r.phase, r.outcome, r.commitmentVerified, got, reason, r.want)
		}
		if reason == "" {
			t.Errorf("Classify(%v,%s,%s): empty reason", r.assigned, r.phase, r.outcome)
		}
	}
}

// Every outcome must be named by the taxonomy. A new outcome that fell
// through to a default arm used to become a FAULT under obligation, which
// made "we have not taught the observer about this yet" indistinguishable
// from "the validator broke its promise".
func TestClassify_NoOutcomeReachesAnUnnamedFault(t *testing.T) {
	for _, o := range AllOutcomes {
		for _, phase := range []Phase{PhaseInWindow, PhaseGrace, PhasePost} {
			got, reason := Classify(Evidence{Assigned: true, Attested: true, Phase: phase, Outcome: o})
			if reason == "" {
				t.Errorf("Classify(%s, %s): empty reason", phase, o)
			}
			if got == ClassFault && strings.Contains(reason, "unrecognised") {
				t.Errorf("Classify(%s, %s) faulted on an outcome it does not recognise", phase, o)
			}
		}
	}
	// and an outcome the taxonomy has never seen is the observer's gap in
	// every phase: not a verdict, for or against, and never TOLERATED or
	// EXPECTED_GONE, which would read as "the validator behaved".
	for _, phase := range []Phase{PhaseInWindow, PhaseGrace, PhasePost} {
		got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: phase, Outcome: Outcome("INVENTED")})
		if got != ClassProbeError {
			t.Errorf("an unknown outcome in %s = %s, want %s", phase, got, ClassProbeError)
		}
	}
}

// The whole product of the evidence space, checked against the one
// invariant the site rests on: FAULT is drawn only for a proven obligation
// (assigned, attested, pin current, host registered) and only from an
// answer that says the shard is missing in window or that the bytes do not
// verify. Every other cell is some named class with a reason. The table
// test above documents the interesting cells; this one closes the gaps
// between them.
func TestClassify_FaultOnlyFromNamedConditions(t *testing.T) {
	phases := []Phase{PhaseInWindow, PhaseGrace, PhasePost}
	outcomes := append(append([]Outcome(nil), AllOutcomes...), Outcome("INVENTED"))
	classes := map[Classification]bool{}
	for _, c := range AllClassifications {
		classes[c] = true
	}
	for _, o := range outcomes {
		for _, ph := range phases {
			for _, assigned := range []bool{false, true} {
				for _, attested := range []bool{false, true} {
					for _, pin := range []bool{false, true} {
						for _, cv := range []bool{false, true} {
							in := Evidence{Assigned: assigned, Attested: attested, Phase: ph, Outcome: o, PinStale: pin, CommitmentVerified: cv}
							got, reason := Classify(in)
							if !classes[got] || reason == "" {
								t.Errorf("Classify(%+v) = %q (%q): not a named class with a reason", in, got, reason)
							}
							provable := assigned && attested && !pin
							faultAnswer := (o == OutcomeNotFound && ph == PhaseInWindow) || o == OutcomeInvalidRows ||
								((o == OutcomeWrongRows || o == OutcomePartial) && !cv && ph != PhasePost)
							if got == ClassFault && !(provable && faultAnswer) {
								t.Errorf("Classify(%+v) = FAULT outside the named conditions (%s)", in, reason)
							}
							if provable && faultAnswer && got != ClassFault {
								t.Errorf("Classify(%+v) = %s, want FAULT (%s)", in, got, reason)
							}
						}
					}
				}
			}
		}
	}
}

// A validator that cannot be reached has not been shown to break anything.
// Publishing that as a fault is the accusation this product must not make
// from a single vantage.
func TestClassify_UnreachableIsNotAFault(t *testing.T) {
	for _, o := range []Outcome{
		OutcomeDNSFail, OutcomeTCPRefused, OutcomeTCPTimeout, OutcomeTCPUnreachable,
		OutcomeTLSFail, OutcomeRPCUnavailable, OutcomeRPCError,
	} {
		got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: o})
		if got != ClassUnreachable {
			t.Errorf("in-window %s = %s, want %s", o, got, ClassUnreachable)
		}
		if got.CountsAgainst() || got.Rated() {
			t.Errorf("%s reached the serve rate as %s", o, got)
		}
	}
	// but a server that answered and said it has no shard, or handed over
	// bytes that do not verify, is a fault
	for _, o := range []Outcome{OutcomeNotFound, OutcomeInvalidRows} {
		got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: o})
		if !got.CountsAgainst() {
			t.Errorf("in-window %s = %s, want a fault: the validator answered and did not serve", o, got)
		}
	}
	// an application error is neither: the server was reached and did not
	// say it lacks the shard. It has its own class, outside the rate.
	got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: OutcomeServerError})
	if got != ClassServerError || got.Rated() {
		t.Errorf("in-window SERVER_ERROR = %s, want %s outside the rate", got, ClassServerError)
	}
}

// A second, never-settled promise over the same blob makes an honest
// validator answer with the other promise's rows, because DownloadShard is
// addressed by commitment alone. That must never be published as a fault.
func TestClassify_ShadowedShardIsNeverAFault(t *testing.T) {
	for _, phase := range []Phase{PhaseInWindow, PhaseGrace, PhasePost} {
		for _, o := range []Outcome{OutcomeWrongRows, OutcomePartial} {
			got, reason := Classify(Evidence{
				Assigned: true, Attested: true, Phase: phase, Outcome: o, CommitmentVerified: true, Shadowed: true,
			})
			if got != ClassShadowedShard {
				t.Errorf("%s %s with verified rows = %s (%q), want %s", phase, o, got, reason, ClassShadowedShard)
			}
			if got.CountsAgainst() {
				t.Errorf("%s %s with verified rows counts against the validator", phase, o)
			}
		}
	}
	// rows that do not verify against the commitment are still corrupt data
	if got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: OutcomeWrongRows}); !got.CountsAgainst() {
		t.Errorf("unverifiable rows in window = %s, want a fault", got)
	}
}

// Verified rows that are not this promise's assignment, with no other
// settled promise over the commitment assigning them, are an incomplete or
// wrong delivery of this shard. Calling that "shadowed" made a validator
// that lost half its shard and served the rest un-faultable, for as long as
// the half it served verified.
func TestClassify_PartialDeliveryWithoutAShadowIsHeldOut(t *testing.T) {
	// Genuine rows that no settled promise assigns are not a fault the
	// evidence supports: the store serves the first shard by promise-hash
	// order and an upload whose promise never settled can answer. Their
	// own class, held out of the rate; FAULT stays for no shard at all and
	// for bytes that do not verify.
	for _, phase := range []Phase{PhaseInWindow, PhaseGrace} {
		for _, o := range []Outcome{OutcomeWrongRows, OutcomePartial} {
			got, reason := Classify(Evidence{Assigned: true, Attested: true, Phase: phase, Outcome: o, CommitmentVerified: true})
			if got != ClassUnmatchedGenuine {
				t.Errorf("%s %s verified but unshadowed = %s (%q), want UNMATCHED_GENUINE", phase, o, got, reason)
			}
			if !strings.Contains(reason, "no settled promise") {
				t.Errorf("%s %s: reason must say why it is not shadowed: %q", phase, o, reason)
			}
			// rows that do not verify are still corrupt data, and a fault
			if got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: phase, Outcome: o}); got != ClassFault {
				t.Errorf("%s %s unverified = %s, want FAULT", phase, o, got)
			}
		}
	}
}

// After a chain upgrade past the pinned major this build may assign rows the
// chain does not. Every verdict that rests on assignment is then the
// observer's problem, and must not become the whole set's fault at once.
func TestClassify_StalePinIsAnObserverGapNotAFault(t *testing.T) {
	for _, o := range []Outcome{OutcomeNotFound, OutcomeWrongRows, OutcomePartial, OutcomeInvalidRows, OutcomeServedOK} {
		got, reason := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: o, PinStale: true})
		if got != ClassProbeError {
			t.Errorf("%s with a stale pin = %s (%q), want PROBE_ERROR", o, got, reason)
		}
	}
	// identity and registry do not depend on assignment and are still judged
	if got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: OutcomeIdentityFail, PinStale: true}); got != ClassIdentityMismatch {
		t.Errorf("identity failure with a stale pin = %s, want IDENTITY_MISMATCH", got)
	}
	if got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: OutcomeNoHost, PinStale: true}); got != ClassNotRegistered {
		t.Errorf("no host with a stale pin = %s, want NOT_REGISTERED", got)
	}
}

// Genuine rows that no known promise assigns, while a scan gap overlaps the
// lifetime a shard over the commitment could have: the observer knows it did
// not look, so it says so instead of accusing.
func TestClassify_ScanGapMakesAnUnmatchedShadowAGapNotAFault(t *testing.T) {
	for _, phase := range []Phase{PhaseInWindow, PhaseGrace} {
		for _, o := range []Outcome{OutcomeWrongRows, OutcomePartial} {
			got, reason := Classify(Evidence{Assigned: true, Attested: true, Phase: phase, Outcome: o, CommitmentVerified: true, ShadowUncertain: true})
			if got != ClassProbeError {
				t.Errorf("%s %s verified, unmatched, gap = %s (%q), want PROBE_ERROR", phase, o, got, reason)
			}
		}
	}
	// a matched shadow wins over the gap
	if got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: OutcomePartial, CommitmentVerified: true, Shadowed: true, ShadowUncertain: true}); got != ClassShadowedShard {
		t.Errorf("matched shadow with a gap = %s, want SHADOWED_SHARD", got)
	}
	// unverifiable rows are corrupt data whatever the scanner missed
	if got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: OutcomeWrongRows, ShadowUncertain: true}); got != ClassFault {
		t.Errorf("unverifiable rows with a gap = %s, want FAULT", got)
	}
}

func TestShadowGapFor(t *testing.T) {
	at := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	life := 4*time.Hour + 150*time.Second
	tm := func(h, m int) *time.Time { t := time.Date(2026, 9, 18, h, m, 0, 0, time.UTC); return &t }
	gaps := []scan.ScanGap{
		{From: 10, To: 12, FromTime: tm(5, 0), ToTime: tm(5, 1), At: at},   // long before: no shard could survive
		{From: 20, To: 21, FromTime: tm(9, 30), ToTime: tm(9, 31), At: at}, // inside the lifetime
	}
	if got := shadowGapFor(gaps, at, life); got == "" || !strings.Contains(got, "#20-#21") {
		t.Errorf("gap inside the lifetime not named: %q", got)
	}
	if got := shadowGapFor(gaps[:1], at, life); got != "" {
		t.Errorf("gap outside the lifetime named: %q", got)
	}
	// no block time: the scanner's clock stands in, conservatively
	noTime := []scan.ScanGap{{From: 30, To: 30, At: at.Add(-time.Hour)}}
	if got := shadowGapFor(noTime, at, life); got == "" {
		t.Error("a gap with only a scanner timestamp inside the lifetime must count")
	}
	if got := shadowGapFor(noTime, at.Add(6*time.Hour), life); got != "" {
		t.Errorf("a gap with only a scanner timestamp outside the lifetime named: %q", got)
	}
	if got := shadowGapFor(gaps, at, 0); got != "" {
		t.Errorf("zero lifetime named a gap: %q", got)
	}
}

// The scanner's frontier is an open-ended gap: a promise that settled after
// it is not in the feed. A candidate must settle within the payment-promise
// timeout of a creation that preceded this publication's settlement, so the
// set is complete once the scanner has read past settlement + timeout, and
// incomplete before that, however small the lag.
func TestShadowPending(t *testing.T) {
	settled := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	now := settled.Add(20 * time.Minute)
	pub := scan.Publication{SettlementTime: settled}
	pub.ParamsAtPublication.PaymentPromiseTimeoutSeconds = 3600
	mark := scannedMark{height: 100, timedFor: 100, at: now}
	// always pending at the probe, and the bound is the probe time plus the
	// timeout, not the settlement: the store serves by promise-hash order
	got := shadowPending(now, pub, mark)
	if !strings.Contains(got, "shadow_pending") || !strings.Contains(got, now.Add(time.Hour).Format(time.RFC3339)) || !strings.Contains(got, "#100") {
		t.Errorf("got %q", got)
	}
	if got := shadowPending(now, pub, scannedMark{}); !strings.Contains(got, "frontier unknown") {
		t.Errorf("unknown frontier: %q", got)
	}
	if got := shadowPending(now, scan.Publication{SettlementTime: settled}, mark); !strings.Contains(got, "not on record") {
		t.Errorf("no timeout on record: %q", got)
	}
}

func TestShadowedBy(t *testing.T) {
	cands := []ShadowCandidate{{PromiseHash: "a", Rows: []int{1, 2, 3}}, {PromiseHash: "b", Rows: []int{4, 5}}}
	if got := shadowedBy([]uint32{3, 1, 2}, cands); got != "a" {
		t.Errorf("exact set in another order = %q, want a", got)
	}
	if got := shadowedBy([]uint32{1, 2}, cands); got != "" {
		t.Errorf("strict subset = %q, want none: a subset is an incomplete delivery, not another promise's shard", got)
	}
	if got := shadowedBy([]uint32{4, 5}, cands); got != "b" {
		t.Errorf("second candidate = %q, want b", got)
	}
	if got := shadowedBy(nil, cands); got != "" {
		t.Errorf("no rows = %q, want none", got)
	}
}

// A certificate that is signed by the right key but has lapsed is a missed
// renewal. Publishing it in the same column as "someone else is answering on
// this endpoint" would put those two in the same sentence.
func TestClassify_StaleIdentityIsNotImpersonation(t *testing.T) {
	stale, reason := Classify(Evidence{
		Assigned: true, Attested: true, Phase: PhaseInWindow,
		Outcome: OutcomeIdentityFail, IdentityStale: true,
	})
	if stale != ClassIdentityExpired {
		t.Fatalf("lapsed identity = %s (%q), want %s", stale, reason, ClassIdentityExpired)
	}
	if stale.CountsAgainst() || stale.Rated() {
		t.Errorf("lapsed identity reached the serve rate as %s", stale)
	}
	// A certificate signed by the wrong key is an unusable endpoint, not a
	// shard the validator failed to serve: its own class, outside the rate.
	wrong, _ := Classify(Evidence{
		Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: OutcomeIdentityFail,
	})
	if wrong != ClassIdentityMismatch || wrong.Rated() {
		t.Errorf("a wrong consensus key = %s, want %s outside the rate", wrong, ClassIdentityMismatch)
	}
}

// A validator with no Fibre host in x/valaddr cannot be reached by anyone,
// but jailing and unbonding remove it from the bonded provider list while the
// chain keeps its registration. That is a registry state, not a refusal.
func TestClassify_NoRegisteredHostIsNotAFault(t *testing.T) {
	for _, phase := range []Phase{PhaseInWindow, PhaseGrace, PhasePost} {
		got, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: phase, Outcome: OutcomeNoHost})
		if got != ClassNotRegistered {
			t.Errorf("%s NO_REGISTERED_HOST = %s, want %s", phase, got, ClassNotRegistered)
		}
		if got.CountsAgainst() || got.Rated() {
			t.Errorf("%s NO_REGISTERED_HOST reached the serve rate as %s", phase, got)
		}
	}
}

// An assigned validator whose signature is not in the settled promise is never
// accused: nothing proves it was ever sent the shard. It is never credited
// either, so the serve rate cannot be gamed by publishing blobs nobody signed.
func TestClassify_UnattestedIsNeverAFault(t *testing.T) {
	outcomes := []Outcome{
		OutcomeNotFound, OutcomeTCPRefused, OutcomeTCPTimeout, OutcomeTLSFail,
		OutcomeRPCUnavailable, OutcomeRPCError, OutcomeWrongRows,
		OutcomeInvalidRows, OutcomePartial, OutcomeServedOK,
	}
	for _, phase := range []Phase{PhaseInWindow, PhaseGrace, PhasePost} {
		for _, o := range outcomes {
			got, reason := Classify(Evidence{Assigned: true, Phase: phase, Outcome: o})
			if got != ClassUnattested {
				t.Errorf("Classify(assigned, UNattested, %s, %s) = %s, want %s", phase, o, got, ClassUnattested)
			}
			if reason == "" {
				t.Errorf("Classify(assigned, UNattested, %s, %s): empty reason", phase, o)
			}
		}
	}
	// serving anyway is reported as such: it proves the validator does hold it
	_, reason := Classify(Evidence{Assigned: true, Phase: PhaseInWindow, Outcome: OutcomeServedOK})
	if !strings.Contains(reason, "served") {
		t.Errorf("a served shard from an unattested validator should say so: %q", reason)
	}

	// identity is a property of the endpoint, not of one shard, so it is
	// judged the same with or without attestation for this blob
	if got, _ := Classify(Evidence{Assigned: true, Phase: PhaseInWindow, Outcome: OutcomeIdentityFail}); got != ClassIdentityMismatch {
		t.Errorf("unattested IDENTITY_FAIL = %s, want %s", got, ClassIdentityMismatch)
	}
	// observer-side classes are unchanged by attestation
	for _, o := range []Outcome{OutcomeProbeError, OutcomeMissed, OutcomeRPCDeadline, OutcomeReachable} {
		a, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: o})
		b, _ := Classify(Evidence{Assigned: true, Phase: PhaseInWindow, Outcome: o})
		if a != b {
			t.Errorf("attestation changed an observer-side class for %s: %s vs %s", o, a, b)
		}
	}
	// an UNassigned validator is judged as before, attested or not
	for _, att := range []bool{true, false} {
		if got, _ := Classify(Evidence{Attested: att, Phase: PhaseInWindow, Outcome: OutcomeNotFound}); got != ClassExpectedUnassigned {
			t.Errorf("unassigned attested=%v NOT_FOUND = %s", att, got)
		}
	}
}

func TestClassifyDialError(t *testing.T) {
	cases := map[string]Outcome{
		"dial tcp 127.0.0.1:7980: connectex: No connection could be made because the target machine actively refused it.": OutcomeTCPRefused,
		"dial tcp 10.0.0.1:7980: i/o timeout": OutcomeTCPTimeout,
		// a name that does not resolve is a DNS failure, not an unreachable network
		"dial tcp: lookup nonexistent.invalid: no such host": OutcomeDNSFail,
	}
	for in, want := range cases {
		if got := classifyDialError(errStr(in)); got != want {
			t.Errorf("classifyDialError(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestMeasurementStore_DedupeResume(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Second)
	m := Measurement{
		SchemaVersion: MeasurementSchemaVersion, Vantage: "v1",
		PromiseHash: "ph1", ValidatorAddress: "aa", ScheduledAt: at,
		Outcome: OutcomeServedOK, Classification: ClassHealthy,
	}
	if err := s.Append(m); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(m); err != nil { // dupe
		t.Fatal(err)
	}
	s.Close()

	s2, err := OpenMeasurementStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if !s2.Has("v1", "ph1", "aa", at) {
		t.Fatal("dedupe key not reloaded")
	}
	if !s2.HandledPoint("v1", "ph1", at) {
		t.Fatal("point key not reloaded")
	}
	if s2.Has("v1", "ph1", "bb", at) {
		t.Fatal("unexpected dedupe hit for other validator")
	}
	ms, err := LoadMeasurements(s2.Path())
	if err != nil || len(ms) != 1 {
		t.Fatalf("LoadMeasurements: n=%d err=%v", len(ms), err)
	}
}

type errStr string

func (e errStr) Error() string { return string(e) }

func TestClassifyDownloadError_StatusCodes(t *testing.T) {
	cases := []struct {
		err  error
		want Outcome
	}{
		{status.Error(codes.NotFound, "no blob shard found"), OutcomeNotFound},
		{status.Error(codes.Unavailable, "connection error"), OutcomeRPCUnavailable},
		{status.Error(codes.DeadlineExceeded, "context deadline exceeded"), OutcomeRPCDeadline},
		{status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000000 vs. 4194304)"), OutcomeProbeError},
		{status.Error(codes.ResourceExhausted, "grpc: received message after decompression larger than max 4194304"), OutcomeProbeError},
		// the server's own send bound refusing to deliver a shard it holds
		// is the validator's, reached and answered: never our gap, never a
		// throttle
		{status.Error(codes.ResourceExhausted, "grpc: trying to send message larger than max (5000000 vs. 4194304)"), OutcomeServerError},
		{status.Error(codes.InvalidArgument, "bad blob id"), OutcomeProbeError},
		// a server that no longer serves the unary read (a streaming-only
		// build) is the observer's client being behind, never a verdict
		{status.Error(codes.Unimplemented, "unknown method DownloadShard"), OutcomeProbeError},
		// the endpoint was reached, proved its identity and answered; calling
		// that "unreachable" would be false about a server we just talked to
		{status.Error(codes.Internal, "store: i/o error"), OutcomeServerError},
		{status.Error(codes.Unknown, "boom"), OutcomeServerError},
		{status.Error(codes.Unavailable, "connection error: desc = \"transport: authentication handshake failed: fibre tls identity [signature_invalid]: bad\""), OutcomeIdentityFail},
		{context.DeadlineExceeded, OutcomeRPCDeadline},
		{errors.New("dial tcp: connection refused"), OutcomeRPCUnavailable},
	}
	for _, c := range cases {
		if got := classifyDownloadError(c.err); got != c.want {
			t.Errorf("classifyDownloadError(%v) = %s, want %s", c.err, got, c.want)
		}
	}
}

func TestOrderAddrsAndDownloadDeadline(t *testing.T) {
	got := orderAddrs([]string{"2001:db8::1", "10.0.0.1", "::1", "192.0.2.7"})
	want := []string{"10.0.0.1", "192.0.2.7", "2001:db8::1", "::1"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("orderAddrs = %v, want %v", got, want)
		}
	}
	to := StepTimeouts{}.withDefaults()
	if d := to.downloadDeadline(0); d != 25*time.Second {
		t.Errorf("base deadline = %s", d)
	}
	// 122 MB at 1 MiB/s adds ~116 s
	if d := to.downloadDeadline(122_000_000); d < 25*time.Second+110*time.Second || d > 25*time.Second+120*time.Second {
		t.Errorf("scaled deadline = %s", d)
	}
	if defaultMaxRecvMsgSize <= 4<<20 {
		t.Errorf("defaultMaxRecvMsgSize = %d, must exceed grpc's 4 MiB default", defaultMaxRecvMsgSize)
	}
}

func TestRun_NilCoderIsProbeErrorNotFault(t *testing.T) {
	in := Input{
		Vantage: "t", ChainID: "c", PromiseHash: "aa", MustServeUntil: time.Now().Add(time.Hour),
		Target:        Target{AddressHex: "aa", Host: "192.0.2.1:7980", Assigned: true, RowCount: 148, PubKey: make([]byte, 32)},
		SchedulePoint: SchedulePoint{Label: "w1", At: time.Now()},
	}
	m := Run(context.Background(), in, nil, StepTimeouts{})
	if m.Outcome != OutcomeProbeError {
		t.Fatalf("outcome = %s, want %s", m.Outcome, OutcomeProbeError)
	}
	if m.Classification != ClassProbeError {
		t.Fatalf("classification = %s, want %s", m.Classification, ClassProbeError)
	}
	if m.TCP.Attempted {
		t.Error("a probe with no coder must not open a connection")
	}
	// the same input with the download skipped is a normal reachability probe
	in.SkipDownload = true
	m = Run(context.Background(), in, nil, StepTimeouts{TCP: 10 * time.Millisecond})
	if m.Outcome == OutcomeProbeError {
		t.Errorf("reachability probe must not need a coder: %s (%s)", m.Outcome, m.RawError)
	}
}

func TestRun_StampsClockOffset(t *testing.T) {
	in := Input{
		Vantage: "t", PromiseHash: "aa", MustServeUntil: time.Now().Add(time.Hour),
		Target:        Target{AddressHex: "aa", Host: "", Assigned: true},
		SchedulePoint: SchedulePoint{Label: "w1", At: time.Now()},
		ClockOffsetMS: -4200,
	}
	m := Run(context.Background(), in, nil, StepTimeouts{})
	if m.ClockOffsetMS != -4200 {
		t.Fatalf("ClockOffsetMS = %d, want -4200", m.ClockOffsetMS)
	}
}

// A publication record from before signature verification carries no
// attestation evidence. Such a blob is judged under the older rules, never
// filed as UNATTESTED: "not recorded" is not "did not attest".
func TestClassify_UnknownAttestationUsesOlderRules(t *testing.T) {
	cases := []struct {
		outcome Outcome
		want    Classification
	}{
		{OutcomeServedOK, ClassHealthy},
		{OutcomeNotFound, ClassFault},
		{OutcomeTCPRefused, ClassUnreachable},
	}
	for _, c := range cases {
		got, _ := Classify(Evidence{Assigned: true, Attested: false, AttestationUnknown: true, Phase: PhaseInWindow, Outcome: c.outcome})
		if got != c.want {
			t.Errorf("unknown attestation, %s: got %s, want %s", c.outcome, got, c.want)
		}
		if got == ClassUnattested {
			t.Errorf("unknown attestation, %s: filed as UNATTESTED", c.outcome)
		}
	}
	// and the measurement says so
	m := Measurement{SchemaVersion: MeasurementSchemaVersion, AttestationUnknown: true}
	if m.HasAttestation() {
		t.Errorf("HasAttestation() = true with AttestationUnknown set")
	}
}

// ResourceExhausted is two things: the observer's own receive bound (a probe
// error) and a server-side limit (a throttle, never a fault, never ours).
func TestClassifyDownloadError_ResourceExhausted(t *testing.T) {
	if got := classifyDownloadError(status.Error(codes.ResourceExhausted, "rpc error: grpc: received message larger than max (5000000 vs. 4194304)")); got != OutcomeProbeError {
		t.Errorf("own receive bound: got %s, want %s", got, OutcomeProbeError)
	}
	if got := classifyDownloadError(status.Error(codes.ResourceExhausted, "rate limit exceeded for peer")); got != OutcomeThrottled {
		t.Errorf("server limit: got %s, want %s", got, OutcomeThrottled)
	}
	if cls, _ := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: OutcomeThrottled}); cls.Rated() {
		t.Errorf("THROTTLED is rated: %s", cls)
	}
}

// A NOT_FOUND that arrives inside NotFoundGuard of the deadline is graded in
// grace; one that arrives earlier keeps its in-window phase, and a probe that
// started outside the window is never regraded.
func TestNotFoundPhaseGuard(t *testing.T) {
	msu := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	tol := 150 * time.Second
	if p, re := notFoundPhase(msu.Add(-NotFoundGuard-time.Second), PhaseInWindow, msu, tol, 0); re || p != PhaseInWindow {
		t.Errorf("outside the guard: regraded=%v phase=%s", re, p)
	}
	if p, re := notFoundPhase(msu.Add(-NotFoundGuard+time.Second), PhaseInWindow, msu, tol, 0); !re || p != PhaseGrace {
		t.Errorf("inside the guard: regraded=%v phase=%s, want grace", re, p)
	}
	if p, re := notFoundPhase(msu.Add(time.Hour), PhaseGrace, msu, tol, 0); re || p != PhaseGrace {
		t.Errorf("started in grace: regraded=%v phase=%s", re, p)
	}
	// An observer clock 45 s behind the chain's: a NOT_FOUND it reads as a
	// minute before the deadline may be a server pruning on time, so the
	// band reaches back by the skew.
	if p, re := notFoundPhase(msu.Add(-time.Minute), PhaseInWindow, msu, tol, -45_000); !re || p != PhaseGrace {
		t.Errorf("clock 45 s behind, a minute out: regraded=%v phase=%s, want grace", re, p)
	}
	if p, re := notFoundPhase(msu.Add(-80*time.Second), PhaseInWindow, msu, tol, -45_000); re || p != PhaseInWindow {
		t.Errorf("clock 45 s behind, 80 s out: regraded=%v phase=%s, want in_window", re, p)
	}
	// A clock ahead of the chain's does not widen it.
	if p, re := notFoundPhase(msu.Add(-time.Minute), PhaseInWindow, msu, tol, 45_000); re || p != PhaseInWindow {
		t.Errorf("clock ahead: regraded=%v phase=%s, want in_window", re, p)
	}
}
