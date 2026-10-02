package verdict

import (
	"fmt"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// try is one answer of a validator at a full reading: what came back on
// the wire, and how it was classified when probe.Classify alone does not
// say (a shadowed shard, a deferred verdict).
type try struct {
	out probe.Outcome
	cls probe.Classification
	// stale: an identity failure on a certificate whose window lapsed.
	stale bool
	// foreign: a verified short answer (PARTIAL) whose rows are not the
	// validator's own; without it a short answer is a part of its own.
	foreign bool
	// owed: the answer is the validator's last on record and still owed
	// it another attempt (NextAttemptOwed), which is not on record.
	owed bool
}

// fullReading builds the rows of one full reading of a blob: each
// validator's answers, attempt after attempt 90 s apart, under label.
type fullReading struct {
	label            string
	settled, msu, at time.Time
	rows             []Row
	facts            BlobFacts
	next             int
}

// newFullReading is a reading of a blob needing needed rows, settled after
// probe.FullReadSince, under label.
func newFullReading(label string, needed int) *fullReading {
	settled := probe.FullReadSince.Add(24 * time.Hour)
	return newFullReadingAt(label, needed, settled)
}

func newFullReadingAt(label string, needed int, settled time.Time) *fullReading {
	msu := settled.Add(4 * time.Hour)
	return &fullReading{label: label, settled: settled, msu: msu, at: msu.Add(-10 * time.Minute),
		facts: BlobFacts{Needed: needed, Endorsed: map[string]int{}}}
}

// reachedServer reports whether a request with this outcome opened its
// connection (or was refused): everything but the answers from before one.
func reachedServer(o probe.Outcome) bool {
	switch o {
	case probe.OutcomeNoHost, probe.OutcomeBadHost, probe.OutcomeDNSFail, probe.OutcomeTCPTimeout, probe.OutcomeTCPUnreachable,
		probe.OutcomeProbeError, probe.OutcomeMissed:
		return false
	}
	return true
}

// add records validator v, endorsed for holds rows, and its answers in
// order. A served answer hands over its rows; a verified short one
// (PARTIAL with a class saying they verified) hands over one of them, or
// one row that is not its own (foreign); a verified WRONG_ROWS hands over
// as many rows as it holds, not its own. Every answer but the last that did
// not serve and was made says the next attempt is owed (NextAttemptOwed),
// as the prober writes it; the last says so only when the try is owed.
func (fr *fullReading) add(v string, holds int, tries ...try) {
	own := span(fr.next, holds)
	fr.next += holds
	fr.facts.Endorsed[v] = holds
	for k, tr := range tries {
		verified := tr.out == probe.OutcomeServedOK || (tr.out == probe.OutcomePartial || tr.out == probe.OutcomeWrongRows) &&
			tr.cls != "" && tr.cls != probe.ClassFault
		cls := tr.cls
		if cls == "" {
			cls, _ = probe.Classify(probe.Evidence{Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Outcome: tr.out,
				CommitmentVerified: verified, IdentityStale: tr.stale})
		}
		start := fr.at.Add(time.Duration(len(fr.facts.Endorsed))*time.Second + time.Duration(k)*90*time.Second)
		r := Row{PromiseHash: "p", Validator: v, ScheduleLabel: fr.label, ScheduledAt: fr.at, StartedAt: start, MustServeUntil: fr.msu,
			Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Classification: cls, Outcome: tr.out,
			TCPOK: reachedServer(tr.out), TLSOK: reachedServer(tr.out), AssignedRowCount: holds}
		if verified {
			r.CommitmentVerified = true
			r.RowIndices = own
			switch {
			case tr.out == probe.OutcomePartial && tr.foreign:
				r.RowIndices = []uint32{uint32(1000 + fr.next)}
			case tr.out == probe.OutcomePartial:
				r.RowIndices, r.RowsSubsetOfAssignment = own[:1], true
			case tr.out == probe.OutcomeWrongRows:
				r.RowIndices = span(2000+fr.next, holds)
			}
			r.RowsReturned = len(r.RowIndices)
		}
		last := k == len(tries)-1
		made := cls != probe.ClassNotProbed && !probe.FullServed(r.CommitmentVerified, r.Outcome, r.Classification)
		r.NextAttemptOwed = made && (!last || tr.owed)
		fr.rows = append(fr.rows, r)
	}
}

// counted is every row's counted class, by validator, in attempt order.
func (fr *fullReading) counted() map[string][]probe.Classification {
	rd := ReadingOf(fr.rows, fr.facts)
	out := map[string][]probe.Classification{}
	for _, r := range fr.rows {
		out[r.Validator] = append(out[r.Validator], r.CountedClass(rd))
	}
	return out
}

func (fr *fullReading) obligations() map[string]Obligations {
	w := Window{All: true, End: fr.msu.Add(time.Hour)}
	_, by := ComputeObligations(fr.rows, map[string]time.Time{"p": fr.settled}, w, Blobs{"p": fr.facts})
	return by
}

func classes(cs ...probe.Classification) string { return fmt.Sprint(cs) }

// At a full reading every endorser is judged on its own answer, whatever
// the blob came to: on a blob that is Available, served is its own rows,
// verified (or exactly another settled promise's); every failure of the
// validator's own is not served (no such shard, rows that do not verify or
// fewer of its own than it holds, a wrong or lapsed certificate, an
// endpoint that refused, timed out or could not be routed to, no
// registered host, a timeout, a rate limit, a server error or a shard no
// client can use); this observer's own failures are its gap and count
// neither way, and so do genuine rows that are not the validator's own and
// that no settled promise explains (more of them, or fewer). One answer
// each, as the end readings made between the full-reading deploy and the
// attempts have, under both labels.
func TestAFullReadingJudgesEachEndorserOnItsOwn(t *testing.T) {
	cases := []struct {
		v    string
		try  try
		want probe.Classification
	}{
		{"served", try{out: probe.OutcomeServedOK}, probe.ClassHealthy},
		{"notfound", try{out: probe.OutcomeNotFound}, probe.ClassFault},
		{"corrupt", try{out: probe.OutcomeInvalidRows}, probe.ClassFault},
		{"short", try{out: probe.OutcomePartial, cls: probe.ClassUnmatchedGenuine}, probe.ClassFault},
		{"shortunverified", try{out: probe.OutcomePartial}, probe.ClassFault},
		{"shadowed", try{out: probe.OutcomePartial, cls: probe.ClassShadowedShard}, probe.ClassHealthy},
		{"shadowedwhole", try{out: probe.OutcomeWrongRows, cls: probe.ClassShadowedShard}, probe.ClassHealthy},
		{"foreign", try{out: probe.OutcomeWrongRows, cls: probe.ClassUnmatchedGenuine}, NotCounted},
		{"shortforeign", try{out: probe.OutcomePartial, cls: probe.ClassUnmatchedGenuine, foreign: true}, NotCounted},
		{"badcert", try{out: probe.OutcomeIdentityFail}, probe.ClassFault},
		{"lapsed", try{out: probe.OutcomeIdentityFail, stale: true}, probe.ClassFault},
		{"refused", try{out: probe.OutcomeTCPRefused}, probe.ClassFault},
		{"dialtimeout", try{out: probe.OutcomeTCPTimeout}, probe.ClassFault},
		{"noroute", try{out: probe.OutcomeTCPUnreachable}, probe.ClassFault},
		{"nosuchhost", try{out: probe.OutcomeDNSFail}, probe.ClassFault},
		{"tls", try{out: probe.OutcomeTLSFail}, probe.ClassFault},
		{"nohost", try{out: probe.OutcomeNoHost}, probe.ClassFault},
		{"badhost", try{out: probe.OutcomeBadHost}, probe.ClassFault},
		{"rpctimeout", try{out: probe.OutcomeRPCTimeout}, probe.ClassFault},
		{"unavailable", try{out: probe.OutcomeRPCUnavailable}, probe.ClassFault},
		{"throttled", try{out: probe.OutcomeThrottled}, probe.ClassFault},
		{"servererror", try{out: probe.OutcomeServerError}, probe.ClassFault},
		{"malformed", try{out: probe.OutcomeMalformedShard}, probe.ClassFault},
		{"ourside", try{out: probe.OutcomeProbeError}, probe.ClassProbeError},
		{"notmade", try{out: probe.OutcomeMissed}, probe.ClassNotProbed},
		{"deferred", try{out: probe.OutcomePartial, cls: probe.ClassProbeError}, probe.ClassProbeError},
		{"owednotrecorded", try{out: probe.OutcomeNotFound, owed: true}, NotCounted},
	}
	for _, label := range []string{probe.FullReadLabel, probe.EndReadLabel} {
		t.Run(label, func(t *testing.T) {
			fr := newFullReading(label, 4)
			fr.add("whole", 4, try{out: probe.OutcomeServedOK})
			for _, c := range cases {
				fr.add(c.v, 2, c.try)
			}
			if res := BlobReading(fr.rows, fr.facts, false); res.Status != BlobAvailable {
				t.Fatalf("blob %s, want Available: a failure counts whatever the blob came to", res.Status)
			}
			got := fr.counted()
			obl := fr.obligations()
			for _, c := range cases {
				if g := got[c.v]; len(g) != 1 || g[0] != c.want {
					t.Errorf("%s (%s): counted %v, want %s", c.v, c.try.out, g, c.want)
				}
				want := notCounted
				switch c.want {
				case probe.ClassHealthy:
					want = served
				case probe.ClassFault:
					want = notServed
				}
				if obl[c.v] != want {
					t.Errorf("%s (%s): obligation %+v, want %+v", c.v, c.try.out, obl[c.v], want)
				}
			}
		})
	}
}

// A validator whose answer did not serve is asked again, and is judged by
// the best of its answers, the reason being the last: served when a later
// answer served; not served only when every answer failed its own way, by
// the last of them; counted neither way when one of them was this
// observer's gap, among them an attempt that could not start before
// must_serve_until less the margin (NOT_PROBED). Only one answer of each
// counts either way.
func TestRetriesAtAFullReading(t *testing.T) {
	nf, nfp := try{out: probe.OutcomeNotFound}, try{out: probe.OutcomeMissed}
	fr := newFullReading(probe.FullReadLabel, 4)
	fr.add("whole", 4, try{out: probe.OutcomeServedOK})
	fr.add("later", 2, nf, try{out: probe.OutcomeServedOK})
	fr.add("never", 2, nf, nf, nf)
	fr.add("busy", 2, try{out: probe.OutcomeThrottled}, try{out: probe.OutcomeThrottled}, try{out: probe.OutcomeServedOK})
	fr.add("toolate", 2, nf, nfp)
	fr.add("ourdns", 2, try{out: probe.OutcomeTCPTimeout}, try{out: probe.OutcomeProbeError}, try{out: probe.OutcomeTCPTimeout})
	fr.add("mixed", 2, try{out: probe.OutcomeServerError}, try{out: probe.OutcomeRPCTimeout}, nf)
	fr.add("gapfirst", 2, try{out: probe.OutcomeProbeError}, try{out: probe.OutcomeServedOK})
	// The next attempt could not have started in time: none was owed, and
	// the last answer made decides.
	fr.add("endsearly", 2, nf, nf)
	// The next attempt was owed and is not on record (the prober stopped
	// past its cutoff, and never wrote it): counted neither way.
	fr.add("owedgone", 2, nf, try{out: probe.OutcomeNotFound, owed: true})
	fr.add("foreignlater", 2, nf, try{out: probe.OutcomeWrongRows, cls: probe.ClassUnmatchedGenuine})
	nc, h, f := NotCounted, probe.ClassHealthy, probe.ClassFault
	want := map[string]string{
		"whole":        classes(h),
		"later":        classes(nc, h),
		"never":        classes(nc, nc, f),
		"busy":         classes(nc, nc, h),
		"toolate":      classes(nc, probe.ClassNotProbed),
		"ourdns":       classes(nc, probe.ClassProbeError, nc),
		"mixed":        classes(nc, nc, f),
		"gapfirst":     classes(probe.ClassProbeError, h),
		"endsearly":    classes(nc, f),
		"owedgone":     classes(nc, nc),
		"foreignlater": classes(nc, nc),
	}
	got := fr.counted()
	for v, w := range want {
		if g := classes(got[v]...); g != w {
			t.Errorf("%s: counted %s, want %s", v, g, w)
		}
	}
	obl := fr.obligations()
	for v, w := range map[string]Obligations{"whole": served, "later": served, "never": notServed, "busy": served,
		"toolate": notCounted, "ourdns": notCounted, "mixed": notServed, "gapfirst": served,
		"endsearly": notServed, "owedgone": notCounted, "foreignlater": notCounted} {
		if obl[v] != w {
			t.Errorf("%s: obligation %+v, want %+v", v, obl[v], w)
		}
	}
}

// A full reading in which not a single request reached a server, attempts
// included, did not happen: every endorser is this observer's gap, and the
// blob was not read by Tensile. One that reached a server on a later
// attempt makes it a reading, and the validators that then failed their
// own way are not served.
func TestAFullReadingThatReachedNoServerIsAGapForEveryone(t *testing.T) {
	down := try{out: probe.OutcomeTCPTimeout}
	fr := newFullReading(probe.FullReadLabel, 4)
	fr.add("t1", 2, down, down, down)
	fr.add("t2", 2, try{out: probe.OutcomeNoHost}, try{out: probe.OutcomeNoHost}, try{out: probe.OutcomeNoHost})
	fr.add("t3", 2, try{out: probe.OutcomeProbeError})
	fr.add("t4", 2, try{out: probe.OutcomeDNSFail}, try{out: probe.OutcomeTCPUnreachable}, down)
	if res := BlobReading(fr.rows, fr.facts, false); res.Status != BlobNotRead {
		t.Fatalf("blob %s, want not read", res.Status)
	}
	for v, cs := range fr.counted() {
		for _, c := range cs {
			if c != NotCounted && c != probe.ClassProbeError {
				t.Errorf("%s: counted %v, want nothing counted", v, cs)
			}
		}
	}
	for v, o := range fr.obligations() {
		if o != notCounted {
			t.Errorf("%s: %+v, want counted neither way", v, o)
		}
	}

	// The same, but one validator's last attempt was refused: a server was
	// reached, the reading happened.
	fr.add("t5", 2, down, down, try{out: probe.OutcomeTCPRefused})
	obl := fr.obligations()
	for v, w := range map[string]Obligations{"t1": notServed, "t2": notServed, "t3": notCounted, "t4": notServed, "t5": notServed} {
		if obl[v] != w {
			t.Errorf("reached: %s %+v, want %+v", v, obl[v], w)
		}
	}
}

// Readings that were not full keep the rule of their time, with no record
// rewritten: an end reading started before probe.FullReadSince counts a
// failure only on a blob that was Unavailable (there, the earlier order
// makes even this observer's PROBE_ERROR not served), and an earlier
// schedule's point is the same; the end label from FullReadSince on is a
// full reading.
func TestTheEarlierRuleIsUnchangedForEarlierReadings(t *testing.T) {
	before := probe.FullReadSince.Add(-48 * time.Hour)
	build := func(label string, settled time.Time, available bool) *fullReading {
		fr := newFullReadingAt(label, 4, settled)
		if available {
			fr.add("whole", 4, try{out: probe.OutcomeServedOK})
		} else {
			fr.add("part", 4, try{out: probe.OutcomePartial, cls: probe.ClassUnmatchedGenuine})
		}
		fr.add("gone", 2, try{out: probe.OutcomeNotFound})
		fr.add("slow", 2, try{out: probe.OutcomeRPCTimeout})
		fr.add("ours", 2, try{out: probe.OutcomeProbeError})
		return fr
	}
	for _, c := range []struct {
		name      string
		label     string
		settled   time.Time
		available bool
		want      map[string]probe.Classification
	}{
		{"end, available, before", probe.EndReadLabel, before, true,
			map[string]probe.Classification{"whole": probe.ClassHealthy, "gone": NotCounted, "slow": NotCounted, "ours": probe.ClassProbeError}},
		{"end, unavailable, before", probe.EndReadLabel, before, false,
			map[string]probe.Classification{"part": probe.ClassHealthy, "gone": probe.ClassFault, "slow": probe.ClassFault, "ours": probe.ClassFault}},
		{"w4, available", "w4", probe.FullReadSince.Add(24 * time.Hour), true,
			map[string]probe.Classification{"whole": probe.ClassHealthy, "gone": NotCounted, "slow": NotCounted, "ours": probe.ClassProbeError}},
		{"end, available, after", probe.EndReadLabel, probe.FullReadSince.Add(-4*time.Hour + 10*time.Minute), true,
			map[string]probe.Classification{"whole": probe.ClassHealthy, "gone": probe.ClassFault, "slow": probe.ClassFault, "ours": probe.ClassProbeError}},
		{"enough, available, after", probe.EnoughReadLabel, probe.FullReadSince.Add(24 * time.Hour), true,
			map[string]probe.Classification{"whole": probe.ClassHealthy, "gone": NotCounted, "slow": NotCounted, "ours": probe.ClassProbeError}},
		{"enough, unavailable, after", probe.EnoughReadLabel, probe.FullReadSince.Add(24 * time.Hour), false,
			map[string]probe.Classification{"part": probe.ClassHealthy, "gone": probe.ClassFault, "slow": probe.ClassFault, "ours": probe.ClassFault}},
		{"full, unavailable", probe.FullReadLabel, before, false,
			map[string]probe.Classification{"part": probe.ClassFault, "gone": probe.ClassFault, "slow": probe.ClassFault, "ours": probe.ClassProbeError}},
	} {
		t.Run(c.name, func(t *testing.T) {
			fr := build(c.label, c.settled, c.available)
			got := fr.counted()
			for v, w := range c.want {
				if g := got[v]; len(g) != 1 || g[0] != w {
					t.Errorf("%s: %v, want %s", v, g, w)
				}
			}
		})
	}
}

// A reading that was not full keeps its rule for a missed request: any
// NOT_PROBED row of an assigned validator in the window leaves a short blob
// not read, even beside an answer of the same validator at the point (from
// another vantage), so no one is not served on it.
func TestAMissedRequestOfAnEarlierReadingStillLeavesItNotRead(t *testing.T) {
	fr := newFullReadingAt(probe.EndReadLabel, 4, probe.FullReadSince.Add(-48*time.Hour))
	fr.add("some", 2, try{out: probe.OutcomeServedOK})
	fr.add("gone", 2, try{out: probe.OutcomeNotFound})
	missed := fr.rows[len(fr.rows)-1]
	missed.Outcome, missed.Classification, missed.TCPOK, missed.TLSOK = probe.OutcomeMissed, probe.ClassNotProbed, false, false
	missed.StartedAt = missed.StartedAt.Add(time.Second)
	fr.rows = append(fr.rows, missed)
	if res := BlobReading(fr.rows, fr.facts, false); res.Status != BlobNotRead || !res.Missed {
		t.Fatalf("blob %s (missed %v), want not read", res.Status, res.Missed)
	}
	if got := fr.counted()["gone"]; got[0] != NotCounted {
		t.Fatalf("gone: %v, want counted neither way on a blob not read", got)
	}
}

// A later attempt that could not be made leaves its validator asked all the
// same: the blob its reading left Unavailable stays Unavailable (the
// earlier rule's "a missed request leaves the blob not read" is about a
// validator not asked at all, which still holds).
func TestAnAttemptNotMadeIsNotAMissedReading(t *testing.T) {
	fr := newFullReading(probe.FullReadLabel, 4)
	fr.add("some", 2, try{out: probe.OutcomeServedOK})
	fr.add("gone", 4, try{out: probe.OutcomeNotFound}, try{out: probe.OutcomeMissed})
	if res := BlobReading(fr.rows, fr.facts, false); res.Status != BlobUnavailable || res.Missed {
		t.Fatalf("blob %s (missed %v), want Unavailable", res.Status, res.Missed)
	}
	fr.add("never", 2, try{out: probe.OutcomeMissed})
	if res := BlobReading(fr.rows, fr.facts, false); res.Status != BlobNotRead || !res.Missed {
		t.Fatalf("blob %s (missed %v), want not read: a validator was not asked at all", res.Status, res.Missed)
	}
}
