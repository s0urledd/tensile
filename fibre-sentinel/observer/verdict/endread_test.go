package verdict

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// reading builds the rows of one blob's reading: every validator is asked
// at the end-of-window point, with the rows it returned.
type reading struct {
	settled, msu, at time.Time
	rows             []Row
}

func newReading() *reading {
	settled := time.Date(2026, 9, 27, 17, 0, 0, 0, time.UTC)
	msu := settled.Add(4 * time.Hour)
	return &reading{settled: settled, msu: msu, at: msu.Add(-10 * time.Minute)}
}

// add records validator v's answer: its class, and when rows came back
// verified, their indices. holds is the rows it is endorsed for.
func (rd *reading) add(v string, cls probe.Classification, holds int, rows ...uint32) {
	out := probe.OutcomeServedOK
	switch cls {
	case probe.ClassFault:
		out = probe.OutcomeNotFound
	case probe.ClassUnreachable:
		out = probe.OutcomeTLSFail
	case probe.ClassServerError:
		out = probe.OutcomeServerError
	case probe.ClassIdentityMismatch:
		out = probe.OutcomeIdentityFail
	case probe.ClassThrottled:
		out = probe.OutcomeThrottled
	case probe.ClassProbeError:
		out = probe.OutcomeProbeError
	case probe.ClassNotProbed:
		out = probe.OutcomeMissed
	case probe.ClassUnmatchedGenuine:
		out = probe.OutcomePartial
	}
	rd.rows = append(rd.rows, Row{PromiseHash: "p", Validator: v, ScheduleLabel: probe.EndReadLabel, ScheduledAt: rd.at, StartedAt: rd.at,
		MustServeUntil: rd.msu, Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Classification: cls, Outcome: out,
		TLSOK:      cls != probe.ClassUnreachable && cls != probe.ClassNotProbed && cls != probe.ClassProbeError,
		RowIndices: rows, RowsReturned: len(rows), CommitmentVerified: len(rows) > 0, AssignedRowCount: holds})
}

func span(from, n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(from + i)
	}
	return out
}

func (rd *reading) obligations(facts BlobFacts) (Obligations, map[string]Obligations) {
	w := Window{All: true, End: rd.msu.Add(time.Hour)}
	blobs := Blobs{"p": facts}
	sus := SuspectPoints(rd.rows, w, blobs)
	return ComputeObligations(rd.rows, map[string]time.Time{"p": rd.settled}, w, sus, blobs)
}

var (
	served     = Obligations{Total: 1, Served: 1}
	notServed  = Obligations{Total: 1, Broken: 1}
	notCounted = Obligations{Total: 1, NotCounted: 1}
)

// The rule the owner set: a validator that timed out on a blob the other
// validators' rows rebuilt all the same did nothing a reader noticed, and
// this observer's own path is as likely the cause. It counts neither way; a
// validator the reading never got to has no obligation here at all.
func TestAnAvailableBlobDoesNotCountAValidatorThatTimedOut(t *testing.T) {
	rd := newReading()
	rd.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	rd.add("b", probe.ClassHealthy, 4, span(4, 4)...)
	rd.add("slow", probe.ClassUnreachable, 4)
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"a": 4, "b": 4, "slow": 4, "never": 4}}
	net, by := rd.obligations(facts)
	for v, want := range map[string]Obligations{"a": served, "b": served, "slow": notCounted} {
		if got := by[v]; got != want {
			t.Errorf("%s: %+v, want %+v", v, got, want)
		}
	}
	if _, ok := by["never"]; ok {
		t.Error("a validator the reading never asked has an obligation")
	}
	if net.Broken != 0 {
		t.Errorf("broken = %d on an Available blob", net.Broken)
	}
	if res := BlobReading(rd.rows, facts, false, false); res.Status != BlobAvailable || res.Have != 8 || res.Asked != 3 || res.Served != 2 {
		t.Errorf("reading = %+v, want Available with 8 rows, 3 asked, 2 served", res)
	}
}

// An Unavailable blob: the validators whose rows did not come back are not
// served, whatever the reason the reader got; the ones whose rows came back
// are served. A validator this observer could not read (its own gap) counts
// neither way, and its rows are counted as if they might have come back.
func TestAnUnavailableBlobCountsTheValidatorsWhoseRowsDidNotComeBack(t *testing.T) {
	rd := newReading()
	rd.add("big", probe.ClassFault, 8)
	for i, v := range []string{"s1", "s2", "s3", "s4", "s5"} {
		rd.add(v, probe.ClassHealthy, 1, uint32(8+i))
	}
	rd.add("gap", probe.ClassProbeError, 2)
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"big": 8, "s1": 1, "s2": 1, "s3": 1, "s4": 1, "s5": 1, "gap": 2}}
	_, by := rd.obligations(facts)
	want := map[string]Obligations{"big": notServed, "s1": served, "s5": served, "gap": notCounted}
	for v, w := range want {
		if got := by[v]; got != w {
			t.Errorf("%s: %+v, want %+v", v, got, w)
		}
	}
	res := BlobReading(rd.rows, facts, false, false)
	if res.Status != BlobUnavailable || res.Have != 5 || res.Potential != 2 {
		t.Errorf("reading = %+v, want Unavailable, 5 rows, 2 that might have come back", res)
	}

	// Had the validator this observer failed to read held three rows, they
	// could have made the blob readable: nobody is counted, and the blob
	// was not read by Tensile.
	facts.Endorsed["gap"] = 3
	_, by = rd.obligations(facts)
	if got := by["big"]; got != notCounted {
		t.Errorf("big with a gap that could have filled the blob: %+v, want not counted", got)
	}
	if res := BlobReading(rd.rows, facts, false, false); res.Status != BlobNotRead {
		t.Errorf("status %s, want not_read", res.Status)
	}
}

// A rate limit may be this observer's own request rate: it is no answer of
// the validator's, counted neither way, and its rows count as ones that
// might have come back.
func TestARateLimitIsNotAnAnswer(t *testing.T) {
	rd := newReading()
	rd.add("big", probe.ClassFault, 4)
	rd.add("limited", probe.ClassThrottled, 4)
	rd.add("s1", probe.ClassHealthy, 2, 8, 9)
	rd.add("s2", probe.ClassHealthy, 2, 10, 11)
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"big": 4, "limited": 4, "s1": 2, "s2": 2}}
	_, by := rd.obligations(facts)
	if by["big"] != notCounted || by["limited"] != notCounted {
		t.Errorf("big %+v limited %+v: the throttled rows could have filled the blob, so nothing counts", by["big"], by["limited"])
	}
}

// On an Unavailable blob, served means the rows the validator holds came
// back, not a token of them: genuine rows fewer than it holds are not
// served. On an Available blob the same answer is served.
func TestAShortShardIsNotServedOnAnUnavailableBlob(t *testing.T) {
	rd := newReading()
	rd.add("big", probe.ClassFault, 8)
	rd.add("short", probe.ClassUnmatchedGenuine, 4, 8)
	rd.add("s1", probe.ClassHealthy, 1, 9)
	rd.add("s2", probe.ClassHealthy, 1, 10)
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"big": 8, "short": 4, "s1": 1, "s2": 1}}
	_, by := rd.obligations(facts)
	if by["short"] != notServed || by["big"] != notServed || by["s1"] != served {
		t.Errorf("short %+v big %+v s1 %+v", by["short"], by["big"], by["s1"])
	}

	whole := newReading()
	whole.add("short", probe.ClassUnmatchedGenuine, 4, 8)
	whole.add("a", probe.ClassHealthy, 8, span(0, 8)...)
	_, by = whole.obligations(BlobFacts{Needed: 8, Endorsed: map[string]int{"short": 4, "a": 8}})
	if by["short"] != served {
		t.Errorf("short on an Available blob: %+v, want served", by["short"])
	}
}

// Every validator failing at once is this observer's trouble as likely as
// theirs: the reading is set aside and nothing counts, either way.
func TestEveryValidatorFailingAtOnceCountsNeitherWay(t *testing.T) {
	rd := newReading()
	for _, v := range []string{"a", "b", "c", "d"} {
		rd.add(v, probe.ClassUnreachable, 2)
	}
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"a": 2, "b": 2, "c": 2, "d": 2}}
	w := Window{All: true, End: rd.msu.Add(time.Hour)}
	sus := SuspectPoints(rd.rows, w, Blobs{"p": facts})
	if len(sus) != 1 {
		t.Fatalf("suspect points %+v, want the one reading", sus)
	}
	net, _ := rd.obligations(facts)
	if net.Total != 0 || net.Broken != 0 {
		t.Errorf("obligations %+v, want none: the reading is set aside", net)
	}
	if res := BlobReading(rd.rows, facts, true, false); res.Status != BlobNotRead {
		t.Errorf("status %s, want not_read", res.Status)
	}
	// The same failures, but every validator failed with a different answer
	// that leaves the reader without rows: the guard counts them together.
	mixed := newReading()
	mixed.add("a", probe.ClassUnreachable, 2)
	mixed.add("b", probe.ClassServerError, 2)
	mixed.add("c", probe.ClassIdentityMismatch, 2)
	mixed.add("d", probe.ClassFault, 2)
	if sus := SuspectPoints(mixed.rows, w, Blobs{"p": facts}); len(sus) != 1 || sus[0].Faulted != 4 {
		t.Errorf("mixed failures: %+v, want one point with 4 failed", sus)
	}
}

// A reading the blob came through whole is not the observer's trouble,
// however many of the validators it asked failed beside it: the guard does
// not set it aside, so the ones that served are credited.
func TestTheGuardLeavesAnAvailableBlobAlone(t *testing.T) {
	rd := newReading()
	rd.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	rd.add("b", probe.ClassHealthy, 4, span(4, 4)...)
	for _, v := range []string{"x", "y", "z"} {
		rd.add(v, probe.ClassUnreachable, 4)
	}
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"a": 4, "b": 4, "x": 4, "y": 4, "z": 4}}
	w := Window{All: true, End: rd.msu.Add(time.Hour)}
	if sus := SuspectPoints(rd.rows, w, Blobs{"p": facts}); len(sus) != 0 {
		t.Fatalf("an Available reading was set aside: %+v", sus)
	}
	_, by := rd.obligations(facts)
	if by["a"] != served || by["x"] != notCounted {
		t.Errorf("a %+v x %+v", by["a"], by["x"])
	}
}

// BlobReading, case by case.
func TestBlobReadingStatuses(t *testing.T) {
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"a": 4, "b": 4, "c": 4}}
	avail := newReading()
	avail.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	avail.add("b", probe.ClassHealthy, 4, span(4, 4)...)
	unavail := newReading()
	unavail.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	unavail.add("b", probe.ClassFault, 4)
	unavail.add("c", probe.ClassUnreachable, 4)
	incomplete := newReading()
	incomplete.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	incomplete.add("b", probe.ClassFault, 4)
	for _, c := range []struct {
		name          string
		rows          []Row
		suspect, open bool
		want          string
	}{
		{"available", avail.rows, false, false, BlobAvailable},
		{"available, suspect all the same", avail.rows, true, false, BlobAvailable},
		{"available, window open", avail.rows, false, true, BlobAvailable},
		{"unavailable", unavail.rows, false, false, BlobUnavailable},
		{"unavailable, window still open", unavail.rows, false, true, BlobUnavailable},
		{"unavailable at a suspect reading", unavail.rows, true, false, BlobNotRead},
		{"incomplete, window over", incomplete.rows, false, false, BlobNotRead},
		{"incomplete, window open", incomplete.rows, false, true, BlobPending},
		{"no reading, window open", nil, false, true, BlobPending},
		{"no reading, window over", nil, false, false, BlobNotRead},
	} {
		if got := BlobReading(c.rows, facts, c.suspect, c.open); got.Status != c.want {
			t.Errorf("%s: %s, want %s", c.name, got.Status, c.want)
		}
	}
	// Needed unknown: nothing can be said.
	if got := BlobReading(avail.rows, BlobFacts{}, false, false); got.Status != BlobNotRead {
		t.Errorf("unknown needed rows: %s", got.Status)
	}
}

// The bounds decide nearly every reading without the row lists. Wherever
// they decide, they must agree with the exact count, over overlapping
// assignments of every shape.
func TestTheBoundsAgreeWithTheExactCount(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for iter := 0; iter < 2000; iter++ {
		const total = 64
		needed := 8 + rng.IntN(24)
		nv := 2 + rng.IntN(10)
		// an assignment that wraps, as the protocol's does
		assigned := map[string][]uint32{}
		endorsed := map[string]int{}
		off, sigma, distinct := 0, 0, map[uint32]bool{}
		for i := 0; i < nv; i++ {
			v := string(rune('a' + i))
			n := 1 + rng.IntN(12)
			for j := 0; j < n; j++ {
				r := uint32((off + j) % total)
				assigned[v] = append(assigned[v], r)
				distinct[r] = true
			}
			off += n
			sigma += n
			endorsed[v] = n
		}
		facts := BlobFacts{Needed: needed, Excess: sigma - len(distinct), Endorsed: endorsed}
		rd := newReading()
		for v, rows := range assigned {
			switch rng.IntN(5) {
			case 0:
				rd.add(v, probe.ClassFault, len(rows))
			case 1:
				rd.add(v, probe.ClassProbeError, len(rows))
			case 2: // not asked
			default:
				rd.add(v, probe.ClassHealthy, len(rows), rows...)
			}
		}
		got := ReadingOf(rd.rows, facts)
		if got.Lower > got.Have || got.Have > got.Upper {
			t.Fatalf("bounds %d <= %d <= %d do not hold", got.Lower, got.Have, got.Upper)
		}
		if want := got.Have+got.Potential < needed; got.Unavailable() != want {
			t.Fatalf("unavailable %v, the exact count says %v: %+v", got.Unavailable(), want, got)
		}
		if want := got.Have >= needed; got.Available() != want {
			t.Fatalf("available %v, the exact count says %v: %+v", got.Available(), want, got)
		}
	}
}

// A held deadline withholds a reading's verdict in both directions, as it
// does every other reading's: the no-rows FAULT and the genuine-rows
// HEALTHY alike, whatever the blob's reading came to.
func TestEndReadingHeldDeadline(t *testing.T) {
	for _, c := range []probe.Classification{probe.ClassUnreachable, probe.ClassShadowedShard, probe.ClassFault, probe.ClassHealthy} {
		r := Row{ScheduleLabel: probe.EndReadLabel, Classification: c, Outcome: probe.OutcomeTCPTimeout, RetentionUnverified: true}
		for _, unavailable := range []bool{false, true} {
			if got := r.CountedClass(unavailable); got != probe.ClassRetentionUnverified {
				t.Errorf("held %s (unavailable %v): %s, want RETENTION_UNVERIFIED", c, unavailable, got)
			}
		}
	}
}
