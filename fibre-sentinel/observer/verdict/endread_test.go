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

// add records validator v's answer, as the client-shaped reading writes
// it: its class, and when rows came back verified, their indices. holds is
// the rows it is endorsed for.
func (rd *reading) add(v string, cls probe.Classification, holds int, rows ...uint32) {
	out := probe.OutcomeServedOK
	switch cls {
	case probe.ClassFault:
		out = probe.OutcomeNotFound
	case probe.ClassUnreachable:
		out = probe.OutcomeRPCTimeout
	case probe.ClassServerError:
		out = probe.OutcomeServerError
	case probe.ClassIdentityMismatch:
		out = probe.OutcomeIdentityFail
	case probe.ClassThrottled:
		out = probe.OutcomeThrottled
	case probe.ClassNotRegistered:
		out = probe.OutcomeNoHost
	case probe.ClassProbeError:
		out = probe.OutcomeProbeError
	case probe.ClassNotProbed:
		out = probe.OutcomeMissed
	case probe.ClassUnmatchedGenuine:
		out = probe.OutcomePartial
	}
	// the connection was opened unless the request never left (no host, a
	// local failure, a missed reading)
	connected := cls != probe.ClassNotRegistered && cls != probe.ClassNotProbed && cls != probe.ClassProbeError
	rd.rows = append(rd.rows, Row{PromiseHash: "p", Validator: v, ScheduleLabel: probe.EndReadLabel, ScheduledAt: rd.at, StartedAt: rd.at,
		MustServeUntil: rd.msu, Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Classification: cls, Outcome: out,
		TLSOK:      cls != probe.ClassUnreachable && connected,
		TCPOK:      connected,
		RowIndices: rows, RowsReturned: len(rows), CommitmentVerified: len(rows) > 0, AssignedRowCount: holds})
}

// unconnected records validator v's connect timing out: UNREACHABLE, and no
// connection opened.
func (rd *reading) unconnected(v string, holds int) {
	rd.add(v, probe.ClassUnreachable, holds)
	r := &rd.rows[len(rd.rows)-1]
	r.Outcome, r.TCPOK = probe.OutcomeTCPTimeout, false
}

// other records a validator that did not endorse the promise: asked like
// the rest, owing nothing.
func (rd *reading) other(v string, holds int, rows ...uint32) {
	out := probe.OutcomeNotFound
	if len(rows) > 0 {
		out = probe.OutcomeServedOK
	}
	rd.rows = append(rd.rows, Row{PromiseHash: "p", Validator: v, ScheduleLabel: probe.EndReadLabel, ScheduledAt: rd.at, StartedAt: rd.at,
		MustServeUntil: rd.msu, Assigned: true, Phase: probe.PhaseInWindow, Classification: probe.ClassUnattested, Outcome: out,
		TLSOK: true, TCPOK: true, RowIndices: rows, RowsReturned: len(rows), CommitmentVerified: len(rows) > 0, AssignedRowCount: holds})
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
	return ComputeObligations(rd.rows, map[string]time.Time{"p": rd.settled}, w, Blobs{"p": facts})
}

var (
	served     = Obligations{Total: 1, Served: 1}
	notServed  = Obligations{Total: 1, Broken: 1}
	notCounted = Obligations{Total: 1, NotCounted: 1}
)

// The three outcomes, in the client's words: Available; Unavailable with
// "no shards retrieved" when no verified row came back; Unavailable with
// "not enough shards to reconstruct blob" when some did, fewer than
// needed.
func TestTheThreeOutcomes(t *testing.T) {
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"a": 4, "b": 4, "c": 4}}
	avail := newReading()
	avail.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	avail.add("b", probe.ClassFault, 4)
	avail.add("c", probe.ClassHealthy, 4, span(4, 4)...)
	none := newReading()
	none.add("a", probe.ClassFault, 4)
	none.add("b", probe.ClassThrottled, 4)
	none.add("c", probe.ClassUnreachable, 4)
	some := newReading()
	some.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	some.add("b", probe.ClassFault, 4)
	some.add("c", probe.ClassServerError, 4)
	for _, c := range []struct {
		name          string
		rows          []Row
		status, error string
	}{
		{"available", avail.rows, BlobAvailable, ""},
		{"no shards retrieved", none.rows, BlobUnavailable, probe.ClientErrNoShards},
		{"not enough shards", some.rows, BlobUnavailable, probe.ClientErrNotEnoughShards},
	} {
		if got := BlobReading(c.rows, facts, false); got.Status != c.status || got.Error != c.error {
			t.Errorf("%s: %s %q, want %s %q", c.name, got.Status, got.Error, c.status, c.error)
		}
	}
}

// On an Available blob nothing is counted against anyone: a validator
// that failed, for whatever reason, counts neither way, and one the reading
// never got to has no obligation at all. A validator whose rows came back
// is served.
func TestAnAvailableBlobCountsNothingAgainstAnyone(t *testing.T) {
	rd := newReading()
	rd.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	rd.add("b", probe.ClassHealthy, 4, span(4, 4)...)
	rd.add("slow", probe.ClassUnreachable, 4)
	rd.add("gone", probe.ClassFault, 4)
	rd.add("local", probe.ClassProbeError, 4)
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"a": 4, "b": 4, "slow": 4, "gone": 4, "local": 4, "never": 4}}
	net, by := rd.obligations(facts)
	for v, want := range map[string]Obligations{"a": served, "b": served, "slow": notCounted, "gone": notCounted, "local": notCounted} {
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
	if res := BlobReading(rd.rows, facts, false); res.Status != BlobAvailable || res.Have != 8 || res.Asked != 4 || res.Served != 2 {
		t.Errorf("reading = %+v, want Available with 8 rows, 4 asked, 2 served", res)
	}
}

// On an Unavailable blob every endorsing validator whose rows did not come
// back is not served, whatever the client met: not found, a timeout, a
// rate limit, a server error or a CANCELLED it sent, a certificate the
// client rejects, no registered host, a connect that timed out or found no
// route, or a request that failed on this observer's side while others
// reached their validators. Every validator whose rows came back verified
// is served, a short shard included. A validator that did not endorse owes
// nothing and is never counted.
func TestNotServedOnlyOnAnUnavailableBlob(t *testing.T) {
	rd := newReading()
	rd.add("gone", probe.ClassFault, 4)
	rd.add("slow", probe.ClassUnreachable, 4)
	rd.add("limited", probe.ClassThrottled, 4)
	rd.add("errs", probe.ClassServerError, 4)
	rd.add("wrongkey", probe.ClassIdentityMismatch, 4)
	rd.add("nohost", probe.ClassNotRegistered, 4)
	rd.unconnected("noroute", 4)
	rd.add("local", probe.ClassProbeError, 4)
	rd.add("s1", probe.ClassHealthy, 2, 8, 9)
	rd.add("short", probe.ClassUnmatchedGenuine, 4, 10)
	rd.other("other", 4)
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"gone": 4, "slow": 4, "limited": 4, "errs": 4, "wrongkey": 4, "nohost": 4,
		"noroute": 4, "local": 4, "s1": 2, "short": 4}}
	res := BlobReading(rd.rows, facts, false)
	if res.Status != BlobUnavailable || res.Error != probe.ClientErrNotEnoughShards || res.Have != 3 {
		t.Fatalf("reading = %+v, want Unavailable, not enough shards, 3 rows", res)
	}
	net, by := rd.obligations(facts)
	for _, v := range []string{"gone", "slow", "limited", "errs", "wrongkey", "nohost", "noroute", "local"} {
		if by[v] != notServed {
			t.Errorf("%s: %+v, want not served", v, by[v])
		}
	}
	for _, v := range []string{"s1", "short"} {
		if by[v] != served {
			t.Errorf("%s: %+v, want served", v, by[v])
		}
	}
	if _, ok := by["other"]; ok {
		t.Errorf("a validator that did not endorse has an obligation: %+v", by["other"])
	}
	if net.Broken != 8 || net.Served != 2 {
		t.Errorf("network %+v, want 8 not served and 2 served", net)
	}
}

// A validator that did not endorse is asked like the rest, and its verified
// rows count toward the blob like anyone's.
func TestTheWholeSetCountsTowardTheBlob(t *testing.T) {
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"e1": 4, "e2": 2}}
	rd := newReading()
	rd.add("e1", probe.ClassFault, 4)
	rd.add("e2", probe.ClassHealthy, 2, 4, 5)
	rd.other("other", 8, span(8, 8)...)
	if res := BlobReading(rd.rows, facts, false); res.Status != BlobAvailable {
		t.Errorf("reading %+v, want Available on the rows of a validator that did not endorse", res)
	}
	if _, by := rd.obligations(facts); by["e1"] != notCounted || by["e2"] != served {
		t.Errorf("e1 %+v e2 %+v", by["e1"], by["e2"])
	}
}

// A reading that did not happen is not read and counts nothing against
// anyone: not a single request reached a server (this observer's own
// network was down: a local failure, no host to connect to, a connect that
// timed out), or the prober missed the reading, or part of it (NOT_PROBED)
// and the rows are short. Rows that came back are served all the same.
// Blobs whose window is still open are in their retention window instead.
func TestAReadingThatDidNotHappenIsNotRead(t *testing.T) {
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"a": 4, "b": 4, "c": 4}}
	local := newReading()
	local.add("a", probe.ClassProbeError, 4)
	local.add("b", probe.ClassProbeError, 4)
	local.add("c", probe.ClassNotRegistered, 4)
	unconnected := newReading()
	for _, v := range []string{"a", "b"} {
		unconnected.unconnected(v, 4)
	}
	unconnected.add("c", probe.ClassNotRegistered, 4)
	missed := newReading()
	for _, v := range []string{"a", "b", "c"} {
		missed.add(v, probe.ClassNotProbed, 4)
	}
	part := newReading()
	part.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	part.add("b", probe.ClassNotProbed, 4)
	part.add("c", probe.ClassNotProbed, 4)
	for _, c := range []struct {
		name string
		rd   *reading
	}{
		{"every request failed here", local},
		{"not a single connection opened", unconnected},
		{"missed", missed},
		{"partly missed", part},
	} {
		if res := BlobReading(c.rd.rows, facts, false); res.Status != BlobNotRead || res.Error != "" {
			t.Errorf("%s: %s %q, want not read", c.name, res.Status, res.Error)
		}
		if res := BlobReading(c.rd.rows, facts, true); res.Status != BlobPending {
			t.Errorf("%s, window open: %s, want pending", c.name, res.Status)
		}
		net, by := c.rd.obligations(facts)
		if net.Broken != 0 {
			t.Errorf("%s: %+v, want no one not served", c.name, net)
		}
		for v, o := range by {
			want := notCounted
			if c.rd == part && v == "a" {
				want = served
			}
			if o != want {
				t.Errorf("%s: %s %+v, want %+v", c.name, v, o, want)
			}
		}
	}
	// One refusal is a server's answer: the reading happened.
	refused := newReading()
	refused.unconnected("a", 4)
	refused.unconnected("b", 4)
	refused.rows[1].Outcome = probe.OutcomeTCPRefused
	if res := BlobReading(refused.rows, facts, false); res.Status != BlobUnavailable || res.Error != probe.ClientErrNoShards {
		t.Errorf("a refused connection: %s %q, want Unavailable", res.Status, res.Error)
	}
	// Rows the prober missed never stand between a blob and Available.
	whole := newReading()
	whole.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	whole.add("b", probe.ClassHealthy, 4, span(4, 4)...)
	whole.add("c", probe.ClassNotProbed, 4)
	if res := BlobReading(whole.rows, facts, false); res.Status != BlobAvailable {
		t.Errorf("available with a missed request: %s", res.Status)
	}
}

// Verified rows came back, whatever class the row is filed under while its
// verdict waits (the late shadow judgement's PROBE_ERROR over genuine
// rows): they count toward the blob, they are an answer, and the validator
// is served.
func TestVerifiedRowsCameBack(t *testing.T) {
	rd := newReading()
	rd.add("big", probe.ClassFault, 8)
	rd.rows = append(rd.rows, Row{PromiseHash: "p", Validator: "deferred", ScheduleLabel: probe.EndReadLabel, ScheduledAt: rd.at, StartedAt: rd.at,
		MustServeUntil: rd.msu, Assigned: true, Attested: true, Phase: probe.PhaseInWindow, Classification: probe.ClassProbeError,
		Outcome: probe.OutcomePartial, TLSOK: true, RowIndices: []uint32{20}, RowsReturned: 1, CommitmentVerified: true, AssignedRowCount: 6})
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"big": 8, "deferred": 6}}
	res := BlobReading(rd.rows, facts, false)
	if res.Status != BlobUnavailable || res.Have != 1 || res.Error != probe.ClientErrNotEnoughShards {
		t.Errorf("reading %+v, want Unavailable with 1 row", res)
	}
	_, by := rd.obligations(facts)
	if by["big"] != notServed || by["deferred"] != served {
		t.Errorf("big %+v deferred %+v", by["big"], by["deferred"])
	}
}

// A reading is judged from all of its rows, whatever phase each carries:
// the rows that came back are the client's result. Here a stored reading
// whose request to b started after must_serve_until (the phase its own
// start gave it, before a request took the reading's) handed over the rows
// that made the blob Available, so the validator that timed out in the
// window is not held to a blob the client could read.
func TestAReadingIsJudgedFromAllItsRows(t *testing.T) {
	facts := BlobFacts{Needed: 8, Endorsed: map[string]int{"a": 4, "b": 4, "c": 4}}
	rd := newReading()
	rd.add("a", probe.ClassHealthy, 4, span(0, 4)...)
	rd.add("b", probe.ClassHealthy, 4, span(4, 4)...)
	rd.add("c", probe.ClassUnreachable, 4)
	late := &rd.rows[1]
	late.Phase, late.StartedAt = probe.PhaseGrace, rd.msu.Add(5*time.Second)
	res := BlobReading(rd.rows, facts, false)
	if res.Status != BlobAvailable || res.Have != 8 {
		t.Fatalf("reading %+v, want Available with 8 rows", res)
	}
	if got := BlobOf(rd.rows, facts, rd.msu, rd.msu.Add(time.Hour)); got.Status != BlobAvailable || got.Have != 8 {
		t.Fatalf("BlobOf %+v, want Available with 8 rows", got)
	}
	_, by := rd.obligations(facts)
	if by["a"] != served || by["c"] != notCounted {
		t.Errorf("a %+v c %+v, want served and not counted", by["a"], by["c"])
	}

	// The same reading short of the rows: the validator that timed out is
	// not served, and a verified row after the deadline still counts toward
	// the blob.
	short := newReading()
	short.add("a", probe.ClassHealthy, 4, span(0, 2)...)
	short.add("b", probe.ClassHealthy, 4, span(4, 2)...)
	short.add("c", probe.ClassUnreachable, 4)
	short.rows[1].Phase = probe.PhaseGrace
	if res := BlobReading(short.rows, facts, false); res.Status != BlobUnavailable || res.Have != 4 {
		t.Fatalf("short reading %+v, want Unavailable with 4 rows", res)
	}
	if _, by := short.obligations(facts); by["c"] != notServed {
		t.Errorf("c %+v, want not served", by["c"])
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
	for _, c := range []struct {
		name string
		rows []Row
		open bool
		want string
	}{
		{"available", avail.rows, false, BlobAvailable},
		{"available, window open", avail.rows, true, BlobAvailable},
		{"unavailable", unavail.rows, false, BlobUnavailable},
		{"unavailable, window still open", unavail.rows, true, BlobUnavailable},
		{"no reading, window open", nil, true, BlobPending},
		{"no reading, window over", nil, false, BlobNotRead},
	} {
		if got := BlobReading(c.rows, facts, c.open); got.Status != c.want {
			t.Errorf("%s: %s, want %s", c.name, got.Status, c.want)
		}
	}
	// Needed unknown: nothing can be said.
	if got := BlobReading(avail.rows, BlobFacts{}, false); got.Status != BlobNotRead {
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
		if want := got.Have >= needed; got.Available() != want {
			t.Fatalf("available %v, the exact count says %v: %+v", got.Available(), want, got)
		}
		if want := got.Have < needed && got.Ran; got.Unavailable() != want {
			t.Fatalf("unavailable %v, the exact count says %v: %+v", got.Unavailable(), want, got)
		}
	}
}

// A held deadline withholds a counted reading in both directions: the rows
// that came back and the not-served ones alike. What counts neither way
// stays so.
func TestEndReadingHeldDeadline(t *testing.T) {
	for _, c := range []struct {
		cls         probe.Classification
		verified    bool
		unavailable bool
		want        probe.Classification
	}{
		{probe.ClassHealthy, true, false, probe.ClassRetentionUnverified},
		{probe.ClassShadowedShard, true, true, probe.ClassRetentionUnverified},
		{probe.ClassFault, false, true, probe.ClassRetentionUnverified},
		{probe.ClassUnreachable, false, true, probe.ClassRetentionUnverified},
		{probe.ClassUnreachable, false, false, NotCounted},
	} {
		r := Row{ScheduleLabel: probe.EndReadLabel, Phase: probe.PhaseInWindow, Assigned: true, Attested: true, Classification: c.cls,
			Outcome: probe.OutcomeTCPTimeout, CommitmentVerified: c.verified, RetentionUnverified: true}
		rd := Reading{Needed: 8, Ran: true, Have: 8}
		if c.unavailable {
			rd.Have = 2
		}
		if got := r.CountedClass(rd); got != c.want {
			t.Errorf("held %s (unavailable %v): %s, want %s", c.cls, c.unavailable, got, c.want)
		}
	}
}
