package probe

import (
	"math"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// Phase labels where in a publication's life a scheduled probe falls.
type Phase string

const (
	// PhaseInWindow: before must_serve_until. The validator is under its
	// retention obligation; NOT_FOUND here is a fault.
	PhaseInWindow Phase = "in_window"
	// PhaseGrace: from must_serve_until to must_serve_until + prune tolerance.
	// The blob may already be pruned (measured lag ~1-2 min); NOT_FOUND is
	// tolerated, SERVED_OK is still fine.
	PhaseGrace Phase = "grace"
	// PhasePost: after the tolerance. The blob is expected to be gone;
	// NOT_FOUND is the expected result.
	PhasePost Phase = "post"
)

// ScheduleConfig controls how probe times are derived from a publication.
// Nothing here is a fixed interval — the points are fractions of that
// publication's own retention window plus two points past its end.
type ScheduleConfig struct {
	// InWindowFractions are positions inside [settlement, must_serve_until],
	// as fractions of that span. They are clustered toward the end, where a
	// retention breach is most likely. Empty -> DefaultInWindowFractions.
	InWindowFractions []float64
	// GraceOffset is how far past must_serve_until the grace probe sits.
	GraceOffset time.Duration
	// PruneTolerance is how long after must_serve_until a NOT_FOUND is still
	// considered normal. Measured on the devnet at must_serve_until + ~1m45s
	// (60s prune loop + minute-resolution prune key); default 2m30s leaves
	// margin. This is also the grace/post phase boundary.
	PruneTolerance time.Duration
	// PostMargin is how far past (must_serve_until + PruneTolerance) the final
	// "expected gone" probe sits.
	PostMargin time.Duration
	// MinSpacing drops schedule points that would land within this of an
	// earlier one (keeps a very short window from generating a burst).
	MinSpacing time.Duration
	// LastPointMargin caps how far before must_serve_until the final
	// in-window probe may sit. The in-window points are fractions of the
	// publication's own window, which was written for a devnet whose window
	// was ten minutes: 0.92 of it put the last reading 48 seconds out. On
	// mocha, where shard_retention is four hours, the same fraction puts it
	// 19 minutes out, and the only later point is the grace probe, where a
	// missing shard is TOLERATED by construction. So nothing could see a
	// validator that pruned inside the last 19 minutes of its obligation,
	// and the obligation bucketed as served — the exact failure this
	// observer exists to catch, silently passed.
	//
	// The margin only ever moves the last point later, never earlier, so a
	// short window keeps the tighter reading its fraction already gives it.
	//
	// It cannot usefully go below the chain's own prune granularity: the
	// Fibre server's prune loop runs once a minute against a
	// minute-granularity key, so a shard can legitimately survive up to two
	// minutes past the deadline, and by the same token a validator whose
	// loop ran early is not misbehaving on the order of a minute. Anything
	// under that would be accusing an operator of the clock. 150s is that
	// floor with room, and the same value the grace phase already allows in
	// the other direction (PruneTolerance).
	LastPointMargin time.Duration
	// EndReadOffset, when set, replaces the whole schedule with one reading
	// at the end of the retention window, this long before
	// must_serve_until: nothing earlier in the window and nothing after it.
	// A reading that cannot start on time may still start late, up to the
	// deadline (Prober.latenessAt), so a short outage of this observer does
	// not cost it; one that fails on a transport timeout gets the prober's
	// one retry. It is the reading the chain's own client would take near
	// the end, once.
	EndReadOffset time.Duration
	// EndReadSince limits EndReadOffset to publications settled at or after
	// it; an earlier one keeps the schedule it was read under. Without it,
	// a prober switched to the end reading would find every past
	// publication's "end" point elapsed and never recorded, and write a
	// NOT_PROBED row for it: thousands of gaps for readings that were never
	// planned. Zero applies the end reading to every publication.
	EndReadSince time.Time
}

// DefaultInWindowFractions: one early reading, then three clustered toward the
// deadline.
var DefaultInWindowFractions = []float64{0.12, 0.45, 0.72, 0.92}

// InWindowFractions returns n in-window probe positions in (0,1). n == 4 is
// the documented default set; other n spread x^0.7 over (0,1] with the last
// reading pinned at 0.92, so the final in-window probe always sits close to
// the deadline where a retention breach is most likely to show. A single
// probe sits at 0.6.
func InWindowFractions(n int) []float64 {
	switch {
	case n <= 0:
		return nil
	case n == 1:
		return []float64{0.6}
	case n == len(DefaultInWindowFractions):
		return append([]float64(nil), DefaultInWindowFractions...)
	}
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		x := float64(i+1) / float64(n)
		out[i] = 0.92 * math.Pow(x, 0.7)
	}
	return out
}

// DefaultScheduleConfig fills the zero value.
func DefaultScheduleConfig() ScheduleConfig {
	return ScheduleConfig{
		InWindowFractions: DefaultInWindowFractions,
		GraceOffset:       30 * time.Second,
		PruneTolerance:    150 * time.Second,
		PostMargin:        60 * time.Second,
		MinSpacing:        20 * time.Second,
		LastPointMargin:   150 * time.Second,
	}
}

func (c ScheduleConfig) withDefaults() ScheduleConfig {
	d := DefaultScheduleConfig()
	if len(c.InWindowFractions) == 0 {
		c.InWindowFractions = d.InWindowFractions
	}
	if c.GraceOffset <= 0 {
		c.GraceOffset = d.GraceOffset
	}
	if c.PruneTolerance <= 0 {
		c.PruneTolerance = d.PruneTolerance
	}
	if c.PostMargin <= 0 {
		c.PostMargin = d.PostMargin
	}
	if c.MinSpacing <= 0 {
		c.MinSpacing = d.MinSpacing
	}
	if c.LastPointMargin <= 0 {
		c.LastPointMargin = d.LastPointMargin
	}
	return c
}

// fallbackSpan is the window length used when a record's settlement time is
// not before its must_serve_until: the publication's own retention (or
// promise timeout, whichever the deadline was built from), and 10 minutes
// only when the record carries neither.
func fallbackSpan(p scan.Publication) time.Duration {
	span := time.Duration(p.ParamsAtPublication.ShardRetentionSeconds) * time.Second
	if t := time.Duration(p.ParamsAtPublication.PaymentPromiseTimeoutSeconds) * time.Second; t > span {
		span = t
	}
	if span <= 0 {
		span = 10 * time.Minute
	}
	return span
}

// SchedulePoint is one moment the Sentinel should probe a publication's
// assigned validators.
type SchedulePoint struct {
	At    time.Time
	Phase Phase
	// Label is a short human tag ("w1".."wN", "grace", "post"; "end" under
	// EndReadOffset).
	Label string
}

// ScheduleFor derives the probe schedule for one publication. The window is
// [settlement_time, must_serve_until]; must_serve_until itself comes from the
// record (creation + max(payment_promise_timeout, shard_retention)), so the
// schedule follows the on-chain params in force when the blob was published.
func ScheduleFor(p scan.Publication, cfg ScheduleConfig) []SchedulePoint {
	cfg = cfg.withDefaults()

	start := p.SettlementTime
	msu := p.MustServeUntil
	if !msu.After(start) {
		// Degenerate record (the promise's creation timestamp is later than
		// the block that settled it, which the chain allows within its skew
		// window): anchor a window of the publication's own retention at
		// must_serve_until, so the fallback follows the chain params rather
		// than a constant.
		start = msu.Add(-fallbackSpan(p))
	}
	span := msu.Sub(start)

	if cfg.EndReadOffset > 0 && !p.SettlementTime.Before(cfg.EndReadSince) {
		return []SchedulePoint{endReadPoint(start, msu, cfg)}
	}

	var pts []SchedulePoint
	// The latest a last reading may sit and still be a reading of the window:
	// see LastPointMargin. A point is pulled forward to it, never pushed back.
	deadline := msu.Add(-cfg.LastPointMargin)
	for i, f := range cfg.InWindowFractions {
		if f <= 0 || f >= 1 {
			continue
		}
		at := start.Add(time.Duration(float64(span) * f))
		if at.Before(deadline) && f == maxFraction(cfg.InWindowFractions) {
			at = deadline
		}
		pts = append(pts, SchedulePoint{
			At:    at,
			Phase: PhaseInWindow,
			Label: "w" + itoa(i+1),
		})
	}
	pts = append(pts,
		SchedulePoint{At: msu.Add(cfg.GraceOffset), Phase: PhaseGrace, Label: "grace"},
		SchedulePoint{At: msu.Add(cfg.PruneTolerance + cfg.PostMargin), Phase: PhasePost, Label: "post"},
	)

	// enforce ordering + MinSpacing.
	sortByTime(pts)
	if cfg.MinSpacing > 0 {
		out := pts[:0:0]
		var last time.Time
		for _, pt := range pts {
			if !last.IsZero() && pt.At.Sub(last) < cfg.MinSpacing {
				continue
			}
			out = append(out, pt)
			last = pt.At
		}
		pts = out
	}
	return pts
}

// endReadPoint is the one point of an end-of-window schedule, labelled
// "end": EndReadOffset before must_serve_until, or, in a window shorter than
// that, LastPointMargin before its end (half way through, if even that falls
// before the settlement), so no publication goes unread.
func endReadPoint(start, msu time.Time, cfg ScheduleConfig) SchedulePoint {
	at := msu.Add(-cfg.EndReadOffset)
	if !at.After(start) {
		at = msu.Add(-cfg.LastPointMargin)
	}
	if !at.After(start) {
		at = start.Add(msu.Sub(start) / 2)
	}
	return SchedulePoint{At: at, Phase: PhaseInWindow, Label: "end"}
}

// PhaseAt classifies an arbitrary instant against a publication's window, using
// the same tolerance the schedule used. Used by the classifier on the ACTUAL
// probe start time (not the scheduled one).
func PhaseAt(t time.Time, p scan.Publication, cfg ScheduleConfig) Phase {
	cfg = cfg.withDefaults()
	return PhaseAtWindow(t, p.MustServeUntil, cfg.PruneTolerance)
}

// PhaseAtWindow is PhaseAt without a Publication — just the boundaries.
func PhaseAtWindow(t, mustServeUntil time.Time, pruneTolerance time.Duration) Phase {
	switch {
	case t.Before(mustServeUntil):
		return PhaseInWindow
	case !t.After(mustServeUntil.Add(pruneTolerance)):
		return PhaseGrace
	default:
		return PhasePost
	}
}

// maxFraction is the last reading's position, whichever entry holds it: the
// fractions are given in order by every caller here, but the clamp must not
// depend on that.
func maxFraction(fs []float64) float64 {
	m := 0.0
	for _, f := range fs {
		if f > 0 && f < 1 && f > m {
			m = f
		}
	}
	return m
}

func sortByTime(pts []SchedulePoint) {
	for i := 1; i < len(pts); i++ {
		for j := i; j > 0 && pts[j].At.Before(pts[j-1].At); j-- {
			pts[j], pts[j-1] = pts[j-1], pts[j]
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}

// EndSegmentDivisor cuts the tail off a retention window: the final
// 1/EndSegmentDivisor of it, where a HEALTHY reading has to fall before an
// obligation counts as served. observer/verdict takes the value from here
// (its EndSegmentDivisor explains the rule), and the prober uses it to give a
// download that timed out in that tail its one retry (shouldRetry).
const EndSegmentDivisor = 4.0

// InEndSegment reports whether a probe scheduled at scheduledAt falls in the
// tail of pub's retention window, drawn from the promise's own settlement
// time and must_serve_until exactly as the verdict draws it.
func InEndSegment(scheduledAt time.Time, pub scan.Publication) bool {
	msu := pub.MustServeUntil
	return !scheduledAt.Before(msu.Add(-time.Duration(float64(msu.Sub(pub.SettlementTime)) / EndSegmentDivisor)))
}
