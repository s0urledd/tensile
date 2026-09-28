package probe

import (
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// Phase labels where in a publication's life a request falls.
type Phase string

const (
	// PhaseInWindow: before must_serve_until. The validator is under its
	// retention obligation.
	PhaseInWindow Phase = "in_window"
	// PhaseGrace: from must_serve_until to must_serve_until + prune tolerance.
	// The blob may already be pruned (measured lag ~1-2 min); NOT_FOUND is
	// tolerated, SERVED_OK is still fine.
	PhaseGrace Phase = "grace"
	// PhasePost: after the tolerance. The blob is expected to be gone;
	// NOT_FOUND is the expected result.
	PhasePost Phase = "post"
)

// ScheduleConfig is when a blob is read. Each blob is read once, the way
// celestia-app's Fibre client downloads it, EndReadOffset before its
// must_serve_until, with one second pass RetryAfter later when the first
// did not bring back enough rows to reconstruct it.
type ScheduleConfig struct {
	// PruneTolerance is how long after must_serve_until a NOT_FOUND is still
	// considered normal: the grace/post phase boundary. Measured on the
	// devnet at must_serve_until + ~1m45s (60s prune loop + minute-resolution
	// prune key); default 2m30s leaves margin.
	PruneTolerance time.Duration
	// EndReadOffset is how long before must_serve_until the reading is
	// scheduled (10 minutes). In a window shorter than that, half way
	// through it.
	EndReadOffset time.Duration
	// ReadDeadline is how long before must_serve_until a reading must have
	// started (3 minutes); one that cannot start by then is not made, and
	// its blob was not read by Tensile.
	ReadDeadline time.Duration
	// RetryAfter is how long after the first pass the second one starts,
	// when the first did not bring back enough rows (60 s): a back-off, so
	// a limit this observer's own requests ran into can clear.
	RetryAfter time.Duration
	// RetryDeadline is how long before must_serve_until the second pass must
	// start (90 s). Its requests then finish before RequestCutoff.
	RetryDeadline time.Duration
	// RequestCutoff is how long before must_serve_until the last request of
	// a reading may start (60 s): the NotFoundGuard band plus two requests
	// of RPCTimeout. A request that would start later is not sent, and the
	// validator's answer is this observer's gap: an answer from past the
	// deadline says nothing about the obligation.
	RequestCutoff time.Duration
	// Since limits the reading to publications settled at or after it; an
	// earlier one was read on the schedule of its time and is not read
	// again. Zero reads every publication.
	Since time.Time
}

// DefaultScheduleConfig fills the zero value.
func DefaultScheduleConfig() ScheduleConfig {
	return ScheduleConfig{
		PruneTolerance: 150 * time.Second,
		EndReadOffset:  10 * time.Minute,
		ReadDeadline:   3 * time.Minute,
		RetryAfter:     time.Minute,
		RetryDeadline:  90 * time.Second,
		RequestCutoff:  time.Minute,
	}
}

func (c ScheduleConfig) withDefaults() ScheduleConfig {
	d := DefaultScheduleConfig()
	if c.PruneTolerance <= 0 {
		c.PruneTolerance = d.PruneTolerance
	}
	if c.EndReadOffset <= 0 {
		c.EndReadOffset = d.EndReadOffset
	}
	if c.ReadDeadline <= 0 {
		c.ReadDeadline = d.ReadDeadline
	}
	if c.RetryAfter <= 0 {
		c.RetryAfter = d.RetryAfter
	}
	if c.RetryDeadline <= 0 {
		c.RetryDeadline = d.RetryDeadline
	}
	if c.RequestCutoff <= 0 {
		c.RequestCutoff = d.RequestCutoff
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

// SchedulePoint is the moment a publication is read.
type SchedulePoint struct {
	At    time.Time
	Phase Phase
	// Label is EndReadLabel for the one reading. Rows of the earlier
	// schedule carry w1..w4, grace and post.
	Label string
}

// ReadPoint is when a publication is read: EndReadOffset before
// must_serve_until, or half way through a window shorter than that. The
// window is [settlement_time, must_serve_until]; must_serve_until comes from
// the record (creation + max(payment_promise_timeout, shard_retention)).
func ReadPoint(p scan.Publication, cfg ScheduleConfig) SchedulePoint {
	cfg = cfg.withDefaults()
	start, msu := p.SettlementTime, p.MustServeUntil
	if !msu.After(start) {
		// Degenerate record (the promise's creation timestamp is later than
		// the block that settled it, which the chain allows within its skew
		// window): anchor a window of the publication's own retention at
		// must_serve_until.
		start = msu.Add(-fallbackSpan(p))
	}
	at := msu.Add(-cfg.EndReadOffset)
	if !at.After(start) {
		at = start.Add(msu.Sub(start) / 2)
	}
	return SchedulePoint{At: at, Phase: PhaseInWindow, Label: EndReadLabel}
}

// latestStart is the last moment a publication's reading may start:
// ReadDeadline before must_serve_until, and never before the reading's own
// time (a very short window gets half its remaining span).
func latestStart(p scan.Publication, pt SchedulePoint, cfg ScheduleConfig) time.Time {
	cfg = cfg.withDefaults()
	last := p.MustServeUntil.Add(-cfg.ReadDeadline)
	if !last.After(pt.At) {
		last = pt.At.Add(p.MustServeUntil.Sub(pt.At) / 2)
	}
	return last
}

// PhaseAt classifies an arbitrary instant against a publication's window, using
// the configured tolerance. Used by the classifier on the ACTUAL request
// start time (not the scheduled one).
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
