package probe

// The reading-rate ceiling.
//
// The observer shares its network port (and its disk) with other work, a
// validator of its owner's among them. Without a ceiling a full reading
// lets every request go at once: a 128 MiB blob's 62 to 67 requests pulled
// 376 to 392 MB in about 3 s, nearly the whole of a 1 Gbit/s port. The
// ceiling paces what is let go: every request, the first pass's and every
// later attempt's, is charged its shard's expected bytes (Input.
// ExpectedShardBytes, the figure the byte budget charges) against a token
// bucket of Config.MaxReadMbps, and is let go only once the bucket has
// them. Over any stretch of time the shard bytes let go are at most the
// bucket's burst plus the rate times that stretch.
//
// The ceiling is the last of this observer's limits a request passes
// (Prober.admitBy), after a request slot and the byte budget, so the bytes
// are let go when they are charged and requests that waited for room do
// not leave together. Like the other limits it only delays: the wait is
// bounded by the request's start cutoff (a full reading's request or later
// attempt, must_serve_until less RequestStartMargin), a request whose turn
// would come at or after it is not made and is not charged (NOT_PROBED,
// this observer's gap), and the request's own time starts once it is let
// go, never while it waits. The row records the wait (LoadInfo.RateWaitMS).
//
// The client's one re-dial within a first-pass request is not charged
// again: it follows a request that failed before an answer came back, so
// the shard the request was charged for moves once.

import (
	"errors"
	"math"
	"sync"
	"time"
)

// DefaultMaxReadMbps is the reading-rate ceiling sentinel-probe sets when
// -max-read-mbps is not given: 400 Mbit/s, 50 MB a second, which leaves
// six tenths of the observer's shared 1 Gbit/s port to the work beside it.
// Today's load, about 20 blobs a minute, nearly all of 16 MiB, full
// readings of about 55 MB each, is about a third of it; a 128 MiB blob's
// full reading (about 390 MB) is let go over about 6.5 s instead of 3.
const DefaultMaxReadMbps = 400

// readBurstFloor is the least burst the ceiling keeps: above the largest
// shard a request asks for today, so one shard is charged in full. Mocha's
// largest validator holds 1463 of a blob's 16384 rows, 48.7 MB of a
// 128 MiB blob.
const readBurstFloor = 64 << 20

// ReadCeiling is the ceiling's rate, bytes a second, and its burst for a
// ceiling of mbps megabits a second: about one second of the rate, and at
// least readBurstFloor. Zero when mbps is not set (no ceiling).
func ReadCeiling(mbps int) (rate, burst float64) {
	if mbps <= 0 {
		return 0, 0
	}
	rate = float64(mbps) * 1e6 / 8
	return rate, math.Max(rate, readBurstFloor)
}

// The reasons a request did not get through this observer's limits before
// its start cutoff, as the NOT_PROBED row gives them.
var (
	errLimitsFull = errors.New("this observer's own request limits were full")
	errRateHeld   = errors.New("this observer's own reading-rate ceiling (-max-read-mbps) could not let it go in time")
)

// readCeiling is a token bucket of shard bytes. tokens is what the bucket
// holds; it goes below zero while requests wait for bytes already promised
// to those charged before them, so requests are let go in the order they
// were charged.
type readCeiling struct {
	mu     sync.Mutex
	rate   float64 // bytes a second
	burst  float64 // the most the bucket holds
	tokens float64
	at     time.Time // when tokens was last brought up to date
	now    func() time.Time
}

// newReadCeiling is a bucket of rate bytes a second holding at most burst,
// full to start with; nil (no ceiling) when rate is not positive.
func newReadCeiling(rate, burst float64) *readCeiling {
	if rate <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = rate
	}
	return &readCeiling{rate: rate, burst: burst, tokens: burst, now: time.Now}
}

// reserve charges a request of n bytes and says how long it waits before it
// is let go. A request larger than the burst is charged the burst and let
// go once the bucket is full, as the byte budget lets a shard larger than
// itself go alone. ok is false, and nothing is charged, when the request's
// turn would come at or after by; a zero by is no bound.
func (c *readCeiling) reserve(n int64, by time.Time) (wait time.Duration, charged float64, ok bool) {
	if c == nil {
		return 0, 0, true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if now.After(c.at) {
		c.tokens = math.Min(c.burst, c.tokens+c.rate*now.Sub(c.at).Seconds())
		c.at = now
	}
	charged = math.Min(float64(max(n, 0)), c.burst)
	left := c.tokens - charged
	if left < 0 {
		wait = secondsToDuration(-left / c.rate)
	}
	if !by.IsZero() && !now.Add(wait).Before(by) {
		return 0, 0, false
	}
	c.tokens = left
	return wait, charged, true
}

// refund gives back a charge whose request was not let go after all (the
// run stopped while it waited).
func (c *readCeiling) refund(charged float64) {
	if c == nil || charged <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tokens = math.Min(c.burst, c.tokens+charged)
}

// secondsToDuration converts without overflowing: a wait past a year is a
// year (only a request with no cutoff can be told to wait that long).
func secondsToDuration(s float64) time.Duration {
	const most = 365 * 24 * time.Hour
	if s >= most.Seconds() {
		return most
	}
	return time.Duration(s * float64(time.Second))
}
