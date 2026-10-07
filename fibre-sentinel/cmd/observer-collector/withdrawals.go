package main

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// escrowChain is the part of scan.Chain the escrow poll uses, so the poll
// can be tested against a fake chain.
type escrowChain interface {
	StatusAt(ctx context.Context) (string, int64, time.Time, error)
	HeaderTime(ctx context.Context, height int64) (time.Time, error)
	EscrowAccount(ctx context.Context, signer string, height int64) (scan.Escrow, error)
	Withdrawals(ctx context.Context, signer string, height int64) ([]scan.PendingWithdrawal, int64, error)
}

// headerTimer is the part of scan.Chain that dates a height.
type headerTimer interface {
	HeaderTime(ctx context.Context, height int64) (time.Time, error)
}

type logf func(format string, a ...any)

// escrowBudget bounds the chain queries of one escrow poll, and, apart, of
// one fillParamTimes. The poll runs in the collector's one loop, between the
// ingest and the next pass, with two queries per publisher at -rpc-timeout
// each: a node that accepts queries and stalls them held the ingest up for
// a minute per publisher. Past the budget the rest wait for the next poll,
// which goes on from there (escrowRound). A var so a test can shorten it.
var escrowBudget = time.Minute

// escrowRound carries the escrow poll from one poll to the next. A round is
// one turn through every known publisher in name order. A poll the budget
// cuts short leaves the rest of the round to the next poll, which starts
// with the publisher it stopped at; a poll that reaches the end of the list
// goes on into the next round from the top. Without it every poll began at
// the top: once the publishers outnumbered what one budget covers, the cut
// fell at about the same name every time, the publishers after it were
// never read again, their balances froze, and no poll ever counted as done.
//
// Kept in memory: a restarted collector begins a round at the top, as every
// poll did before.
type escrowRound struct {
	next    string    // the first publisher the next poll reads; "" = the top of the list
	started time.Time // the poll the round under way began in; zero = none under way
	escrows int       // escrow accounts read and stored in the round under way
	missed  bool      // an account of the round under way could not be read
}

// escrowPoll is what one poll did, for the log and for tests.
type escrowPoll struct {
	Height     int64
	Publishers int
	Escrows    int // escrow accounts read and stored
	Queues     int // withdrawal queues read and stored
	Change     store.WithdrawalChange
	Resolved   int
	// OK: this poll finished a round in which every known publisher's
	// escrow account was read and stored (or there is none to read), each
	// at or after Since, the time of the poll the round began in. What
	// escrow_polled_at says.
	OK    bool
	Since time.Time
}

// pollEscrow reads, for the publishers the payments table knows, the escrow
// balance and the withdrawal queue, both at one height: the last block the
// chain had committed when the poll started. It reads as many as its budget
// covers, going on from where the round r stopped (escrowRound).
//
// One height for both reads, and for every publisher one poll reads, is the
// point. The
// module keeps balance - available equal to the sum of the queue (see the
// migration note in observer/store/withdrawals.go), so two reads from the
// same state can be checked against each other, and the block time of that
// height is what says whether a withdrawal that vanished could have been
// paid yet. Reading "latest" twice would straddle a block whenever one
// lands between the two queries.
//
// That height is the one below the tip /status names. /status reports the
// block store's height, and a node saves block h before it executes it and
// the app commits it, so for a moment every query at h is answered "cannot
// query with height in the future" (ABCI code 26). A poll that started in
// that moment failed for every publisher, about one poll in a hundred on
// Mocha, and the next was five minutes away. h-1 is committed whenever h is
// named, and it is dated by its own header.
//
// A publisher whose escrow read fails is skipped whole, as before, and its
// round does not count as done. One whose queue read fails keeps its escrow
// row and its queue history untouched: an error must never be stored as an
// empty queue, which would close every pending withdrawal it has. One whose
// turn the budget cut is the first the next poll reads.
func pollEscrow(ctx context.Context, c escrowChain, st *store.Store, r *escrowRound, now time.Time, log logf) escrowPoll {
	var out escrowPoll
	pubs, err := st.Publishers()
	if err != nil {
		log("escrow: publishers: %v", err)
		return out
	}
	out.Publishers = len(pubs)
	if len(pubs) == 0 {
		*r = escrowRound{}
		out.OK, out.Since = true, now
		return out
	}
	// The round's place is a name: made certain of the order it is kept in.
	sort.Strings(pubs)
	// The budget covers the chain; the store calls below keep ctx.
	cctx, cancel := context.WithTimeout(ctx, escrowBudget)
	defer cancel()
	_, tip, _, err := c.StatusAt(cctx)
	if err != nil {
		log("escrow: chain tip: %v", err)
		return out
	}
	h := tip
	if h > 1 {
		h--
	}
	blockTime, err := c.HeaderTime(cctx, h)
	if err != nil {
		log("escrow: header %d: %v", h, err)
		return out
	}
	out.Height = h

	// The end of the list ends a round: done when every account in it was
	// read. The next round begins with the next publisher read.
	endRound := func() {
		if !r.started.IsZero() && !r.missed {
			out.OK, out.Since = true, r.started
		}
		if r.escrows > 0 {
			_ = st.SetMeta("escrow_accounts", itoa(int64(r.escrows)), now)
		}
		*r = escrowRound{}
	}
	i := sort.SearchStrings(pubs, r.next)
	// At most one turn through the list per poll.
	for n := 0; n < len(pubs); n++ {
		if i == len(pubs) {
			endRound()
			i = 0
		}
		if cctx.Err() != nil {
			break
		}
		read, cut := escrowTurn(cctx, c, st, pubs[i], h, blockTime, now, &out, log)
		if cut {
			break
		}
		if r.started.IsZero() {
			r.started = now
		}
		if read {
			r.escrows++
		} else {
			r.missed = true
		}
		i++
	}
	if i == len(pubs) {
		endRound()
	} else if i > 0 {
		r.next = pubs[i]
	}
	if cctx.Err() != nil && ctx.Err() == nil {
		from := r.next
		if from == "" {
			from = pubs[0]
		}
		log("escrow: poll stopped at the %s budget with %d account(s) read; the next poll goes on from %s", escrowBudget, out.Escrows, from)
	}
	if out.Queues > 0 {
		_ = st.SetMeta("withdrawals_polled_height", itoa(h), now)
		_ = st.SetMeta("withdrawals_polled_at", store.TS(blockTime), now)
	}

	// Outcomes of withdrawals that left the queue, searched only through
	// the blocks the scanner has read (and the collector has ingested:
	// state.json is written after the payments it covers, and both were
	// ingested earlier in this pass).
	scanned := int64(0)
	if v, err := st.Meta("last_scanned_height"); err == nil && v != "" {
		scanned, _ = strconv.ParseInt(v, 10, 64)
	}
	if n, err := st.ResolveWithdrawals(ctx, scanned); err != nil {
		log("withdrawals: resolve: %v", err)
	} else {
		out.Resolved = n
		if n > 0 {
			log("withdrawals: %d outcome(s) settled", n)
		}
	}
	return out
}

// escrowTurn is one publisher's part of a poll: its escrow account and its
// withdrawal queue, both at h. read: the escrow account was read and stored.
// cut: the budget (cctx) ended the turn before it was done, so it is the
// next poll's first; reading the account again then is harmless.
func escrowTurn(cctx context.Context, c escrowChain, st *store.Store, pub string, h int64, blockTime, now time.Time, out *escrowPoll, log logf) (read, cut bool) {
	e, err := c.EscrowAccount(cctx, pub, h)
	if err != nil {
		if cctx.Err() != nil {
			return false, true
		}
		log("escrow: %s: %v", pub, err)
		return false, false
	}
	if err := st.UpsertEscrowAccount(e, now); err != nil {
		log("escrow: store: %v", err)
		return false, false
	}
	out.Escrows++

	ws, answered, err := c.Withdrawals(cctx, pub, h)
	if err != nil {
		if cctx.Err() != nil {
			return true, true
		}
		log("withdrawals: %s: %v", pub, err)
		return true, false
	}
	if answered != h {
		// The block time below is h's. A node that answered from another
		// height would pair this queue with the wrong clock.
		log("withdrawals: %s: asked for height %d, node answered at %d; queue not stored", pub, h, answered)
		return true, false
	}
	ch, err := st.ObserveWithdrawals(store.WithdrawalRead{Publisher: pub, Height: h, BlockTime: blockTime, Withdrawals: ws}, now)
	if err != nil {
		log("withdrawals: store %s: %v", pub, err)
		return true, false
	}
	if ch.Stale {
		log("withdrawals: %s: read at height %d is older than the last one stored; ignored", pub, h)
		return true, false
	}
	out.Queues++
	out.Change.Opened += ch.Opened
	out.Change.Reduced += ch.Reduced
	out.Change.Closed += ch.Closed
	out.Change.Reopened += ch.Reopened
	if ch.Opened+ch.Reduced+ch.Closed+ch.Reopened > 0 {
		log("WITHDRAWALS h=%d %s: %d queued now, %d new, %d reduced by a settlement shortfall, %d left the queue, %d reopened",
			h, pub, len(ws), ch.Opened, ch.Reduced, ch.Closed, ch.Reopened)
	}
	return true, false
}

// paramTimesPerPass bounds the header reads one pass spends dating params
// changes. There are a handful in the whole history; the bound only matters
// for a node that cannot serve them, which would otherwise be asked for
// every one on every pass.
const paramTimesPerPass = 16

// fillParamTimes records the block time of every params_history height not
// yet dated. A height the node has pruned stays undated and is published as
// such; it is retried on later passes in case the operator points the
// collector at an archive.
func fillParamTimes(ctx context.Context, c headerTimer, st *store.Store, log logf) int {
	hs, err := st.ParamHeightsWithoutTime(ctx)
	if err != nil {
		log("params: undated heights: %v", err)
		return 0
	}
	cctx, cancel := context.WithTimeout(ctx, escrowBudget)
	defer cancel()
	done := 0
	for i, h := range hs {
		if i >= paramTimesPerPass || cctx.Err() != nil {
			break
		}
		t, err := c.HeaderTime(cctx, h)
		if err != nil {
			if !scan.IsHeightUnavailable(err) {
				log("params: header %d: %v", h, err)
			}
			continue
		}
		if err := st.SetParamTime(h, t); err != nil {
			log("params: store time of %d: %v", h, err)
			continue
		}
		done++
	}
	return done
}
