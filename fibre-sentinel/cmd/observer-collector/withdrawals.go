package main

import (
	"context"
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
// and the poll does not count as done (escrow_polled_at stays). A var so a
// test can shorten it.
var escrowBudget = time.Minute

// escrowPoll is what one poll did, for the log and for tests.
type escrowPoll struct {
	Height     int64
	Publishers int
	Escrows    int // escrow accounts read and stored
	Queues     int // withdrawal queues read and stored
	Change     store.WithdrawalChange
	Resolved   int
	// OK: every known publisher's escrow account was read and stored (or
	// there is none to read). What escrow_polled_at says.
	OK bool
}

// pollEscrow reads, for every publisher the payments table knows, the
// escrow balance and the withdrawal queue, both at one height: the last
// block the chain had committed when the poll started.
//
// One height for both reads, and for every publisher, is the point. The
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
// A publisher whose escrow read fails is skipped whole, as before. One
// whose queue read fails keeps its escrow row and its queue history
// untouched: an error must never be stored as an empty queue, which would
// close every pending withdrawal it has.
func pollEscrow(ctx context.Context, c escrowChain, st *store.Store, now time.Time, log logf) escrowPoll {
	var out escrowPoll
	pubs, err := st.Publishers()
	if err != nil {
		log("escrow: publishers: %v", err)
		return out
	}
	out.Publishers = len(pubs)
	if len(pubs) == 0 {
		out.OK = true
		return out
	}
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
	for _, pub := range pubs {
		if cctx.Err() != nil {
			if ctx.Err() == nil {
				log("escrow: poll stopped at the %s budget with %d of %d account(s) read; the rest wait for the next poll", escrowBudget, out.Escrows, len(pubs))
			}
			break
		}
		e, err := c.EscrowAccount(cctx, pub, h)
		if err != nil {
			log("escrow: %s: %v", pub, err)
			continue
		}
		if err := st.UpsertEscrowAccount(e, now); err != nil {
			log("escrow: store: %v", err)
			continue
		}
		out.Escrows++

		ws, answered, err := c.Withdrawals(cctx, pub, h)
		if err != nil {
			log("withdrawals: %s: %v", pub, err)
			continue
		}
		if answered != h {
			// The block time below is h's. A node that answered from
			// another height would pair this queue with the wrong clock.
			log("withdrawals: %s: asked for height %d, node answered at %d; queue not stored", pub, h, answered)
			continue
		}
		ch, err := st.ObserveWithdrawals(store.WithdrawalRead{Publisher: pub, Height: h, BlockTime: blockTime, Withdrawals: ws}, now)
		if err != nil {
			log("withdrawals: store %s: %v", pub, err)
			continue
		}
		if ch.Stale {
			log("withdrawals: %s: read at height %d is older than the last one stored; ignored", pub, h)
			continue
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
	}
	out.OK = out.Escrows == len(pubs)
	if out.Escrows > 0 {
		_ = st.SetMeta("escrow_accounts", itoa(int64(out.Escrows)), now)
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
