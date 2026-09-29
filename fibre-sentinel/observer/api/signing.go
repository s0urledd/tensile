package api

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Signing participation: how often a validator's signature is on the settled
// promises it was assigned rows of, and, network-wide, how much voting power
// each settled promise collected. EigenDA publishes the same pair as
// "signing info"; the difference here is what the figures are allowed to mean.
//
// What is counted. Every settled MsgPayForFibre (settlement_tx_code = 0) whose
// assignment the scanner could compute is one promise. The scanner runs
// fibre-assign over the validator set at the promise height, so a validator is
// ASSIGNED a promise exactly when that table gives it rows (row_count > 0):
// the minimum-rows floor gives every member of the set rows, so in practice
// that is "every validator in the set at the promise height". The protocol
// assigns rows to validators, not to hosts, so a validator with no Fibre host
// registered when the promise settled is assigned all the same, but it cannot
// receive the upload and so cannot sign. Those assignments are left out of
// the rate and counted apart (NoHost), the same exclusion NOT_REGISTERED gets
// everywhere else: counting them made a validator that registered late look
// as if it missed every promise from before it existed. An assignment whose
// host could not be read at settlement (NULL) stays in. A validator
// SIGNED a promise when its signature over the promise verified against its
// consensus key (internal/scan/attest.go): the observer checks every entry
// itself, it never counts them.
//
// Promises recorded before signatures were verified (scan schema 1, attested
// NULL) say nothing either way, so they are outside both sides of the rate and
// counted as unknown, the same rule the probe-level attestation split follows.
//
// What it does NOT mean. An unsigned promise is not a fault and is never
// published as one. The reference client stops collecting signatures the
// moment it holds two thirds of voting power and keeps delivering to everyone
// else in the background (fibre/client_upload.go), so on any promise about a
// third of the stake is unsigned by construction and very often holds the
// shard all the same. The rate describes how often a validator was among the
// signatures the publisher kept: roughly, how often it answered an upload
// before the quorum closed. It is published beside the Service rate, never
// folded into it, and it does not rank anybody.
//
// Publications are never pruned (observer/rollup prunes probe rows only), so
// every window, "all" included, is computed from the raw record and needs no
// rollup fold-in.

// signingStats is one validator's signing participation over a window.
type signingStats struct {
	// Assigned is how many settled promises in the window gave this validator
	// rows while it had a Fibre host registered, and whose signatures were
	// verified: the rate's denominator.
	Assigned int64 `json:"assigned"`
	// Signed is how many of those carry this validator's verified signature.
	Signed int64 `json:"signed"`
	// Rate is Signed / Assigned; value null when nothing was assigned.
	Rate Rate `json:"rate"`
	// Unknown counts assigned promises recorded before signatures were
	// verified, which are in neither side of Rate.
	Unknown int64 `json:"unknown"`
	// NoHost counts assigned promises that settled while this validator had
	// no Fibre host registered: it could not sign them, so they are in
	// neither side of Rate.
	NoHost int64 `json:"no_host"`
	// LastEndorsedAt is the settlement time of the newest promise carrying
	// this validator's verified signature, whatever the window; null when
	// none does.
	LastEndorsedAt *string `json:"last_endorsed_at"`
	// Recent counts the newest recentEndorsements promises assigned to it
	// while it had a host and whose signatures were verified, and how many
	// of them it endorsed, whatever the window. A run of zeros is an
	// endorsement that stopped, which a rate over a long period hides.
	Recent recentEndorsement `json:"recent"`
}

// recentEndorsement is the newest assigned promises, and how many of them
// carry the validator's endorsement.
type recentEndorsement struct {
	Assigned int64 `json:"assigned"`
	Endorsed int64 `json:"endorsed"`
}

// recentEndorsements is how many of a validator's newest assigned promises
// Recent looks at.
const recentEndorsements = 20

// signingPopulation is the promise population both figures are drawn from:
// settled in the window, the transaction succeeded, and the assignment was
// computed. The arguments are the window's start and end, in that order.
const signingPopulation = `p.settlement_time >= ? AND p.settlement_time <= ?
	AND p.settlement_tx_code = 0 AND p.assignment_error = ''`

// signingByValidatorSQL is signingByValidator's query; filter narrows it to
// one validator.
//
// It starts from the window's publications and seeks their assignments by
// primary key. Joined the other way round it walked every assignment ever
// stored (assignments_validator), and assignments are never pruned, so a
// day's figure cost the whole history. Every figure here is a count, so the
// join order cannot move one.
func signingByValidatorSQL(filter string) string {
	// host_at_settlement is '' when no host was registered, NULL when the
	// registry could not be read at that height.
	return `SELECT a.validator_address,
			COALESCE(SUM(CASE WHEN a.attested IS NOT NULL AND a.host_at_settlement IS NOT '' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a.attested = 1 AND a.host_at_settlement IS NOT '' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a.attested IS NULL AND a.host_at_settlement IS NOT '' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a.host_at_settlement = '' THEN 1 ELSE 0 END), 0)
		FROM publications p CROSS JOIN assignments a ON a.promise_hash = p.promise_hash
		WHERE ` + signingPopulation + ` AND a.row_count > 0` + filter + `
		GROUP BY a.validator_address`
}

// signingByValidator counts signing participation per validator over win,
// only for one validator when only is set.
func (s *Server) signingByValidator(ctx context.Context, win Window, only string) (map[string]signingStats, error) {
	filter, args := "", []any{win.startArg(), win.endArg()}
	if only != "" {
		filter = ` AND a.validator_address = ?`
		args = append(args, only)
	}
	rows, err := s.st.DB().QueryContext(ctx, signingByValidatorSQL(filter), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]signingStats{}
	for rows.Next() {
		var addr string
		var st signingStats
		if err := rows.Scan(&addr, &st.Assigned, &st.Signed, &st.Unknown, &st.NoHost); err != nil {
			return nil, err
		}
		st.Rate = rate(st.Signed, st.Assigned)
		out[addr] = st
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.recentSigning(ctx, only, out)
}

// recentSigning adds LastEndorsedAt and Recent to every validator in out
// (and to any validator with an endorsement that out lacks): the newest
// record, not the window. The figures come from the endorsement ledger,
// brought up to date first.
func (s *Server) recentSigning(ctx context.Context, only string, out map[string]signingStats) error {
	return s.recent.fill(ctx, s.st.DB(), only, out)
}

// recentPopulationSQL is the assignments recentSigning describes: a settled
// promise whose assignment was computed, rows given to the validator while it
// had a host (or the registry could not be read), and signatures verified.
const recentPopulationSQL = `p.settlement_tx_code = 0 AND p.assignment_error = '' AND a.row_count > 0
	AND a.host_at_settlement IS NOT '' AND a.attested IS NOT NULL`

// endorsementLedger is what recentSigning publishes, kept per validator and
// brought up to date from the assignments stored since it last looked.
//
// Both figures are about the whole record, not the window: the newest
// endorsement's settlement time, and the newest recentEndorsements assigned
// promises in settlement order. Computed from scratch they walked every
// assignment ever stored with two window functions over it, on every
// snapshot, and assignments are never pruned, so the one column the table
// is read for ("Last endorsement") grew slowest to refresh. Kept here, a
// refresh reads only the assignments added since the last one.
//
// That is exact because every input is written once. Assignments are
// inserted with their publication in one transaction (UpsertPublication),
// both with ON CONFLICT DO NOTHING; no UPDATE touches attested,
// host_at_settlement, row_count, or the publication's settlement columns,
// transaction code or assignment error; and nothing deletes either. So the
// newest endorsement over old rows and new rows is the newer of the two, and
// the newest twenty of all rows are the newest twenty of the old twenty and
// the new rows. Rowids only grow: a rowid table without deletes takes
// max(rowid)+1, and the one writer commits in order, so every row with a
// rowid at or below the highest one a read sees was committed before it.
//
// The ledger does not assume block times increase: the newest endorsement is
// the largest settlement time, compared as SQLite compared it (bytewise), and
// the order of "newest" is (settlement height, transaction index), as it was.
// A tie on both, which the chain does not produce, is broken by rowid, newest
// first, where the window function left it to the plan.
//
// The zero value is ready to use.
type endorsementLedger struct {
	mu sync.Mutex
	// upTo is the highest assignments rowid folded in.
	upTo int64
	vals map[string]*ledgerEntry
}

type ledgerEntry struct {
	// top is the newest assigned promises, newest first, at most
	// recentEndorsements of them.
	top []ledgerRow
	// last is the settlement time of the newest endorsement; set says there
	// is one.
	last string
	set  bool
}

type ledgerRow struct {
	height, txIndex, rowid, attested int64
}

// newer orders ledger rows newest first.
func (r ledgerRow) newer(o ledgerRow) bool {
	if r.height != o.height {
		return r.height > o.height
	}
	if r.txIndex != o.txIndex {
		return r.txIndex > o.txIndex
	}
	return r.rowid > o.rowid
}

// add folds one row in. Adding a row that is already there changes nothing,
// so a refresh that failed half way can be run again.
func (e *ledgerEntry) add(r ledgerRow, at string) {
	if r.attested == 1 && (!e.set || at > e.last) {
		e.last, e.set = at, true
	}
	i := 0
	for i < len(e.top) && e.top[i].newer(r) {
		i++
	}
	if i < len(e.top) && e.top[i].rowid == r.rowid {
		return
	}
	if i >= recentEndorsements {
		return
	}
	e.top = append(e.top, ledgerRow{})
	copy(e.top[i+1:], e.top[i:])
	e.top[i] = r
	if len(e.top) > recentEndorsements {
		e.top = e.top[:recentEndorsements]
	}
}

// refresh folds in every assignment stored since the last refresh. The
// caller holds l.mu.
func (l *endorsementLedger) refresh(ctx context.Context, db *sql.DB) error {
	var hi int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid), 0) FROM assignments`).Scan(&hi); err != nil {
		return err
	}
	if hi < l.upTo || l.vals == nil {
		// Fewer rows than already read: the table is not the one this
		// ledger was built from. Start again.
		l.upTo, l.vals = 0, map[string]*ledgerEntry{}
	}
	if hi == l.upTo {
		return nil
	}
	rows, err := db.QueryContext(ctx, `SELECT a.rowid, a.validator_address, a.attested,
			p.settlement_height, p.settlement_tx_index, p.settlement_time
		FROM assignments a JOIN publications p ON p.promise_hash = a.promise_hash
		WHERE a.rowid > ? AND a.rowid <= ? AND `+recentPopulationSQL, l.upTo, hi)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var addr, at string
		var r ledgerRow
		if err := rows.Scan(&r.rowid, &addr, &r.attested, &r.height, &r.txIndex, &at); err != nil {
			return err
		}
		e := l.vals[addr]
		if e == nil {
			e = &ledgerEntry{}
			l.vals[addr] = e
		}
		e.add(r, at)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	l.upTo = hi
	return nil
}

// fill brings the ledger up to date and sets Recent and LastEndorsedAt on
// out, for one validator when only is set.
func (l *endorsementLedger) fill(ctx context.Context, db *sql.DB, only string, out map[string]signingStats) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.refresh(ctx, db); err != nil {
		return err
	}
	for addr, e := range l.vals {
		if only != "" && addr != only {
			continue
		}
		st := out[addr]
		st.Recent = recentEndorsement{Assigned: int64(len(e.top))}
		for _, r := range e.top {
			st.Recent.Endorsed += r.attested
		}
		if e.set {
			v := e.last
			st.LastEndorsedAt = &v
		}
		out[addr] = st
	}
	return nil
}

// fillSigning sets Signing on every row validatorRows built. A validator with
// no assigned promise in the window keeps the zero value, whose rate has a
// null value: nothing to say, not zero per cent.
func (s *Server) fillSigning(ctx context.Context, win Window, only string, byAddr map[string]*validatorRow) error {
	sig, err := s.signingByValidator(ctx, win, only)
	if err != nil {
		return err
	}
	for addr, v := range byAddr {
		st, ok := sig[addr]
		if !ok {
			st.Rate = rate(0, 0)
		}
		v.Signing = st
	}
	return nil
}

// ---- network: the distribution of signatures collected per promise ----

// signingBucket is one bar of the distribution: promises whose verified
// signatures cover a share of total voting power in [From, To). The last
// bucket is the closed point 1 (every validator's signature verified).
type signingBucket struct {
	Key   string  `json:"key"`
	Label string  `json:"label"`
	From  float64 `json:"from"`
	To    float64 `json:"to"`
	// AboveThreshold is false only for the bucket below the chain's quorum.
	AboveThreshold bool  `json:"above_threshold"`
	Count          int64 `json:"count"`
}

// signingResponse is /v1/signing. A promise's share is the voting power
// whose signature over it verified, over the total voting power of the set
// at the promise height. Publishers stop collecting at the quorum, so mass
// just above two thirds is the protocol working, not validators failing;
// an unsigned validator is unproven, never at fault.
type signingResponse struct {
	Window Window `json:"window"`
	// Threshold is the quorum the chain checks: attested_voting_power >=
	// floor(total_voting_power * num / den), as x/fibre checks it when the
	// transaction settles (keeper/msg_server.go,
	// fibre/validator/signature_set.go).
	Threshold struct {
		Num int64 `json:"num"`
		Den int64 `json:"den"`
	} `json:"threshold"`
	// Promises is every settled promise in the window whose signatures were
	// verified and whose validator set has voting power: the histogram's
	// population. Unknown is the settled promises recorded before signatures
	// were verified, outside it.
	Promises int64 `json:"promises"`
	Unknown  int64 `json:"unknown"`
	// MeetsThreshold is the promises whose verified signatures reach the
	// quorum, over Promises. Below 1 does not mean the chain accepted a
	// promise without quorum: the chain stops verifying at the quorum and
	// this observer re-verifies every entry against its own reading of the
	// validator set, so a promise below the line here is one whose
	// signatures this observer could not all match. It is published because
	// it is a statement about the observer's view, not hidden because it is
	// awkward.
	MeetsThreshold Rate            `json:"meets_threshold"`
	Buckets        []signingBucket `json:"buckets"`
	// SignersMedian is the median, over the population, of how many assigned
	// validators' signatures verified on a promise (attested_with_rows): how
	// many validators it took to close the quorum. Null when it is empty.
	SignersMedian *int64 `json:"signers_median"`
	AsOfNote      string `json:"as_of_note,omitempty"`
	ComputedAt    string `json:"computed_at"`
}

// The bucket edges, as fractions of total voting power. The first edge is the
// chain's quorum, applied as the chain applies it (integer floor); the rest
// are plain fractions compared exactly in integers, so no promise lands in a
// bucket because of float rounding.
var signingEdges = []struct {
	key, label string
	num, den   int64
}{
	{"q_70", "⅔ – 70%", 70, 100},
	{"70_75", "70 – 75%", 75, 100},
	{"75_80", "75 – 80%", 80, 100},
	{"80_90", "80 – 90%", 90, 100},
	{"90_100", "90 – <100%", 1, 1},
}

func (s *Server) computeSigning(ctx context.Context, win Window) (*signingResponse, error) {
	db := s.st.DB()
	resp := &signingResponse{Window: win}
	resp.Threshold.Num, resp.Threshold.Den = 2, 3
	if win.AsOf {
		resp.AsOfNote = AsOfNote
	}

	// One pass, every bucket a SUM of an exact integer comparison.
	// attested_voting_power is NULL on a record from before verification;
	// such a row matches none of the CASEs below except the unknown one.
	sel := `COALESCE(SUM(CASE WHEN p.attested_voting_power IS NULL THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN p.attested_voting_power IS NOT NULL AND p.total_voting_power > 0 THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN p.total_voting_power > 0 AND p.attested_voting_power < p.total_voting_power * 2 / 3 THEN 1 ELSE 0 END), 0)`
	lower := "p.attested_voting_power >= p.total_voting_power * 2 / 3"
	for _, e := range signingEdges {
		sel += `,
		COALESCE(SUM(CASE WHEN p.total_voting_power > 0 AND ` + lower + ` AND p.attested_voting_power * ? < p.total_voting_power * ? THEN 1 ELSE 0 END), 0)`
		lower = "p.attested_voting_power * " + strconv.FormatInt(e.den, 10) + " >= p.total_voting_power * " + strconv.FormatInt(e.num, 10)
	}
	sel += `,
		COALESCE(SUM(CASE WHEN p.total_voting_power > 0 AND p.attested_voting_power >= p.total_voting_power THEN 1 ELSE 0 END), 0)`

	args := []any{}
	for _, e := range signingEdges {
		args = append(args, e.den, e.num)
	}
	args = append(args, win.startArg(), win.endArg())

	counts := make([]int64, len(signingEdges)+2) // below, the edges, full
	dest := []any{&resp.Unknown, &resp.Promises}
	for i := range counts {
		dest = append(dest, &counts[i])
	}
	if err := db.QueryRowContext(ctx, `SELECT `+sel+` FROM publications p WHERE `+signingPopulation, args...).Scan(dest...); err != nil {
		return nil, err
	}

	third := 2.0 / 3.0
	resp.Buckets = append(resp.Buckets, signingBucket{Key: "below", Label: "below ⅔", From: 0, To: third, Count: counts[0]})
	from := third
	for i, e := range signingEdges {
		to := float64(e.num) / float64(e.den)
		resp.Buckets = append(resp.Buckets, signingBucket{Key: e.key, Label: e.label, From: from, To: to, AboveThreshold: true, Count: counts[i+1]})
		from = to
	}
	resp.Buckets = append(resp.Buckets, signingBucket{Key: "all", Label: "100%", From: 1, To: 1, AboveThreshold: true, Count: counts[len(counts)-1]})
	resp.MeetsThreshold = rate(resp.Promises-counts[0], resp.Promises)

	// The median number of assigned signers: how many validators it took. With
	// an even population it is the lower of the two middle values, so it is
	// always a count some promise actually had.
	if resp.Promises > 0 {
		var med int64
		err := db.QueryRowContext(ctx, `SELECT p.attested_with_rows FROM publications p
			WHERE `+signingPopulation+` AND p.attested_voting_power IS NOT NULL AND p.attested_with_rows IS NOT NULL AND p.total_voting_power > 0
			ORDER BY p.attested_with_rows LIMIT 1 OFFSET ?`, win.startArg(), win.endArg(), (resp.Promises-1)/2).Scan(&med)
		switch {
		case err == nil:
			resp.SignersMedian = &med
		case !errors.Is(err, sql.ErrNoRows):
			return nil, err
		}
	}
	return resp, nil
}

// signingCache keeps the unpinned answer per window for signingTTL. The
// query is one pass over the window's publications, cheap next to the probe
// aggregates, but the page it feeds polls every thirty seconds. Usable as its
// zero value.
type signingCache struct {
	mu      sync.Mutex
	entries map[string]signingEntry
}

type signingEntry struct {
	resp *signingResponse
	at   time.Time
}

const signingTTL = 60 * time.Second

func (s *Server) handleSigning(w http.ResponseWriter, r *http.Request) {
	win, err := parseWindow(r, time.Now())
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if win.AsOf {
		// Rationed like every other pinned computation, and never cached.
		if !s.asOf.allow(time.Now()) {
			w.Header().Set("Retry-After", "2")
			writeErr(w, 429, "as_of requests are limited to one every two seconds")
			return
		}
		if !s.asOf.enter() {
			w.Header().Set("Retry-After", "5")
			writeErr(w, 429, "as_of computations already in flight; try again shortly")
			return
		}
		defer s.asOf.leave()
		resp, err := s.computeSigning(r.Context(), win)
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		resp.ComputedAt = time.Now().UTC().Format(time.RFC3339Nano)
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, 200, resp)
		return
	}
	c := &s.signing
	c.mu.Lock()
	e, ok := c.entries[win.Name]
	c.mu.Unlock()
	if !ok || time.Since(e.at) >= signingTTL {
		resp, err := s.computeSigning(r.Context(), win)
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		e = signingEntry{resp: resp, at: time.Now()}
		resp.ComputedAt = e.at.UTC().Format(time.RFC3339Nano)
		c.mu.Lock()
		if c.entries == nil {
			c.entries = map[string]signingEntry{}
		}
		c.entries[win.Name] = e
		c.mu.Unlock()
	}
	writeJSON(w, 200, e.resp)
}
