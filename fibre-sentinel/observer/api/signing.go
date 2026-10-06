package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
	if e := epochOf(ctx); e != nil && partsFor(win) {
		// The ledger's whole days and the partial days at the ends
		// (signingWindow).
		out, err := s.signingWindow(ctx, e, win, only)
		if err == nil {
			return out, s.recentSigning(ctx, only, out)
		}
		if !errors.Is(err, errNoParts) {
			return nil, err
		}
	}
	filter, args := "", []any{win.startArg(), win.endArg()}
	if only != "" {
		filter = ` AND a.validator_address = ?`
		args = append(args, only)
	}
	rows, err := s.q(ctx).QueryContext(ctx, signingByValidatorSQL(filter), args...)
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
// brought up to date first. Within a comparison of the two ways a figure is
// computed, both get one reading of it (ledgerOnce).
func (s *Server) recentSigning(ctx context.Context, only string, out map[string]signingStats) error {
	if once, ok := ctx.Value(ledgerOnceKey{}).(*ledgerOnce); ok {
		return once.fill(ctx, s, only, out)
	}
	return s.recent.fill(ctx, s.st.DB(), only, out)
}

// ledgerOnce keeps what the endorsement ledger answered through one
// comparison of the partials with the shipped statements (comparePaths).
// The ledger is a cache of the whole record, read outside the comparison's
// read transaction (readtx.go), and the same code both ways. Read once per
// way, a publication stored between the two computations moved the
// second one's newest endorsements alone, and the audit took that for
// partials that are not what the store holds (mocha, 2026-10-06 15:09: a
// blob settled at 15:09:02 was stored at 15:09:09.110, between the shipped
// computation and the partials'; every validator that endorsed it differed).
// Both ways now get the first reading, per validator asked for.
type ledgerOnce struct {
	mu  sync.Mutex
	got map[string]map[string]signingStats // by only: Recent and LastEndorsedAt, as the ledger set them
}

type ledgerOnceKey struct{}

// withLedgerOnce makes the computations under ctx share one reading of the
// ledger.
func withLedgerOnce(ctx context.Context) context.Context {
	return context.WithValue(ctx, ledgerOnceKey{}, &ledgerOnce{got: map[string]map[string]signingStats{}})
}

// fill is endorsementLedger.fill, the ledger read the first time only.
func (o *ledgerOnce) fill(ctx context.Context, s *Server, only string, out map[string]signingStats) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	got, ok := o.got[only]
	if !ok {
		got = map[string]signingStats{}
		if err := s.recent.fill(ctx, s.st.DB(), only, got); err != nil {
			return err
		}
		o.got[only] = got
	}
	for addr, g := range got {
		st := out[addr]
		st.Recent = g.Recent
		if g.LastEndorsedAt != nil {
			v := *g.LastEndorsedAt
			st.LastEndorsedAt = &v
		}
		out[addr] = st
	}
	return nil
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
// With file set it is kept across restarts (derived.go): read back before
// the first computation uses it, checked row by row against the store, and
// written again after a refresh that moved it, at most every
// ledgerSaveEvery. A restart then folds in the assignments stored since the
// last write, as a refresh does.
//
// The file names the store the ledger was built from (store), not the store
// as it is when the file is written: a migration between the two may have
// rewritten rows at or below upTo, which the ledger never reads again. A
// write that finds the store at another identity writes nothing and drops
// the ledger, which the next refresh builds again from the store as it is
// now.
//
// The zero value is ready to use.
type endorsementLedger struct {
	mu sync.Mutex
	// upTo is the highest assignments rowid folded in.
	upTo int64
	vals map[string]*ledgerEntry
	// store is the identity of the store the ledger was built from, and the
	// one the file is written under: the file's, when open kept it, or the
	// store's, read before the first row was, when refresh built it from
	// nothing. Only kept with file set.
	store storeIdentity

	// file is where the ledger is kept, or "" for nowhere; opened says it
	// has been read (or found missing, or refused) by this process,
	// savedUpTo and savedAt are the file's upTo and when it was written,
	// and origin says how this process's ledger began, which log (when
	// set) is told.
	file      string
	log       logf
	opened    bool
	savedUpTo int64
	savedAt   time.Time
	origin    string
}

type ledgerEntry struct {
	// top is the newest assigned promises, newest first, at most
	// recentEndorsements of them.
	top []ledgerRow
	// last is the settlement time of the newest endorsement; set says there
	// is one, and lastRow is the assignment it was read from.
	last    string
	set     bool
	lastRow int64
}

type ledgerRow struct {
	height, txIndex, rowid, attested int64
}

// ledgerVersion names how the ledger folds rows in: the order newer puts
// them in, what add keeps (the newest recentEndorsements rows, and the
// newest endorsement by settlement time, the first one read on a tie), and
// which rows refresh hands it. It must be bumped whenever any of them
// changes, so that a file folded the old way is rebuilt rather than caught
// up (ledgerDefinition). TestTheLedgerFoldIsTheOneItsVersionNames holds the
// fold to the version.
const ledgerVersion = 1

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
		e.last, e.set, e.lastRow = at, true, r.rowid
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

// ledgerRowsSQL reads assignments as the ledger folds them in, from a and
// its publication p; the caller appends the WHERE clause.
const ledgerRowsSQL = `SELECT a.rowid, a.validator_address, a.attested,
			p.settlement_height, p.settlement_tx_index, p.settlement_time
		FROM assignments a JOIN publications p ON p.promise_hash = a.promise_hash`

// ledgerCheckSQL reads again, as the ledger folds them in, the assignments
// a file names (?, a JSON list of rowids), each sought by its rowid.
const ledgerCheckSQL = `SELECT a.rowid, a.validator_address, a.attested,
			p.settlement_height, p.settlement_tx_index, p.settlement_time
		FROM json_each(?) j CROSS JOIN assignments a ON a.rowid = j.value JOIN publications p ON p.promise_hash = a.promise_hash
		WHERE ` + recentPopulationSQL

// ledgerSaveEvery is how often a ledger that moved is written out.
const ledgerSaveEvery = time.Minute

// ledgerDefinition is what the ledger is computed with: the rows it reads,
// how many it keeps, and the fold (ledgerVersion).
var ledgerDefinition = definitionOf(ledgerRowsSQL, recentPopulationSQL, strconv.Itoa(recentEndorsements),
	"ledger "+strconv.Itoa(ledgerVersion))

// ledgerFile is the ledger on disk.
type ledgerFile struct {
	derivedHeader
	// UpTo is the highest assignments rowid folded in, and UpToRow that
	// assignment's key.
	UpTo    int64 `json:"up_to"`
	UpToRow struct {
		PromiseHash string `json:"promise_hash"`
		Validator   string `json:"validator_address"`
	} `json:"up_to_row"`
	Validators map[string]ledgerFileEntry `json:"validators"`
}

type ledgerFileEntry struct {
	// Top is ledgerEntry.top, each row as [height, tx index, rowid,
	// attested].
	Top     [][4]int64 `json:"top"`
	Last    *string    `json:"last,omitempty"`
	LastRow int64      `json:"last_row,omitempty"`
}

// open reads the ledger's file the first time the ledger is used, and keeps
// it if the store is still the one it was computed from (derived.go):
// otherwise the ledger is built from every assignment, as it was before
// there were files, and the file is left for its first write to replace.
// An error is a query that failed, and the next computation tries again.
// The caller holds l.mu.
func (l *endorsementLedger) open(ctx context.Context, db *sql.DB) error {
	if l.opened {
		return nil
	}
	if l.file == "" {
		l.opened, l.origin = true, "not kept on disk"
		return nil
	}
	t0 := time.Now()
	sweepDerivedTemps(l.file, t0)
	f, vals, why, err := l.load(ctx, db)
	if err != nil {
		return err
	}
	l.opened = true
	defer func() {
		if l.log != nil {
			l.log("endorsement ledger: %s (%s)", l.origin, time.Since(t0).Round(time.Millisecond))
		}
	}()
	switch {
	case why != "":
		l.origin = "built from the store: " + l.file + " refused: " + string(why)
		return nil
	case vals == nil:
		l.origin = "built from the store: no " + l.file
		return nil
	}
	l.upTo, l.vals, l.store = f.UpTo, vals, f.Store
	l.savedUpTo, l.savedAt = f.UpTo, time.Now()
	l.origin = fmt.Sprintf("loaded %d validators from %s (assignments through rowid %d)", len(vals), l.file, f.UpTo)
	return nil
}

// load reads and checks the file: the ledger when it may be used, a refusal
// when it may not, nothing when there is none.
func (l *endorsementLedger) load(ctx context.Context, db *sql.DB) (ledgerFile, map[string]*ledgerEntry, refusal, error) {
	var f ledgerFile
	ok, why := readDerived(l.file, &f)
	if !ok {
		return f, nil, why, nil
	}
	if why, err := checkHeader(ctx, db, f.derivedHeader, "endorsement-ledger", ledgerDefinition); why != "" || err != nil {
		return f, nil, why, err
	}
	refuse := func(format string, args ...any) (ledgerFile, map[string]*ledgerEntry, refusal, error) {
		return f, nil, refusal(fmt.Sprintf(format, args...)), nil
	}
	if f.UpTo < 0 {
		return refuse("up_to %d", f.UpTo)
	}
	// The mark: the newest assignment folded in must be the same one now,
	// under the same rowid.
	if f.UpTo > 0 {
		var h, v string
		switch err := db.QueryRowContext(ctx, `SELECT promise_hash, validator_address FROM assignments WHERE rowid = ?`, f.UpTo).Scan(&h, &v); {
		case errors.Is(err, sql.ErrNoRows):
			return refuse("the store has no assignment %d: it is older than the file", f.UpTo)
		case err != nil:
			return f, nil, "", err
		case h != f.UpToRow.PromiseHash || v != f.UpToRow.Validator:
			return refuse("assignment %d is another one now", f.UpTo)
		}
	}
	// Every row the ledger publishes is read again: it must still be in
	// the population, the same validator's, with the same height,
	// transaction index and endorsement, and the newest endorsement's
	// settlement time must be what the ledger says.
	vals := map[string]*ledgerEntry{}
	type want struct {
		addr string
		row  ledgerRow
		at   *string
	}
	wants := map[int64][]want{}
	var ids []int64
	for addr, fe := range f.Validators {
		// A validator is in the ledger from its first row on, and has an
		// endorsement once any row it holds was one.
		if len(fe.Top) == 0 || len(fe.Top) > recentEndorsements {
			return refuse("%s keeps %d rows", addr, len(fe.Top))
		}
		e := &ledgerEntry{}
		for i, t := range fe.Top {
			r := ledgerRow{height: t[0], txIndex: t[1], rowid: t[2], attested: t[3]}
			if r.rowid <= 0 || r.rowid > f.UpTo || (i > 0 && !e.top[i-1].newer(r)) {
				return refuse("%s's rows are out of order or past up_to", addr)
			}
			if r.attested == 1 && fe.Last == nil {
				return refuse("%s has an endorsement in its rows and none recorded", addr)
			}
			e.top = append(e.top, r)
			wants[r.rowid] = append(wants[r.rowid], want{addr: addr, row: r})
			ids = append(ids, r.rowid)
		}
		if fe.Last != nil {
			if fe.LastRow <= 0 || fe.LastRow > f.UpTo {
				return refuse("%s's newest endorsement is past up_to", addr)
			}
			e.last, e.set, e.lastRow = *fe.Last, true, fe.LastRow
			wants[fe.LastRow] = append(wants[fe.LastRow], want{addr: addr, row: ledgerRow{rowid: fe.LastRow, attested: 1}, at: fe.Last})
			ids = append(ids, fe.LastRow)
		}
		vals[addr] = e
	}
	list, err := json.Marshal(ids)
	if err != nil {
		return f, nil, "", err
	}
	rows, err := db.QueryContext(ctx, ledgerCheckSQL, string(list))
	if err != nil {
		return f, nil, "", err
	}
	defer rows.Close()
	seen := map[int64]bool{}
	for rows.Next() {
		var addr, at string
		var r ledgerRow
		if err := rows.Scan(&r.rowid, &addr, &r.attested, &r.height, &r.txIndex, &at); err != nil {
			return f, nil, "", err
		}
		if seen[r.rowid] {
			continue // listed twice: in the top rows and as the newest endorsement
		}
		seen[r.rowid] = true
		for _, w := range wants[r.rowid] {
			switch {
			case addr != w.addr:
				return refuse("assignment %d is %s's now, not %s's", r.rowid, addr, w.addr)
			case w.at != nil:
				if r.attested != 1 || at != *w.at {
					return refuse("%s's newest endorsement, assignment %d, is not what the file says", w.addr, r.rowid)
				}
			case r != w.row:
				return refuse("assignment %d of %s is not what the file says", r.rowid, w.addr)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return f, nil, "", err
	}
	for id := range wants {
		if !seen[id] {
			return refuse("assignment %d is no longer one the ledger reads", id)
		}
	}
	return f, vals, "", nil
}

// save writes the ledger out when it has moved since the last write and
// ledgerSaveEvery has passed; force writes whenever it has moved. For a
// store no longer at the identity the ledger was built from nothing is
// written, and the ledger is dropped (drop). The caller holds l.mu.
func (l *endorsementLedger) save(ctx context.Context, db *sql.DB, force bool) error {
	if l.file == "" || !l.opened || l.vals == nil || l.upTo == l.savedUpTo {
		return nil
	}
	if !force && !l.savedAt.IsZero() && time.Since(l.savedAt) < ledgerSaveEvery {
		return nil
	}
	id, err := readStoreIdentity(ctx, db)
	if err != nil {
		return err
	}
	if id != l.store {
		l.drop(id)
		return nil
	}
	f := ledgerFile{UpTo: l.upTo, Validators: make(map[string]ledgerFileEntry, len(l.vals))}
	f.derivedHeader = derivedHeader{Kind: "endorsement-ledger", Format: derivedFormat, Definition: ledgerDefinition, Store: l.store}
	if l.upTo > 0 {
		if err := db.QueryRowContext(ctx, `SELECT promise_hash, validator_address FROM assignments WHERE rowid = ?`, l.upTo).
			Scan(&f.UpToRow.PromiseHash, &f.UpToRow.Validator); err != nil {
			return err
		}
	}
	for addr, e := range l.vals {
		fe := ledgerFileEntry{Top: make([][4]int64, 0, len(e.top))}
		for _, r := range e.top {
			fe.Top = append(fe.Top, [4]int64{r.height, r.txIndex, r.rowid, r.attested})
		}
		if e.set {
			last := e.last
			fe.Last, fe.LastRow = &last, e.lastRow
		}
		f.Validators[addr] = fe
	}
	if err := writeDerived(l.file, f); err != nil {
		return err
	}
	l.savedUpTo, l.savedAt = l.upTo, time.Now()
	return nil
}

// drop forgets the ledger, for a store no longer at the identity it was
// built from (save), which is now id: a migration since may have rewritten
// rows at or below upTo, which refresh never reads again, and a file of it
// would be believed under the identity the store has now. The next refresh
// builds it again from the store, and the next save writes that. The caller
// holds l.mu.
func (l *endorsementLedger) drop(id storeIdentity) {
	l.origin = "built from the store again: " + storeChange(l.store, id)
	l.upTo, l.vals, l.store = 0, nil, storeIdentity{}
	l.savedUpTo, l.savedAt = 0, time.Time{}
	if l.log != nil {
		l.log("endorsement ledger: %s", l.origin)
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
		// Nothing built yet, or fewer rows than already read: the table is
		// not the one this ledger was built from. Start again, under the
		// identity the store has before the first row is read.
		var id storeIdentity
		if l.file != "" {
			var err error
			if id, err = readStoreIdentity(ctx, db); err != nil {
				return err
			}
		}
		l.upTo, l.vals, l.store = 0, map[string]*ledgerEntry{}, id
	}
	if hi == l.upTo {
		return nil
	}
	rows, err := db.QueryContext(ctx, ledgerRowsSQL+` WHERE a.rowid > ? AND a.rowid <= ? AND `+recentPopulationSQL, l.upTo, hi)
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
	if err := l.open(ctx, db); err != nil {
		return err
	}
	if err := l.refresh(ctx, db); err != nil {
		return err
	}
	// Not being able to write the file costs the next start a longer
	// catch-up, not this computation its answer.
	_ = l.save(ctx, db, false)
	if l.vals == nil {
		// save dropped it: the store is not the one it was built from.
		// Built again before it answers.
		if err := l.refresh(ctx, db); err != nil {
			return err
		}
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
	win, err := parseWindow(r, s.now())
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
		resp.ComputedAt = s.now().UTC().Format(time.RFC3339Nano)
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
		resp.ComputedAt = s.now().UTC().Format(time.RFC3339Nano)
		c.mu.Lock()
		if c.entries == nil {
			c.entries = map[string]signingEntry{}
		}
		c.entries[win.Name] = e
		c.mu.Unlock()
	}
	writeJSON(w, 200, e.resp)
}
