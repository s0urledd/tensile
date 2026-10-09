package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
)

// The failed blob payments of the Blobs list (/v1/blobs?include_failed=1):
// the transactions that carried a MsgPayForFibre, at the top level or one
// level inside a MsgExec, and failed in their block. Each settled nothing,
// so the list's default answer, every count, rollup and figure leave them
// out. A reader who asks for them gets them among the blobs, in the blobs'
// own order (newest first by height, then by transaction index), one row per
// failed inclusion, paged with the blobs by limit and offset, and counted in
// total.
//
// A row is what the scanner's record of the failure says (failed_txs,
// migration 30): where the transaction stood, what its first
// MsgPayForFibre's promise named (firstOfKind: its namespace, publisher and
// hash, the publisher left out when the message's strings were cut, as
// /v1/txs/{hash} leaves it), the chain's code, failedtx.Explain's reason,
// and whether the failure is final. Nothing it would have moved: no blob,
// size, fee, endorsement or reading.
//
// failed_txs has no index by height, and its rows are few beside the
// blobs, so the API keeps the failed payments in memory (failedPays): read
// once, then only the rows stored since, by rowid. The collector is the one
// writer and only inserts (ON CONFLICT DO NOTHING, no update, no delete), so
// rowids only grow and every row at or below the highest one a read sees
// was committed before it. A request filters them in memory, then places
// the few near its page among the blobs by counting, with a bound, the
// blobs that stand before each (blobsBeforeSQL).

// failedPayRow is a failed blob payment as /v1/blobs?include_failed=1 lists
// it among the blobs. Its place has the blob rows' own names, so the list
// keeps one order and one cursor (next_before_height, next_before_tx_index):
// the height and index of the transaction that failed, its hash, and its
// block's time.
type failedPayRow struct {
	Status      string `json:"status"` // always "failed"
	PromiseHash string `json:"promise_hash,omitempty"`
	Namespace   string `json:"namespace,omitempty"`
	Publisher   string `json:"publisher,omitempty"`
	// The transaction that failed, where a blob row has its settlement.
	SettlementHeight  int64  `json:"settlement_height"`
	SettlementTxIndex int    `json:"settlement_tx_index"`
	SettlementTxHash  string `json:"settlement_tx_hash"`
	SettlementTime    string `json:"settlement_time"`
	// Code and Codespace are what the chain returned; Reason is
	// failedtx.Explain's, absent where it has none.
	Code      uint32 `json:"code"`
	Codespace string `json:"codespace"`
	Reason    string `json:"reason,omitempty"`
	// Final is the record's ante_passed, as /v1/txs/{hash} names it: the
	// fee and every signer's sequence were taken, so the same transaction
	// can never be in a block again.
	Final bool `json:"final"`
	// msgIndex is where its MsgPayForFibre stands in its transaction: its
	// own index, or the MsgExec's that carries it (pubtxs.go lists it so)
	msgIndex int
}

// failedPayOf is r as a failed blob payment, ok false when it carries no
// MsgPayForFibre.
func failedPayOf(r failedtx.Record) (failedPayRow, bool) {
	m, _ := firstOfKind(r.Messages, failedtx.KindSettlement)
	if m == nil {
		return failedPayRow{}, false
	}
	row := failedPayRow{Status: "failed", Code: r.Code, Codespace: r.Codespace, Reason: failedtx.Explain(r).Reason, Final: r.AntePassed,
		msgIndex: topIndexOf(r.Messages, m)}
	if d := m.Detail; d != nil {
		row.PromiseHash, row.Namespace = d.PromiseHash, d.Namespace
		if !m.Cut {
			row.Publisher = d.Publisher
		}
	}
	return row, true
}

// standsBefore reports whether the place (h1, i1) comes before (h2, i2) in
// the list's order, newest first.
func standsBefore(h1 int64, i1 int, h2 int64, i2 int) bool {
	return h1 > h2 || (h1 == h2 && i1 > i2)
}

// failedPays is the record's failed blob payments, newest first, kept in
// memory and read on from the highest failed_txs rowid already read. The
// zero value is ready to use.
type failedPays struct {
	mu   sync.Mutex
	upTo int64
	rows []failedPayRow
	// faults are the rows whose record does not decode: none is listed,
	// and each is noted as met whenever the list is read with them
	// (failedPaysFor), as a lookup of its hash notes it.
	faults []*errRowFault
}

// The statements failedPays reads failed_txs with: its highest rowid, and
// the rows above one rowid up to another, sought by the rowid itself, in
// its order.
const (
	failedTxsTopSQL   = `SELECT COALESCE(MAX(rowid), 0) FROM failed_txs`
	failedTxsSinceSQL = `SELECT tx_hash, height, tx_index, time, raw_json FROM failed_txs WHERE rowid > ? AND rowid <= ? ORDER BY rowid`
)

// refresh reads the rows stored since the last refresh. A table whose
// highest rowid is below the one already read is not the table these rows
// came from, and is read again whole. The caller holds f.mu.
func (f *failedPays) refresh(ctx context.Context, db *sql.DB) error {
	var hi int64
	if err := db.QueryRowContext(ctx, failedTxsTopSQL).Scan(&hi); err != nil {
		return err
	}
	if hi < f.upTo {
		f.upTo, f.rows, f.faults = 0, nil, nil
	}
	if hi == f.upTo {
		return nil
	}
	q, err := db.QueryContext(ctx, failedTxsSinceSQL, f.upTo, hi)
	if err != nil {
		return err
	}
	defer q.Close()
	// kept aside until every row is read, so a read that fails half way
	// leaves the rows as they were, to be read again from the same rowid
	var rows []failedPayRow
	var faults []*errRowFault
	for q.Next() {
		var hash, at, raw string
		var height int64
		var txIndex int
		if err := q.Scan(&hash, &height, &txIndex, &at, &raw); err != nil {
			return err
		}
		var r failedtx.Record
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			fault, ok := isRowFault(rowFault(ctx, hash, "failed transaction", err))
			if !ok {
				return err
			}
			faults = append(faults, fault)
			continue
		}
		row, ok := failedPayOf(r)
		if !ok {
			continue
		}
		row.SettlementHeight, row.SettlementTxIndex, row.SettlementTxHash, row.SettlementTime = height, txIndex, strings.ToLower(hash), at
		rows = append(rows, row)
	}
	if err := q.Err(); err != nil {
		return err
	}
	if len(rows) > 0 {
		f.rows = append(f.rows, rows...)
		sort.SliceStable(f.rows, func(i, j int) bool {
			return standsBefore(f.rows[i].SettlementHeight, f.rows[i].SettlementTxIndex, f.rows[j].SettlementHeight, f.rows[j].SettlementTxIndex)
		})
	}
	f.faults = append(f.faults, faults...)
	f.upTo = hi
	return nil
}

// warmFailedPays reads the failed blob payments once, at the start, so the
// first reader of the list does not wait for it. A row that does not decode
// is noted only when the list is read.
func (s *Server) warmFailedPays(ctx context.Context) error {
	s.failedPays.mu.Lock()
	defer s.failedPays.mu.Unlock()
	return s.failedPays.refresh(ctx, s.st.DB())
}

// blobsAsk is a /v1/blobs request as handleBlobs read it: the WHERE term
// over publications and its arguments, and the filters it was built from.
type blobsAsk struct {
	where         string
	args          []any
	limit, offset int
	namespace     string // lower case, "" for none
	publisher     string // "" for none
	// before is before_height and before_tx_index, nil without the cursor
	before *[2]int64
}

// failedPaysFor is the failed blob payments the list's filters select,
// newest first: in the namespace and paid for by the publisher when they
// are set, and before the cursor when it is.
func (s *Server) failedPaysFor(ctx context.Context, a blobsAsk) ([]failedPayRow, error) {
	f := &s.failedPays
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.refresh(ctx, s.st.DB()); err != nil {
		return nil, err
	}
	for _, fault := range f.faults {
		s.noteRowFault(fault)
	}
	var out []failedPayRow
	for _, r := range f.rows {
		if a.namespace != "" && strings.ToLower(r.Namespace) != a.namespace {
			continue
		}
		if a.publisher != "" && strings.ToLower(r.Publisher) != a.publisher {
			continue
		}
		if a.before != nil && !standsBefore(a.before[0], int(a.before[1]), r.SettlementHeight, r.SettlementTxIndex) {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

// blobsBeforeSQL counts, up to a bound, the blobs where selects that stand
// before a failed payment at (height, tx index): settled at a greater
// height, or at the same height with a greater index. The height is a range
// of publications_settlement, and the bound keeps the count to the rows a
// page needs, never the whole of a deep list. Its arguments: where's, the
// height twice, the index, the bound.
func blobsBeforeSQL(where string) string {
	cond := `settlement_height >= ? AND (settlement_height > ? OR settlement_tx_index > ?)`
	if where != "" {
		cond = where + " AND " + cond
	}
	return `SELECT COUNT(*) FROM (SELECT 1 FROM publications WHERE ` + cond + ` LIMIT ?)`
}

// failedOnPage places fails (newest first, the filters' selection) among the
// blobs a selects: k is how many of them stand before the page's first row,
// m how many stand on the page. A failure's place is its index among the
// failures plus the blobs before it, which grows with the index: k is found
// by halving, m by counting on from k.
func (s *Server) failedOnPage(ctx context.Context, a blobsAsk, fails []failedPayRow) (k, m int, err error) {
	q := blobsBeforeSQL(a.where)
	// ahead reports whether failure j stands before the at-th row of the
	// list: fewer than at-j blobs stand before it
	ahead := func(j, at int) (bool, error) {
		room := at - j
		if room <= 0 {
			return false, nil
		}
		f := fails[j]
		var n int
		args := append(append([]any{}, a.args...), f.SettlementHeight, f.SettlementHeight, f.SettlementTxIndex, room)
		if err := s.st.DB().QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
			return false, err
		}
		return n < room, nil
	}
	lo, hi := 0, min(len(fails), a.offset)
	for lo < hi {
		mid := (lo + hi) / 2
		in, err := ahead(mid, a.offset)
		if err != nil {
			return 0, 0, err
		}
		if in {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	k = lo
	for j := k; j < len(fails); j++ {
		in, err := ahead(j, a.offset+a.limit)
		if err != nil {
			return 0, 0, err
		}
		if !in {
			break
		}
		m++
	}
	return k, m, nil
}

// blobsWithFailed answers /v1/blobs?include_failed=1: the blobs a selects
// and the failed blob payments its filters select, as one list in the
// blobs' order, paged by limit and offset over both. Every blob row carries
// status "success" and every failure "failed"; total counts both, and
// failed_total the failures among them. A page holding a failure that is not
// final is not cached: as on /v1/txs/{hash}, its transaction could still
// take effect in a later block.
func (s *Server) blobsWithFailed(w http.ResponseWriter, r *http.Request, a blobsAsk) {
	ctx := r.Context()
	fails, err := s.failedPaysFor(ctx, a)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	k, m, err := s.failedOnPage(ctx, a, fails)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	blobs, err := s.blobRowsAt(ctx, a.where, a.limit-m, a.offset-k, a.args...)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	blobs, more := trim(blobs, a.limit-m)
	var total int64
	countQ := `SELECT COUNT(*) FROM publications`
	if a.where != "" {
		countQ += " WHERE " + a.where
	}
	if err := s.st.DB().QueryRowContext(ctx, countQ, a.args...).Scan(&total); err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	// the page: its blobs and its failures, merged by place; a failure
	// stands before a blob of the same place, as failedOnPage counts it
	here := fails[k : k+m]
	page := make([]any, 0, len(blobs)+len(here))
	final := true
	var lastH int64
	var lastI int
	for i, j := 0, 0; i < len(blobs) || j < len(here); {
		if j < len(here) && (i == len(blobs) || !standsBefore(blobs[i].SettlementHeight, blobs[i].SettlementTxIndex, here[j].SettlementHeight, here[j].SettlementTxIndex)) {
			f := here[j]
			page, final, lastH, lastI = append(page, f), final && f.Final, f.SettlementHeight, f.SettlementTxIndex
			j++
			continue
		}
		b := blobs[i]
		b.CreationTimestamp, b.Status = "", "success"
		page, lastH, lastI = append(page, b), b.SettlementHeight, b.SettlementTxIndex
		i++
	}
	truncated := more || k+m < len(fails)
	out := map[string]any{"blobs": page, "limit": a.limit, "offset": a.offset, "total": total + int64(len(fails)), "failed_total": len(fails),
		"truncated": truncated, "namespace": a.namespace}
	if a.publisher != "" {
		out["publisher"] = a.publisher
	}
	if truncated && len(page) > 0 {
		out["next_before_height"], out["next_before_tx_index"] = lastH, lastI
	}
	if !final {
		w.Header().Set("Cache-Control", "no-store")
	}
	writeJSON(w, 200, out)
}
