package api

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// A validator's Fibre endpoint registrations on chain, newest first, as
// /v1/validators/{addr} carries them (endpoint_history): every registration
// the scanner read from the chain's events (host_history.jsonl), the
// endpoint it had when Tensile's record began, a change a scan gap hid, and
// the final failed registrations its current operator address signed at the
// top level of a transaction (failed_tx_msgs, the lists' rule). A failed one
// changed nothing.
//
// A row of a transaction carries its hash when the scanner recorded what it
// cost (tx_costs, at the event's tx index), so the page opens the
// transaction's own page from it; one recorded before cost lines has none.

// The outcomes of an endpoint_history row; registered, changed and same are
// also a successful registration's action on /v1/txs/{hash}.
const (
	endpointRegistered   = "registered"    // the validator's first endpoint
	endpointChanged      = "changed"       // another endpoint than the one before
	endpointSame         = "same"          // the endpoint it had, registered again
	endpointFailed       = "failed"        // a final failed registration its operator signed
	endpointBeforeRecord = "before_record" // the endpoint it had when Tensile's record began
	endpointAfterGap     = "after_gap"     // registered while Tensile's record had a gap
)

// endpointHistoryMax is how many rows other than before_record an answer
// carries: the newest. The answer has no older page.
const endpointHistoryMax = 50

// endpointEvent is one endpoint_history row.
type endpointEvent struct {
	Outcome      string     `json:"outcome"` // registered | changed | same | failed | before_record | after_gap
	Height       int64      `json:"height,omitempty"`
	TxIndex      *int       `json:"tx_index,omitempty"`
	Time         *time.Time `json:"time,omitempty"`
	TxHash       string     `json:"tx_hash,omitempty"`
	Host         string     `json:"host"`
	PreviousHost string     `json:"previous_host,omitempty"`
	First        bool       `json:"first,omitempty"`     // after_gap only: the validator had no endpoint before the gap
	Attempted    string     `json:"attempted,omitempty"` // failed only: change | registration
	Reason       string     `json:"reason,omitempty"`    // failed only: failedtx.Explain
}

// hostStep is one host_events row of a validator, oldest first, with the
// hash of the transaction an event came from when its cost line is on
// record.
type hostStep struct {
	FromHeight  int64
	FromTxIndex int
	Host        string
	Source      string
	Time        time.Time
	TxHash      string
}

// hostWalkSQL is a validator's registrations oldest first
// (sqlite_autoindex_host_events_1 gives the order), each event's
// transaction hash joined from the cost line at its tx index
// (from_tx_index − 1).
const hostWalkSQL = `SELECT h.from_height, h.from_tx_index, h.host, h.source, h.time, COALESCE(c.tx_hash, '')
	FROM host_events h
	LEFT JOIN tx_costs c ON h.source = 'event' AND c.height = h.from_height AND c.tx_index = h.from_tx_index - 1
	WHERE h.cons_address = ?
	ORDER BY h.from_height, h.from_tx_index`

// failedSetHostsSQL is the failed registrations an operator address signed
// at the top level of a final failure, newest first, with the failure's
// record: failed_tx_msgs holds only those messages (store.InsertFailedTx).
const failedSetHostsSQL = `SELECT m.height, m.tx_index, m.msg_index, f.tx_hash, f.raw_json
	FROM failed_tx_msgs m JOIN failed_txs f ON f.dedupe_key = m.dedupe_key
	WHERE m.account = ? AND m.type_url = '/celestia.valaddr.v1.MsgSetFibreProviderInfo'
	ORDER BY m.height DESC, m.tx_index DESC, m.msg_index DESC`

// hostWalk is cons's registrations, oldest first.
func (s *Server) hostWalk(ctx context.Context, cons string) ([]hostStep, error) {
	rows, err := s.q(ctx).QueryContext(ctx, hostWalkSQL, cons)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []hostStep
	for rows.Next() {
		var st hostStep
		var at string
		if err := rows.Scan(&st.FromHeight, &st.FromTxIndex, &st.Host, &st.Source, &at, &st.TxHash); err != nil {
			return nil, err
		}
		st.Time, st.TxHash = parseTS(at), strings.ToLower(st.TxHash)
		out = append(out, st)
	}
	return out, rows.Err()
}

// failedSetHost is one failed registration transaction of an operator.
type failedSetHost struct {
	Height  int64
	TxIndex int
	Time    time.Time
	TxHash  string
	Host    string // what it asked for: its message's host
	Reason  string // failedtx.Explain's
}

// failedSetHosts are op's failed registrations, newest first, one per
// transaction: the one of its messages with the lowest index. A record that
// does not decode is a row fault and left out.
func (s *Server) failedSetHosts(ctx context.Context, op string) ([]failedSetHost, error) {
	type msgRow struct {
		height          int64
		txIndex, msgIdx int
		hash, raw       string
	}
	rows, err := s.q(ctx).QueryContext(ctx, failedSetHostsSQL, op)
	if err != nil {
		return nil, err
	}
	var txs []msgRow
	for rows.Next() {
		var m msgRow
		if err := rows.Scan(&m.height, &m.txIndex, &m.msgIdx, &m.hash, &m.raw); err != nil {
			rows.Close()
			return nil, err
		}
		// a transaction's rows are together, its lowest index last
		if n := len(txs); n > 0 && txs[n-1].height == m.height && txs[n-1].txIndex == m.txIndex {
			txs[n-1] = m
			continue
		}
		txs = append(txs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]failedSetHost, 0, len(txs))
	for _, m := range txs {
		var r failedtx.Record
		if err := json.Unmarshal([]byte(m.raw), &r); err != nil {
			if f, ok := isRowFault(rowFault(ctx, strings.ToLower(m.hash), "failed transaction", err)); ok {
				s.noteRowFault(f)
				continue
			}
			return nil, err
		}
		f := failedSetHost{Height: m.height, TxIndex: m.txIndex, Time: r.Time.UTC(), TxHash: strings.ToLower(r.TxHash), Reason: failedtx.Explain(r).Reason}
		for _, msg := range r.Messages {
			if msg.Index == m.msgIdx && msg.Detail != nil {
				f.Host = msg.Detail.Host
				break
			}
		}
		out = append(out, f)
	}
	return out, nil
}

// endpointHistory is addr's endpoint_history: its registrations, and the
// failed ones its current operator address signed (a superseded consensus
// key has none, so it lists none). more is true when rows past the newest
// endpointHistoryMax were left out.
func (s *Server) endpointHistory(ctx context.Context, addr string) ([]endpointEvent, bool, error) {
	steps, err := s.hostWalk(ctx, addr)
	if err != nil {
		return nil, false, err
	}
	ops, err := s.operatorAddrs(ctx)
	if err != nil {
		return nil, false, err
	}
	var fails []failedSetHost
	if op := ops[addr]; op != "" {
		if fails, err = s.failedSetHosts(ctx, op); err != nil {
			return nil, false, err
		}
	}
	rows, more := endpointRows(steps, fails)
	return rows, more, nil
}

// eventOutcome is the walk's word for an event registering host after a
// step that left prev (prevSet false before the first step).
func eventOutcome(prevSet bool, prev, host string) string {
	switch {
	case !prevSet || prev == "":
		return endpointRegistered
	case host == prev:
		return endpointSame
	default:
		return endpointChanged
	}
}

// walkEventAt is the walk's word for the event at (height, fromTxIndex):
// the host it registered and the one before it ("" for a registration).
func walkEventAt(steps []hostStep, height int64, fromTxIndex int) (outcome, host, previous string, ok bool) {
	prev, prevSet := "", false
	for _, st := range steps {
		if st.Source == scan.HostFromEvent && st.FromHeight == height && st.FromTxIndex == fromTxIndex {
			outcome = eventOutcome(prevSet, prev, st.Host)
			if outcome == endpointRegistered {
				prev = ""
			}
			return outcome, st.Host, prev, true
		}
		prev, prevSet = st.Host, true
	}
	return "", "", "", false
}

// hostAt is the host in force at the transaction at (height, txIndex): the
// newest step at or before it, "" when there is none.
func hostAt(steps []hostStep, height int64, txIndex int) string {
	host := ""
	for _, st := range steps {
		if st.FromHeight > height || (st.FromHeight == height && st.FromTxIndex > txIndex) {
			break
		}
		host = st.Host
	}
	return host
}

// attemptedOf is what a failed registration tried, by the host in force at
// its block: a change of an endpoint, or a first registration.
func attemptedOf(hostAtBlock string) string {
	if hostAtBlock != "" {
		return "change"
	}
	return "registration"
}

// endpointRows is endpoint_history from a validator's walk and its failed
// registrations: newest first by (height, tx index), an after_gap at its
// height before any transaction of that block, and the endpoint it had
// when the record began last. At most endpointHistoryMax rows other than
// that one; more says rows were left out.
//
// The walk: an event registers (no endpoint before it), changes or repeats
// the endpoint; a reseed whose host is not the one before it is a
// registration a scan gap hid (first when there was none before the gap);
// the first step, when it is no event and no such reseed, is the endpoint
// the record began with; every other step only says what was in force.
func endpointRows(steps []hostStep, fails []failedSetHost) ([]endpointEvent, bool) {
	var rows []endpointEvent
	var before *endpointEvent
	prev, prevSet := "", false
	for i, st := range steps {
		switch {
		case st.Source == scan.HostFromEvent:
			ti, t := st.FromTxIndex-1, st.Time
			e := endpointEvent{Outcome: eventOutcome(prevSet, prev, st.Host), Height: st.FromHeight, TxIndex: &ti, Time: &t,
				TxHash: st.TxHash, Host: st.Host}
			if e.Outcome == endpointChanged {
				e.PreviousHost = prev
			}
			rows = append(rows, e)
		case st.Source == scan.HostFromReseed && st.Host != "" && (!prevSet || st.Host != prev):
			e := endpointEvent{Outcome: endpointAfterGap, Height: st.FromHeight, Host: st.Host}
			if prev != "" {
				e.PreviousHost = prev
			} else {
				e.First = true
			}
			rows = append(rows, e)
		case i == 0 && st.Host != "":
			before = &endpointEvent{Outcome: endpointBeforeRecord, Host: st.Host}
		}
		prev, prevSet = st.Host, true
	}
	for _, f := range fails {
		ti, t := f.TxIndex, f.Time
		rows = append(rows, endpointEvent{Outcome: endpointFailed, Height: f.Height, TxIndex: &ti, Time: &t, TxHash: f.TxHash,
			Host: f.Host, Attempted: attemptedOf(hostAt(steps, f.Height, f.TxIndex)), Reason: f.Reason})
	}
	at := func(e endpointEvent) (int64, int) {
		if e.TxIndex == nil {
			return e.Height, -1
		}
		return e.Height, *e.TxIndex
	}
	sort.SliceStable(rows, func(i, j int) bool {
		hi, ti := at(rows[i])
		hj, tj := at(rows[j])
		if hi != hj {
			return hi > hj
		}
		return ti > tj
	})
	more := len(rows) > endpointHistoryMax
	if more {
		rows = rows[:endpointHistoryMax]
	}
	if before != nil {
		rows = append(rows, *before)
	}
	return rows, more
}
