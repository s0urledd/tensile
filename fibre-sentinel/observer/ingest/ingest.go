// Package ingest tails the append-only JSONL files written by sentinel-scan
// and sentinel-probe into the observer store. Each file has a byte-offset
// cursor in the store; records are idempotent on their primary keys, so a
// cursor that is behind (or a file that is re-ingested from zero) only costs
// re-reading, never duplicate rows.
package ingest

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// maxLine bounds one JSONL record. A publication with full row lists for 100
// validators is a few hundred KB; 64 MiB leaves a wide margin.
const maxLine = 64 << 20

// Result summarises one ingest pass over a file.
type Result struct {
	Read     int64 // lines read this pass
	Inserted int64 // rows actually inserted (rest were already present)
	Skipped  int64 // undecodable lines stepped over this pass (see ErrBadRecord)
	Offset   int64 // byte offset after the pass
	Line     int64 // line number after the pass
	// LastSkipped is the error of the most recent skipped line, for the log.
	LastSkipped string
	// Deferred names the line the pass stopped before because its row is
	// not in the store yet (ErrRetryLater); empty when the pass read to EOF.
	Deferred string
}

// ErrBadRecord marks a line that cannot be decoded. A torn write (a crash
// mid-record followed by the next record appended after it) leaves exactly
// one such line; stopping at it would stall the file forever, so the tailer
// logs it, counts it and advances past it. Store errors are never wrapped in
// it and still stop the pass.
var ErrBadRecord = errors.New("bad record")

// ErrRetryLater marks a line whose row is not in the store yet (an
// amendment for a probe row that has not been ingested). The pass stops
// before it without advancing the cursor, so the next pass tries again
// once the measurements have caught up; after retryPasses passes the line
// is stepped over like a bad record, so a row that never arrives cannot
// stall the file.
var ErrRetryLater = errors.New("retry later")

const retryPasses = 3

// ErrOtherChain marks a row copied in from another vantage that was not
// taken on this observer's chain (chainGuard). Like a store error it stops
// the file before the row; the collector also holds the export build while
// a file is stopped by it, since the exports copy the vantage files whole.
var ErrOtherChain = errors.New("not from this observer's chain")

// retries counts the passes on which a line has asked to be retried, by
// file and line number.
var retries = map[string]int{}

// handler consumes one raw JSONL line and reports whether it inserted a row.
type handler func(raw []byte) (bool, error)

// tail reads file from the stored cursor to EOF, feeding complete lines to fn
// and persisting the cursor after every line. A trailing partial line (a
// write in progress) is left for the next pass.
func tail(st *store.Store, path string, fn handler, now time.Time) (Result, error) {
	offset, line, err := st.Cursor(path)
	if err != nil {
		return Result{}, err
	}
	// The cursor is a logical offset (internal/record): a file whose older
	// lines were archived keeps every byte at the offset it was written at,
	// its live copy starting at a base past zero. A cursor at or past the
	// base reads the live file; one behind it (a rebuild from scratch, a
	// collector that was down over a rotation) reads the archived segments
	// first. A file never archived has base 0, which is what every cursor
	// already meant.
	s, err := record.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return Result{Offset: offset, Line: line}, nil
	}
	if err != nil {
		return Result{}, err
	}
	defer s.Close()

	if s.End() < offset {
		// The file was truncated or replaced: start over. Idempotent keys
		// make this safe; it just re-reads.
		offset, line = 0, 0
	}
	src, err := s.ReaderFrom(offset)
	if err != nil {
		return Result{}, err
	}

	r := bufio.NewReaderSize(src, 1<<20)
	res := Result{Offset: offset, Line: line}
	// The cursor was written after every line: a second tiny transaction per
	// record, on top of the insert's own. In WAL mode each of those dirties
	// scattered pages across the table and its indexes and writes them all as
	// frames, and the auto-checkpoint can copy pages back but cannot reset
	// the WAL while any reader holds a snapshot — which, during an API
	// snapshot refresh, is much of the time. A day of measurements ingested
	// in one pass grew the -wal to about a gigabyte and left it there.
	//
	// Writing it every cursorEvery lines is safe for the same reason
	// restarting mid-file is: every insert is idempotent on its natural key,
	// so a cursor behind the real position re-reads lines that are already
	// there and inserts nothing. It is never ahead.
	since := 0
	flush := func() error {
		if since == 0 {
			return nil
		}
		since = 0
		return st.SetCursor(path, res.Offset, res.Line, now)
	}
	for {
		raw, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // partial trailing line stays unread until it is complete
			}
			_ = flush()
			return res, err
		}
		trimmed := bytes.TrimSpace(raw)
		res.Read++
		res.Line++
		res.Offset += int64(len(raw))
		if len(raw) > maxLine {
			// Too long to be one of our records: step over it like any other
			// undecodable line rather than stalling the file forever.
			res.Skipped++
			res.LastSkipped = fmt.Sprintf("%s line %d: record longer than %d bytes", path, res.Line, maxLine)
			since++
			continue
		}
		if len(trimmed) == 0 {
			continue
		}
		ins, err := fn(trimmed)
		if err != nil {
			if errors.Is(err, ErrRetryLater) {
				k := fmt.Sprintf("%s#%d", path, res.Line)
				retries[k]++
				if retries[k] < retryPasses {
					// leave the cursor before this line; the next pass retries
					res.Read--
					res.Line--
					res.Offset -= int64(len(raw))
					res.Deferred = fmt.Sprintf("%s line %d: %v (pass %d of %d)", path, res.Line+1, err, retries[k], retryPasses)
					if ferr := flush(); ferr != nil {
						return res, ferr
					}
					return res, nil
				}
				delete(retries, k)
				err = fmt.Errorf("%w: %v after %d passes", ErrBadRecord, err, retryPasses)
			}
			if !errors.Is(err, ErrBadRecord) {
				// The store refused this line, so the cursor must not move
				// past it. res.Offset was advanced before the handler ran,
				// and flush() writes whatever it holds whenever any earlier
				// line in this pass has not been flushed yet — so without
				// this rewind a failed insert followed by a successful
				// cursor write left the line in the JSONL and nowhere in
				// SQL, permanently, because the next pass starts after it.
				// Only a full re-ingest from zero would have found it.
				//
				// Rewinding is exactly what the ErrRetryLater path above
				// does, for the same reason. A flush that fails here is the
				// more serious of the two errors: the cursor is then ahead
				// of what was stored, which is the state this rewind exists
				// to prevent, so it is reported rather than discarded.
				res.Read--
				res.Line--
				res.Offset -= int64(len(raw))
				if ferr := flush(); ferr != nil {
					return res, fmt.Errorf("%s line %d: %w (and the ingest cursor could not be written: %v)", path, res.Line+1, err, ferr)
				}
				return res, fmt.Errorf("%s line %d: %w", path, res.Line+1, err)
			}
			res.Skipped++
			res.LastSkipped = fmt.Sprintf("%s line %d: %v", path, res.Line, err)
		} else if ins {
			res.Inserted++
		}
		if since++; since >= cursorEvery {
			if err := flush(); err != nil {
				return res, err
			}
		}
	}
	if err := flush(); err != nil {
		return res, err
	}
	return res, nil
}

// cursorEvery is how many lines are read between cursor writes. A crash
// between them re-reads at most this many records, all of which insert
// nothing the second time.
const cursorEvery = 500

// Publications ingests publications.jsonl.
func Publications(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var p scan.Publication
		if err := json.Unmarshal(raw, &p); err != nil {
			return false, fmt.Errorf("%w: decode publication: %v", ErrBadRecord, err)
		}
		if p.PromiseHash == "" {
			return false, fmt.Errorf("%w: publication without promise_hash", ErrBadRecord)
		}
		return st.UpsertPublication(p, raw)
	}, now)
}

// Measurements ingests measurements.jsonl.
func Measurements(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var m probe.Measurement
		if err := json.Unmarshal(raw, &m); err != nil {
			return false, fmt.Errorf("%w: decode measurement: %v", ErrBadRecord, err)
		}
		if m.PromiseHash == "" || m.ValidatorAddress == "" {
			return false, fmt.Errorf("%w: measurement without promise_hash or validator_address", ErrBadRecord)
		}
		return st.InsertProbe(m, raw)
	}, now)
}

// SampledOut ingests sampling_decisions.jsonl: one line per publication the
// load policy drew out of the sample (probe.SampledOut), which stands for a
// NOT_PROBED row per assigned validator per point.
func SampledOut(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var d probe.SampledOut
		if err := json.Unmarshal(raw, &d); err != nil {
			return false, fmt.Errorf("%w: decode sampling decision: %v", ErrBadRecord, err)
		}
		if d.PromiseHash == "" || d.Vantage == "" || d.DecidedAt.IsZero() || d.Kind != probe.SampledOutKind {
			return false, fmt.Errorf("%w: sampling decision without promise_hash, vantage, decided_at or kind %q", ErrBadRecord, probe.SampledOutKind)
		}
		return st.InsertSampledOut(d, raw)
	}, now)
}

// State copies the scanner's state.json (param history, cursor, chain id)
// into the store. It is small and fully re-read every pass.
func State(st *store.Store, path string, now time.Time) error {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var ps scan.PersistState
	if err := json.Unmarshal(b, &ps); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	if err := st.UpsertParams(ps.ParamHistory); err != nil {
		return err
	}
	gaps := "[]"
	if ps.Gaps = scan.DedupeGaps(ps.Gaps); len(ps.Gaps) > 0 {
		if b, err := json.Marshal(ps.Gaps); err == nil {
			gaps = string(b)
		}
	}
	for k, v := range map[string]string{
		"chain_id":                    ps.ChainID,
		"scan_start_height":           fmt.Sprint(ps.StartHeight),
		"last_scanned_height":         fmt.Sprint(ps.LastScannedHeight),
		"protocol_params_fingerprint": ps.ParamFingerprint,
		"scan_gaps":                   gaps,
		"last_scanned_time":           scannedTime(ps),
	} {
		if err := st.SetMeta(k, v, now); err != nil {
			return err
		}
	}
	return nil
}

// Reachability ingests reachability.jsonl written by observer-heartbeat.
func Reachability(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var m probe.Measurement
		if err := json.Unmarshal(raw, &m); err != nil {
			return false, fmt.Errorf("%w: decode reachability: %v", ErrBadRecord, err)
		}
		if m.ValidatorAddress == "" {
			return false, fmt.Errorf("%w: reachability without validator_address", ErrBadRecord)
		}
		return st.InsertReachability(m, raw)
	}, now)
}

// VantagesDir is where the files of other vantages are copied to under the
// data dir: <data-dir>/vantages/<name>/reachability.jsonl, their heartbeats.
const VantagesDir = "vantages"

// VantageFiles lists the reachability files copied in from other vantages
// under dir, one per subdirectory. It is asked again on every pass, so a
// vantage whose directory appears is picked up without a restart. A missing
// dir is no vantages, not an error.
func VantageFiles(dir string) ([]string, error) {
	// Glob only fails on a malformed pattern, and returns its matches sorted,
	// so the files are always tailed in the same order.
	return filepath.Glob(filepath.Join(filepath.Clean(dir), "*", "reachability.jsonl"))
}

// VantageReachability ingests a reachability.jsonl copied in from another
// vantage's heartbeat. It is tailed exactly as the observer's own file is,
// with its own cursor (cursors are keyed by path), so the copy being
// replaced by a longer one only reads what is new, and one that ends in a
// half-copied line leaves that line for the next pass.
//
// Rows keep the vantage recorded in them. One that carries this observer's
// own vantage (own), or none, is stepped over as a bad record: every
// published figure is counted over own's rows, and a second heartbeat
// configured with the same name would otherwise be counted as this one, its
// rows merged into the same keys.
//
// Every row must also be from this observer's chain (chainGuard). A file
// pulled from another network stops at its first row, as a line the store
// refuses does: nothing of it reaches the record.
func VantageReachability(st *store.Store, path, own string, now time.Time) (Result, error) {
	g := newChainGuard(st.DB(), own)
	return tail(st, path, func(raw []byte) (bool, error) {
		var m probe.Measurement
		if err := json.Unmarshal(raw, &m); err != nil {
			return false, fmt.Errorf("%w: decode reachability: %v", ErrBadRecord, err)
		}
		if m.ValidatorAddress == "" {
			return false, fmt.Errorf("%w: reachability without validator_address", ErrBadRecord)
		}
		if m.Vantage == "" || m.Vantage == own {
			return false, fmt.Errorf("%w: reachability from another vantage's file carries vantage %q", ErrBadRecord, m.Vantage)
		}
		if err := g.check(m); err != nil {
			return false, err
		}
		return st.InsertReachability(m, raw)
	}, now)
}

// chainGuard decides whether a row copied in from another vantage was taken
// on this observer's chain.
//
// The vantage files are pulled by name (vantage-pull.sh), and the name says
// nothing about the network: a mainnet observer set up from the one
// documented second-vantage example pulls the Mocha heartbeat, and every row
// of it would go into the mainnet record, its archive and its signed
// exports, which nothing is ever deleted from. A heartbeat row does not
// carry its chain id, so two things it does carry stand in for it:
//
//   - its validator, which must be one this chain knows: in the staking set
//     the collector polls (validator_identities), in the endpoint history
//     the collector keeps from the chain's Fibre registry (endpoints, every
//     validator that ever had a bonded endpoint, the set a heartbeat
//     reaches), or among the validators this observer's own heartbeat has
//     reached. Another network's validators sign with other keys.
//   - its height (validator_set_height, the tip when the round began), which
//     must be one this chain could have been at when the row was taken,
//     against the newest tip on record (the collector's chain poll, else
//     the scanner's frontier). The window is wide on purpose: it allows a
//     block a second however far the row is from that tip, plus maxLag
//     blocks for a vantage whose node lagged, so a row of this chain is
//     never refused by it, while a height from a chain millions of blocks
//     away is.
//
// A row that fails either is an error (ErrOtherChain), not a bad record:
// the file stops before it, the pass names it on every pass, and the
// collector is not OK until the operator removes the file. Stepping over it
// would quietly drop a whole vantage, and a validator that registered a
// moment ago is known to this chain by the next pass, when its row goes in.
type chainGuard struct {
	db    *sql.DB
	own   string
	known map[string]bool

	refLoaded bool
	refHeight int64
	refTime   time.Time
}

// minBlockInterval and maxLag set the height window (see chainGuard).
// Celestia's blocks are about six seconds apart on both networks.
const (
	minBlockInterval = time.Second
	maxLag           = 100_000
)

func newChainGuard(db *sql.DB, own string) *chainGuard {
	return &chainGuard{db: db, own: own, known: map[string]bool{}}
}

func (g *chainGuard) check(m probe.Measurement) error {
	addr := strings.ToLower(m.ValidatorAddress)
	if !g.known[addr] {
		// The endpoint history keys a validator by its bech32 consensus
		// address; an address that is not hex matches nothing there.
		var bech string
		if raw, err := hex.DecodeString(addr); err == nil && len(raw) > 0 {
			bech, _ = bech32.ConvertAndEncode("celestiavalcons", raw)
		}
		var ok bool
		// One short query per validator per file per pass, read whole
		// before anything else uses the store's one connection.
		if err := g.db.QueryRow(`SELECT EXISTS (SELECT 1 FROM validator_identities WHERE cons_address = ?)
			OR EXISTS (SELECT 1 FROM endpoints WHERE validator_cons_address = ?)
			OR EXISTS (SELECT 1 FROM reachability WHERE validator_address = ? AND vantage = ?)`, addr, bech, addr, g.own).Scan(&ok); err != nil {
			return fmt.Errorf("chain check: %w", err)
		}
		if !ok {
			return fmt.Errorf("%w: vantage %s: validator %s is not one this observer's chain knows (not in its staking set or its endpoint history, never reached by its own heartbeat); is the file from another network?", ErrOtherChain, m.Vantage, m.ValidatorAddress)
		}
		g.known[addr] = true
	}
	if m.ValidatorSetHeight <= 0 {
		return nil // no height to place: the validator test alone
	}
	if !g.refLoaded {
		if err := g.loadRef(); err != nil {
			return fmt.Errorf("chain check: %w", err)
		}
	}
	if g.refHeight <= 0 {
		return nil // no tip on record yet
	}
	at := m.ScheduledAt
	if at.IsZero() {
		at = m.StartedAt
	}
	dt := at.Sub(g.refTime)
	if dt < 0 {
		dt = -dt
	}
	window := int64(dt/minBlockInterval) + maxLag
	if d := m.ValidatorSetHeight - g.refHeight; d > window || -d > window {
		return fmt.Errorf("%w: vantage %s: row at height %d taken %s, but this observer's chain was at %d at %s; is the file from another network?",
			ErrOtherChain, m.Vantage, m.ValidatorSetHeight, at.UTC().Format(time.RFC3339), g.refHeight, g.refTime.UTC().Format(time.RFC3339))
	}
	return nil
}

// loadRef reads the newest tip on record: the collector's last chain poll,
// else the scanner's frontier.
func (g *chainGuard) loadRef() error {
	meta := map[string]string{}
	rows, err := g.db.Query(`SELECT key, value FROM meta WHERE key IN ('chain_height', 'chain_tip_time', 'last_scanned_height', 'last_scanned_time')`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return err
		}
		meta[k] = v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	g.refLoaded = true
	for _, k := range [][2]string{{"chain_height", "chain_tip_time"}, {"last_scanned_height", "last_scanned_time"}} {
		h, _ := strconv.ParseInt(meta[k[0]], 10, 64)
		t, err := time.Parse(store.TimeLayout, meta[k[1]])
		if h > 0 && err == nil {
			g.refHeight, g.refTime = h, t
			return nil
		}
	}
	return nil
}

// Payments ingests payments.jsonl written by sentinel-scan.
func Payments(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var p scan.Payment
		if err := json.Unmarshal(raw, &p); err != nil {
			return false, fmt.Errorf("%w: decode payment: %v", ErrBadRecord, err)
		}
		if p.DedupeKey == "" || p.Publisher == "" {
			return false, fmt.Errorf("%w: payment without dedupe_key or publisher", ErrBadRecord)
		}
		return st.UpsertPayment(p, raw)
	}, now)
}

// Registry replays registry.jsonl, the collector's own log of endpoint
// openings and closings, so a database rebuilt from the JSONL files keeps
// the endpoint history the live polls produced.
func Registry(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var e store.EndpointEvent
		if err := json.Unmarshal(raw, &e); err != nil {
			return false, fmt.Errorf("%w: decode endpoint event: %v", ErrBadRecord, err)
		}
		if e.ConsAddress == "" || e.Host == "" || e.At.IsZero() {
			return false, fmt.Errorf("%w: endpoint event without address, host or time", ErrBadRecord)
		}
		return st.ReplayEndpointEvent(e)
	}, now)
}

// Runs replays runs.jsonl, every component's own record of its starts and
// stops with the configuration it ran under (status.RunEvent).
func Runs(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var e status.RunEvent
		if err := json.Unmarshal(raw, &e); err != nil {
			return false, fmt.Errorf("%w: decode run event: %v", ErrBadRecord, err)
		}
		if e.Component == "" || e.At.IsZero() {
			return false, fmt.Errorf("%w: run event without component or time", ErrBadRecord)
		}
		return st.ReplayRunEvent(e)
	}, now)
}

// SamplingSecrets replays sampling-secrets.jsonl, the prober's reveals of
// past days' sampling secrets.
func SamplingSecrets(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var e store.SamplingSecret
		if err := json.Unmarshal(raw, &e); err != nil {
			return false, fmt.Errorf("%w: decode sampling secret: %v", ErrBadRecord, err)
		}
		if e.Day == "" || e.Commitment == "" || e.Secret == "" || e.RevealedAt.IsZero() {
			return false, fmt.Errorf("%w: sampling secret without day, commitment, secret or time", ErrBadRecord)
		}
		return st.UpsertSamplingSecret(e)
	}, now)
}

// scannedTime is the scanner's frontier on the chain's clock, or "" when
// the state predates the field.
func scannedTime(ps scan.PersistState) string {
	if ps.LastScannedTime.IsZero() {
		return ""
	}
	return store.TS(ps.LastScannedTime)
}

// HostEvents replays host_history.jsonl, the scanner's record of every
// Fibre host registration read from the chain's events and its seed.
func HostEvents(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var e scan.HostEvent
		if err := json.Unmarshal(raw, &e); err != nil {
			return false, fmt.Errorf("%w: decode host event: %v", ErrBadRecord, err)
		}
		if e.ConsAddress == "" || e.Source == "" {
			return false, fmt.Errorf("%w: host event without address or source", ErrBadRecord)
		}
		return st.ReplayHostEvent(e)
	}, now)
}

// ParamUncertainty ingests param_uncertainty.jsonl: the height ranges the
// scanner could not say which x/fibre params were in force over, and what
// came of trying to close them. A range is written once when it opens and
// again if it later closes, both under the same id, so the handler is an
// upsert whose second write only latches the resolution.
func ParamUncertainty(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var u scan.ParamUncertainty
		if err := json.Unmarshal(raw, &u); err != nil {
			return false, fmt.Errorf("%w: decode param uncertainty: %v", ErrBadRecord, err)
		}
		if u.ID == "" || u.Kind == "" || u.ToHeight < u.FromHeight {
			return false, fmt.Errorf("%w: param uncertainty without an id, a kind or a usable range", ErrBadRecord)
		}
		return st.UpsertParamUncertainty(u, raw, now)
	}, now)
}

// Corrections replays corrections.jsonl, the collector's own log of the
// deadlines and verdicts a verified params range moved, so a rebuilt
// database carries them without re-deriving.
func Corrections(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var c store.Correction
		if err := json.Unmarshal(raw, &c); err != nil {
			return false, fmt.Errorf("%w: decode correction: %v", ErrBadRecord, err)
		}
		if c.UncertaintyID == "" || c.JudgedAt.IsZero() {
			return false, fmt.Errorf("%w: correction without an uncertainty id or a judged_at", ErrBadRecord)
		}
		switch c.Kind {
		case store.CorrectionRangeComplete:
			// Every deadline and verdict the range covers has been
			// re-derived. Replaying this is what lets a database rebuilt
			// from the export reach the same holds as the live one,
			// instead of re-holding rows whose corrections are already in
			// the file above it.
			return true, st.MarkRangeCorrected(context.Background(), c.UncertaintyID, c.JudgedAt)
		case store.CorrectionPublicationDeadline:
			if c.PromiseHash == "" {
				return false, fmt.Errorf("%w: publication correction without a promise_hash", ErrBadRecord)
			}
			ok, err := st.ApplyPublicationCorrection(c)
			if errors.Is(err, store.ErrNoSuchRow) {
				// The publication line has not been ingested yet, or came
				// after this one in the same pass. Defer rather than skip:
				// a correction dropped on the floor leaves a deadline this
				// observer has already decided is wrong.
				return false, fmt.Errorf("%w: no publication %s yet", ErrRetryLater, c.PromiseHash)
			}
			return ok, err
		case store.CorrectionProbeVerdict:
			if c.DedupeKey == "" {
				return false, fmt.Errorf("%w: probe correction without a dedupe_key", ErrBadRecord)
			}
			ok, err := st.ApplyProbeCorrection(c)
			if errors.Is(err, store.ErrNoSuchRow) {
				return false, fmt.Errorf("%w: no probe row %s yet", ErrRetryLater, c.DedupeKey)
			}
			return ok, err
		case store.CorrectionSampledOutPoint:
			if c.Vantage == "" || c.PromiseHash == "" || c.ScheduledAt.IsZero() {
				return false, fmt.Errorf("%w: sampled-out point correction without a vantage, promise_hash or scheduled_at", ErrBadRecord)
			}
			ok, err := st.ApplySampledOutCorrection(c)
			if errors.Is(err, store.ErrNoSuchRow) {
				return false, fmt.Errorf("%w: no sampled-out decision %s yet", ErrRetryLater, c.DedupeKey)
			}
			return ok, err
		}
		return false, fmt.Errorf("%w: correction of unknown kind %q", ErrBadRecord, c.Kind)
	}, now)
}

// Amendments replays amendments.jsonl, the collector's own log of late
// shadow verdicts, so a rebuilt database carries them without re-judging.
func Amendments(st *store.Store, path string, now time.Time) (Result, error) {
	return tail(st, path, func(raw []byte) (bool, error) {
		var a store.Amendment
		if err := json.Unmarshal(raw, &a); err != nil {
			return false, fmt.Errorf("%w: decode amendment: %v", ErrBadRecord, err)
		}
		if a.DedupeKey == "" || a.To == "" || a.JudgedAt.IsZero() {
			return false, fmt.Errorf("%w: amendment without key, verdict or time", ErrBadRecord)
		}
		ok, err := st.ApplyAmendment(a)
		if errors.Is(err, store.ErrNoSuchRow) {
			return false, fmt.Errorf("%w: %v", ErrRetryLater, err)
		}
		return ok, err
	}, now)
}
