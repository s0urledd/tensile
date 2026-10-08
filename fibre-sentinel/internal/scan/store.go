package scan

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
)

// Store is the scanner's durable state on disk:
//
//	<dir>/state.json          scan cursor + param history + protocol-params pin
//	<dir>/publications.jsonl  one Publication per line, append-only
//	<dir>/payments.jsonl      one Payment (escrow movement) per line, append-only
//
// Restart safety: publications for a block are appended and fsynced BEFORE the
// cursor in state.json advances past that block (atomic temp+rename). A crash in
// between re-scans the block; DedupeKey (settlement tx hash) makes the re-append
// a no-op because seen keys are loaded on startup.
//
// publications.jsonl and payments.jsonl are archived like the other record
// files (observer-archive moves their older lines into segments and replaces
// the live file with its tail), so both are written through record.Appender:
// a write made while the archiver swaps the file lands in the new one, never
// in the copy it has already taken.
type Store struct {
	dir      string
	pubPath  string
	payPath  string
	statePth string

	pubFile  *record.Appender
	payFile  *record.Appender
	hostFile *os.File
	uncFile  *os.File
	seen     map[string]bool
	paySeen  map[string]bool
	// uncSeen keys the param-uncertainty lines this process already wrote,
	// by (id, resolution). A range opened and later closed writes two
	// lines under the same id, so the id alone would swallow the closing
	// one.
	uncSeen map[string]bool
	// settled holds the settlement height of every publication appended
	// by this process, so a silent params change can say how many records
	// it left with a window computed from the old params.
	settled []int64
	// coverFrom is the first height settled can speak for.
	coverFrom int64
}

// CountSettledBetween counts the publications this process appended whose
// settlement height lies in [from, to].
func (s *Store) CountSettledBetween(from, to int64) int {
	n := 0
	for _, h := range s.settled {
		if h >= from && h <= to {
			n++
		}
	}
	return n
}

// SettledCoverFrom is the first height CountSettledBetween can speak for:
// this process's own start or resume height. A count over a range that
// begins before it is a floor, not a total, because the publications of
// earlier heights were appended by an earlier process and are not in
// memory here.
func (s *Store) SettledCoverFrom() int64 { return s.coverFrom }

// AppendParamUncertainty appends one range this observer was blind over to
// param_uncertainty.jsonl, the record the collector derives its holds and
// its deadline corrections from. Fsynced per line like AppendHostEvent: it
// is written before the cursor moves past the height that produced it, and
// the range is the only thing that says which publications carry a
// deadline the observer cannot vouch for.
func (s *Store) AppendParamUncertainty(u ParamUncertainty) error {
	k := u.ID + "|" + u.Resolution
	if s.uncSeen[k] {
		return nil
	}
	b, err := json.Marshal(u)
	if err != nil {
		return err
	}
	if err := appendLine(&s.uncFile, filepath.Join(s.dir, "param_uncertainty.jsonl"), b); err != nil {
		return err
	}
	if s.uncSeen == nil {
		s.uncSeen = map[string]bool{}
	}
	s.uncSeen[k] = true
	return nil
}

// PersistState is state.json.
type PersistState struct {
	SchemaVersion     int    `json:"schema_version"`
	ChainID           string `json:"chain_id"`
	StartHeight       int64  `json:"start_height"`
	LastScannedHeight int64  `json:"last_scanned_height"`
	// LastScannedTime is the block time of LastScannedHeight: the frontier
	// on the chain's clock, which the deferred shadow verdict is drawn
	// against. Zero when the scanner has not read a block yet.
	LastScannedTime time.Time `json:"last_scanned_time,omitempty"`
	// HostHistory is every Fibre host registration on record (HostHistory),
	// with whether and where the bonded registry was read as its seed.
	HostHistory      []HostEntry  `json:"host_history,omitempty"`
	HostSeeded       bool         `json:"host_seeded,omitempty"`
	HostSeedAt       int64        `json:"host_seed_height,omitempty"`
	ParamFingerprint string       `json:"protocol_params_fingerprint"`
	ParamHistory     []ParamEntry `json:"param_history"`
	// LastReconcileHeight is the height of the last params reconcile. It is
	// persisted because the reconcile's log line is the record of a params
	// change that arrived without an event, and that line names the interval
	// the change landed in. Kept only in memory, the first reconcile after
	// every restart named an interval of one reconcile period ending at the
	// current height — which is wrong whenever the process was down for
	// longer, in the direction that understates how many publications carry
	// the old deadline.
	LastReconcileHeight int64 `json:"last_reconcile_height,omitempty"`
	// ReconcileFailingSince is the first height of the current run of
	// failed params reconciles, or zero when the last one read state. It is
	// persisted so a restart in the middle of an RPC outage does not write
	// a second check_skipped record for a stretch already on the record.
	ReconcileFailingSince int64 `json:"reconcile_failing_since,omitempty"`
	// Gaps are height ranges the scanner had to skip because the node could
	// not serve them, or because the operator listed them in -skip-heights
	// (Reason says which). Published, never hidden: a publication in one of
	// these blocks is unknown to this observer.
	Gaps []ScanGap `json:"gaps,omitempty"`
}

// ScanGap is a run of heights the scanner could not read.
type ScanGap struct {
	From      int64     `json:"from"`
	To        int64     `json:"to"`
	Reason    string    `json:"reason"`
	LastError string    `json:"last_error,omitempty"`
	At        time.Time `json:"at"`
	// FromTime and ToTime are the block times of From and To when the
	// block header could still be read (a node that discards ABCI responses
	// keeps headers). They let a reader place the gap on the chain's clock
	// rather than the scanner's; absent, At is the only clue.
	FromTime *time.Time `json:"from_time,omitempty"`
	ToTime   *time.Time `json:"to_time,omitempty"`
	// HostEventsRead is set when the gap's blocks were read, events and
	// all, and only a publication in them could not be recorded (its
	// validator set at the promise height was pruned). No registration is
	// missing from the record for such a gap, so host_at_settlement ignores
	// it. Absent (a gap from before the field, a block or block_results the
	// node could not serve, an operator skip) means the events were not
	// read, and a registration may be missing.
	HostEventsRead bool `json:"host_events_read,omitempty"`
}

// Spans reports the chain-time interval a gap covers, falling back to the
// scanner's own clock at the moment it hit the gap when no block time was
// read. The fallback is deliberately the conservative one: a gap whose
// blocks are actually old reads as recent, which errs toward "a promise may
// be hiding in there" rather than toward an accusation.
func (g ScanGap) Spans() (from, to time.Time) {
	from, to = g.At, g.At
	if g.FromTime != nil {
		from = *g.FromTime
	}
	if g.ToTime != nil {
		to = *g.ToTime
	}
	return from, to
}

// DedupeGaps drops every gap whose heights an earlier gap of the same kind
// (reason and HostEventsRead) already covers, keeping the order. A state
// written before noteGap refused a height already on record can hold the
// same range twice; the scanner reads its state through this and so does
// the collector, so neither the next state.json nor the API repeats it.
func DedupeGaps(gaps []ScanGap) []ScanGap {
	out := gaps[:0:0]
next:
	for _, g := range gaps {
		for _, k := range out {
			if k.From <= g.From && g.To <= k.To && k.Reason == g.Reason && k.HostEventsRead == g.HostEventsRead {
				continue next
			}
		}
		out = append(out, g)
	}
	return out
}

// OpenStore opens or creates the store in dir.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	s := &Store{
		dir:      dir,
		pubPath:  filepath.Join(dir, "publications.jsonl"),
		payPath:  filepath.Join(dir, "payments.jsonl"),
		statePth: filepath.Join(dir, "state.json"),
		seen:     map[string]bool{},
		paySeen:  map[string]bool{},
		uncSeen:  map[string]bool{},
	}
	if err := s.loadSeen(); err != nil {
		return nil, err
	}
	if err := s.loadPaySeen(); err != nil {
		return nil, err
	}
	f, err := record.OpenAppender(s.pubPath)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", s.pubPath, err)
	}
	s.pubFile = f
	pf, err := record.OpenAppender(s.payPath)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("open %s: %w", s.payPath, err)
	}
	s.payFile = pf
	return s, nil
}

// TruncateTornTail cuts a trailing partial line (no final newline) off an
// append-only JSONL file and reports how many bytes were removed. A crash or
// power loss mid-write leaves exactly such a tail; without this repair the
// tool refuses to start until someone edits the file by hand. Files that end
// in a newline, are empty, or do not exist are left alone. It holds the
// file's exclusive lock meanwhile, so an archive run is never copying the
// bytes it cuts (record.RepairTail).
func TruncateTornTail(path string) (int64, error) {
	return record.RepairTail(path)
}

// loadSeen and loadPaySeen read the live file only, not the archived
// segments before it. The dedupe set has to hold what a restart can append
// again: the blocks after the last checkpoint. Those are in the live file
// unless an archive run came while the scanner was stopped in the middle
// of catching up on heights older than -keep, which can move most of them
// into a segment; a manual re-scan of older heights is the same case.
// Reading the segments too would make every start read the whole history
// of the chain for nothing. A record appended again is a duplicate the
// collector's store ignores, keeping the first copy, and record-verify
// either finds the second copy byte for byte what the store gives back or
// reports it (a key repeated within one day's export, or different bytes),
// so no day is retired on bytes the store does not reproduce.
func (s *Store) loadSeen() error {
	if cut, err := TruncateTornTail(s.pubPath); err != nil {
		return fmt.Errorf("repair %s: %w", s.pubPath, err)
	} else if cut > 0 {
		fmt.Fprintf(os.Stderr, "publications: truncated %d bytes of a torn final line in %s\n", cut, s.pubPath)
	}
	f, err := os.Open(s.pubPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open %s: %w", s.pubPath, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<26)
	n := 0
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var p Publication
		if err := json.Unmarshal(line, &p); err != nil {
			return fmt.Errorf("%s line %d: %w", s.pubPath, n+1, err)
		}
		s.seen[p.SettlementTxHash] = true
		n++
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", s.pubPath, err)
	}
	return nil
}

func (s *Store) loadPaySeen() error {
	if cut, err := TruncateTornTail(s.payPath); err != nil {
		return fmt.Errorf("repair %s: %w", s.payPath, err)
	} else if cut > 0 {
		fmt.Fprintf(os.Stderr, "payments: truncated %d bytes of a torn final line in %s\n", cut, s.payPath)
	}
	f, err := os.Open(s.payPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open %s: %w", s.payPath, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<26)
	n := 0
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var p Payment
		if err := json.Unmarshal(line, &p); err != nil {
			return fmt.Errorf("%s line %d: %w", s.payPath, n+1, err)
		}
		s.paySeen[p.DedupeKey] = true
		n++
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", s.payPath, err)
	}
	return nil
}

// LoadState returns the persisted state, or (nil, nil) if there is none yet.
func (s *Store) LoadState() (*PersistState, error) {
	b, err := os.ReadFile(s.statePth)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", s.statePth, err)
	}
	var st PersistState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("parse %s: %w", s.statePth, err)
	}
	return &st, nil
}

// Seen reports whether a publication with this settlement tx hash is already
// persisted.
func (s *Store) Seen(settlementTxHash string) bool { return s.seen[settlementTxHash] }

// AppendPublication writes one record (skipping an already-seen one) in a
// single write, so a concurrent reader (the prober tails this file) never
// sees a record split across two buffer flushes. It does NOT fsync per call;
// call Sync() before advancing the cursor.
func (s *Store) AppendPublication(p Publication) error {
	if s.seen[p.SettlementTxHash] {
		return nil
	}
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal publication %s: %w", p.PromiseHash, err)
	}
	if _, err := s.pubFile.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write publication: %w", err)
	}
	s.seen[p.SettlementTxHash] = true
	s.settled = append(s.settled, p.SettlementHeight)
	return nil
}

// AppendHostEvent appends one registration to host_history.jsonl, the
// record of every set_fibre_provider_info event (and the seed) the scanner
// read; the collector ingests it and the export carries it, so a verifier
// can derive host_at_settlement for every assignment from the record.
func (s *Store) AppendHostEvent(e HostEvent) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return appendLine(&s.hostFile, filepath.Join(s.dir, "host_history.jsonl"), b)
}

// appendLine appends b and a newline to the record file at path through
// *fp, and fsyncs it. host_history.jsonl and param_uncertainty.jsonl are
// written this way; the collector reads them line by line and skips a line
// that does not decode.
//
// A line is never written onto a partial one. The file is opened on first
// use, after record.RepairTail has cut a torn final line (a crash or a full
// disk in the middle of an earlier write) off it. A write or fsync that
// fails cuts the file back to where the line began, and the handle is
// dropped either way, so the next append opens the file again through the
// same repair. Before, a write that failed partway (ENOSPC on the shared
// disk) left a partial line, the caller's retry wrote the record straight
// after it, and the two came out as one line the collector could not
// decode: the range or registration in it never reached the store, while
// the scanner, which had written it, never wrote it again.
func appendLine(fp **os.File, path string, b []byte) error {
	name := filepath.Base(path)
	if *fp == nil {
		if cut, err := record.RepairTail(path); err != nil {
			return fmt.Errorf("repair %s: %w", name, err)
		} else if cut > 0 {
			fmt.Fprintf(os.Stderr, "%s: truncated %d bytes of a torn final line in %s\n", name, cut, path)
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("open %s: %w", name, err)
		}
		*fp = f
	}
	f := *fp
	info, err := f.Stat()
	if err == nil {
		if _, err = f.Write(append(b, '\n')); err == nil {
			err = f.Sync()
		}
		if err != nil {
			_ = f.Truncate(info.Size())
		}
	}
	if err != nil {
		_ = f.Close()
		*fp = nil
		return fmt.Errorf("append to %s: %w", name, err)
	}
	return nil
}

// AppendPayment writes one escrow movement (skipping an already-seen one)
// in a single write, under the same crash rules as AppendPublication.
func (s *Store) AppendPayment(p Payment) error {
	if p.DedupeKey == "" {
		return fmt.Errorf("payment without a dedupe key (h=%d kind=%s)", p.Height, p.Kind)
	}
	if s.paySeen[p.DedupeKey] {
		return nil
	}
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal payment %s: %w", p.DedupeKey, err)
	}
	if _, err := s.payFile.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write payment: %w", err)
	}
	s.paySeen[p.DedupeKey] = true
	return nil
}

// PaymentSeen reports whether a payment with this dedupe key is persisted.
func (s *Store) PaymentSeen(key string) bool { return s.paySeen[key] }

// Sync fsyncs the publications and payments files.
func (s *Store) Sync() error {
	if err := s.pubFile.Sync(); err != nil {
		return fmt.Errorf("fsync publications: %w", err)
	}
	if err := s.payFile.Sync(); err != nil {
		return fmt.Errorf("fsync payments: %w", err)
	}
	return nil
}

// SaveState atomically replaces state.json. Call after Sync().
func (s *Store) SaveState(st PersistState) error {
	st.SchemaVersion = SchemaVersion
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	tmp := s.statePth + ".tmp"
	if err := writeFileSync(tmp, b); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.statePth); err != nil {
		return fmt.Errorf("rename %s: %w", tmp, err)
	}
	// fsync the directory so the rename itself survives a power loss.
	if d, err := os.Open(s.dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// writeFileSync writes b to path and fsyncs it before returning, so a rename
// over the live file never exposes an empty or partial state.json.
func writeFileSync(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Close closes the publications and payments files.
func (s *Store) Close() error {
	var first error
	if s.pubFile != nil {
		if err := s.pubFile.Close(); err != nil {
			first = err
		}
	}
	if s.payFile != nil {
		if err := s.payFile.Close(); err != nil && first == nil {
			first = err
		}
	}
	if s.hostFile != nil {
		if err := s.hostFile.Close(); err != nil && first == nil {
			first = err
		}
	}
	if s.uncFile != nil {
		if err := s.uncFile.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// SetSettledCoverFrom records the first height this process's own
// publication appends can speak for, so a count over a wider range can say
// it is a floor.
func (s *Store) SetSettledCoverFrom(h int64) { s.coverFrom = h }

// PublicationsPath is the jsonl path (for tooling / tests).
func (s *Store) PublicationsPath() string { return s.pubPath }

// PaymentsPath is the payments jsonl path.
func (s *Store) PaymentsPath() string { return s.payPath }
