package probe

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
)

// MeasurementSchemaVersion is bumped when the Measurement JSON shape changes.
//
// 1: initial shape.
// 2: Attested, and the UNATTESTED classification it can produce.
const MeasurementSchemaVersion = 2

// AttestationSchemaVersion is the first measurement version whose Attested
// field carries evidence. A record below it was written by a prober that did
// not know about attestation: its Attested is false because the field did not
// exist, and its classification can never be UNATTESTED. Consumers must treat
// that as unknown rather than as "did not attest".
const AttestationSchemaVersion = 2

// HasAttestation reports whether Attested carries evidence.
func (m Measurement) HasAttestation() bool {
	return m.SchemaVersion >= AttestationSchemaVersion && !m.AttestationUnknown
}

// Measurement is one probe's raw result: which vantage, when, each network
// layer timed and judged separately, plus the raw error text. No scores — a
// reliability view is derived from these later.
type Measurement struct {
	SchemaVersion int    `json:"schema_version"`
	Vantage       string `json:"vantage"`

	// what was probed
	PromiseHash        string    `json:"promise_hash"`
	Commitment         string    `json:"commitment"`
	BlobVersion        uint32    `json:"blob_version"`
	MustServeUntil     time.Time `json:"must_serve_until"`
	ValidatorSetHeight int64     `json:"validator_set_height"`

	// the target validator
	ValidatorAddress string `json:"validator_address"` // 20-byte consensus addr, hex
	ValidatorHost    string `json:"validator_host"`    // host:port as registered
	Assigned         bool   `json:"assigned"`
	// HostSource is "bonded" when the validator was in
	// AllBondedFibreProviders at the time of the probe, and "last_known" when
	// it was not but this observer had seen it register that host earlier and
	// kept probing it anyway. A validator's retention obligation comes from
	// the promise it signed, not from its bonding status, so leaving the
	// bonded set must not stop the evidence.
	HostSource string `json:"host_source,omitempty"`
	// HostAtSettlement is the host the validator had registered when the
	// promise settled (from the publication record), where the shard was
	// stored. When it differs from ValidatorHost the validator re-registered
	// during the window, and SettlementHost carries what the old endpoint
	// answered when the new one did not serve.
	HostAtSettlement string     `json:"host_at_settlement,omitempty"`
	SettlementHost   *HostProbe `json:"settlement_host_probe,omitempty"`
	// Attested: the settled promise carries a signature from this validator
	// that the observer verified against its consensus key. That is the only
	// on-chain proof the validator ever stored this shard, because a Fibre
	// server writes the shard before it signs. False means unproven, not
	// absent: the publisher stops collecting signatures at the safety
	// threshold and keeps delivering in the background.
	Attested bool `json:"attested"`
	// AttestationUnknown is set when the publication record the probe was
	// built from predates signature verification, so Attested is not
	// evidence. Omitted (false) on every row that carries evidence, which is
	// also every row written before the field existed.
	AttestationUnknown bool `json:"attestation_unknown,omitempty"`
	AssignedRowCount   int  `json:"assigned_row_count"`

	// scheduling
	ScheduleLabel string    `json:"schedule_label"` // "full" for the one reading of a blob that asks every endorser ("enough" for one that stops at enough rows); "end" before it; earlier rows w1..w4, grace, post
	ScheduledAt   time.Time `json:"scheduled_at"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	LatenessMS    int64     `json:"lateness_ms"` // started_at - scheduled_at
	// Attempt is which of a validator's requests in a full reading this row
	// is: 0 the reading's own, 1 and 2 the later ones made when the earlier
	// did not serve (retry.go). Every attempt is its own row, at the
	// reading's point. Additive, omitempty: 0 on every other row.
	Attempt int `json:"attempt,omitempty"`
	// NextAttemptDue, on a row of a full reading that did not serve, is when
	// its validator is to be asked again (retry.go): set only while an
	// attempt is left and it can start before must_serve_until less the
	// request start margin. A row that carries it is not the validator's
	// last answer: until the attempt is on record (made, or recorded as not
	// made) the validator is judged on neither (observer/verdict), so an
	// attempt this observer owed and never recorded is never read as the
	// validator's failure. Additive, omitempty.
	NextAttemptDue *time.Time `json:"next_attempt_due,omitempty"`
	// SharedFrom names, by its dedupe key, the request whose answer this
	// row of a later attempt repeats: the validator's endpoint failed before
	// any blob was asked for (no such host, a connect refused, timed out or
	// unroutable, a failed handshake or certificate) while this attempt was
	// due and waiting for the validator's one attempt in flight, so the
	// endpoint's answer is this attempt's too (retry.go). Empty on a row of
	// a request of its own. Additive, omitempty.
	SharedFrom string `json:"shared_from,omitempty"`

	// per-layer results (each timed and judged on its own)
	DNS      StepResult     `json:"dns"`
	TCP      StepResult     `json:"tcp"`
	TLS      TLSResult      `json:"tls"`
	Identity IdentityResult `json:"identity"`
	Download DownloadResult `json:"download"`

	// verdict
	Phase Phase `json:"phase"` // from StartedAt
	// PhaseNote says why Phase differs from the phase at StartedAt, when it
	// does: "not_found_at_deadline" is a NOT_FOUND that arrived within
	// NotFoundGuard of must_serve_until (widened by how far the observer's
	// clock was behind the chain's, ClockOffsetMS) and was graded as grace.
	PhaseNote            string         `json:"phase_note,omitempty"`
	Outcome              Outcome        `json:"outcome"`
	Classification       Classification `json:"classification"`
	ClassificationReason string         `json:"classification_reason"`
	RawError             string         `json:"raw_error,omitempty"`
	TotalDurationMS      int64          `json:"total_duration_ms"`

	// Sampling is the admission draw a row of the earlier sampling was made
	// under: the probability, the cap that bound it, and the commitment to
	// that day's secret, so the draw can be checked once the secret is
	// revealed. Nothing is sampled any more; new rows leave it empty.
	Sampling *SamplingDecision `json:"sampling,omitempty"`

	// ClockOffsetMS is the observer's clock minus the chain's latest block
	// time when the probe ran. Phases are decided by the local clock, so a
	// reader can judge how much to trust a vantage. Additive, omitempty.
	ClockOffsetMS int64 `json:"clock_offset_ms,omitempty"`

	// Retry is set when this measurement is the second attempt: the
	// client's re-dial at the reading, or the transport-timeout retry of the
	// earlier schedule. Absent on single-attempt measurements; additive, so
	// the schema version is unchanged.
	Retry *RetryInfo `json:"retry,omitempty"`

	// Observer identifies the code that produced and classified this row.
	// A classification is a function of the code; a row that does not say
	// which code cannot be re-derived. Additive, omitempty.
	Observer *ObserverInfo `json:"observer,omitempty"`

	// Read says where this request sat in its blob's reading (blobread.go):
	// the validator's place in the client's order, and what the
	// reading came to. For audit only; no figure is computed from it.
	// Additive, omitempty.
	Read *ReadInfo `json:"read,omitempty"`

	// ClientRules says the request was made and judged under the Fibre
	// client's rules (Input.ClientRules: the client's RPCTimeout, its
	// receive bound, its re-dial). Additive, omitempty.
	ClientRules bool `json:"client_rules,omitempty"`

	// ObserverLoad is this observer's own load when the request was let
	// go, so a timeout can be read beside what the observer itself had in
	// flight. Additive, omitempty.
	ObserverLoad *LoadInfo `json:"observer_load,omitempty"`

	// novel is how many of the returned rows the blob's reading had not
	// already seen. Not recorded.
	novel int
}

// LoadInfo is this observer's load at the moment a request was let go
// (Prober.admitBy): the requests and shard bytes in flight then, this one
// included (not those the reading-rate ceiling was still holding), how long
// the request waited for that room, and how much of that wait was the
// ceiling's (Config.MaxReadMbps). None of the wait is the request's own
// time, which starts once it is let go.
type LoadInfo struct {
	RequestsInFlight   int   `json:"requests_in_flight"`
	ShardBytesInFlight int64 `json:"shard_bytes_in_flight"`
	AdmitWaitMS        int64 `json:"admit_wait_ms"`
	RateWaitMS         int64 `json:"rate_wait_ms"`
}

// ReadInfo places one validator's answer in its blob's reading. A row of a
// later attempt (Measurement.Attempt above 0) verified its rows on its own,
// apart from the reading: it carries the validator's place and what the
// reading's first pass came to, and no row counts.
type ReadInfo struct {
	// Order is the validator's place in the order the reading asked in
	// (celestia-app's validator.Set.Select), from 0.
	Order int `json:"order"`
	// NovelRows is how many of this validator's rows the reading had not
	// already had; BlobHaveAfter how many distinct verified rows the reading
	// held once this answer was in.
	NovelRows     int `json:"novel_rows"`
	BlobHaveAfter int `json:"blob_have_after"`
	// BlobResult is what the whole reading came to: available, unavailable,
	// or not_read (not a single request reached a server: Reached).
	BlobResult string `json:"blob_result"`
	// BlobError is the Fibre client's error on an unavailable reading: "no
	// shards retrieved" or "not enough shards to reconstruct blob".
	BlobError string `json:"blob_error,omitempty"`
}

// ObserverInfo is the build that wrote a measurement and the chain it
// believed it was measuring against.
// HostProbe is the second, evidence-only probe of the host registered at
// settlement, run when the validator's current host did not serve the
// shard and differs from it. It never changes the row's verdict: the
// obligation is served at the endpoint clients are sent to, which is the
// registry now; it records whether the data is still there.
type HostProbe struct {
	Host               string  `json:"host"`
	Outcome            Outcome `json:"outcome"`
	RowsReturned       int     `json:"rows_returned"`
	CommitmentVerified bool    `json:"commitment_verified"`
	AssignmentVerified bool    `json:"assignment_verified"`
	DurationMS         int64   `json:"duration_ms"`
	RawError           string  `json:"raw_error,omitempty"`
}

type ObserverInfo struct {
	// Build is the observer's VCS revision (suffixed "-dirty" when built
	// from a modified tree), or "unknown" when the binary carries none.
	Build string `json:"build"`
	// AssignPin is the celestia-app commit the assignment constants were
	// read from (fibre-assign PinnedCelestiaAppCommit).
	AssignPin string `json:"assign_pin"`
	// AppVersion is the chain's app version at the last poll before this
	// probe, 0 when never read.
	AppVersion uint64 `json:"app_version,omitempty"`
	// PinStale is true when AppVersion is above the pinned major: the row
	// assignment this build computes may not be the one the chain uses, so
	// no retention verdict is drawn (see Classify).
	PinStale bool `json:"pin_stale,omitempty"`
}

// RetryInfo records the first attempt of a validator that was asked twice
// (the client's re-dial at the reading, or the earlier schedule's
// transport-timeout retry): FirstOutcome is the first answer, which the
// store keeps as retry_first_outcome. The enclosing Measurement is the
// second attempt.
type RetryInfo struct {
	Attempts        int       `json:"attempts"` // always 2
	DelayMS         int64     `json:"delay_ms"`
	FirstStartedAt  time.Time `json:"first_started_at"`
	FirstOutcome    Outcome   `json:"first_outcome"`
	FirstError      string    `json:"first_error,omitempty"`
	FirstDurationMS int64     `json:"first_duration_ms"`
}

// StepResult is one timed network step (DNS resolution, TCP connect).
type StepResult struct {
	Attempted  bool   `json:"attempted"`
	OK         bool   `json:"ok"`
	DurationMS int64  `json:"duration_ms"`
	Detail     string `json:"detail,omitempty"` // resolved IPs, remote addr, ...
	Error      string `json:"error,omitempty"`
}

// TLSResult is the raw TLS handshake outcome (no identity judgement — that is
// IdentityResult).
type TLSResult struct {
	Attempted        bool   `json:"attempted"`
	OK               bool   `json:"ok"`
	DurationMS       int64  `json:"duration_ms"`
	Version          string `json:"version,omitempty"` // "1.3"
	CipherSuite      string `json:"cipher_suite,omitempty"`
	PeerCertSHA256   string `json:"peer_cert_sha256,omitempty"`
	PeerCertNotAfter string `json:"peer_cert_not_after,omitempty"`
	// SharedWithDownload: the download in this row rode this same TLS
	// session, so the certificate above is the one that served the rows.
	// Absent on rows from builds that opened a second connection for L4.
	SharedWithDownload bool   `json:"shared_with_download,omitempty"`
	Error              string `json:"error,omitempty"`
}

// SamplingDecision is the load-policy decision a row was produced under.
type SamplingDecision struct {
	// P is the admission probability at the moment the publication was first
	// seen. 1 means no cap was binding and nothing was sampled out.
	P float64 `json:"p"`
	// Binding names the cap that produced P ("none" when P is 1).
	Binding string `json:"binding,omitempty"`
	// DayCommitment is SHA256 of the day secret the draw used. It can be
	// published in advance; revealing the secret afterwards lets anyone
	// recompute the draw for every promise hash of that day.
	DayCommitment string `json:"day_commitment,omitempty"`
}

// IdentityResult is the fibre-tlsverify consensus-key binding check on the peer
// certificate, run as its own step against the handshake's peer cert.
type IdentityResult struct {
	Attempted  bool   `json:"attempted"`
	OK         bool   `json:"ok"`
	DurationMS int64  `json:"duration_ms"`
	Reason     string `json:"reason,omitempty"` // tlsverify.Reason on failure
	// Stale: the certificate is endorsed by the right consensus key but its
	// signed validity window has lapsed or has not started. That is endpoint
	// hygiene, not impersonation, and the taxonomy keeps the two apart.
	Stale bool `json:"stale,omitempty"`
	// ClaimedNotBefore/After come from Inspect — what the peer's extension
	// says regardless of verdict.
	ClaimedNotBefore string `json:"claimed_not_before,omitempty"`
	ClaimedNotAfter  string `json:"claimed_not_after,omitempty"`
	Error            string `json:"error,omitempty"`
}

// DownloadResult is the L4 retrievability step: DownloadShard + verify rows
// against the commitment and against the assignment.
type DownloadResult struct {
	Attempted    bool  `json:"attempted"`
	OK           bool  `json:"ok"`
	DurationMS   int64 `json:"duration_ms"`
	RowsReturned int   `json:"rows_returned"`
	RowsExpected int   `json:"rows_expected"`
	// RowIndices are the row indices the server returned, in the order it
	// returned them. With RowsSHA256 this is the evidence behind every
	// WRONG_ROWS, PARTIAL and SHADOWED_SHARD verdict: without the indices
	// nobody can re-run the assignment check, and without the digest an
	// INVALID_ROWS claim is only "we saw it". Empty when nothing came back.
	RowIndices []uint32 `json:"row_indices,omitempty"`
	// RowsSHA256 is the hex SHA-256 over the returned row payloads
	// concatenated in returned order (proofs and the RLC vector excluded).
	// Anyone holding the blob can recompute it from RowIndices.
	RowsSHA256 string `json:"rows_sha256,omitempty"`
	// RPCCode is the canonical gRPC status code of a failed DownloadShard
	// ("NotFound", "Internal", "ResourceExhausted", ...), separate from the
	// free-text error so the SERVER_ERROR / THROTTLED / NOT_FOUND split is
	// machine-readable. Empty on success and on non-status errors.
	RPCCode string `json:"rpc_code,omitempty"`
	// RecvLimit is the receive bound this probe ran with, so a
	// PROBE_ERROR from "received message larger than max" is checkable
	// against the shard's size.
	RecvLimit int `json:"recv_limit,omitempty"`
	// RPC is the read method the probe called: DownloadShard today; the
	// streaming read once upstream ships it and the prober tries both.
	RPC string `json:"rpc,omitempty"`
	// ShadowedBy is the promise hash of another settled promise over the
	// same commitment whose assignment for this validator is exactly the row
	// set returned. Set only when the rows verify against the commitment
	// but are not this promise's assignment: that is the one case where
	// "another promise answered in this one's place" is shown rather than
	// assumed. Empty means no such promise is known to this prober.
	ShadowedBy string `json:"shadowed_by,omitempty"`
	// ShadowGap says why the shadow candidate set was incomplete at the
	// probe, set when the rows are genuine but no known promise assigns
	// them. It opens with ShadowGapScanPrefix when a scan gap overlaps the
	// lifetime a shard over this commitment could have had (a promise
	// settled in that gap may own the rows; the observer knows it did not
	// look, so no verdict, ever), and with ShadowGapPendingPrefix when
	// every promise that could own them settles by a known bound (the
	// collector judges the row once the scanner has read past it).
	ShadowGap string `json:"shadow_gap,omitempty"`
	// RowsSubsetOfAssignment: every index returned is one this promise
	// assigns this validator, and there are fewer of them than it owes. The
	// hash-order argument that holds verified-but-foreign rows out of the
	// rate does not reach this case — a shard served in this one's place
	// would carry that promise's indices, not a part of this one's — so the
	// reason on the row must not cite it.
	RowsSubsetOfAssignment bool `json:"rows_subset_of_assignment,omitempty"`
	// BytesReturned is the row payload the server handed over: the sum of
	// the row data bytes, proofs and the RLC vector excluded. Rows are not a
	// unit of size — a row is as wide as the blob's square — so this is what
	// makes a transfer rate comparable across blobs. Zero on a record written
	// before the field existed, which the store keeps as unknown, not as
	// zero bytes.
	BytesReturned      int64  `json:"bytes_returned,omitempty"`
	CommitmentVerified bool   `json:"commitment_verified"` // rsema1d reconstructor accepted the proofs
	AssignmentVerified bool   `json:"assignment_verified"` // returned indices == assigned set
	Error              string `json:"error,omitempty"`
}

// The two openings of Download.ShadowGap. The store, the collector's late
// judgement and sentinel-recompute all branch on them, so the prober and
// its readers share the one spelling.
const (
	ShadowGapScanPrefix    = "scan_gap"
	ShadowGapPendingPrefix = "shadow_pending"
)

// DedupeKey identifies a measurement slot: one probe per (vantage, promise,
// validator, scheduled point, attempt). The reading's own request (attempt
// 0) keeps the key of four fields every earlier row has; a later attempt
// adds its number (AttemptOfKey reads it back).
func (m Measurement) DedupeKey() string {
	k := dedupeKey(m.Vantage, m.PromiseHash, m.ValidatorAddress, m.ScheduledAt)
	if m.Attempt > 0 {
		k += "|" + strconv.Itoa(m.Attempt)
	}
	return k
}

func dedupeKey(vantage, promiseHash, validatorAddr string, scheduledAt time.Time) string {
	return vantage + "|" + promiseHash + "|" + validatorAddr + "|" + scheduledAt.UTC().Format(time.RFC3339Nano)
}

// AttemptMark is what the record says of a validator in a full reading
// whose last answer did not serve, while it is owed another attempt: the
// last attempt, when the next is due, and what that attempt needs to be
// made and recorded after a restart (Prober.recoverRetries).
type AttemptMark struct {
	Validator          string
	Attempt            int
	Due                time.Time
	ScheduledAt        time.Time
	Order              int
	BlobResult         string
	BlobError          string
	Host               string
	HostAtSettlement   string
	Assigned           bool
	Attested           bool
	AttestationUnknown bool
	RowCount           int
}

// retryOpen reports whether a row of a full reading leaves its validator
// owed another attempt: the row says when it is due (NextAttemptDue), which
// the prober sets only on an answer that did not serve, is not a request
// that could not be made, and has an attempt left that can start in time.
func retryOpen(m Measurement) bool {
	return m.ScheduleLabel == FullReadLabel && m.NextAttemptDue != nil
}

// MeasurementStore is an append-only measurements.jsonl plus an in-memory set
// of dedupe keys loaded on open, so a restart never re-probes a slot it already
// has. Keys are grouped by promise hash so a finished publication can be
// forgotten in O(1) (Forget) instead of growing the set forever. Safe for
// concurrent use.
//
// It also keeps, for a full reading, the validators whose last answer did
// not serve and who can still be asked again (open): the record is
// appended in time order, so the last row read of a validator is its last
// attempt, and a restart finds the attempts it still owes (PendingAttempts).
type MeasurementStore struct {
	path string
	f    *record.Appender

	mu         sync.Mutex
	seen       map[string]map[string]bool        // promise hash -> full dedupe keys
	seenPoints map[string]map[string]bool        // promise hash -> vantage|promise|scheduledAt
	open       map[string]map[string]AttemptMark // promise hash -> validator -> its last attempt, still to be followed
	dirty      bool                              // appended without fsync since the last Sync
}

func pointKey(vantage, promiseHash string, scheduledAt time.Time) string {
	return vantage + "|" + promiseHash + "|" + scheduledAt.UTC().Format(time.RFC3339Nano)
}

// OpenMeasurementStore opens or creates <dir>/measurements.jsonl. A torn
// final line (a write interrupted by a crash) is truncated away before the
// file is opened for append, so one bad byte sequence at the end never bricks
// the prober; a malformed interior line is still a hard error.
func OpenMeasurementStore(dir string) (*MeasurementStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, "measurements.jsonl")
	s := &MeasurementStore{path: path, seen: map[string]map[string]bool{}, seenPoints: map[string]map[string]bool{},
		open: map[string]map[string]AttemptMark{}}
	if err := s.loadSeen(); err != nil {
		return nil, err
	}
	f, err := record.OpenAppender(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	s.f = f
	return s, nil
}

// TruncateTornTail cuts a trailing partial line (no final newline) off an
// append-only JSONL file and reports how many bytes were removed. Files that
// end in a newline, are empty, or do not exist are left alone. It holds the
// file's exclusive lock meanwhile, so an archive run is never copying the
// bytes it cuts (record.RepairTail).
func TruncateTornTail(path string) (int64, error) {
	return record.RepairTail(path)
}

func (s *MeasurementStore) loadSeen() error {
	if cut, err := TruncateTornTail(s.path); err != nil {
		return fmt.Errorf("repair %s: %w", s.path, err)
	} else if cut > 0 {
		fmt.Fprintf(os.Stderr, "measurements: truncated %d bytes of a torn final line in %s\n", cut, s.path)
	}
	f, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open %s: %w", s.path, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<26)
	n := 0
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var m Measurement
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			return fmt.Errorf("%s line %d: %w", s.path, n+1, err)
		}
		s.remember(m)
		n++
	}
	return sc.Err()
}

func (s *MeasurementStore) remember(m Measurement) {
	if s.seen[m.PromiseHash] == nil {
		s.seen[m.PromiseHash] = map[string]bool{}
		s.seenPoints[m.PromiseHash] = map[string]bool{}
	}
	s.seen[m.PromiseHash][m.DedupeKey()] = true
	s.seenPoints[m.PromiseHash][pointKey(m.Vantage, m.PromiseHash, m.ScheduledAt)] = true
	if m.ScheduleLabel != FullReadLabel {
		return
	}
	open := s.open[m.PromiseHash]
	if prev, ok := open[m.ValidatorAddress]; ok && prev.Attempt > m.Attempt {
		return
	}
	if !retryOpen(m) {
		delete(open, m.ValidatorAddress)
		if len(open) == 0 {
			delete(s.open, m.PromiseHash)
		}
		return
	}
	if open == nil {
		open = map[string]AttemptMark{}
		s.open[m.PromiseHash] = open
	}
	mark := AttemptMark{Validator: m.ValidatorAddress, Attempt: m.Attempt, Due: *m.NextAttemptDue, ScheduledAt: m.ScheduledAt,
		Host: m.ValidatorHost, HostAtSettlement: m.HostAtSettlement, Assigned: m.Assigned, Attested: m.Attested,
		AttestationUnknown: m.AttestationUnknown, RowCount: m.AssignedRowCount}
	if m.Read != nil {
		mark.Order, mark.BlobResult, mark.BlobError = m.Read.Order, m.Read.BlobResult, m.Read.BlobError
	}
	open[m.ValidatorAddress] = mark
}

// PendingAttempts is, for a full reading of promiseHash, every validator
// whose last row on record says it is owed another attempt
// (Measurement.NextAttemptDue).
func (s *MeasurementStore) PendingAttempts(promiseHash string) []AttemptMark {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]AttemptMark, 0, len(s.open[promiseHash]))
	for _, m := range s.open[promiseHash] {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Validator < out[j].Validator })
	return out
}

// Has reports whether a measurement for this exact slot is already recorded.
func (s *MeasurementStore) Has(vantage, promiseHash, validatorAddr string, scheduledAt time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[promiseHash][dedupeKey(vantage, promiseHash, validatorAddr, scheduledAt)]
}

// HandledPoint reports whether this (vantage, publication, schedule point) has
// been touched at all: at least one target has a row. It is a hint that the
// point was started, not that it is complete (see Prober.complete).
func (s *MeasurementStore) HandledPoint(vantage, promiseHash string, scheduledAt time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seenPoints[promiseHash][pointKey(vantage, promiseHash, scheduledAt)]
}

// Forget drops the in-memory keys of a publication whose schedule is entirely
// in the past; the file keeps every row.
func (s *MeasurementStore) Forget(promiseHash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.seen, promiseHash)
	delete(s.seenPoints, promiseHash)
	delete(s.open, promiseHash)
}

// Append writes one measurement (skipping an already-seen slot) and fsyncs,
// so every probe result is durable the moment it is recorded.
func (s *MeasurementStore) Append(m Measurement) error {
	return s.append(m, true)
}

// AppendDeferred writes without fsync; call Sync after a batch (used for the
// NOT_PROBED markers a late start fans out, where one fsync per row would
// take hours).
func (s *MeasurementStore) AppendDeferred(m Measurement) error {
	return s.append(m, false)
}

// AppendReading writes every row of one blob's reading in one write and one
// fsync, skipping rows already on record, so a reading lands whole or, torn
// by a crash, is repaired to the rows before the tear.
func (s *MeasurementStore) AppendReading(ms []Measurement) error {
	return s.appendRows(ms, true)
}

// AppendDeferredRows is AppendReading without the fsync: one write, made
// durable by the next Sync. The later attempts of full readings are written
// this way and synced about once a second (Prober.runRetries), so a burst of
// them costs one fsync, not one each, on a disk the host shares. A row lost
// to a crash before its Sync is owed again after the restart, from what the
// file holds.
func (s *MeasurementStore) AppendDeferredRows(ms []Measurement) error {
	return s.appendRows(ms, false)
}

func (s *MeasurementStore) appendRows(ms []Measurement, sync bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf []byte
	var kept []Measurement
	for _, m := range ms {
		if s.seen[m.PromiseHash][m.DedupeKey()] {
			continue
		}
		b, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("marshal measurement: %w", err)
		}
		buf = append(append(buf, b...), '\n')
		kept = append(kept, m)
	}
	if len(buf) == 0 {
		return nil
	}
	if _, err := s.f.Write(buf); err != nil {
		return fmt.Errorf("write measurements: %w", err)
	}
	s.dirty = true
	if sync {
		if err := s.f.Sync(); err != nil {
			return fmt.Errorf("fsync measurements: %w", err)
		}
		s.dirty = false
	}
	for _, m := range kept {
		s.remember(m)
	}
	return nil
}

func (s *MeasurementStore) append(m Measurement, sync bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[m.PromiseHash][m.DedupeKey()] {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal measurement: %w", err)
	}
	// one write per record: a reader never sees half a line from a buffer
	// flush, and a crash leaves at most one torn tail (repaired on open).
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("write measurement: %w", err)
	}
	s.dirty = true
	if sync {
		if err := s.f.Sync(); err != nil {
			return fmt.Errorf("fsync measurements: %w", err)
		}
		s.dirty = false
	}
	s.remember(m)
	return nil
}

// Sync fsyncs pending deferred appends.
func (s *MeasurementStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("fsync measurements: %w", err)
	}
	s.dirty = false
	return nil
}

// Close syncs and closes the file.
func (s *MeasurementStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		if s.dirty {
			_ = s.f.Sync()
		}
		return s.f.Close()
	}
	return nil
}

// Path returns the measurements file path.
func (s *MeasurementStore) Path() string { return s.path }

// LoadMeasurements reads a measurements.jsonl, its archived segments first
// when it has any: the whole record (for tooling / tests).
func LoadMeasurements(path string) ([]Measurement, error) {
	f, err := record.OpenAll(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<26)
	var out []Measurement
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var m Measurement
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, sc.Err()
}
