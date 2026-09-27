package probe

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"math/bits"
	"net"
	"runtime"
	"strings"
	"syscall"
	"time"

	celfibre "github.com/celestiaorg/celestia-app/v10/fibre"
	"github.com/celestiaorg/celestia-app/v10/pkg/rsema1d"
	"github.com/celestiaorg/celestia-app/v10/pkg/rsema1d/field"
	"github.com/celestiaorg/celestia-app/v10/pkg/rsema1d/rlc"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	assign "github.com/plsgiveup/fibre/fibre-assign"
	tlsverify "github.com/plsgiveup/fibre/fibre-tlsverify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// StepTimeouts bounds every layer of a probe. Nothing in a probe blocks longer
// than the relevant field.
type StepTimeouts struct {
	DNS      time.Duration
	TCP      time.Duration
	TLS      time.Duration
	Identity time.Duration
	// Download is the base download deadline. The effective deadline is
	// Download + ExpectedShardBytes / MinDownloadBytesPerSec, so a 122 MB
	// shard is not judged by the deadline chosen for a 1 MB one.
	Download time.Duration
	// MinDownloadBytesPerSec is the slowest transfer the observer is willing
	// to wait for before recording RPC_DEADLINE (default 1 MiB/s). Negative
	// makes Download the whole deadline, whatever the shard weighs: the rule
	// celestia-app's own client reads with (RPCTimeout, 15s per DownloadShard).
	MinDownloadBytesPerSec int64
}

// DefaultStepTimeouts are conservative for a WAN vantage.
func DefaultStepTimeouts() StepTimeouts {
	return StepTimeouts{
		DNS:      5 * time.Second,
		TCP:      5 * time.Second,
		TLS:      10 * time.Second,
		Identity: 2 * time.Second,
		Download: 25 * time.Second,

		MinDownloadBytesPerSec: 1 << 20,
	}
}

func (t StepTimeouts) withDefaults() StepTimeouts {
	d := DefaultStepTimeouts()
	if t.DNS <= 0 {
		t.DNS = d.DNS
	}
	if t.TCP <= 0 {
		t.TCP = d.TCP
	}
	if t.TLS <= 0 {
		t.TLS = d.TLS
	}
	if t.Identity <= 0 {
		t.Identity = d.Identity
	}
	if t.Download <= 0 {
		t.Download = d.Download
	}
	if t.MinDownloadBytesPerSec == 0 {
		t.MinDownloadBytesPerSec = d.MinDownloadBytesPerSec
	}
	return t
}

// downloadDeadline is the base deadline plus the time a transfer of
// expectedBytes takes at the slowest acceptable rate.
func (t StepTimeouts) downloadDeadline(expectedBytes int64) time.Duration {
	d := t.Download
	if expectedBytes > 0 && t.MinDownloadBytesPerSec > 0 {
		d += time.Duration(expectedBytes/t.MinDownloadBytesPerSec) * time.Second
	}
	return d
}

// Coder wraps the rsema1d coder used to verify returned rows against the
// commitment. Build one per (OriginalRows, TotalRows) and reuse it.
type Coder struct {
	c            *rsema1d.Coder
	originalRows int
	totalRows    int
}

// NewCoder builds a verifier for a blob-v0 K/N split.
func NewCoder(originalRows, totalRows int) (*Coder, error) {
	c, err := rsema1d.NewCoder(&rsema1d.Config{
		K:           originalRows,
		N:           totalRows - originalRows,
		WorkerCount: runtime.GOMAXPROCS(0),
	})
	if err != nil {
		return nil, err
	}
	return &Coder{c: c, originalRows: originalRows, totalRows: totalRows}, nil
}

// Input is everything one probe needs about the publication + target.
type Input struct {
	Vantage            string
	ChainID            string
	PromiseHash        string
	Commitment         [32]byte
	CommitmentHex      string
	BlobVersion        uint32
	MustServeUntil     time.Time
	ValidatorSetHeight int64

	Target Target

	// AllowUnroutableHost dials an address this observer otherwise refuses:
	// loopback, a private or link-local range. Tests and a local devnet
	// only. A public vantage must leave it false, because the registered
	// host is whatever a validator put on chain and connecting to it would
	// make this observer a port scanner driven from the chain.
	AllowUnroutableHost bool

	SchedulePoint  SchedulePoint
	PruneTolerance time.Duration // grace/post boundary; phase is computed from the actual start time

	// ExpectedShardBytes is the estimated wire size of this validator's shard
	// (ShardBytes); it scales the download deadline. 0 = base deadline only.
	ExpectedShardBytes int64

	// MaxMessageSize is the receive bound for this publication's protocol
	// params, not the observer's compile-time defaults. A blob whose params
	// allow a larger message than this binary was built against would
	// otherwise be refused by our own limit and recorded as a gap, so the
	// largest shards — the ones most worth checking — would never be judged.
	// 0 falls back to the pinned defaults.
	MaxMessageSize int

	// ClockOffsetMS is the observer's wall clock minus the chain's latest
	// block time, in milliseconds, as measured by the caller. Every phase
	// decision is made against the local clock, so the offset is recorded
	// with the probe: a reader can discount a vantage whose clock drifted.
	ClockOffsetMS int64

	// SkipDownload stops after the identity step (L1-L3 only). Used by the
	// reachability heartbeat and by the probe policy's backoff, where the
	// expensive DownloadShard would only repeat a transport failure.
	SkipDownload bool

	// Shadowers are the other settled promises over this commitment this
	// prober knows, with the rows each assigns to this validator. Rows that
	// verify against the commitment but are not this promise's assignment
	// are SHADOWED_SHARD only when they are exactly one of these sets.
	Shadowers []ShadowCandidate
	// ShadowGap names a scan gap overlapping the interval in which a promise
	// whose shard could still be on disk at probe time would have settled;
	// empty when the scanner saw every block that matters. With no matching
	// Shadower, it turns a would-be fault into an observer gap.
	ShadowGap string

	// Observer is the build and chain state stamped on the row (see
	// ObserverInfo). Zero value means "not stamped".
	Observer ObserverInfo
}

// ShadowCandidate is another promise over the same commitment and the rows
// it assigns to the validator being probed.
type ShadowCandidate struct {
	PromiseHash string
	Rows        []int
}

// shadowedBy returns the candidate whose assignment is exactly the returned
// index set, or "" when none is.
func shadowedBy(idx []uint32, cands []ShadowCandidate) string {
	if len(idx) == 0 {
		return ""
	}
	got := make(map[uint32]bool, len(idx))
	for _, i := range idx {
		got[i] = true
	}
	for _, c := range cands {
		if len(c.Rows) != len(got) {
			continue
		}
		match := true
		for _, r := range c.Rows {
			if !got[uint32(r)] {
				match = false
				break
			}
		}
		if match {
			return c.PromiseHash
		}
	}
	return ""
}

// Run executes one layered probe and returns a fully-populated Measurement.
// It never returns an error — a probe that cannot run records
// OutcomeProbeError. The named return lets the deferred finaliser stamp
// FinishedAt / TotalDurationMS / Classification after every early return.
func Run(ctx context.Context, in Input, coder *Coder, to StepTimeouts) (m Measurement) {
	to = to.withDefaults()
	now := time.Now().UTC()
	phase := PhaseAtWindow(now, in.MustServeUntil, in.PruneTolerance)
	m = Measurement{
		SchemaVersion:      MeasurementSchemaVersion,
		Vantage:            in.Vantage,
		PromiseHash:        in.PromiseHash,
		Commitment:         in.CommitmentHex,
		BlobVersion:        in.BlobVersion,
		MustServeUntil:     in.MustServeUntil,
		ValidatorSetHeight: in.ValidatorSetHeight,
		ValidatorAddress:   in.Target.AddressHex,
		ValidatorHost:      in.Target.Host,
		HostSource:         in.Target.HostSource,
		Assigned:           in.Target.Assigned,
		Attested:           in.Target.Attested,
		AttestationUnknown: in.Target.AttestationUnknown,
		AssignedRowCount:   in.Target.RowCount,
		ScheduleLabel:      in.SchedulePoint.Label,
		ScheduledAt:        in.SchedulePoint.At.UTC(),
		StartedAt:          now,
		Phase:              phase,
		ClockOffsetMS:      in.ClockOffsetMS,
		LatenessMS:         now.Sub(in.SchedulePoint.At).Milliseconds(),
	}
	if in.Observer != (ObserverInfo{}) {
		o := in.Observer
		m.Observer = &o
	}
	defer func() {
		m.FinishedAt = time.Now().UTC()
		m.TotalDurationMS = m.FinishedAt.Sub(m.StartedAt).Milliseconds()
		// A probe the observer abandoned says nothing about the validator.
		// Shutdown (SIGTERM, a -deadline expiry) cancels mid-flight probes,
		// and without this an ordinary restart would publish DNS_FAIL or
		// TCP_TIMEOUT as a retention failure for whatever was in flight.
		if ctx.Err() != nil && m.Outcome != OutcomeServedOK {
			m.Outcome = OutcomeProbeError
			if m.RawError == "" {
				m.RawError = "probe abandoned: " + ctx.Err().Error()
			} else {
				m.RawError = "probe abandoned (" + ctx.Err().Error() + "): " + m.RawError
			}
		}
		m.Classification, m.ClassificationReason = Classify(Evidence{
			Assigned:           m.Assigned,
			Attested:           m.Attested,
			AttestationUnknown: m.AttestationUnknown,
			Phase:              m.Phase,
			Outcome:            m.Outcome,
			CommitmentVerified: m.Download.CommitmentVerified,
			Shadowed:           m.Download.ShadowedBy != "",
			ShadowUncertain:    m.Download.ShadowedBy == "" && m.Download.ShadowGap != "",
			RowsSubsetOfOwn:    m.Download.RowsSubsetOfAssignment,
			IdentityStale:      m.Identity.Stale,
			PinStale:           m.Observer != nil && m.Observer.PinStale,
		})
	}()

	if !in.SkipDownload && coder == nil {
		// A download needs the rsema1d coder for this (K, N); without it the
		// returned rows could not be verified, so there is nothing to judge
		// and no reason to open a connection.
		m.Outcome = OutcomeProbeError
		m.RawError = "no coder for this blob version (originalRows/totalRows); cannot verify rows"
		return m
	}
	if in.Target.Host == "" {
		// A validator with no x/valaddr registration cannot be reached by
		// anyone; that is a fact about the validator, not about the probe.
		m.Outcome = OutcomeNoHost
		m.RawError = "validator has no registered fibre host (x/valaddr)"
		return m
	}
	if len(in.Target.PubKey) != ed25519.PublicKeySize {
		m.Outcome = OutcomeProbeError
		m.RawError = "no consensus public key for validator"
		return m
	}
	host, port, err := net.SplitHostPort(in.Target.Host)
	if err != nil {
		m.Outcome = OutcomeProbeError
		m.RawError = fmt.Sprintf("bad host %q: %v", in.Target.Host, err)
		return m
	}

	// ---- L1: DNS ----
	var addrs []string
	if pip := net.ParseIP(host); pip != nil {
		if !routableIP(pip) && !in.AllowUnroutableHost {
			// Registered as a literal address this observer will not dial.
			// The chain checks only the host:port shape, so loopback and the
			// private ranges register as readily as anything else, and a
			// public observer that connected would be a port scanner driven
			// from the chain — publishing the address it reached and the
			// exact error. Recorded as what the validator published, with no
			// connection attempted.
			m.DNS = StepResult{Attempted: false, OK: false, Detail: "literal IP " + host, Error: addrClass(pip) + " address"}
			m.Outcome = OutcomeBadHost
			m.RawError = "registered host " + in.Target.Host + " is a " + addrClass(pip) + " address; not dialled"
			return m
		}
		m.DNS = StepResult{Attempted: false, OK: true, Detail: "literal IP " + host}
		addrs = []string{host}
	} else {
		t0 := time.Now()
		dctx, cancel := context.WithTimeout(ctx, to.DNS)
		got, derr := net.DefaultResolver.LookupHost(dctx, host)
		cancel()
		m.DNS = StepResult{Attempted: true, OK: derr == nil, DurationMS: sinceMS(t0)}
		if derr != nil {
			m.DNS.Error = derr.Error()
			m.Outcome = OutcomeDNSFail
			m.RawError = derr.Error()
			return m
		}
		// The same test after resolution: a name under the operator's control
		// can point anywhere, and that is the shape this has to stop.
		routable, dropped := got, []string(nil)
		if !in.AllowUnroutableHost {
			routable, dropped = splitRoutable(got)
		}
		addrs = orderAddrs(routable)
		m.DNS.Detail = strings.Join(addrs, ",")
		if len(addrs) == 0 {
			m.DNS.OK = false
			m.DNS.Error = "resolves only to " + strings.Join(dropped, ",")
			m.Outcome = OutcomeBadHost
			m.RawError = "registered host " + in.Target.Host + " resolves only to addresses this observer does not dial (" + strings.Join(dropped, ",") + ")"
			return m
		}
		if len(dropped) > 0 {
			m.DNS.Detail += " (not dialled: " + strings.Join(dropped, ",") + ")"
		}
	}

	// ---- L2: TCP ----
	// Every resolved address is tried in turn (IPv4 first), like a real
	// client's happy-eyeballs would; the first that connects is the endpoint
	// every later layer talks to. A vantage without IPv6 must not turn a
	// dual-stack validator into a FAULT.
	t0 := time.Now()
	var rawConn net.Conn
	var terr error
	var attempts []string
	allLocal := len(addrs) > 0
	for _, cand := range addrs {
		d := net.Dialer{Timeout: to.TCP}
		c, err := d.DialContext(ctx, "tcp", net.JoinHostPort(cand, port))
		if err == nil {
			rawConn = c
			allLocal = false
			break
		}
		attempts = append(attempts, cand+": "+err.Error())
		local := isNoRoute(err) || localDialFault(err)
		if !local {
			allLocal = false
		}
		// A remote answer always outranks a local one. "Network is
		// unreachable" from this vantage says nothing about the validator,
		// while "connection refused" from another of its addresses does, and
		// the old rule could let the first overwrite the second.
		if terr == nil || (!local && isNoRoute(terr)) {
			terr = err
		}
		if ctx.Err() != nil {
			break
		}
	}
	m.TCP = StepResult{Attempted: true, OK: rawConn != nil, DurationMS: sinceMS(t0)}
	if rawConn == nil {
		if terr == nil {
			terr = errors.New("no address to dial")
		}
		m.TCP.Error = strings.Join(attempts, "; ")
		if allLocal {
			// Every candidate address failed on this machine's own network:
			// no stack for the family, no route, no local source address. The
			// packets never left. That is the observer's problem, and calling
			// it a retention failure would fault an IPv6-only validator for
			// the vantage's lack of IPv6, permanently.
			m.Outcome = OutcomeProbeError
		} else {
			m.Outcome = classifyDialError(terr)
		}
		// The selected error alone loses the evidence a reader needs to tell
		// a routing problem from a validator that is down, so publish the
		// whole attempt list when more than one address was tried.
		if len(attempts) > 1 {
			m.RawError = strings.Join(attempts, "; ")
		} else {
			m.RawError = terr.Error()
		}
		return m
	}
	if len(attempts) > 0 {
		m.TCP.Detail = "failed " + strings.Join(attempts, "; ") + "; "
	}
	m.TCP.Detail += "-> " + rawConn.RemoteAddr().String()

	// ---- L3: TLS handshake and identity, on the connection L4 will use ----
	// One handshake per probe. The certificate check runs inside it (see
	// handshake.verify), and a download rides the same connection: the
	// certificate the row records is the one that served the rows, and the
	// observer holds one of the server's bounded connection slots, as a
	// client does, not two.
	hs := newHandshake(in, to)
	if in.SkipDownload {
		if tc, _ := hs.run(ctx, rawConn); tc != nil {
			_ = tc.Close()
		} else {
			_ = rawConn.Close()
		}
		m.TLS, m.Identity = hs.results()
		if out, raw, failed := hs.failure(); failed {
			m.Outcome, m.RawError = out, raw
			return m
		}
		m.Outcome = OutcomeReachable
		return m
	}

	// ---- L4: retrievability (the handshake runs inside the gRPC dial) ----
	dl := downloadAndVerify(ctx, in, coder, rawConn, hs, to.downloadDeadline(in.ExpectedShardBytes))
	m.TLS, m.Identity = hs.results()
	if out, raw, failed := hs.failure(); failed {
		m.Outcome, m.RawError = out, raw
		return m
	}
	// A download that was refused before any dial (blob version out of
	// range) leaves the TLS step unattempted; only a completed handshake
	// is the session the download rode.
	m.TLS.SharedWithDownload = m.TLS.OK
	m.Download = dl.DownloadResult
	m.Outcome = dl.outcome
	if dl.rawErr != "" {
		m.RawError = dl.rawErr
	}
	if dl.outcome == OutcomeNotFound {
		if p, regraded := notFoundPhase(time.Now().UTC(), m.Phase, in.MustServeUntil, in.PruneTolerance, in.ClockOffsetMS); regraded {
			m.Phase = p
			m.PhaseNote = "not_found_at_deadline"
		}
	}
	return m
}

// NotFoundGuard is how close to must_serve_until a NOT_FOUND answer may
// arrive and still be graded as if it arrived at the deadline. The phase is
// fixed when the probe starts, but the RPC reaches the server after DNS, TCP,
// TLS and the identity check, up to tens of seconds later, and the server
// prunes on a minute tick against its own clock, which the observer's need
// not match to the second. A NOT_FOUND inside this band is TOLERATED, never
// a FAULT. Thirty seconds is the observer's own clock-skew warning level;
// the last in-window schedule point sits well outside it on any real window.
const NotFoundGuard = 30 * time.Second

// notFoundPhase returns the phase a NOT_FOUND answered at now should be graded
// in, and whether that differs from the phase the probe started in. Only an
// in-window start is ever regraded, and only forward.
//
// clockOffsetMS is the observer's clock minus the chain's newest block time,
// as stamped on the row. Negative, the observer's clock is behind: at what it
// reads as thirty seconds before the deadline, a server on the right time may
// already be past it and pruning on schedule. The band is widened by that
// much, so a NOT_FOUND the observer's own slow clock made early is never a
// FAULT. A clock that is ahead only makes the observer see the deadline
// sooner, which can tolerate a real early prune but never invent one.
func notFoundPhase(now time.Time, started Phase, mustServeUntil time.Time, tol time.Duration, clockOffsetMS int64) (Phase, bool) {
	if started != PhaseInWindow {
		return started, false
	}
	p := PhaseAtWindow(now.Add(notFoundGuard(clockOffsetMS)), mustServeUntil, tol)
	return p, p != PhaseInWindow
}

// notFoundGuard is NotFoundGuard widened by how far the observer's clock is
// behind the chain's.
func notFoundGuard(clockOffsetMS int64) time.Duration {
	if clockOffsetMS < 0 {
		return NotFoundGuard + time.Duration(-clockOffsetMS)*time.Millisecond
	}
	return NotFoundGuard
}

// dlResult carries the raw outcome + error out of downloadAndVerify without
// leaking them into the JSON schema.
type dlResult struct {
	DownloadResult
	outcome Outcome
	rawErr  string
}

// downloadRPCUnary names the read RPC this build calls. celestia-app #7857
// adds DownloadShardStream beside it; a row records which one it used so a
// verdict from either can be told apart once both are in play.
const downloadRPCUnary = "DownloadShard"

// recvLimitFor sizes the receive bound for this probe: the publication's
// own protocol bound (or the pinned defaults), never below what this
// validator's shard of this blob is expected to weigh plus a tenth and the
// promise. The upstream bound is derived from the pinned MaxBlobSize; a
// chain that raised it would otherwise turn its largest shards, the ones
// most worth checking, into receive errors on this side.
// recvLimitFor is what this probe may receive: what the shard should weigh,
// with a tenth for framing and room for the promise, and never less than
// grpc-go would need for the smallest real answer.
//
// It used to start from the protocol maximum and take the expected size only
// as a floor, so every probe — of a 1 KiB blob as readily as a 128 MiB one —
// would accept the protocol's whole 132 MiB from an endpoint whose address a
// validator puts on chain. That also defeated the in-flight byte budget in
// the prober, which reserves what the shard should weigh: the ceiling it
// exists to impose was not the one being enforced. The bound is the
// expectation now, and the protocol maximum only when there is no expectation
// to work from.
//
// Tightening it cannot produce a false accusation. A refusal on this side is
// already read as the observer's own gap, not the validator's: see
// classifyDownloadError and TestRun_SizeBoundsAreToldApartFromAThrottle.
func recvLimitFor(in Input) int {
	if in.ExpectedShardBytes > 0 {
		limit := int(in.ExpectedShardBytes+in.ExpectedShardBytes/10) + celfibre.MaxPaymentPromiseSize
		if limit < minRecvMsgSize {
			limit = minRecvMsgSize
		}
		return limit
	}
	if in.MaxMessageSize > 0 {
		return in.MaxMessageSize
	}
	return defaultMaxRecvMsgSize
}

// minRecvMsgSize keeps a tiny blob's bound above the fixed cost of an answer
// so a correct server is never refused on arithmetic: the promise is already
// counted in full, and a mebibyte of slack covers the RLC vector, the merkle
// proofs and gRPC's framing many times over for any shard small enough to
// reach this floor.
const minRecvMsgSize = celfibre.MaxPaymentPromiseSize + (1 << 20)

// defaultMaxRecvMsgSize matches the reference client's receive bound
// (fibre/internal/grpc/fibre_client.go: MaxCallRecvMsgSize(maxMsgSize) with
// maxMsgSize = ProtocolParams.MaxMessageSize()). grpc-go's default is 4 MiB,
// which would turn every shard larger than that into a spurious failure. It is
// only the fallback: Input.MaxMessageSize carries the publication's own bound.
var defaultMaxRecvMsgSize = celfibre.DefaultProtocolParams.MaxMessageSize()

// userAgent identifies this observer on every connection, so an operator seeing the traffic can tell who it is and stop it.
const userAgent = "fibre-sentinel-observer"

// downloadAndVerify runs the L4 step over conn, the connection L2 opened,
// so all layers judge the same address and the same TCP session. The TLS
// handshake and the identity check run inside the gRPC dial (probeCreds);
// when they fail, the caller reads the verdict from hs, not from the
// Unavailable the RPC returns.
//
// This is a deliberate difference from the reference client, which dials the
// registered host string and lets grpc-go's resolver and pick_first try every
// address (celestia-app fibre/internal/grpc/fibre_client.go). Pinning the
// connection buys a property the reference client does not need and this
// observer does: every layer of a measurement describes one endpoint, so a
// recorded TLS identity, a recorded round trip and a recorded download all
// belong to the same peer. Letting grpc re-resolve would let the download
// land on a different address from the one whose certificate was checked, and
// the record could not say which.
//
// The cost is real and is the reason this comment exists. L2 tries every
// resolved address and takes the first that connects, so a host with several
// addresses is not judged on one of them alone; but if that address accepts
// TCP and then fails at the RPC layer, the probe does not fall back to the
// next. The result is UNREACHABLE, which is already outside the serve rate
// and already says the observer could not complete a conversation rather than
// that the validator refused to serve, so the trade costs coverage of a
// multi-address host rather than fairness to it.
func downloadAndVerify(ctx context.Context, in Input, coder *Coder, conn net.Conn, hs *handshake, timeout time.Duration) dlResult {
	r := dlResult{DownloadResult: DownloadResult{Attempted: true, RowsExpected: in.Target.RowCount, RPC: downloadRPCUnary}}
	t0 := time.Now()
	// grpc closes conn once it owns it; until then, and on every early
	// return, it is this function's to close.
	defer conn.Close()
	endpoint := conn.RemoteAddr().String()

	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	recvLimit := recvLimitFor(in)
	r.RecvLimit = recvLimit
	cc, err := grpc.NewClient("passthrough:///"+endpoint,
		grpc.WithTransportCredentials(&probeCreds{hs: hs, abort: cancel}),
		grpc.WithContextDialer(handOff(conn)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(recvLimit)),
		grpc.WithUserAgent(userAgent),
		// grpc-go consults HTTPS_PROXY even for a passthrough target, so
		// without this the download could take a proxy while L1-L3 dialled
		// the address directly, and the layers would not be judging the same
		// endpoint.
		grpc.WithNoProxy(),
	)
	if err != nil {
		r.DurationMS = sinceMS(t0)
		r.Error = err.Error()
		r.outcome, r.rawErr = OutcomeRPCError, err.Error()
		return r
	}
	defer cc.Close()

	if in.BlobVersion > 255 {
		// BlobID carries a uint8. Truncating here would silently probe the
		// wrong version rather than say the observer cannot judge this blob.
		r.DurationMS = sinceMS(t0)
		r.Error = "blob version out of range for BlobID"
		r.outcome = OutcomeProbeError
		r.rawErr = fmt.Sprintf("blob version %d does not fit the 8-bit BlobID field; this observer cannot judge it", in.BlobVersion)
		return r
	}
	dctx = metadata.AppendToOutgoingContext(dctx, "x-fibre-observer", in.Vantage)
	blobID := celfibre.NewBlobID(uint8(in.BlobVersion), celfibre.Commitment(in.Commitment))
	resp, err := fibretypes.NewFibreClient(cc).DownloadShard(dctx, &fibretypes.DownloadShardRequest{BlobId: blobID})
	r.DurationMS = sinceMS(t0)
	if err != nil {
		if _, _, failed := hs.failure(); failed {
			// The connection never got past its handshake: no request was
			// made, and the verdict is the handshake's (Run reads it).
			return dlResult{}
		}
		r.Error = err.Error()
		r.RPCCode = rpcCodeOf(err)
		r.outcome, r.rawErr = classifyDownloadError(err), err.Error()
		return r
	}

	proofs, rlcv, perr := parseShard(resp.Shard, coder.originalRows, coder.totalRows)
	if perr != nil {
		// A response this observer cannot even parse is not evidence about
		// the shard: the RLC length is checked against the observer's own
		// protocol params, and an empty or nil message is a wire or proto
		// mismatch as readily as a server defect. INVALID_ROWS, which is a
		// fault in every phase, is reserved for rows that fail the
		// commitment check below; a shape error is the observer's gap,
		// with the raw reason on the row.
		r.Error = "parse: " + perr.Error()
		r.outcome, r.rawErr = OutcomeProbeError, "shard shape: "+perr.Error()
		return r
	}
	r.RowsReturned = len(proofs)
	digest := sha256.New()
	r.RowIndices = make([]uint32, len(proofs))
	for i, p := range proofs {
		r.BytesReturned += int64(len(p.Row))
		r.RowIndices[i] = uint32(p.Index)
		digest.Write(p.Row)
	}
	r.RowsSHA256 = hex.EncodeToString(digest.Sum(nil))

	rec, rerr := coder.c.NewReconstructor(rsema1d.Commitment(in.Commitment))
	if rerr != nil {
		r.Error = "reconstructor: " + rerr.Error()
		r.outcome, r.rawErr = OutcomeProbeError, rerr.Error()
		return r
	}
	if _, aerr := rec.Add(proofs, rlcv); aerr != nil {
		r.Error = "commitment verify: " + aerr.Error()
		r.outcome, r.rawErr = OutcomeInvalidRows, aerr.Error()
		return r
	}
	r.CommitmentVerified = true

	idx := make([]uint32, len(proofs))
	for i, p := range proofs {
		idx[i] = uint32(p.Index)
	}
	sm := assign.ShardMap{in.Target.Address: in.Target.AssignedRows}
	if verr := sm.Verify(in.Target.Address, idx); verr != nil {
		r.Error = "assignment verify: " + verr.Error()
		r.ShadowedBy = shadowedBy(idx, in.Shadowers)
		if r.ShadowedBy == "" {
			r.ShadowGap = in.ShadowGap
		}
		if len(proofs) < in.Target.RowCount {
			r.outcome, r.rawErr = OutcomePartial, verr.Error()
			r.RowsSubsetOfAssignment = subsetOf(idx, in.Target.AssignedRows)
		} else {
			r.outcome, r.rawErr = OutcomeWrongRows, verr.Error()
		}
		return r
	}
	r.AssignmentVerified = true
	r.OK = true
	r.outcome = OutcomeServedOK
	return r
}

// parseShard mirrors fibre.parseShard (unexported).
// subsetOf reports whether every index returned is one this promise assigns
// this validator. With fewer rows than it owes, that is a short shard of this
// promise, not another promise's shard answering in its place.
func subsetOf(got []uint32, assigned []int) bool {
	if len(got) == 0 || len(assigned) == 0 {
		return false
	}
	owned := make(map[uint32]struct{}, len(assigned))
	for _, r := range assigned {
		if r >= 0 {
			owned[uint32(r)] = struct{}{}
		}
	}
	for _, g := range got {
		if _, ok := owned[g]; !ok {
			return false
		}
	}
	return true
}

func parseShard(shard *fibretypes.BlobShard, originalRows, totalRows int) ([]*rsema1d.RowProof, rlc.Vector, error) {
	if shard == nil {
		return nil, nil, errors.New("nil shard")
	}
	rows := shard.GetRows()
	if len(rows) == 0 {
		return nil, nil, errors.New("no rows")
	}
	// Two of the verifier's shape checks are re-done here, before the rows
	// reach it, because they are the two that depend on this observer's own
	// idea of the code parameters rather than on the response: the row index
	// is checked against K+N, and the proof depth against bits.Len(K+N)-1.
	// The verifier returns both as ordinary errors, and downloadAndVerify
	// reads any error from it as INVALID_ROWS, which is a fault in every
	// phase. So if this observer's K or N ever drifted from the chain's —
	// a governance change it scanned late, a blob version it mapped wrong —
	// every honest validator on the network would be recorded as faulting
	// at once. A disagreement about the parameters is the observer's gap,
	// and it is named as one; the verifier is then left with the errors
	// that a server alone can cause.
	// No shard of this blob can carry more rows than the code has. Without
	// this the count was unbounded: a server could answer with the same legal
	// index millions of times, and every one of them was written to
	// RowIndices — on the measurement, in measurements.jsonl, in the probes
	// table, on /v1/probes and in the daily export — before any verification
	// ran. A shape error like the two below, so it is this observer's gap and
	// never a statement about the validator.
	if len(rows) > totalRows {
		return nil, nil, fmt.Errorf("%d rows returned, more than the %d this blob has", len(rows), totalRows)
	}
	proofDepth := bits.Len(uint(totalRows)) - 1
	proofs := make([]*rsema1d.RowProof, len(rows))
	for i, rw := range rows {
		if rw == nil {
			return nil, nil, fmt.Errorf("nil row %d", i)
		}
		if idx := int(rw.Index); idx < 0 || idx >= totalRows {
			return nil, nil, fmt.Errorf("row index %d outside this observer's code parameters [0, %d)", idx, totalRows)
		}
		if got := len(rw.Proof); got != proofDepth {
			return nil, nil, fmt.Errorf("row %d proof depth %d, this observer expects %d for %d rows", rw.Index, got, proofDepth, totalRows)
		}
		proofs[i] = &rsema1d.RowProof{Index: int(rw.Index), Row: rw.Data, RowProof: rw.Proof}
	}
	want := originalRows * field.GF128Size
	if len(shard.GetRlcs()) != want {
		return nil, nil, fmt.Errorf("rlc bytes %d != %d", len(shard.GetRlcs()), want)
	}
	v, err := rlc.Unmarshal(shard.GetRlcs())
	if err != nil {
		return nil, nil, err
	}
	return proofs, v, nil
}

// routableIP reports whether this observer will open a connection to an
// address. Global unicast only: loopback, the private and link-local ranges,
// the unspecified address and multicast are all things a validator can put
// on chain and none of them is an endpoint a client could fetch from.
func routableIP(ip net.IP) bool {
	return ip != nil && ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast()
}

// addrClass names why an address was not dialled, for the row.
func addrClass(ip net.IP) string {
	switch {
	case ip.IsLoopback():
		return "loopback"
	case ip.IsPrivate():
		return "private"
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
		return "link-local"
	case ip.IsUnspecified():
		return "unspecified"
	case ip.IsMulticast():
		return "multicast"
	default:
		return "non-routable"
	}
}

// splitRoutable divides resolved addresses into the ones this observer will
// dial and the ones it will not, keeping both so the row says what the name
// resolved to rather than hiding it.
func splitRoutable(got []string) (routable, dropped []string) {
	for _, a := range got {
		ip := net.ParseIP(a)
		if ip == nil {
			dropped = append(dropped, a)
			continue
		}
		if routableIP(ip) {
			routable = append(routable, a)
		} else {
			dropped = append(dropped, a+" ("+addrClass(ip)+")")
		}
	}
	return routable, dropped
}

// localDialFault reports the dial failures that are the observer's own: the
// socket never left this machine. They must not become a statement about the
// validator, and the default arm of a string switch is the wrong place to put
// an error nobody recognised.
func localDialFault(err error) bool {
	for _, e := range []syscall.Errno{
		syscall.EAFNOSUPPORT, // this host has no stack for that address family
		syscall.EMFILE,       // out of file descriptors
		syscall.ENFILE,
		syscall.ENOMEM,
		syscall.ENOBUFS,
		syscall.EADDRINUSE,    // local port exhaustion
		syscall.EADDRNOTAVAIL, // no local source address
		syscall.EACCES,        // local policy refused the socket
		syscall.EPERM,
		syscall.EINVAL,
	} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

func classifyDialError(err error) Outcome {
	// Ours before theirs: a local socket failure is not a validator's fault.
	if localDialFault(err) {
		return OutcomeProbeError
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "refused"):
		return OutcomeTCPRefused
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline exceeded") || strings.Contains(s, "i/o timeout"):
		return OutcomeTCPTimeout
	case strings.Contains(s, "no route to host") || strings.Contains(s, "unreachable"):
		return OutcomeTCPUnreachable
	case strings.Contains(s, "no such host"):
		return OutcomeDNSFail
	default:
		// An error nobody recognised is not evidence of anything. Recording
		// it as TCP_UNREACHABLE turned every unrecognised local errno into a
		// published fault.
		return OutcomeProbeError
	}
}

// classifyDownloadError maps an L4 error to an outcome by its gRPC status
// code first, so that "the observer gave up" (deadline, our own limits) is
// never recorded as a verdict about the validator.
// rpcCodeOf is the canonical name of a gRPC status error's code, or "" for
// anything that is not a status error.
func rpcCodeOf(err error) string {
	if st, ok := status.FromError(err); ok && err != nil {
		return st.Code().String()
	}
	return ""
}

func classifyDownloadError(err error) Outcome {
	if err == nil {
		return OutcomeServedOK
	}
	if _, ok := tlsverify.ReasonOf(err); ok {
		return OutcomeIdentityFail
	}
	s := err.Error()
	ls := strings.ToLower(s)
	// The transport stringifies the VerifyConnection error into an
	// Unavailable status; the tlsverify prefix survives that.
	if strings.Contains(s, "fibre tls identity [") {
		return OutcomeIdentityFail
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			return OutcomeNotFound
		case codes.Unavailable:
			return OutcomeRPCUnavailable
		case codes.DeadlineExceeded:
			return OutcomeRPCDeadline
		case codes.ResourceExhausted:
			// Three very different things share this code, told apart by the
			// text grpc-go puts on them. The observer's own receive bound
			// ("received message larger than max", also the after-
			// decompression variants) is a probe error: the bound is sized
			// from the blob (see recvLimitFor) and recorded on the row. The
			// server's own send bound ("trying to send message larger than
			// max", grpc.MaxSendMsgSize on its side) is the validator
			// refusing to deliver a shard it holds: a server error, reached
			// and answered, never the observer's gap and never a throttle.
			// Anything else is the server declining this request: the storage
			// limiter today (upload path only), the per-peer rate limiter
			// Celestia has announced for the download path, or any limiter an
			// operator puts in front of the port.
			switch {
			case strings.Contains(ls, "received message larger than max"), strings.Contains(ls, "after decompression larger than max"):
				return OutcomeProbeError
			case strings.Contains(ls, "trying to send message larger than max"):
				return OutcomeServerError
			}
			return OutcomeThrottled
		case codes.InvalidArgument:
			// our request was refused as malformed: not a retention verdict.
			return OutcomeProbeError
		case codes.Canceled:
			return OutcomeProbeError
		case codes.Unimplemented:
			// The server does not speak the RPC this observer called. A
			// Fibre server that has moved to DownloadShardStream (upstream
			// #7857) and dropped the unary read answers this way; that is
			// the observer's client being behind, never a retention verdict.
			// When both RPCs exist the prober will try the other and record
			// which one answered (download.rpc); until then the row says
			// which one it asked for.
			return OutcomeProbeError
		case codes.Internal, codes.Unknown, codes.DataLoss, codes.Aborted:
			// The endpoint was reached, completed TLS, proved its identity
			// and answered. Calling that "unreachable" is false about a
			// server the observer just talked to.
			return OutcomeServerError
		default:
			return OutcomeRPCError
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(ls, "deadline exceeded"):
		return OutcomeRPCDeadline
	case strings.Contains(ls, "not found"), strings.Contains(ls, "no blob shard"):
		return OutcomeNotFound
	case strings.Contains(ls, "connection refused"), strings.Contains(ls, "actively refused"), strings.Contains(ls, "transport is closing"):
		return OutcomeRPCUnavailable
	case strings.Contains(ls, "authentication handshake"), strings.Contains(ls, "tls"):
		return OutcomeTLSFail
	default:
		return OutcomeRPCError
	}
}

// verifyWithin runs the identity verification under a timeout. Verification is
// CPU-bound (ASN.1 parse plus one ed25519 verify) and normally takes
// microseconds; the bound exists so that no single probe step is unbounded.
func verifyWithin(cert *x509.Certificate, expected ed25519.PublicKey, chainID string, timeout time.Duration) error {
	type result struct{ err error }
	ch := make(chan result, 1)
	go func() { ch <- result{tlsverify.VerifyCertificateAt(cert, expected, chainID, time.Now())} }()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case r := <-ch:
		return r.err
	case <-t.C:
		return fmt.Errorf("%w: did not finish within %s", errVerifyTimeout, timeout)
	}
}

// identityStale separates a certificate whose signed validity window has
// lapsed or not yet started from one signed by the wrong key. The first is a
// rotation the operator ran late; the second means someone else is answering
// on this endpoint. Publishing them as the same verdict would put a missed
// renewal and an impersonation in the same column.
func identityStale(r tlsverify.Reason) bool {
	switch r {
	case tlsverify.ReasonCertExpired, tlsverify.ReasonCertNotYetValid,
		tlsverify.ReasonWindowEmpty, tlsverify.ReasonWindowTooLong:
		return true
	}
	return false
}

// errVerifyTimeout marks an identity verification that the observer gave up
// on. Verification is a few microseconds of CPU, so a timeout is starvation on
// this machine, never a statement about the certificate. Without this the
// timeout produced IDENTITY_FAIL, the harshest class in the taxonomy, with an
// empty reason field.
var errVerifyTimeout = errors.New("identity verification timed out in the observer")

// orderAddrs puts IPv4 literals before IPv6 ones, keeping the resolver's
// order within each family.
func orderAddrs(addrs []string) []string {
	var v4, v6 []string
	for _, a := range addrs {
		if ip := net.ParseIP(a); ip != nil && ip.To4() == nil {
			v6 = append(v6, a)
		} else {
			v4 = append(v4, a)
		}
	}
	return append(v4, v6...)
}

// isNoRoute reports the local "this family is not routed from here" failures.
// They are the observer's own network, so they must never outrank a real
// answer from another address, and if every candidate fails this way the probe
// is an observer error rather than a verdict.
func isNoRoute(err error) bool {
	if errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EAFNOSUPPORT) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.EADDRNOTAVAIL) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "network is unreachable") || strings.Contains(s, "no route to host") ||
		strings.Contains(s, "address family not supported") || strings.Contains(s, "cannot assign requested address")
}

// ShardBytes estimates the wire size of one validator's shard for a blob:
// rows × (row data + 14 proof hashes + framing) plus the row-linear-combination
// vector (originalRows × 16 bytes) and the response envelope. Framing is 36
// bytes per row: 14 proof entries × 2 (tag, length), the row data tag and
// 4-byte length, and the index field. A 128 MiB blob comes to 4,986,836 B
// for a 148-row shard and 136,265,732 B for 4096 rows.
func ShardBytes(blobSize uint32, originalRows, rows int) int64 {
	if originalRows <= 0 {
		return 0
	}
	rowSize := int64(blobSize) / int64(originalRows)
	const proofBytes = 14 * 32
	const framing = 36
	return int64(rows)*(rowSize+proofBytes+framing) + int64(originalRows)*16 + 4
}

func sinceMS(t time.Time) int64 { return time.Since(t).Milliseconds() }

func tlsVersionString(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "1.3"
	case tls.VersionTLS12:
		return "1.2"
	default:
		return fmt.Sprintf("0x%04x", v)
	}
}
