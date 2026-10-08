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
	"net/netip"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

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

// StepTimeouts bounds every layer of a request. Nothing in a request blocks
// longer than the relevant field (the connect step as a whole, every
// address it tries, by TCP), and under Input.ClientRules nothing blocks
// longer than Input.RequestTimeout altogether.
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

// ClientRPCTimeout is the RPCTimeout celestia-app's Fibre client gives one
// request to one validator, dial and DownloadShard together
// (fibre.DefaultClientConfig).
const ClientRPCTimeout = 15 * time.Second

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

	// ClientRules reads the validator the way celestia-app's Fibre client
	// does (fibre/client_download.go), which is how every blob is read:
	//
	//   - the whole request, lookup, dial and DownloadShard, gets
	//     RequestTimeout (ClientRPCTimeout); the lookup, the connect and the
	//     TLS handshake are bounded by it alone, as the client's are; a
	//     request that runs out of it after the connection was made is the
	//     validator's (RPC_TIMEOUT);
	//   - the host's addresses are raced as the client's pick_first races
	//     them (raceDial): the next one DialStagger after the one before it
	//     unless that one has connected or failed, and the first to connect
	//     is the endpoint;
	//   - the receive bound is what this validator's shard of this blob can
	//     weigh, a tenth to spare and a mebibyte at least, never above the
	//     protocol's message bound, which is the client's (recvLimitFor); an
	//     answer over it, which no honest server sends, or one the client
	//     cannot parse, is the validator's (MALFORMED_SHARD);
	//   - an InvalidArgument or Unimplemented answer is the server's error;
	//   - an ICMP unreachable ("no route to host", "host is down", "network
	//     is unreachable" while this machine has a route out to the
	//     address) is the validator's host not answering, as the client
	//     meets it; only a failure that never left this machine (no route
	//     out, no local address, a local socket error) is this observer's.
	//     At a full reading ownSide keeps it the validator's only while this
	//     observer's connections over the same IP version reach servers;
	//   - a DNS failure other than "no such host" is this observer's, unless
	//     at a full reading ownSide finds its resolver working in the same
	//     minutes: then it is the validator's (DNS_FAIL).
	//
	// Without it the request is judged by the earlier schedule's rules.
	ClientRules    bool
	RequestTimeout time.Duration
	// ReadingPhase, when set, is the phase of the reading this request
	// belongs to, taken when the reading started: every request of a
	// reading carries it, as the client asks every validator at once, so a
	// request this observer's own limits held back is judged as if it had
	// not been. Empty takes the phase from the request's own start.
	ReadingPhase Phase
	// Verifier checks the rows against the commitment. A reading shares one
	// across every validator it asks (the blob's Reconstructor), as the
	// client does; nil builds one for this request alone.
	Verifier ShardVerifier

	// hooks stand in for this machine's resolver, connect and route lookup;
	// nil is the machine's own. Tests only.
	hooks *netHooks
}

// netHooks are a request's resolver, connect and route lookup (Input.hooks);
// a nil member is this machine's own.
type netHooks struct {
	lookup func(ctx context.Context, host string) ([]string, error)
	dial   func(ctx context.Context, addr string) (net.Conn, error)
	routed func(addr string) bool
}

// lookupHost resolves a registered host name.
func (in Input) lookupHost(ctx context.Context, host string) ([]string, error) {
	if in.hooks != nil && in.hooks.lookup != nil {
		return in.hooks.lookup(ctx, host)
	}
	return net.DefaultResolver.LookupHost(ctx, host)
}

// dialTCP opens a TCP connection to addr (host:port) within ctx.
func (in Input) dialTCP(ctx context.Context, addr string) (net.Conn, error) {
	if in.hooks != nil && in.hooks.dial != nil {
		return in.hooks.dial(ctx, addr)
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

// routedOut reports whether this machine has a route out to addr
// (host:port): a UDP "connect" looks the route up and sends nothing, and
// fails, as a TCP connect does, when there is none.
func (in Input) routedOut(addr string) bool {
	if in.hooks != nil && in.hooks.routed != nil {
		return in.hooks.routed(addr)
	}
	c, err := net.Dial("udp", addr)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// ShardVerifier checks a shard's rows against the blob commitment and keeps
// the ones it had not seen: *rsema1d.Reconstructor. Add may reorder proofs.
type ShardVerifier interface {
	Add(proofs []*rsema1d.RowProof, rlc rlc.Vector) ([]*rsema1d.RowProof, error)
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

// newMeasurement is the row of a request of in started at now: what was
// asked, of whom, and when, before any layer has run.
func newMeasurement(in Input, now time.Time) Measurement {
	phase := PhaseAtWindow(now, in.MustServeUntil, in.PruneTolerance)
	if in.ReadingPhase != "" {
		phase = in.ReadingPhase
	}
	m := Measurement{
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
		ClientRules:        in.ClientRules && in.RequestTimeout > 0,
	}
	if in.Observer != (ObserverInfo{}) {
		o := in.Observer
		m.Observer = &o
	}
	return m
}

// classifyRow applies the taxonomy to a row's own evidence.
func classifyRow(m *Measurement) {
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
}

// Run executes one layered probe and returns a fully-populated Measurement.
// It never returns an error — a probe that cannot run records
// OutcomeProbeError. The named return lets the deferred finaliser stamp
// FinishedAt / TotalDurationMS / Classification after every early return.
func Run(ctx context.Context, in Input, coder *Coder, to StepTimeouts) (m Measurement) {
	to = to.withDefaults()
	// The caller's context is kept apart from the request's own deadline: a
	// request the caller abandoned (shutdown) is this observer's gap, while
	// one that ran out of the client's RPCTimeout is the validator's answer.
	caller := ctx
	if in.ClientRules && in.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, in.RequestTimeout)
		defer cancel()
		// The client bounds the lookup, the connect and the handshake by the
		// request's time alone (the RPCTimeout around DownloadShard covers
		// gRPC's lazy dial and its resolver), so a lookup or a connect that
		// takes 5 to 15 s is still made here.
		to.DNS, to.TCP, to.TLS = in.RequestTimeout, in.RequestTimeout, in.RequestTimeout
	}
	m = newMeasurement(in, time.Now().UTC())
	defer func() {
		m.FinishedAt = time.Now().UTC()
		m.TotalDurationMS = m.FinishedAt.Sub(m.StartedAt).Milliseconds()
		// A probe the observer abandoned says nothing about the validator.
		// Shutdown (SIGTERM, a -deadline expiry) cancels mid-flight probes,
		// and without this an ordinary restart would publish DNS_FAIL or
		// TCP_TIMEOUT as a retention failure for whatever was in flight.
		if caller.Err() != nil && m.Outcome != OutcomeServedOK {
			m.Outcome = OutcomeProbeError
			if m.RawError == "" {
				m.RawError = "probe abandoned: " + caller.Err().Error()
			} else {
				m.RawError = "probe abandoned (" + caller.Err().Error() + "): " + m.RawError
			}
			// abandoned is abandoned, whatever its lookup did
			m.resolverFailed = false
		}
		clipText(&m)
		classifyRow(&m)
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
		got, derr := in.lookupHost(dctx, host)
		cancel()
		m.DNS = StepResult{Attempted: true, OK: derr == nil, DurationMS: sinceMS(t0)}
		if derr != nil {
			m.DNS.Error = derr.Error()
			m.Outcome = OutcomeDNSFail
			m.RawError = derr.Error()
			var dnsErr *net.DNSError
			if in.ClientRules && !(errors.As(derr, &dnsErr) && dnsErr.IsNotFound) {
				// The resolver did not answer, or answered with an error of
				// its own: a lame or broken zone of the validator's reads
				// the same as this observer's own resolver failing. Only "no
				// such host" is plainly the validator's; anything else is
				// this observer's gap unless, at a full reading, its
				// resolver is shown working in the same minutes (ownSide).
				// A lookup that failed on this machine itself (no socket,
				// no buffer) stays its gap whatever else answered.
				m.Outcome = OutcomeProbeError
				m.RawError = "resolver: " + derr.Error()
				m.resolverFailed = !localDialFault(derr)
			}
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
	// The resolved addresses are raced the way grpc-go's pick_first races
	// them for celestia-app's Fibre client (raceDial): IPv4 first and the
	// families taking turns (orderAddrs), the next address started as soon
	// as the one before it fails, or DialStagger after it started while it
	// is still connecting. The first that connects is the endpoint every
	// later layer talks to, and the rest are let go. One dead address in a
	// validator's DNS must not make it not served when every client
	// connects to the next one a quarter of a second later, and a vantage
	// without IPv6 must not turn a dual-stack validator into a FAULT.
	t0 := time.Now()
	lctx, untried := ctx, 0
	if !(in.ClientRules && in.RequestTimeout > 0) {
		// Outside the client's rules (the reachability heartbeat) the whole
		// step is bounded by to.TCP, and only the first
		// heartbeatAddrsPerFamily addresses of each IP version are tried: a
		// name that resolves to thousands of addresses would otherwise hold
		// a heartbeat round for hours, and send a SYN to every one of them.
		var cancel context.CancelFunc
		lctx, cancel = context.WithTimeout(ctx, to.TCP)
		defer cancel()
		addrs, untried = firstPerFamily(addrs, heartbeatAddrsPerFamily)
	}
	rawConn, tried := raceDial(lctx, in.dialTCP, addrs, port, to.TCP, DialStagger)
	untried += len(addrs) - len(tried)
	if rawConn != nil {
		untried--
	}
	var terr error
	terrLocal := false
	var failed, letGo []string
	allLocal := len(tried) > 0
	for _, a := range tried {
		if a.err == nil {
			letGo = append(letGo, a.addr)
			continue
		}
		failed = append(failed, a.addr+": "+a.err.Error())
		local := isNoRoute(a.err) || localDialFault(a.err)
		if in.ClientRules && pathUnreachable(a.err, func() bool { return in.routedOut(net.JoinHostPort(a.addr, port)) }) {
			// An ICMP unreachable from the path to the host is the host
			// not answering, as the client meets it. ownSide holds it as
			// the validator's only while this observer's connections over
			// the same IP version reach other servers.
			local = false
			m.unreachFamilies = appendOnce(m.unreachFamilies, family(a.addr))
		}
		if !local {
			allLocal = false
		}
		// A remote answer always outranks a local one. "Network is
		// unreachable" from this vantage says nothing about the validator,
		// while "connection refused" from another of its addresses does, and
		// the old rule could let the first overwrite the second.
		if terr == nil || (terrLocal && !local) {
			terr, terrLocal = a.err, local
		}
	}
	m.TCP = StepResult{Attempted: true, OK: rawConn != nil, DurationMS: sinceMS(t0)}
	notTried := ""
	if untried > 0 {
		notTried = fmt.Sprintf("%d more address(es) not tried", untried)
	}
	if rawConn == nil {
		if terr == nil {
			terr = errors.New("no address to dial")
		}
		m.TCP.Error = joinNonEmpty("; ", strings.Join(failed, "; "), notTried)
		switch {
		case allLocal:
			// Every candidate address failed on this machine's own network:
			// no stack for the family, no route, no local source address.
			// The packets never left. That is the observer's problem, and
			// calling it a retention failure would fault an IPv6-only
			// validator for the vantage's lack of IPv6, permanently.
			m.Outcome = OutcomeProbeError
		case in.ClientRules && !terrLocal && pathUnreachable(terr, func() bool { return true }):
			m.Outcome = OutcomeTCPUnreachable
		default:
			m.Outcome = classifyDialError(terr)
		}
		// The selected error alone loses the evidence a reader needs to tell
		// a routing problem from a validator that is down, so publish the
		// whole attempt list when more than one address was tried.
		if len(failed) > 1 || notTried != "" {
			m.RawError = m.TCP.Error
		} else {
			m.RawError = terr.Error()
		}
		return m
	}
	// The endpoint is written last and never cut (clip bounds what comes
	// before it): readers take it from after the last "-> ".
	var tries []string
	if len(failed) > 0 {
		tries = append(tries, "failed "+strings.Join(failed, "; "))
	}
	if len(letGo) > 0 {
		tries = append(tries, "let go "+strings.Join(letGo, ", ")+" (still connecting)")
	}
	if len(tries) > 0 {
		m.TCP.Detail = clip(strings.Join(tries, "; ")) + "; "
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
	m.novel = dl.novel
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
// the reading sits minutes outside it.
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
	// novel is how many of the rows the verifier had not seen before.
	novel int
}

// downloadRPCUnary names the read RPC this build calls. celestia-app #7857
// adds DownloadShardStream beside it; a row records which one it used so a
// verdict from either can be told apart once both are in play.
const downloadRPCUnary = "DownloadShard"

// recvLimitFor is what this probe may receive: what this validator's shard
// of this blob should weigh (ExpectedShardBytes), with a tenth for framing
// and room for the promise, never less than minRecvMsgSize; the protocol's
// message bound (the publication's own, or the pinned defaults) only when
// there is no expectation to work from. Under the client's rules it is
// never above that bound either, the most the client accepts.
//
// It used to start from the protocol maximum and take the expected size only
// as a floor, and under the client's rules it was that maximum outright, so
// every request, of a 1 KiB blob as readily as a 128 MiB one, would accept
// the protocol's whole 132 MiB from an endpoint whose address a validator
// puts on chain. That also defeated the in-flight byte budget in the
// prober, which reserved what the shard should weigh: a padded answer
// (an honest shard and a hundred-odd megabytes in a field nobody reads,
// which still parses and verifies) took sixteen times the budget at once,
// and the link the reading-rate ceiling protects. The bound is the
// expectation now, and the prober charges its byte budget the bound
// (blobReading.ask, Prober.attempt), so the budget holds whatever a server
// sends.
//
// An honest shard cannot exceed it: the estimate is the shard's own rows,
// proofs and RLC vector, and the slack covers a row rounded up to the code's
// row size and a deeper proof many times over. Under the client's rules an
// answer over it is the validator's (MALFORMED_SHARD: clientRulesOutcome),
// an answer no honest server sends; under the earlier rules it is this
// observer's gap (classifyDownloadError,
// TestRun_SizeBoundsAreToldApartFromAThrottle).
func recvLimitFor(in Input) int {
	protocol := defaultMaxRecvMsgSize
	if in.MaxMessageSize > 0 {
		protocol = in.MaxMessageSize
	}
	if in.ExpectedShardBytes <= 0 {
		return protocol
	}
	limit := int(in.ExpectedShardBytes+in.ExpectedShardBytes/10) + celfibre.MaxPaymentPromiseSize
	if limit < minRecvMsgSize {
		limit = minRecvMsgSize
	}
	if in.ClientRules && limit > protocol {
		limit = protocol
	}
	return limit
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
// The cost is real and is the reason this comment exists. L2 races every
// resolved address as the client's pick_first does and takes the first that
// connects, so an address that does not answer costs a host nothing it
// would not cost it with a client. But pick_first counts an address as
// connected once its TLS handshake is done too, and moves on to the next
// when the handshake fails; this probe settles on the address whose TCP
// connect came first, and a handshake or certificate that then fails there
// is the row's answer. At a full reading that answer counts as not served
// when it is the validator's last, so a host that lists an address taking
// connections it cannot serve beside one that serves is judged on the
// first; a client may still read from the second. An answer from the RPC
// itself is final for the client too: it does not fall back after one.
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
		// The headers and trailers a server may send, its error text among
		// them (grpc-message), bounded as the message is: grpc-go's own
		// default is 16 MiB.
		grpc.WithMaxHeaderListSize(maxHeaderListSize),
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
		// The server writes this text: kept to maxRecordedText (clip).
		r.Error = clip(err.Error())
		r.RPCCode = rpcCodeOf(err)
		r.outcome, r.rawErr = classifyDownloadError(err), r.Error
		if in.ClientRules {
			r.outcome = clientRulesOutcome(err, r.outcome, dctx.Err() == nil)
		}
		return r
	}

	proofs, rlcv, perr := parseShard(resp.Shard, coder.originalRows, coder.totalRows)
	if perr != nil && in.ClientRules {
		// The client skips a shard it cannot parse (SkipShard): the
		// validator answered with something no reader can use.
		r.Error = "parse: " + perr.Error()
		r.outcome, r.rawErr = OutcomeMalformedShard, "shard shape: "+perr.Error()
		return r
	}
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

	var rec ShardVerifier = in.Verifier
	if rec == nil {
		own, rerr := coder.c.NewReconstructor(rsema1d.Commitment(in.Commitment))
		if rerr != nil {
			r.Error = "reconstructor: " + rerr.Error()
			r.outcome, r.rawErr = OutcomeProbeError, rerr.Error()
			return r
		}
		rec = own
	}
	// Add compacts proofs in place to the rows it had not seen, so every
	// piece of evidence about this validator's answer (the indices, their
	// digest above, the assignment check below) is taken from RowIndices,
	// copied before it runs.
	novel, aerr := rec.Add(proofs, rlcv)
	if aerr != nil {
		r.Error = "commitment verify: " + aerr.Error()
		r.outcome, r.rawErr = OutcomeInvalidRows, aerr.Error()
		return r
	}
	r.CommitmentVerified = true
	r.novel = len(novel)

	idx := append([]uint32(nil), r.RowIndices...)
	sm := assign.ShardMap{in.Target.Address: in.Target.AssignedRows}
	if verr := sm.Verify(in.Target.Address, idx); verr != nil {
		r.Error = "assignment verify: " + verr.Error()
		r.ShadowedBy = shadowedBy(idx, in.Shadowers)
		if r.ShadowedBy == "" {
			r.ShadowGap = in.ShadowGap
		}
		if len(idx) < in.Target.RowCount {
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
// address (RoutableAddr).
func routableIP(ip net.IP) bool {
	a, ok := netip.AddrFromSlice(ip)
	return ok && RoutableAddr(a)
}

// RoutableAddr reports whether this observer will open a connection to an
// address: global unicast on the public internet only. Loopback, the private
// and link-local ranges, the unspecified address, multicast and the
// special-purpose ranges below are all things a validator can put on chain,
// and none of them is an endpoint a client on the internet could fetch from;
// some of them (shared address space, an overlay network such as Tailscale)
// lead into private networks this observer's host may be part of. A NAT64
// address (64:ff9b::/96) is judged by the IPv4 address it carries.
// observer/hosting draws the same line for the addresses it places.
func RoutableAddr(a netip.Addr) bool {
	a = a.Unmap().WithZone("")
	if !a.IsValid() || !a.IsGlobalUnicast() || a.IsPrivate() || a.IsLinkLocalUnicast() {
		return false
	}
	if nat64.Contains(a) {
		b := a.As16()
		return RoutableAddr(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
	}
	return specialRange(a) == ""
}

// nat64 is the well-known NAT64 prefix (RFC 6052): the last four bytes are
// the IPv4 address a translator connects to.
var nat64 = netip.MustParsePrefix("64:ff9b::/96")

// specialRanges are the ranges of the IANA IPv4 and IPv6 special-purpose
// address registries that are not reachable on the public internet and
// that Go's IsGlobalUnicast and IsPrivate let through, each with the name a
// row gives it.
var specialRanges = []struct {
	prefix netip.Prefix
	name   string
}{
	{netip.MustParsePrefix("0.0.0.0/8"), "this-network"},
	{netip.MustParsePrefix("100.64.0.0/10"), "shared (carrier-grade NAT)"},
	{netip.MustParsePrefix("192.0.0.0/24"), "protocol-assignment"},
	{netip.MustParsePrefix("192.0.2.0/24"), "documentation"},
	{netip.MustParsePrefix("198.18.0.0/15"), "benchmarking"},
	{netip.MustParsePrefix("198.51.100.0/24"), "documentation"},
	{netip.MustParsePrefix("203.0.113.0/24"), "documentation"},
	{netip.MustParsePrefix("240.0.0.0/4"), "reserved"},
	{netip.MustParsePrefix("::/96"), "IPv4-compatible"},
	{netip.MustParsePrefix("64:ff9b:1::/48"), "local-use NAT64"},
	{netip.MustParsePrefix("100::/64"), "discard-only"},
	{netip.MustParsePrefix("2001:2::/48"), "benchmarking"},
	{netip.MustParsePrefix("2001:10::/28"), "ORCHID"},
	{netip.MustParsePrefix("2001:20::/28"), "ORCHID"},
	{netip.MustParsePrefix("2001:db8::/32"), "documentation"},
	{netip.MustParsePrefix("3fff::/20"), "documentation"},
	{netip.MustParsePrefix("fec0::/10"), "site-local"},
}

// specialRange names the special-purpose range a holds, "" when none.
func specialRange(a netip.Addr) string {
	for _, r := range specialRanges {
		if r.prefix.Contains(a) {
			return r.name
		}
	}
	return ""
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
	}
	if a, ok := netip.AddrFromSlice(ip); ok {
		if name := specialRange(a.Unmap()); name != "" {
			return name
		}
	}
	return "non-routable"
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

// clientRulesOutcome re-reads a download error the way the Fibre client
// meets it (Input.ClientRules): running out of the request's time after
// connecting is the validator's slowness, a refusal as malformed or
// unimplemented is the server's error, and a reply over the protocol's
// message bound is one no client accepts. A CANCELLED status while the
// request's own context is still alive (alive) was sent by the server: the
// client meets it as a failed shard and skips it, so it is the server's
// error too. Everything else keeps its earlier reading; the caller's own
// cancel stays a gap (Run reads it from the caller's context).
func clientRulesOutcome(err error, o Outcome, alive bool) Outcome {
	ls := strings.ToLower(err.Error())
	switch {
	case o == OutcomeRPCDeadline:
		return OutcomeRPCTimeout
	case status.Code(err) == codes.InvalidArgument, status.Code(err) == codes.Unimplemented:
		return OutcomeServerError
	case status.Code(err) == codes.Canceled && alive:
		return OutcomeServerError
	case strings.Contains(ls, "received message larger than max"), strings.Contains(ls, "after decompression larger than max"):
		return OutcomeMalformedShard
	}
	return o
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

// orderAddrs is the order a host's addresses are dialled in: each once, an
// IPv4 address first and the two families taking turns after it, as
// grpc-go's pick_first interleaves them (RFC 8305), each family in the
// resolver's order.
func orderAddrs(addrs []string) []string {
	var v4, v6 []string
	seen := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		if seen[a] {
			continue
		}
		seen[a] = true
		if family(a) == "IPv6" {
			v6 = append(v6, a)
		} else {
			v4 = append(v4, a)
		}
	}
	out := make([]string, 0, len(v4)+len(v6))
	for i := 0; i < len(v4) || i < len(v6); i++ {
		if i < len(v4) {
			out = append(out, v4[i])
		}
		if i < len(v6) {
			out = append(out, v6[i])
		}
	}
	return out
}

// family names an address's IP version: "IPv6", or "IPv4" for anything else.
func family(addr string) string {
	if ip := net.ParseIP(addr); ip != nil && ip.To4() == nil {
		return "IPv6"
	}
	return "IPv4"
}

// firstPerFamily keeps the first n addresses of each IP version, in order,
// and says how many it left out.
func firstPerFamily(addrs []string, n int) ([]string, int) {
	kept := map[string]int{}
	var out []string
	for _, a := range addrs {
		if f := family(a); kept[f] < n {
			kept[f]++
			out = append(out, a)
		}
	}
	return out, len(addrs) - len(out)
}

// DialStagger is how long a connect to one of a host's addresses runs alone
// before the next address is started beside it: grpc-go's pick_first, which
// celestia-app's Fibre client connects with, waits connectionDelayInterval,
// 250 ms (RFC 8305, Happy Eyeballs).
const DialStagger = 250 * time.Millisecond

// heartbeatAddrsPerFamily is how many of a host's addresses of each IP
// version a request outside the client's rules (the reachability heartbeat)
// tries.
const heartbeatAddrsPerFamily = 2

// dialAttempt is the connect to one address of a race that it did not win:
// err is why it failed, nil when it was still connecting as another
// address connected and was let go.
type dialAttempt struct {
	addr string
	err  error
}

// raceDial connects to one of addrs the way grpc-go's pick_first connects
// the Fibre client (balancer/pickfirst): in order, the next address started
// as soon as the newest one fails, or stagger after it started while it is
// still connecting; every connect is bounded by each, and all of them by
// ctx. The first to connect wins and the others are let go. tried is every
// address started except the winner, in the order started; an address
// never started (a winner came first, or ctx ended) is in neither.
func raceDial(ctx context.Context, dial func(context.Context, string) (net.Conn, error), addrs []string, port string, each, stagger time.Duration) (net.Conn, []dialAttempt) {
	type result struct {
		i   int
		c   net.Conn
		err error
	}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	res := make(chan result, len(addrs))
	errs := make([]error, len(addrs))
	var won net.Conn
	winner, started, pending := -1, 0, 0
	startNext := func() {
		if started >= len(addrs) || won != nil || rctx.Err() != nil {
			return
		}
		i := started
		started++
		pending++
		go func() {
			actx, acancel := context.WithTimeout(rctx, each)
			defer acancel()
			c, err := dial(actx, net.JoinHostPort(addrs[i], port))
			res <- result{i, c, err}
		}()
	}
	timer := time.NewTimer(stagger)
	defer timer.Stop()
	startNext()
	for pending > 0 {
		select {
		case r := <-res:
			pending--
			switch {
			case r.err == nil && won == nil:
				won, winner = r.c, r.i
				cancel()
			case r.err == nil:
				_ = r.c.Close()
			case won == nil:
				errs[r.i] = r.err
				if r.i == started-1 {
					// The newest address failed: the next one at once.
					startNext()
					timer.Reset(stagger)
				}
			}
		case <-timer.C:
			startNext()
			timer.Reset(stagger)
		}
	}
	var tried []dialAttempt
	for i := 0; i < started; i++ {
		if i != winner {
			tried = append(tried, dialAttempt{addr: addrs[i], err: errs[i]})
		}
	}
	return won, tried
}

// appendOnce appends s to list unless it is there already.
func appendOnce(list []string, s string) []string {
	for _, x := range list {
		if x == s {
			return list
		}
	}
	return append(list, s)
}

// joinNonEmpty joins the parts that are not empty.
func joinNonEmpty(sep string, parts ...string) string {
	var out []string
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, sep)
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

// isHostUnreachable reports "no route to host" (EHOSTUNREACH): under the
// client's rules the validator's host not answering, not this observer's
// network (isNoRoute still counts it as local for the heartbeat).
func isHostUnreachable(err error) bool {
	return errors.Is(err, syscall.EHOSTUNREACH) || strings.Contains(strings.ToLower(err.Error()), "no route to host")
}

// pathUnreachable reports a connect turned away by an ICMP unreachable from
// the path to the host: Linux ends a connect still waiting for its SYN-ACK
// with the errno the ICMP code maps to, "no route to host" (host, filtered),
// "host is down", "machine is not on the network", "protocol not
// available", and "network is unreachable" (net unreachable or unknown).
// The last is also this machine's own answer when it has no route out to
// the address, and then the connect never left it: it is the path's only
// when routed (a route out exists) says so. All of these can be the
// validator's own doing (an iptables REJECT of that type) or a router's on
// its side.
func pathUnreachable(err error, routed func() bool) bool {
	ls := strings.ToLower(err.Error())
	switch {
	case isHostUnreachable(err), errors.Is(err, syscall.EHOSTDOWN), errors.Is(err, syscall.ENOPROTOOPT),
		strings.Contains(ls, "host is down"), strings.Contains(ls, "machine is not on the network"):
		return true
	case errors.Is(err, syscall.ENETUNREACH), strings.Contains(ls, "network is unreachable"):
		return routed()
	}
	return false
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

// maxRecordedText bounds every piece of text a row keeps from the wire or
// the resolver: an error, a list of attempts, a list of resolved addresses.
// A validator's server writes the text of its own errors (gRPC's
// grpc-message), and a row's text is written out twice (raw_error and the
// step's error), JSON-escaped at up to six bytes a character. Unbounded,
// one endpoint could fill the disk this host shares, and a line too long
// for the restart's reader would stop the prober until the file was edited
// by hand.
const maxRecordedText = 4 << 10

// maxHeaderListSize bounds the headers and trailers a server may send on the
// download connection: a Fibre server's take a few hundred bytes.
const maxHeaderListSize = 64 << 10

// clip cuts s to maxRecordedText bytes, at a character boundary, and says
// how long it was.
func clip(s string) string {
	if len(s) <= maxRecordedText {
		return s
	}
	cut := maxRecordedText
	for i := 0; i < utf8.UTFMax && cut > 0 && !utf8.RuneStart(s[cut]); i++ {
		cut--
	}
	return s[:cut] + fmt.Sprintf("… (%d bytes in all)", len(s))
}

// clipText bounds the text of a row (clip). TCP.Detail is built bounded,
// with the endpoint it reached last, where readers look for it.
func clipText(m *Measurement) {
	for _, s := range []*string{&m.RawError, &m.DNS.Detail, &m.DNS.Error, &m.TCP.Error,
		&m.TLS.Error, &m.Identity.Error, &m.Download.Error} {
		*s = clip(*s)
	}
}

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
