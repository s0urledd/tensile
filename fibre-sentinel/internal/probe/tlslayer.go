package probe

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"

	tlsverify "github.com/plsgiveup/fibre/fibre-tlsverify"
)

// handshake is the TLS layer of one probe: the handshake itself (L3a) and,
// inside its VerifyConnection callback, the consensus-key identity check
// (L3b). Both are recorded as they happen, so the record is complete even
// when grpc-go drives the handshake on the download connection and the
// failure surfaces there as an opaque Unavailable.
//
// One handshake serves one connection. A probe that downloads runs it on
// the connection the download uses, so the certificate the row describes is
// the one that served the rows; a reachability probe (SkipDownload) runs it
// on the L2 connection directly and stops.
type handshake struct {
	in      Input
	to      StepTimeouts
	started time.Time

	mu  sync.Mutex
	tls TLSResult
	id  IdentityResult
	// verifyErr is what VerifyConnection returned, kept apart from the
	// handshake's own error so an identity refusal is never filed as a
	// transport failure.
	verifyErr error
	// done reports that an attempt ran to a conclusion, success or not.
	done bool
	ok   bool
}

func newHandshake(in Input, to StepTimeouts) *handshake {
	return &handshake{in: in, to: to}
}

// config is the client TLS configuration: TLS 1.3, no CA or hostname check
// (the peer's certificate is self-signed by design), identity checked by the
// consensus-key binding in verify. NextProtos carries h2 so the same
// handshake serves the gRPC connection.
func (h *handshake) config() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // identity verified by verify below
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{"h2"},
		VerifyConnection:   h.verify,
	}
}

// run performs the handshake on conn under the TLS timeout and records the
// result. The returned error is the handshake's; the caller reads the
// outcome from failure().
func (h *handshake) run(ctx context.Context, conn net.Conn) (*tls.Conn, error) {
	h.mu.Lock()
	h.started = time.Now()
	h.tls = TLSResult{Attempted: true}
	h.id = IdentityResult{}
	h.verifyErr = nil
	h.done, h.ok = false, false
	h.mu.Unlock()

	tc := tls.Client(conn, h.config())
	hctx, cancel := context.WithTimeout(ctx, h.to.TLS)
	err := tc.HandshakeContext(hctx)
	cancel()

	h.mu.Lock()
	defer h.mu.Unlock()
	h.done = true
	if err == nil {
		h.ok = true
		return tc, nil
	}
	if h.verifyErr == nil {
		// The handshake failed before or after the identity check, on the
		// wire. Before it, no certificate was judged. After it, the
		// certificate's endorsement verified but the peer never proved it
		// holds the certificate's key (its CertificateVerify, which TLS 1.3
		// checks after this callback, failed, or the handshake did not get
		// that far): anyone can present a validator's genuine certificate,
		// so the identity is not verified either.
		h.tls.OK = false
		h.tls.DurationMS = sinceMS(h.started)
		h.tls.Error = err.Error()
		if h.id.OK {
			h.id.OK = false
			h.id.Error = "the certificate's endorsement verified, but the handshake did not finish, so the peer did not prove it holds the certificate's key: " + err.Error()
		}
	}
	return nil, err
}

// verify is the VerifyConnection callback. Go's TLS 1.3 client calls it
// once the peer's certificate has been received and before the peer's
// CertificateVerify is checked: the certificate is judged here, but the
// peer has not yet proved it holds its key. The TLS step is recorded OK and
// the identity judged here; run takes both back if the handshake then
// fails.
func (h *handshake) verify(cs tls.ConnectionState) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tls.OK = true
	h.tls.DurationMS = sinceMS(h.started)
	h.tls.Version = tlsVersionString(cs.Version)
	h.tls.CipherSuite = tls.CipherSuiteName(cs.CipherSuite)
	var peerCert *x509.Certificate
	if len(cs.PeerCertificates) > 0 {
		peerCert = cs.PeerCertificates[0]
	}
	if peerCert != nil {
		sum := sha256.Sum256(peerCert.Raw)
		h.tls.PeerCertSHA256 = hex.EncodeToString(sum[:])
		h.tls.PeerCertNotAfter = peerCert.NotAfter.UTC().Format(time.RFC3339)
	}

	t0 := time.Now()
	h.id = IdentityResult{Attempted: true}
	if peerCert == nil {
		h.id.Error = "peer presented no certificate"
		h.id.DurationMS = sinceMS(t0)
		h.verifyErr = errors.New(h.id.Error)
		return h.verifyErr
	}
	if claimed, ierr := tlsverify.Inspect(peerCert); ierr == nil {
		h.id.ClaimedNotBefore = claimed.NotBefore.UTC().Format(time.RFC3339)
		h.id.ClaimedNotAfter = claimed.NotAfter.UTC().Format(time.RFC3339)
	}
	// The identity check is pure computation on the peer cert; the timeout
	// bounds it anyway so a pathological certificate cannot stall a probe.
	verr := verifyWithin(peerCert, h.in.Target.PubKey, h.in.ChainID, h.to.Identity)
	h.id.DurationMS = sinceMS(t0)
	h.id.OK = verr == nil
	if verr != nil {
		if !errors.Is(verr, errVerifyTimeout) {
			if reason, ok := tlsverify.ReasonOf(verr); ok {
				h.id.Reason = string(reason)
				h.id.Stale = identityStale(reason)
			}
		} else {
			h.id.Reason = "observer_timeout"
		}
		h.id.Error = verr.Error()
		h.verifyErr = verr
		return verr
	}
	return nil
}

// results returns the recorded TLS and identity steps.
func (h *handshake) results() (TLSResult, IdentityResult) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.tls, h.id
}

// failure returns the outcome a failed attempt is filed under, and false
// when no attempt failed (none ran, or it succeeded).
func (h *handshake) failure() (Outcome, string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.done || h.ok {
		return "", "", false
	}
	if h.verifyErr == nil {
		return OutcomeTLSFail, h.tls.Error, true
	}
	if errors.Is(h.verifyErr, errVerifyTimeout) {
		// The observer ran out of CPU, not the validator out of honesty.
		return OutcomeProbeError, h.verifyErr.Error(), true
	}
	return OutcomeIdentityFail, h.verifyErr.Error(), true
}

// probeCreds is the gRPC transport credential for the download connection.
// grpc-go hands it the connection L2 opened (see handOff) and it runs the
// probe's own handshake on it, so the layers of one row describe one TCP
// session. A failed handshake aborts the RPC at once: left alone, grpc-go
// would redial with backoff until the call's deadline, and the dialer has
// nothing more to hand over.
type probeCreds struct {
	hs    *handshake
	abort context.CancelFunc
}

func (c *probeCreds) ClientHandshake(ctx context.Context, _ string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	tc, err := c.hs.run(ctx, raw)
	if err != nil {
		if c.abort != nil {
			c.abort()
		}
		return nil, nil, err
	}
	return tc, credentials.TLSInfo{
		State:          tc.ConnectionState(),
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
	}, nil
}

func (c *probeCreds) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("probeCreds: client side only")
}

func (c *probeCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls", SecurityVersion: "1.3"}
}

func (c *probeCreds) Clone() credentials.TransportCredentials { return c }

func (c *probeCreds) OverrideServerName(string) error { return nil }

// handOff returns a grpc dialer that yields conn exactly once. A second
// call is grpc-go reconnecting after a failure, and there is no second
// connection: the probe judges one.
func handOff(conn net.Conn) func(context.Context, string) (net.Conn, error) {
	var once sync.Once
	return func(context.Context, string) (net.Conn, error) {
		var out net.Conn
		once.Do(func() { out = conn })
		if out == nil {
			return nil, errors.New("probe connection already used")
		}
		return out, nil
	}
}
