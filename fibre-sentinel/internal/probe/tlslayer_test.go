package probe

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/binary"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	tlsverify "github.com/plsgiveup/fibre/fibre-tlsverify"
)

// fibreCert builds the certificate an honest Fibre server presents: a
// self-signed TLS certificate carrying the identity extension, the binding
// payload endorsed by the validator's consensus key over cometbft's raw-bytes
// envelope. It mirrors celestia-app's tlsid builder from the protocol
// constants tlsverify exports; tlsverify.VerifyCertificate accepting the
// result is what pins it to the golden vectors.
func fibreCert(t *testing.T, cons ed25519.PrivateKey, chainID string, notBefore, notAfter time.Time) tls.Certificate {
	t.Helper()
	tlsPub, tlsPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tlsPubDER, err := x509.MarshalPKIXPublicKey(tlsPub)
	if err != nil {
		t.Fatal(err)
	}
	notBefore, notAfter = notBefore.Truncate(time.Second), notAfter.Truncate(time.Second)
	payload, err := asn1.Marshal(struct {
		Version   int
		NotBefore int64
		NotAfter  int64
		TLSPubKey []byte
	}{tlsverify.BindingVersion, notBefore.Unix(), notAfter.Unix(), tlsPubDER})
	if err != nil {
		t.Fatal(err)
	}
	signBytes := append([]byte(tlsverify.SignPrefix), payload...)
	var msg []byte
	for i, f := range [][]byte{[]byte(chainID), signBytes, []byte(tlsverify.SignUniqueID)} {
		msg = binary.AppendUvarint(msg, uint64(i+1)<<3|2)
		msg = binary.AppendUvarint(msg, uint64(len(f)))
		msg = append(msg, f...)
	}
	envelope := append([]byte("COMET::RAW_BYTES::SIGN"), binary.AppendUvarint(nil, uint64(len(msg)))...)
	envelope = append(envelope, msg...)
	ext, err := asn1.Marshal(struct {
		Payload   []byte
		Signature []byte
	}{payload, ed25519.Sign(cons, envelope)})
	if err != nil {
		t.Fatal(err)
	}
	var oid asn1.ObjectIdentifier
	for _, part := range []int{1, 3, 6, 1, 4, 1, 66463, 1, 1} {
		oid = append(oid, part)
	}
	tmpl := &x509.Certificate{
		SerialNumber:    big.NewInt(1),
		Subject:         pkix.Name{CommonName: "fibre-test"},
		NotBefore:       notBefore,
		NotAfter:        notAfter,
		ExtKeyUsage:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		ExtraExtensions: []pkix.Extension{{Id: oid, Value: ext}},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, tlsPub, tlsPriv)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: tlsPriv, Leaf: leaf}
}

// countingListener counts the TCP connections a test server accepted: the
// property under test is that one probe is one connection.
type countingListener struct {
	net.Listener
	accepted atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

type fakeFibre struct {
	fibretypes.UnimplementedFibreServer
	download func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error)
}

func (f *fakeFibre) DownloadShard(ctx context.Context, req *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
	return f.download(ctx, req)
}

// startFibre serves a Fibre gRPC endpoint with cert and returns its
// host:port and the listener's connection counter.
func startFibre(t *testing.T, cert tls.Certificate, srv fibretypes.FibreServer) (string, *countingListener) {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := &countingListener{Listener: raw}
	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	})))
	fibretypes.RegisterFibreServer(gs, srv)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(gs.Stop)
	return ln.Addr().String(), ln
}

func probeInput(host string, consPub ed25519.PublicKey) Input {
	return Input{
		Vantage: "t", ChainID: "test-chain", PromiseHash: "aa", MustServeUntil: time.Now().Add(time.Hour),
		Target: Target{
			AddressHex: "aa", Host: host, Assigned: true, Attested: true, RowCount: 4,
			PubKey: consPub, AssignedRows: []int{0, 1, 2, 3},
		},
		SchedulePoint: SchedulePoint{Label: "w1", At: time.Now()},
		// The test servers are on loopback, which a public vantage refuses.
		AllowUnroutableHost: true,
	}
}

func mustCoder(t *testing.T) *Coder {
	t.Helper()
	c, err := NewCoder(4, 8)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRun_OneConnectionCarriesHandshakeIdentityAndDownload(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	if err := tlsverify.VerifyCertificate(cert.Leaf, consPub, "test-chain"); err != nil {
		t.Fatalf("test certificate does not verify: %v", err)
	}
	host, ln := startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		return nil, status.Error(codes.NotFound, "shard not found")
	}})

	m := Run(context.Background(), probeInput(host, consPub), mustCoder(t), StepTimeouts{})
	if m.Outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s (%s), want %s", m.Outcome, m.RawError, OutcomeNotFound)
	}
	if !m.TLS.Attempted || !m.TLS.OK || m.TLS.PeerCertSHA256 == "" || m.TLS.Version != "1.3" {
		t.Errorf("tls step = %+v", m.TLS)
	}
	if !m.Identity.Attempted || !m.Identity.OK || m.Identity.ClaimedNotAfter == "" {
		t.Errorf("identity step = %+v", m.Identity)
	}
	if !m.TLS.SharedWithDownload {
		t.Error("the download must be recorded as riding the handshake's connection")
	}
	if !m.Download.Attempted || m.Download.RPCCode != "NotFound" || m.Download.RPC != "DownloadShard" {
		t.Errorf("download = %+v", m.Download)
	}
	if n := ln.accepted.Load(); n != 1 {
		t.Errorf("server accepted %d connections for one probe, want 1", n)
	}
}

func TestRun_IdentityRefusalIsOneConnectionAndPrompt(t *testing.T) {
	_, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	host, ln := startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		t.Error("DownloadShard reached the server on a connection whose identity was refused")
		return nil, status.Error(codes.NotFound, "")
	}})

	t0 := time.Now()
	// the probe expects another validator's key: the server is an impostor
	m := Run(context.Background(), probeInput(host, otherPub), mustCoder(t), StepTimeouts{})
	if m.Outcome != OutcomeIdentityFail {
		t.Fatalf("outcome = %s (%s), want %s", m.Outcome, m.RawError, OutcomeIdentityFail)
	}
	if !m.TLS.OK || m.Identity.OK || m.Identity.Reason != string(tlsverify.ReasonSignatureInvalid) || m.Identity.Stale {
		t.Errorf("tls=%+v identity=%+v", m.TLS, m.Identity)
	}
	if m.Download.Attempted || m.TLS.SharedWithDownload {
		t.Errorf("no download must be recorded after an identity refusal: %+v", m.Download)
	}
	if n := ln.accepted.Load(); n != 1 {
		t.Errorf("server accepted %d connections, want 1", n)
	}
	// a refused handshake must abort the call, not sit out the download
	// deadline while grpc redials a connection that is not there
	if el := time.Since(t0); el > 5*time.Second {
		t.Errorf("identity refusal took %s; the RPC was not aborted", el)
	}
}

func TestRun_LapsedCertificateIsStaleNotImpostor(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-48*time.Hour), now.Add(-24*time.Hour))
	host, _ := startFibre(t, cert, &fakeFibre{})
	m := Run(context.Background(), probeInput(host, consPub), mustCoder(t), StepTimeouts{})
	if m.Outcome != OutcomeIdentityFail || !m.Identity.Stale || m.Identity.Reason != string(tlsverify.ReasonCertExpired) {
		t.Fatalf("outcome=%s identity=%+v", m.Outcome, m.Identity)
	}
}

func TestRun_ReachabilityProbeStopsAfterIdentity(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	host, ln := startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		t.Error("a SkipDownload probe must not call DownloadShard")
		return nil, nil
	}})
	in := probeInput(host, consPub)
	in.SkipDownload = true
	m := Run(context.Background(), in, nil, StepTimeouts{})
	if m.Outcome != OutcomeReachable || !m.TLS.OK || !m.Identity.OK || m.TLS.SharedWithDownload {
		t.Fatalf("outcome=%s tls=%+v identity=%+v", m.Outcome, m.TLS, m.Identity)
	}
	if n := ln.accepted.Load(); n != 1 {
		t.Errorf("server accepted %d connections, want 1", n)
	}
}

func TestRun_NoTLSIsATransportFailure(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	go func() {
		for {
			c, err := raw.Accept()
			if err != nil {
				return
			}
			_, _ = c.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
			_ = c.Close()
		}
	}()
	consPub, _, _ := ed25519.GenerateKey(rand.Reader)
	m := Run(context.Background(), probeInput(raw.Addr().String(), consPub), mustCoder(t), StepTimeouts{})
	if m.Outcome != OutcomeTLSFail {
		t.Fatalf("outcome = %s (%s), want %s", m.Outcome, m.RawError, OutcomeTLSFail)
	}
	if !m.TLS.Attempted || m.TLS.OK || m.TLS.Error == "" || m.Identity.Attempted || m.Download.Attempted {
		t.Errorf("tls=%+v identity=%+v download=%+v", m.TLS, m.Identity, m.Download)
	}
}

func bigShard(n int) *fibretypes.DownloadShardResponse {
	return &fibretypes.DownloadShardResponse{Shard: &fibretypes.BlobShard{Rlcs: make([]byte, n)}}
}

// ResourceExhausted from this side's receive bound is the observer's own
// gap, with the bound on the row; the same code from the server's send
// bound is the validator's, a server error; neither is a throttle.
func TestRun_SizeBoundsAreToldApartFromAThrottle(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))

	// a shard larger than the probe's receive bound
	// Larger than the floor a tiny blob's bound cannot go below, so the
	// refusal below is the bound doing its job and not arithmetic.
	const shardBytes = 3 << 20
	host, _ := startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		return bigShard(shardBytes), nil
	}})
	in := probeInput(host, consPub)
	// The bound is what this blob's shard should weigh, not the protocol's
	// maximum: a probe of a small blob must not stand ready to accept a
	// hundred-odd megabytes from an address a validator put on chain.
	in.MaxMessageSize, in.ExpectedShardBytes = 100_000_000, shardBytes
	m := Run(context.Background(), in, mustCoder(t), StepTimeouts{})
	if m.Download.RecvLimit >= in.MaxMessageSize {
		t.Fatalf("the bound is the protocol maximum (%d), not this shard's size", m.Download.RecvLimit)
	}
	// A shard past that bound is refused here, and a refusal on this side is
	// the observer's own gap, never the validator's.
	in.ExpectedShardBytes = 50_000
	m = Run(context.Background(), in, mustCoder(t), StepTimeouts{})
	if m.Outcome != OutcomeProbeError || m.Download.RPCCode != "ResourceExhausted" {
		t.Fatalf("receive bound: outcome=%s code=%s limit=%d (%s)", m.Outcome, m.Download.RPCCode, m.Download.RecvLimit, m.RawError)
	}
	if m.Classification == ClassFault {
		t.Fatalf("this observer's own receive bound was recorded as a fault (%s)", m.RawError)
	}
	// With room for it, the same shard is received, and then fails to parse:
	// still the observer's gap, with the shape on the row.
	in.ExpectedShardBytes = shardBytes
	m = Run(context.Background(), in, mustCoder(t), StepTimeouts{})
	if m.Download.RPCCode == "ResourceExhausted" {
		t.Fatalf("a shard well inside the bound was refused: limit=%d (%s)", m.Download.RecvLimit, m.RawError)
	}
	if m.Outcome != OutcomeProbeError || !strings.Contains(m.RawError, "shard shape") || m.Classification == ClassFault {
		t.Fatalf("a shard this observer cannot parse: outcome=%s class=%s (%s), want PROBE_ERROR, never a fault", m.Outcome, m.Classification, m.RawError)
	}

	// a server whose own send bound refuses the shard
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})), grpc.MaxSendMsgSize(1000))
	fibretypes.RegisterFibreServer(gs, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		return bigShard(10_000), nil
	}})
	go func() { _ = gs.Serve(raw) }()
	t.Cleanup(gs.Stop)
	m = Run(context.Background(), probeInput(raw.Addr().String(), consPub), mustCoder(t), StepTimeouts{})
	if m.Outcome != OutcomeServerError || m.Download.RPCCode != "ResourceExhausted" {
		t.Fatalf("send bound: outcome=%s code=%s (%s)", m.Outcome, m.Download.RPCCode, m.RawError)
	}
	// SERVER_ERROR in window is held out of the rate (unobserved,
	// reachable): never a fault and never our gap
	if m.Classification == ClassFault || m.Classification == ClassProbeError {
		t.Fatalf("a server's send bound classified %s", m.Classification)
	}
}

// The registered host is whatever a validator put on chain, and the chain
// checks only the host:port shape. A public observer that dialled a loopback
// or private address would be a port scanner and a DNS resolver driven from
// the chain, publishing the address it reached and the exact error. Nothing
// is dialled, and the row says what was registered without holding it against
// the shard.
func TestRun_UnroutableRegisteredHostIsNotDialled(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	host, _ := startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		return bigShard(64), nil
	}})

	for _, addr := range []string{"127.0.0.1:9000", "10.0.0.5:443", "192.168.1.1:443", "169.254.1.1:443", "[::1]:443", "0.0.0.0:443",
		// shared address space: carrier-grade NAT, and overlay networks
		// such as Tailscale
		"100.64.0.1:7980", "100.100.1.1:7980",
		// benchmarking, documentation, reserved, a NAT64 address of a
		// private one, IPv6 documentation
		"198.18.0.1:443", "203.0.113.9:7980", "240.0.0.1:443", "[64:ff9b::a00:1]:443", "[2001:db8::1]:443"} {
		in := probeInput(addr, consPub)
		in.AllowUnroutableHost = false
		m := Run(context.Background(), in, mustCoder(t), StepTimeouts{})
		if m.Outcome != OutcomeBadHost {
			t.Errorf("%s: outcome=%s, want %s", addr, m.Outcome, OutcomeBadHost)
			continue
		}
		if m.TCP.Attempted {
			t.Errorf("%s: a connection was attempted", addr)
		}
		cls, reason := Classify(Evidence{Assigned: true, Attested: true, Phase: PhaseInWindow, Outcome: m.Outcome})
		if cls == ClassFault {
			t.Errorf("%s: recorded as a fault (%s)", addr, reason)
		}
		if cls != ClassNotRegistered {
			t.Errorf("%s: class=%s, want %s", addr, cls, ClassNotRegistered)
		}
	}

	// A real address is still probed, and the devnet escape still works.
	in := probeInput(host, consPub)
	if m := Run(context.Background(), in, mustCoder(t), StepTimeouts{}); m.Outcome == OutcomeBadHost {
		t.Fatalf("the escape did not admit a loopback test server: %s", m.RawError)
	}
}

// The identity check runs inside the handshake, before the peer proves it
// holds the certificate's key: TLS 1.3 checks its CertificateVerify after
// tls.Config.VerifyConnection. An endpoint that presents a validator's
// genuine certificate, which anyone can fetch from the validator, without
// its key fails the handshake, and its identity is not verified either, on
// the reachability check and on a reading alike. Such an answer fails
// before any blob is asked for, so it answers the validator's other
// waiting attempts too (shareable).
func TestRun_AGenuineCertificateWithoutItsKeyIsNoIdentity(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	genuine := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	_, otherKey, _ := ed25519.GenerateKey(rand.Reader)
	replayed := tls.Certificate{Certificate: genuine.Certificate, PrivateKey: otherKey, Leaf: genuine.Leaf}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{replayed}, MinVersion: tls.VersionTLS13})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_ = c.(*tls.Conn).Handshake()
				_ = c.Close()
			}()
		}
	}()
	for _, skip := range []bool{true, false} {
		in := probeInput(ln.Addr().String(), consPub)
		in.SkipDownload = skip
		m := Run(context.Background(), in, mustCoder(t), StepTimeouts{})
		if m.Outcome != OutcomeTLSFail || m.TLS.OK {
			t.Fatalf("skip download %v: outcome %s (%s), want TLS_HANDSHAKE_FAIL", skip, m.Outcome, m.RawError)
		}
		if !m.Identity.Attempted || m.Identity.OK || !strings.Contains(m.Identity.Error, "did not prove it holds") {
			t.Fatalf("skip download %v: identity %+v, verified without proof of the key", skip, m.Identity)
		}
		m.ScheduleLabel = FullReadLabel
		if !shareable(m) {
			t.Fatalf("skip download %v: a handshake that failed before any blob was asked for is not shareable", skip)
		}
	}
}
