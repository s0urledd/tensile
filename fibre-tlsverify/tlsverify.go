package tlsverify

import (
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"slices"
	"time"
)

// Protocol constants. These mirror celestia-app/fibre/internal/tlsid and are
// pinned by the "constants" block of testdata/identity_vectors.json (checked in
// TestGoldenVectorConstants). Bumping any of them upstream is a protocol break
// that must be mirrored here.
const (
	// ExtensionOID is the dotted OID of the identity extension.
	// 66463 is Celestia's IANA Private Enterprise Number; .1 is Fibre, .1.1 is
	// this TLS signed-identity extension.
	ExtensionOID = "1.3.6.1.4.1.66463.1.1"

	// SignUniqueID is the cometbft privval domain-separation tag.
	SignUniqueID = "celestia-fibre-tls-v1"
	// SignPrefix is mixed into the signed bytes ahead of the payload DER.
	SignPrefix = "celestia-fibre-tls:"

	// BindingVersion is the only accepted BindingPayload schema version.
	BindingVersion = 1

	// MaxPayloadDERSize bounds the signed BindingPayload DER accepted from a
	// peer certificate.
	MaxPayloadDERSize = 4096
	// MaxIdentityExtensionSize bounds the custom extension DER.
	MaxIdentityExtensionSize = 8192

	// ClockSkew is tolerated on both edges of the validity window.
	ClockSkew = 5 * time.Minute
	// CertValidity is the window length a well-formed server issues.
	CertValidity = 365 * 24 * time.Hour
	// MaxCertValidity is the largest signed window a verifier will accept.
	MaxCertValidity = CertValidity + 2*ClockSkew
)

const signPrefix = SignPrefix

var extensionOID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 66463, 1, 1}

// signedIdentity is the ASN.1 payload of the custom extension:
//
//	SignedIdentity ::= SEQUENCE { payload OCTET STRING, signature OCTET STRING }
type signedIdentity struct {
	Payload   []byte
	Signature []byte
}

// bindingPayload is the signed statement binding the ephemeral TLS key to its
// validity window:
//
//	BindingPayload ::= SEQUENCE {
//	  version   INTEGER,
//	  notBefore INTEGER,  -- unix seconds
//	  notAfter  INTEGER,  -- unix seconds
//	  tlsPubKey OCTET STRING  -- DER SubjectPublicKeyInfo of the TLS key
//	}
type bindingPayload struct {
	Version   int
	NotBefore int64
	NotAfter  int64
	TLSPubKey []byte
}

// Identity is the validated (or, from Inspect, merely parsed) content of a
// Fibre identity extension. A Sentinel probe stores these fields alongside the
// verdict as a raw measurement.
type Identity struct {
	// NotBefore/NotAfter are the signed validity window.
	NotBefore time.Time
	NotAfter  time.Time
	// TLSPubKeyDER is the SubjectPublicKeyInfo DER that was signed.
	TLSPubKeyDER []byte
	// ExtensionDER is the raw extension value (the SignedIdentity DER).
	ExtensionDER []byte
	// PayloadDER is the raw signed BindingPayload DER.
	PayloadDER []byte
	// Signature is the consensus-key signature over the envelope.
	Signature []byte
}

// Signature verification uses crypto/ed25519, which enforces canonical
// encodings. CometBFT verifies consensus-key signatures under ZIP-215, which
// accepts some non-canonical R/A encodings this package rejects. An honest
// signer never produces such a signature, so the two agree on every
// certificate a correct Fibre server presents; a signature that the network
// would accept and this package would not is recorded as IDENTITY_FAIL with
// reason signature_invalid. Revisit if celestia-app ever publishes a vector
// for it.
//
// VerifyCertificateAt checks that cert carries a Fibre identity extension validly
// endorsed by the expected validator consensus key for chainID, and that the
// certificate is internally consistent with the signed binding at instant now.
//
// A nil error means: the holder of the consensus key `expected` authorized
// this certificate's TLS key, and the binding is in force at `now`. It does
// not mean the peer that presented the certificate holds that TLS key:
// anyone can present a validator's genuine certificate. Only a TLS handshake
// that completes proves that (TLS 1.3 checks the peer's CertificateVerify
// after tls.Config.VerifyConnection runs), so a caller that verifies inside
// the handshake must treat the identity as verified only once the handshake
// has succeeded. Any error is a *VerificationError; switch on its Reason.
//
// The checks run in a fixed order so the first failing property determines the
// Reason. That order matches celestia-app's tlsid.verifyCertAt.
func VerifyCertificateAt(cert *x509.Certificate, expected ed25519.PublicKey, chainID string, now time.Time) error {
	_, err := verify(cert, expected, chainID, now)
	return err
}

// VerifyCertificate is VerifyCertificateAt at time.Now().
func VerifyCertificate(cert *x509.Certificate, expected ed25519.PublicKey, chainID string) error {
	return VerifyCertificateAt(cert, expected, chainID, time.Now())
}

// VerifyAndInspectAt is VerifyCertificateAt that also returns the validated
// Identity on success (nil, err on failure).
func VerifyAndInspectAt(cert *x509.Certificate, expected ed25519.PublicKey, chainID string, now time.Time) (*Identity, error) {
	return verify(cert, expected, chainID, now)
}

func verify(cert *x509.Certificate, expected ed25519.PublicKey, chainID string, now time.Time) (*Identity, error) {
	if l := len(expected); l != ed25519.PublicKeySize {
		return nil, failf(ReasonBadInput, "expected consensus key must be %d bytes, got %d", ed25519.PublicKeySize, l)
	}
	if chainID == "" {
		return nil, failf(ReasonBadInput, "chain ID must not be empty")
	}
	if cert == nil {
		return nil, failf(ReasonBadInput, "nil certificate")
	}

	// 1. Locate the identity extension by OID.
	var extBytes []byte
	for _, ext := range cert.Extensions {
		if ext.Id.Equal(extensionOID) {
			extBytes = ext.Value
			break
		}
	}
	if extBytes == nil {
		return nil, failf(ReasonExtensionMissing, "certificate is missing the fibre identity extension %s", ExtensionOID)
	}
	if len(extBytes) > MaxIdentityExtensionSize {
		return nil, failf(ReasonExtensionTooLarge, "identity extension size %d exceeds maximum %d", len(extBytes), MaxIdentityExtensionSize)
	}

	// 2. Parse SignedIdentity and bound its parts.
	var id signedIdentity
	rest, err := asn1.Unmarshal(extBytes, &id)
	if err != nil {
		return nil, wrapErr(ReasonExtensionMalformed, "unmarshal identity extension", err)
	}
	if len(rest) != 0 {
		return nil, failf(ReasonExtensionTrailingData, "trailing bytes in identity extension")
	}
	if len(id.Payload) == 0 {
		return nil, failf(ReasonPayloadEmpty, "empty identity payload")
	}
	if len(id.Payload) > MaxPayloadDERSize {
		return nil, failf(ReasonPayloadTooLarge, "identity payload size %d exceeds maximum %d", len(id.Payload), MaxPayloadDERSize)
	}
	if len(id.Signature) == 0 {
		return nil, failf(ReasonSignatureEmpty, "empty identity signature")
	}

	// 3. Parse BindingPayload from the exact signed bytes (no re-encode).
	var bp bindingPayload
	rest, err = asn1.Unmarshal(id.Payload, &bp)
	if err != nil {
		return nil, wrapErr(ReasonPayloadMalformed, "unmarshal binding payload", err)
	}
	if len(rest) != 0 {
		return nil, failf(ReasonBindingTrailingData, "trailing bytes in binding payload")
	}
	if bp.Version != BindingVersion {
		return nil, failf(ReasonUnsupportedVersion, "unsupported fibre identity version %d (want %d)", bp.Version, BindingVersion)
	}

	// 4. The endorsement signature MUST verify under `expected` over the
	// recomputed envelope. This is the whole trust anchor.
	envelope := rawBytesMessageSignBytes(chainID, SignUniqueID, signInputBytes(id.Payload))
	if !ed25519.Verify(expected, envelope, id.Signature) {
		return nil, failf(ReasonSignatureInvalid,
			"endorsement signature does not verify under the expected consensus key (also fires on wrong chain ID or a tampered signature)")
	}

	// 5. Bind the presented TLS key to the signed key. TLS 1.3 CertificateVerify
	// proves the peer holds the private half.
	tlsPubDER, err := x509.MarshalPKIXPublicKey(cert.PublicKey)
	if err != nil {
		return nil, wrapErr(ReasonTLSKeyMismatch, "marshal peer TLS public key", err)
	}
	if !bytes.Equal(tlsPubDER, bp.TLSPubKey) {
		return nil, failf(ReasonTLSKeyMismatch, "certificate public key does not match the signed tlsPubKey")
	}

	// 6. Validity window: well-formed, bounded, and in force now.
	notBefore := time.Unix(bp.NotBefore, 0)
	notAfter := time.Unix(bp.NotAfter, 0)
	if !notAfter.After(notBefore) {
		return nil, failf(ReasonWindowEmpty, "signed validity window is empty (notAfter %s <= notBefore %s)",
			notAfter.UTC().Format(time.RFC3339), notBefore.UTC().Format(time.RFC3339))
	}
	if notAfter.Sub(notBefore) > MaxCertValidity {
		return nil, failf(ReasonWindowTooLong, "signed validity window %s exceeds maximum %s",
			notAfter.Sub(notBefore), MaxCertValidity)
	}
	if now.Before(notBefore.Add(-ClockSkew)) {
		return nil, failf(ReasonCertNotYetValid, "identity not valid until %s (now %s)",
			notBefore.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	}
	if now.After(notAfter.Add(ClockSkew)) {
		return nil, failf(ReasonCertExpired, "identity expired at %s (now %s)",
			notAfter.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	}

	// 7. The certificate's own NotBefore/NotAfter MUST equal the signed window,
	// so a reissued cert cannot widen it without breaking the signature.
	if cert.NotBefore.Unix() != bp.NotBefore || cert.NotAfter.Unix() != bp.NotAfter {
		return nil, failf(ReasonCertWindowMismatch, "certificate validity does not match signed identity")
	}

	// 8. serverAuth EKU, as the honest builder sets.
	if !slices.Contains(cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth) {
		return nil, failf(ReasonEKUMissing, "certificate missing serverAuth extended key usage")
	}

	return &Identity{
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		TLSPubKeyDER: bytes.Clone(bp.TLSPubKey),
		ExtensionDER: bytes.Clone(extBytes),
		PayloadDER:   bytes.Clone(id.Payload),
		Signature:    bytes.Clone(id.Signature),
	}, nil
}

// Inspect parses the Fibre identity extension out of cert without verifying the
// signature, window, or certificate consistency. It is for diagnostics — a
// Sentinel probe recording "the peer presented an extension claiming validity
// until X" regardless of verdict, or a Fibre Doctor showing an operator what
// their own server is serving. Returns ReasonExtensionMissing / *Malformed /
// *PayloadMalformed on structural problems.
func Inspect(cert *x509.Certificate) (*Identity, error) {
	if cert == nil {
		return nil, failf(ReasonBadInput, "nil certificate")
	}
	var extBytes []byte
	for _, ext := range cert.Extensions {
		if ext.Id.Equal(extensionOID) {
			extBytes = ext.Value
			break
		}
	}
	if extBytes == nil {
		return nil, failf(ReasonExtensionMissing, "certificate is missing the fibre identity extension %s", ExtensionOID)
	}
	var id signedIdentity
	if _, err := asn1.Unmarshal(extBytes, &id); err != nil {
		return nil, wrapErr(ReasonExtensionMalformed, "unmarshal identity extension", err)
	}
	var bp bindingPayload
	if _, err := asn1.Unmarshal(id.Payload, &bp); err != nil {
		return nil, wrapErr(ReasonPayloadMalformed, "unmarshal binding payload", err)
	}
	return &Identity{
		NotBefore:    time.Unix(bp.NotBefore, 0),
		NotAfter:     time.Unix(bp.NotAfter, 0),
		TLSPubKeyDER: bytes.Clone(bp.TLSPubKey),
		ExtensionDER: bytes.Clone(extBytes),
		PayloadDER:   bytes.Clone(id.Payload),
		Signature:    bytes.Clone(id.Signature),
	}, nil
}

// VerifyConnection returns a callback for tls.Config.VerifyConnection. Prefer it
// over VerifyPeerCertificate: it also runs on resumed TLS 1.3 sessions, so peer
// identity is re-checked on every connection. Use with InsecureSkipVerify=true;
// this callback replaces CA/hostname validation with the consensus-key binding.
func VerifyConnection(expected ed25519.PublicKey, chainID string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return failf(ReasonNoCertificate, "peer presented no certificate")
		}
		return VerifyCertificate(cs.PeerCertificates[0], expected, chainID)
	}
}

// VerifyPeerCertificate returns a callback for tls.Config.VerifyPeerCertificate.
// It does not run on resumed TLS 1.3 sessions — prefer VerifyConnection.
func VerifyPeerCertificate(expected ed25519.PublicKey, chainID string) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return failf(ReasonNoCertificate, "peer presented no certificate")
		}
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return wrapErr(ReasonExtensionMalformed, "parse peer certificate", err)
		}
		return VerifyCertificate(cert, expected, chainID)
	}
}

// ClientTLSConfig returns a *tls.Config for dialing a Fibre server whose
// identity is `expected` on `chainID`. It pins TLS 1.3 and installs
// VerifyConnection; InsecureSkipVerify is set because the peer certificate is
// self-signed and authenticity comes from the consensus-key binding, not a CA.
//
// The returned config has no ServerName, matching the protocol: nothing about
// the network location is bound, so a validator may be reached by IP or DNS.
func ClientTLSConfig(expected ed25519.PublicKey, chainID string) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // identity verified by VerifyConnection below
		MinVersion:         tls.VersionTLS13,
		VerifyConnection:   VerifyConnection(expected, chainID),
	}
}
