# fibre-tlsverify

A standalone verifier for the **validator-endorsed TLS identity** a Celestia
Fibre server presents.

```
import "github.com/plsgiveup/fibre/fibre-tlsverify"
```

## What it does

A Fibre server presents a self-signed TLS certificate. There is no certificate
authority. Instead, the certificate carries a custom X.509 extension (OID
`1.3.6.1.4.1.66463.1.1`) holding a short statement — signed by the validator's
**consensus key** — that authorizes this certificate's ephemeral TLS key for a
validity window.

Given the consensus public key you already know from the chain's validator set,
plus the chain ID, this package answers one question:

> Is the server on the other end of this TLS connection really the validator I
> think it is?

`ClientTLSConfig(consensusKey, chainID)` returns a `*tls.Config` that enforces
exactly that on every connection (including resumed TLS 1.3 sessions). Lower-level
entry points let you verify a `*x509.Certificate` directly or just parse the
extension for diagnostics.

## Why it exists

The verification logic lives in celestia-app's `fibre/internal/tlsid` package,
which is `internal/` — you cannot import it from outside the celestia-app module.
An independent observer (a monitor, a health check, a block explorer, a
retrieval client) that wants to confirm it is talking to the right validator
would otherwise have to vendor all of celestia-app.

This package is a from-scratch re-implementation of the **protocol logic** only.
It does not re-implement cryptography — `crypto/ed25519`, `crypto/x509`,
`encoding/asn1` do that. The single piece of framing reproduced by hand is
cometbft's `RawBytesMessageSignBytes` envelope (`envelope.go` — plain protobuf +
varint), and there is a test that proves that reproduction byte-exact against
committed vectors, with no crypto involved.

**Zero dependencies.** Standard library only. No `go.sum`. Builds and tests in
under a second on any Go 1.23+.

## How it was verified

`testdata/identity_vectors.json` is a **byte-for-byte copy** of celestia-app's
golden vectors (`fibre/internal/tlsid/testdata`, commit
`dba155084505a8f6c5d37260a94f70f939fb96de`). One command runs all of them
through this verifier:

```
cd fibre-tlsverify && go test ./...
```

| test | what it proves |
|---|---|
| `TestGoldenVectors` | the **1 valid** vector passes; each of the **20 failure** vectors fails with the exact machine-readable `Reason` the file specifies — 21 cases total |
| `TestGoldenVectorConstants` | the protocol constants compiled into this package match the vector file's `constants` block |
| `TestEnvelopeMatchesVectors` | `rawBytesMessageSignBytes` reproduces every committed `signed_bytes` **byte-for-byte** (no crypto — pure serialization proof) |
| `TestInspectSurvivesFailures` | `Inspect` still surfaces the claimed validity window for a certificate that fails full verification |
| `TestVerifyConnectionAndConfig` | the `tls.Config` wiring accepts the valid certificate |
| `TestReasonHelpers` | `ReasonOf` / `*VerificationError` behaviour |

CI runs this with `-race` on every push.

To refresh the vectors after a celestia-app change:

```
cp "$CELESTIA_APP/fibre/internal/tlsid/testdata/identity_vectors.json" testdata/
go test ./...   # adjust wantValid/wantInvalid in tlsverify_test.go if the case set changed
```

## Usage

```go
cfg := tlsverify.ClientTLSConfig(expectedConsensusKey, chainID) // ed25519.PublicKey, string
conn, err := tls.Dial("tcp", host, cfg)
```

`ClientTLSConfig` pins TLS 1.3, sets `InsecureSkipVerify` (the consensus-key
check replaces CA/SAN validation), and installs `VerifyConnection` — which,
unlike `VerifyPeerCertificate`, also runs on resumed sessions, so identity is
re-checked on every connection. It sets no `ServerName`: the protocol binds
nothing about network location, so a validator may be reached by IP or DNS.

Direct entry points:

```go
tlsverify.VerifyCertificate(cert, expected, chainID)            // now = time.Now()
tlsverify.VerifyCertificateAt(cert, expected, chainID, instant) // testable
tlsverify.VerifyAndInspectAt(cert, expected, chainID, instant)  // + *Identity on success
tlsverify.Inspect(cert)                                          // parse only, no verdict
```

A nil error from these says the certificate carries a valid endorsement of
its TLS key by the validator's consensus key. It does not say the peer holds
that key: anyone can present a validator's genuine certificate. Only a TLS
handshake that completes proves it, and TLS 1.3 checks the peer's
`CertificateVerify` after `VerifyConnection` has run, so count the identity
as verified only once the handshake has succeeded.

## Error classification

Every failure is a `*VerificationError` with a stable `Reason` string; extract it
with `tlsverify.ReasonOf(err)`. The strings are part of the contract — probes and
dashboards key on them. They mirror the enum in celestia-app's
`specs/src/fibre_tls_identity.md`, with two deliberate refinements:

- `outside_validity_window` → `cert_not_yet_valid` **or** `cert_expired` (so a
  probe can tell a clock problem from an expiry)
- everything else is 1:1

`signature_invalid` is **one** class covering a wrong expected key, a wrong chain
ID in the envelope, and a tampered signature — the verifier genuinely cannot
tell them apart. A caller certain of its `chainID` reads `signature_invalid` as
"this is not the validator I expected". Full list in `errors.go`.

## License

Apache-2.0. See [`../LICENSE`](../LICENSE) and [`../NOTICE`](../NOTICE).
