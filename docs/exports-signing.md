# Signed and anchored daily exports

The daily exports (`/v1/exports`) already carry digests: the tarball's sha256
in the index and in `<name>.sha256`, and every member's sha256, byte count
and line count in `manifest.json`. Digests let two people agree they hold the
same bytes. They do not say who published them, or when. This page covers the
two additions that do:

- **a signature**: an ed25519 signature by the observer operator's key over
  each day's manifest digest, beside the export as `<name>.sig` and in its
  index entry. Optional; off unless a key is configured.
- **an anchor** (optional, not enabled): the same digest written into a
  regular Celestia blob, so a block fixes the time it existed. The command
  that builds the transaction exists and runs dry; broadcasting is not
  implemented.

Code: `fibre-sentinel/observer/export/sign.go`, `verify.go`, `anchor.go`;
`cmd/sentinel-verify-export`; `cmd/sentinel-anchor`.

## What is signed

```
message   = "tensile-export-manifest/v1:" + lowercase hex(sha256(manifest.json))
signature = ed25519(message)          # 64 bytes, base64 in the .sig
```

`manifest.json` is the member as the tarball holds it. The manifest names
every other member with its sha256, so signing it commits to every record in
the export; the tarball digest adds only gzip and tar framing. No trailing
newline, no other bytes. The domain prefix stops the signature being reused
as a signature over some other hex string.

`<name>.sig` (the index entry's `signature` is the same without
`public_key`, which `/v1/exports` carries once, in `signing.current`):

```json
{
  "algorithm": "ed25519",
  "manifest_sha256": "<hex>",
  "message": "tensile-export-manifest/v1:<hex>",
  "signature": "<base64, 64 bytes>",
  "key_fingerprint": "sha256:<hex of sha256(raw 32-byte public key)>",
  "public_key": "<base64, 32 bytes>"
}
```

`public_key` is a convenience. A signature checked against the key it carries
proves nothing, so every verifier here checks `key_fingerprint` against a key
obtained separately.

## Getting the key

`GET /v1/exports/pubkey` returns the current key (`current.public_key_pem`,
`current.key_fingerprint`) and every key that ever signed an export here,
with the first and last day it signed (`keys`), so a rotation leaves older
exports verifiable. It is a JSON 404 while nothing has been signed.

Fetch it once and keep it. A key fetched from the same server as the export
only proves both came from that server; it is worth more the more
independently you got it (the operator's announcement, an earlier download,
the fingerprint in an on-chain anchor).

## Verifying

With the repository's tool, offline:

```sh
go run ./cmd/sentinel-verify-export -pubkey tensile-exports.pem \
    [-fingerprint sha256:<hex>] [-require-signature] \
    tensile-<vantage>-2026-09-10.tar.gz
```

It checks the `.sha256` sidecar, every member against `manifest.json`
(digest, bytes, lines, nothing extra in the tarball), and the `.sig` against
the manifest digest and the key. Exit 0 pass, 1 a check failed, 2 usage, 3
unsigned under `-require-signature`. `-pubkey` accepts the PEM, the raw key
in hex or base64, or the JSON the pubkey route returns; given the JSON, each
export is checked against the listed key its `.sig` names, so exports signed
before a rotation still verify.

With nothing but coreutils, tar, jq and OpenSSL 3:

```sh
API=https://<observer>          # the API origin
N=tensile-<vantage>-2026-09-10.tar.gz
curl -sO "$API/v1/exports/$N" -O "$API/v1/exports/$N.sha256" -O "$API/v1/exports/$N.sig"
curl -s "$API/v1/exports/pubkey" > tensile-exports.json                        # once; keep it
jq -r --arg fp "$(jq -r .key_fingerprint "$N.sig")" \
  '.keys[] | select(.key_fingerprint == $fp) | .public_key_pem' tensile-exports.json > tensile-exports.pem

sha256sum -c "$N.sha256"                                              # the tarball
printf '%s' "tensile-export-manifest/v1:$(tar -xzOf "$N" manifest.json | sha256sum | cut -c1-64)" > msg
jq -r .signature "$N.sig" | base64 -d > sig.bin
openssl pkeyutl -verify -pubin -inkey tensile-exports.pem -rawin -in msg -sigfile sig.bin
# -> "Signature Verified Successfully"
```

That proves the manifest. To tie the members to it, compare each member's
`sha256sum` with its entry in `manifest.json` (the tool does this).

## Turning signing on (operator)

1. Make a key, on the observer host, readable only by the collector's user:

   ```sh
   openssl genpkey -algorithm ed25519 -out export-signing.pem
   # or: sentinel-verify-export -keygen export-signing.pem   (writes .pub too, prints the fingerprint)
   chmod 600 export-signing.pem
   ```

2. Give the collector the path: `-export-signing-key /path/export-signing.pem`
   or `TENSILE_EXPORT_SIGNING_KEY=/path/export-signing.pem`. A configured key
   that cannot be loaded stops the collector rather than falling back to
   unsigned. The collector logs `exports are signed by sha256:…` at start.

3. Publish the fingerprint somewhere that is not this API (the repository
   README, a signed announcement), so readers have an independent copy.

Only exports built after that are signed. Exports built before stay
unsigned; nothing rewrites them. With no key configured the index, the
tarballs and the sidecars are byte-for-byte what they were before signing
existed (`signature` is omitted from the index, not written empty).

Rotating: make the new key and publish its fingerprint (step 3) first, beside
the old one, then configure it. The new key reaches `signing-keys.json`
before the first `.sig` and index entry that name it, and becomes `current`
with that export; the file keeps the old key with the days it signed, so
the exports it signed stay verifiable. Do not delete it, and do not remove
the old fingerprint from wherever you announced it.

## Anchoring (optional; dry run only)

A signature says who; an anchor says *when*. Whoever holds the key could
sign a replacement for a past day at any time, but cannot back-date a block.
`sentinel-anchor` builds a `MsgPayForBlobs` carrying the day's anchor payload
under the version-0 namespace ID `tensileexp` (hex `74656e73696c65657870`),
signs it with a key from an existing keyring, and prints it. **It never
broadcasts; this build has no broadcast path.**

Payload (canonical JSON, about 500 bytes):

```json
{"type":"tensile-export-anchor/v1","vantage":"…","day":"2026-09-10","export":"tensile-…-2026-09-10.tar.gz",
 "manifest_sha256":"…","tarball_sha256":"…","key_fingerprint":"sha256:…","signature":"…"}
```

The manifest digest is recomputed from the tarball on disk, and the command
refuses an export whose members do not match its manifest.

On the observer host (the ops key; `--keyring-dir` has celestia-appd's
meaning, so the test backend reads `<dir>/keyring-test`):

```sh
sentinel-anchor -exports-dir <data-dir>/exports -day 2026-09-10 \
  -keyring-backend test -keyring-dir /etc/fibre-observer/keyring-mocha -key-name tensile-ops \
  -grpc <node:9090>
# or, offline: -chain-id <chain id> -account-number <n> -sequence <s>
```

With `-grpc` the chain id, account number and sequence are read from the
node (two read-only queries); without it `-chain-id` is required, since
there is no default to go stale at the next hardspoon, and given both it
must match the node.

Run it as the unit user (`fibre-observer`), which owns the keyring. It
prints `dry_run: true`, the signer
(`celestia1jw8afsj3j0c23fxs09nu8pq5asxwes5e3kkxdx`), the namespace, the
payload, the share commitment, gas (`DefaultEstimateGas`, the chain's own
linear model), the fee at `-gas-price` (default 0.004 utia/gas), the tx hash
and the signed `BlobTx` in base64.

Cost: this is a plain PayForBlobs, not Fibre. A one- or two-share blob
estimates at about 80,000 gas (79,796 for a 276-byte payload in the test), so about 320 utia at 0.004 utia/gas:
a year of daily anchors is about 0.12 TIA. (Fibre's 650,000 + 45,000 × chunks
escrow charge does not apply.)

To enable it later, the owner decides to spend the funds and then either
adds a broadcast call (`user.TxClient.BroadcastTx` on the same bytes, the
path `cmd/sentinel-pub` uses) behind an explicit flag, or broadcasts the
printed bytes with their own tooling. Until then, anchors are not published
and nothing on the site claims they are.

Verifying an anchor, once published: find the blob in namespace
`tensileexp` at the block the operator names (any Celestia node's
`blob.GetAll` for that namespace and height), check its `manifest_sha256`
against the export, and its `key_fingerprint` against the key you hold.
