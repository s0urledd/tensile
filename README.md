# Tensile

**The explorer for Celestia Fibre.** Tensile follows every Fibre blob in real
time, from the block that settles it to the end of its retention window, and
verifies that each validator still serves the rows it signed for. Built by
Huginn Tech.

Live at **https://tensile.huginn.tech** ·
[methodology](https://tensile.huginn.tech/methodology/) ·
[API](https://tensile.huginn.tech/api/)

## What Tensile does

- **Real time.** Blocks are read the moment the node announces them: a new
  blob is on the site within seconds of its block, and the live pages update
  on their own.
- **Every blob, searchable.** Each settlement with its publisher, namespace,
  size, fee, endorsing voting power and transaction. Any blob is one search
  away by its blob ID (base64 or hex), transaction hash or payment promise
  hash.
- **Service, verified.** Before each blob's retention window ends, every
  validator that endorsed it is asked for its own rows the way the Fibre
  client asks, and every row is checked against the on-chain commitment.
  Each validator's service rate comes from what it actually served
  ([`docs/verdicts.md`](docs/verdicts.md)).
- **Endpoints, watched.** Every registered Fibre endpoint is checked every five
  minutes from two locations: DNS, TCP, TLS 1.3 and the identity signed by
  the validator's consensus key. Reachability and throughput for every
  validator.
- **Publishers and escrow.** Who pays for blobs, what they posted and paid,
  and the escrow they pay from, straight from the chain.
- **A record anyone can check.** Nothing is ever deleted, so every page reads
  the same later as it does today. Signed daily exports
  ([`docs/exports-signing.md`](docs/exports-signing.md)) and an open API, with
  every rate beside its numerator and denominator, let anyone recompute every
  figure.

## Modules

| module | what it is | dependencies |
|---|---|---|
| [`fibre-tlsverify`](fibre-tlsverify/) | verifies the TLS identity a Fibre server presents (consensus-key signed extension, no CA) | stdlib only |
| [`fibre-assign`](fibre-assign/) | recomputes which validator must serve which rows; `ShardMap.Verify` classifies what one returned | stdlib only (the differential test in `reftest/` pulls celestia-app) |
| [`fibre-sentinel`](fibre-sentinel/) | the observer: chain scanner, prober, heartbeat, collector, store, verdicts, exports and API | celestia-app (pinned) and the two above |
| [`fibre-devnet`](fibre-devnet/) | a multi-validator local devnet with retention at the 10-minute protocol floor, for end-to-end tests | shell and a celestia-app build |
| [`web`](web/) | the explorer: a static Next.js export that reads the API | Node 22 |

`fibre-sentinel` and `fibre-assign/reftest` resolve their siblings through
relative `replace` directives, so build them from a full checkout.
`fibre-tlsverify` and `fibre-assign` are importable as libraries on their own.

How the running system fits together (processes, record files, schema,
invariants) is in [`docs/SYSTEM.md`](docs/SYSTEM.md).

## Run and deploy

```bash
make verify   # unit tests, script checks and the web build (what CI runs)
make build    # binaries in fibre-sentinel/bin/ and the site in web/out/
```

Deployment on one VM with systemd and Caddy, or with docker compose, is in
[`deploy/README.md`](deploy/README.md).

The full devnet run with fault injection needs `celestia-app` and `fibre` on
`PATH` and takes about fourteen minutes:

```bash
cd fibre-sentinel && ./probe-devtest.sh 4 3
```

To re-derive every verdict and obligation figure from a record or an
untarred daily export and compare it with the API at the same moment:

```bash
cd fibre-sentinel && go run ./cmd/sentinel-recompute -data-dir <dir> -window 7d -as-of <RFC3339> -api https://tensile.huginn.tech/api
```

## Pinned celestia-app

celestia-app `v10.2.0-mocha` (commit `3b77dc2f5b00e1a646a2e9dd98b5c024a0d9ad8a`),
celestia-core `v0.42.1`, cosmos-sdk fork `v0.52.11`. TLS golden vectors come
from celestia-app commit `dba155084505a8f6c5d37260a94f70f939fb96de`.
`fibre-assign/reftest/go.mod` and `fibre-sentinel/go.mod` each carry a copy of
celestia-app's `replace` block; refresh the pin and the block together.

## License

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
