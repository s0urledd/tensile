# Tensile

Tensile is an independent observer for Celestia Fibre, built by Huginn Tech.
When validators sign for a Fibre blob they take on an obligation to serve
their assigned rows for a retention window; the chain records the signature
and nothing after it. Tensile reads those obligations from the chain, checks
from the outside whether each validator keeps them, and publishes every
reading with the evidence behind it.

Live at **https://tensile.huginn.tech**.

## What it checks

- **Endpoint reachability and TLS identity** of every registered Fibre
  endpoint, from two locations: DNS, TCP, TLS 1.3 and the validator-endorsed
  identity signed by its consensus key.
- **One reading of each blob**, 10 minutes before its retention window ends,
  the way celestia-app's Fibre client downloads it, with every row verified
  against the on-chain commitment and the recomputed assignment.
- **Verdicts** per blob (available or unavailable) and per validator, from a
  fixed taxonomy in which a validator counts as not served only when the blob
  could not be reconstructed and its rows did not come back
  ([`docs/verdicts.md`](docs/verdicts.md)).
- **Signed daily exports** of the full record, so every figure can be
  recomputed offline ([`docs/exports-signing.md`](docs/exports-signing.md)).
- **A public API**: read-only JSON at `https://tensile.huginn.tech/api/v1/`, with every rate published
  beside its numerator and denominator.

## Modules

| module | what it is | dependencies |
|---|---|---|
| [`fibre-tlsverify`](fibre-tlsverify/) | verifies the validator-endorsed TLS identity a Fibre server presents (consensus-key signed extension, no CA) | stdlib only |
| [`fibre-assign`](fibre-assign/) | recomputes which validator must serve which rows; `ShardMap.Verify` classifies what one returned | stdlib only (the differential test in `reftest/` pulls celestia-app) |
| [`fibre-sentinel`](fibre-sentinel/) | the observer: chain scanner, prober, heartbeat, collector, store, verdicts, exports and API | celestia-app (pinned) and the two above |
| [`fibre-devnet`](fibre-devnet/) | a multi-validator local devnet with retention at the 10-minute protocol floor, for end-to-end tests | shell and a celestia-app build |
| [`web`](web/) | the dashboard: a static Next.js export that reads the API | Node 22 |

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
