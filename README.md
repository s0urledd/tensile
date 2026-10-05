# Tensile

Tensile is an independent explorer and observer for Celestia Fibre, built by
Huginn Tech. Validators that sign for a Fibre blob owe its rows to anyone who
asks until its retention window ends; the chain records the signature and
nothing after it. Tensile indexes every Fibre settlement, checks from the
outside whether each validator still serves the rows it signed for, and
publishes every reading with the evidence behind it.

Live on Celestia's Mocha testnet at **https://tensile.huginn.tech**:
[methodology](https://tensile.huginn.tech/methodology/) ·
[API](https://tensile.huginn.tech/api/).

## What it does

- **Indexes every Fibre blob** within seconds of its block: publisher,
  namespace, size, fee, endorsing voting power and the settling transaction.
  A blob is found by its blob ID (base64 or hex), its transaction hash or its
  payment promise hash.
- **Checks every registered Fibre endpoint** every five minutes from two
  locations: DNS, TCP, TLS 1.3 and the identity signed by the validator's
  consensus key.
- **Reads each blob once**, 10 minutes before its retention window ends:
  every validator that endorsed it is asked for its own rows, the way
  celestia-app's Fibre client asks, and every row is verified against the
  on-chain commitment and the recomputed assignment. A validator that did
  not serve is asked again, up to two more times.
- **Judges each blob and each validator**: the blob by the client's own
  result, each endorsing validator on its own answers. Tensile's own gaps
  never count against a validator ([`docs/verdicts.md`](docs/verdicts.md)).
- **Keeps the record for good**: nothing is deleted, so every page and rate
  reads the same later as it does today. Signed daily exports let anyone
  recompute every figure offline
  ([`docs/exports-signing.md`](docs/exports-signing.md)).
- **Serves a public API**: read-only JSON at
  `https://tensile.huginn.tech/api/v1/`, every rate with its numerator and
  denominator.

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
