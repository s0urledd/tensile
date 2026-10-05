# Tensile

Tensile is Huginn Tech's open-source explorer for Celestia Fibre. It follows
blob settlements, publishers and validators in real time, and checks that
every validator serves the rows it endorsed.

[Explorer](https://tensile.huginn.tech) ·
[Methodology](https://tensile.huginn.tech/methodology/) ·
[API](https://tensile.huginn.tech/api/)

## Features

- **Real time.** Blocks are read the moment the node announces them, so a new
  blob is on the site within seconds and the live pages update on their own.
- **Every blob, searchable.** Find any blob by its blob ID (base64 or hex),
  transaction hash, commitment or payment promise hash.
- **Settlements, publishers and escrow.** Each settlement with its namespace,
  size, fee and endorsements; who pays for blobs, and their escrow deposits,
  withdrawals and balances.
- **Validators, checked.** Endpoint reachability, TLS identity, and whether
  each validator serves the rows it endorsed.
- **An open record.** A public API, signed daily exports, and a tool that
  recomputes every result from them.

## How it works

Settlements, endorsements, protocol parameters and escrow balances are read
from the chain. On top of that, Tensile runs two checks of its own: every
registered Fibre endpoint is checked every five minutes, and every blob is
read once before its retention window ends, asking each validator that
endorsed it for its own rows and verifying them against the commitment. The
[methodology](https://tensile.huginn.tech/methodology/) covers timing, rules
and result classes.

## Build from source

Requires Go 1.26.6 or newer, Node 22 and make. Build from a full checkout:
`fibre-sentinel` finds its sibling modules through relative `replace`
directives.

```bash
make build   # binaries in fibre-sentinel/bin/ and the site in web/out/
make clean   # remove both
```

To run and configure an observer (one VM with systemd and Caddy, or docker
compose), see the deployment guide, [`deploy/README.md`](deploy/README.md).

## Architecture

| module | what it is | dependencies |
|---|---|---|
| [`fibre-tlsverify`](fibre-tlsverify/) | verifies the TLS identity a Fibre server presents (consensus-key signed extension, no CA) | stdlib only |
| [`fibre-assign`](fibre-assign/) | recomputes which validator must serve which rows; `ShardMap.Verify` classifies what one returned | stdlib only (the differential test in `reftest/` pulls celestia-app) |
| [`fibre-sentinel`](fibre-sentinel/) | the observer: chain scanner, prober, heartbeat, collector, store, verdicts, exports and API | celestia-app (pinned) and the two above |
| [`fibre-devnet`](fibre-devnet/) | a multi-validator local devnet with retention at the 10-minute protocol floor, for end-to-end tests | shell and a celestia-app build |
| [`web`](web/) | the explorer: a static Next.js export that reads the API | Node 22 |

`fibre-tlsverify` and `fibre-assign` are importable as libraries on their own.

The observer runs as five processes over one data directory:

```text
chain ──► scanner ────┐
          prober ─────┼──► record files ──► collector ──► SQLite ──► API ──► site
          heartbeat ──┘                         └──► signed daily exports
```

The scanner reads each block as it arrives. The prober reads every blob before
its window ends, and the heartbeat checks every endpoint; both talk to the
validators' Fibre servers. All three append to record files, which the
collector loads into SQLite, rolls up by day and exports. The API serves that
database read-only, and the site is a static export that calls it. Record
files, schema, invariants and the path from a block to a figure are in
[`docs/SYSTEM.md`](docs/SYSTEM.md).

### Pinned celestia-app

celestia-app `v10.2.0-mocha` (commit `3b77dc2f5b00e1a646a2e9dd98b5c024a0d9ad8a`),
celestia-core `v0.42.1`, cosmos-sdk fork `v0.52.11`. TLS golden vectors come
from celestia-app commit `dba155084505a8f6c5d37260a94f70f939fb96de`.
`fibre-assign/reftest/go.mod` and `fibre-sentinel/go.mod` each carry a copy of
celestia-app's `replace` block; refresh the pin and the block together.

## Development and testing

```bash
make verify   # unit tests, script checks and the web build: what CI runs
```

Besides the build requirements it needs `python3` and `curl`. `shellcheck`
runs when installed; CI always runs it.

The end-to-end devnet run starts four validators, publishes three blobs,
stops one validator's Fibre server and checks every reading. It needs
`celestia-appd` and `fibre` on `PATH`:

```bash
make devtest
```

To re-derive every verdict and obligation figure from a record or an untarred
daily export, and compare it with the API at the same moment:

```bash
cd fibre-sentinel && go run ./cmd/sentinel-recompute -data-dir <dir> -window 7d -as-of <RFC3339> -api https://tensile.huginn.tech/api
```

## Documentation

- [Methodology](https://tensile.huginn.tech/methodology/): what every figure
  means, how it is computed and what it counts.
- [API](https://tensile.huginn.tech/api/): every endpoint, with real responses.
- [`docs/SYSTEM.md`](docs/SYSTEM.md): how the running system fits together:
  processes, record files, store and invariants.
- [`docs/verdicts.md`](docs/verdicts.md): every outcome and classification a
  request can get, and how each one counts.
- [`docs/exports-signing.md`](docs/exports-signing.md): the signed daily
  exports, and how to verify them offline.
- [`deploy/README.md`](deploy/README.md): running an observer: configuration,
  systemd and Caddy or docker compose, backups and checks after deploy.

## Contributing

Report bugs and suggest changes in
[GitHub issues](https://github.com/s0urledd/tensile/issues); pull requests are
welcome.

## License

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
