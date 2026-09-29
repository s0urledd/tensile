# fibre-sentinel

The measurement core of Tensile. It watches the chain for Fibre publications
and then, from the outside, reads each blob near the end of its retention
window the way celestia-app's own client downloads it, and records which
validators served their rows. The observer service built on it (store,
collector, API) lives in `observer/` and `cmd/observer-*`.

```
git clone https://github.com/s0urledd/tensile && cd tensile/fibre-sentinel
go build -o bin/ ./cmd/...
```

(`fibre-sentinel` depends on the sibling `fibre-assign` and `fibre-tlsverify`
modules via in-repo `replace` directives, so it builds from a repo checkout, not
via `go install <path>@latest`.)

Two core programs:

- **`sentinel-scan`** — discovery + recording. Walks the chain over one CometBFT
  RPC endpoint, finds every publication (a transaction whose sole message is
  `MsgPayForFibre`), decodes the full `PaymentPromise`, and writes one record
  per publication to `publications.jsonl`. Never contacts a validator.
- **`sentinel-probe`** — measurement. Reads each recorded blob once, 10
  minutes before its retention window ends, and appends one raw
  `Measurement` per validator asked to `measurements.jsonl`, classified
  against a fault taxonomy.

---

## What it checks, and why

**Fibre in one paragraph.** Fibre is Celestia's optional low-latency data
availability path. When a client pays for a Fibre blob, the validators sign that
they received the data, and each validator takes on an obligation to serve its
assigned slice of the erasure-coded rows for a retention window —
`shard_retention`, **4 hours by default**. The chain records the payment. It
does **not** record, or enforce, whether validators actually keep serving after
that.

### Why external observation

The serving obligation is off-chain and unenforced. A validator that collects
the payment and then quietly stops answering an hour later has broken the
guarantee Fibre sells, and nothing on chain reflects it. Validator-reported
metrics are not evidence — the entity being measured is the entity reporting.

The only trustworthy check is to *be a client*: dial the validator's registered
Fibre endpoint with no special access, ask for the shard exactly as a real
retrieval would, and verify what comes back against the commitment. A Sentinel
does that continuously, from infrastructure that has nothing to do with any
validator. Everything it needs — the validator set, each validator's registered
host, the consensus key that endorses each TLS identity, the assignment — it
reads from the chain and recomputes itself (`fibre-assign`, `fibre-tlsverify`).

### Why one reading, as the client makes it

What a Fibre blob promises its reader is that it can be downloaded until its
deadline. So the Sentinel downloads it the way a reader does, with
celestia-app's own rules (`fibre/download.go`): the whole validator set in
the client's order (`validator.Set.Select`), the next one asked while the
rows still wanted outnumber the rows on their way, 15 s per request
(connect and TLS included), one re-dial after a failed dial or a timeout,
every row verified against the commitment, and the download done at the
rows that reconstruct the blob (4096 of 16384 for blob version 0). It reads
**once, 10 minutes before the deadline**, where a validator that pruned
early or moved on shows. The result is the client's: **available**, or
**unavailable** with the client's error, "no shards retrieved" or "not
enough shards to reconstruct blob". A reading in which every request failed
on this observer's side, or one the prober missed, did not happen: the blob
was not read by Tensile.

A validator counts as **not served** only when it endorsed the promise, the
blob was unavailable, and its rows did not come back. On an available blob a
validator that failed, or one the reading did not need to ask, counts
neither way: the blob was there for any reader. A validator that did not
endorse owes nothing and is never counted.

### Why the tolerance is set from the *measured* prune lag

The protocol deadline is exact:

```
must_serve_until  =  creation_timestamp + max(payment_promise_timeout, shard_retention)
```

But an honest fibre server does not delete the blob at that instant. Its prune
loop runs once a minute and keys on a minute-resolution timestamp, so in
practice the blob stays downloadable until **`must_serve_until + ~1m45s`**. This
was measured directly on the devnet: last `SERVED_OK` at `pruneAt + 1m18s`,
first `NOT_FOUND` at `pruneAt + 1m43s`, all three surviving validators flipping
together.

If the Sentinel flagged the first `NOT_FOUND` after `must_serve_until` as a
fault, it would raise a false alarm on **every single publication**, because
that lag *is* the honest behaviour. So the **grace window** — the span after
`must_serve_until` where `NOT_FOUND` is tolerated, not faulted — is set from the
measured lag plus margin (`-prune-tolerance`, default **2m30s**), never from the
protocol number. The reading stays clear of it: no request starts later than
60 s before the deadline. This is the line between a monitor operators trust and one they
mute. Run against a network with a different `shard_retention` or prune cadence,
re-measure, and set `-prune-tolerance` to match.

---

## sentinel-scan

At each height it does two things.

**1. Tracks fibre params.** `EventUpdateFibreParams` carries a full `Params`
snapshot. The scanner keeps an ordered history keyed by `(height, tx index)`,
seeded once by an ABCI query of `Query/Params` at the start height, and
re-read from state every 60 blocks so a change that arrived without an event
is still caught. Nothing is hard-coded.

`must_serve_until` is **not** taken from the params at the settlement point.
The server reads the params when the shard is uploaded, which happens
somewhere between the promise height and the settlement tx, so the scanner
evaluates every params entry in force anywhere in `[promise height - 1,
settlement]` and records the **earliest** bound, setting
`must_serve_until_ambiguous` when they do not all agree. The earliest bound is
the one no server could have undershot, so a longer window on the server can
only produce `SERVED_PAST_WINDOW` or `EXPECTED_GONE`, never a fault. Taking
the settlement point's params instead would compute a later deadline, which is
the accusing direction. docs/verdicts.md states the rule in full; an operator
checking a FAULT should compute the deadline that way.

**2. Records publications.** For each single-message `MsgPayForFibre` (the shape
consensus enforces, detected with `x/fibre/types.TryParseFibreTx`) it persists:

| field | source |
|---|---|
| every `PaymentPromise` field | decoded from the tx |
| `promise_hash` | `fibre.PaymentPromise.Hash()` — the on-chain identity |
| `settlement_height` / `settlement_time` / `settlement_tx_hash` / `_tx_index` / `_tx_code` | the block + tx |
| `params_at_publication` | param history at `(settlement_height, tx_index)` |
| `must_serve_until` | `creation_timestamp + max(payment_promise_timeout, shard_retention)`, over the **earliest** params in force in `[promise height - 1, settlement]` |
| `must_serve_until_ambiguous` | set when those params did not all agree, so the deadline is a bound rather than a value |
| `assignment` | `fibre-assign` shard table over the validator set at the **promise height** |

```
sentinel-scan -rpc http://127.0.0.1:26657 -data-dir ./data -start-height 1        # scan to tip, exit
sentinel-scan -rpc http://127.0.0.1:26657 -data-dir ./data -follow                # then keep following
```

Output: `<data-dir>/state.json` (cursor + full param history + protocol-params
fingerprint), `<data-dir>/publications.jsonl` (one record per line,
append-only) and `<data-dir>/payments.jsonl` (one escrow movement per line:
settlement, timeout, deposit, withdrawal request, withdrawal payout).

**Payments.** The scanner records the economy side of `x/fibre` beside the
publications, from the same blocks. A settlement or timeout carries no amount
in any chain event, so the charge is recomputed from the promise's padded
`blob_size` with the module's own formula (`650,000 + 45,000 × ⌈size / 256
KiB⌉` gas at 1 utia/gas), which is exactly what the module charges; the
publisher is the account whose key signed the promise, whoever broadcast the
transaction. A timeout is recorded only when somebody submitted it: an
abandoned promise nobody reports leaves no trace, so the timeout count is a
floor. The collector ingests the file and polls each known publisher's escrow
balance by state query (there is no list-all query); the API publishes it all
under `/v1/market`, `/v1/publishers` and `/v1/publishers/{addr}`, and names
accounts from an optional `publishers.yaml` registry (`observer-api
-publishers`), whose source is shown with every label.

**Assignment constants.** `OriginalRows`, `TotalRows`, `MinRowsPerValidator`,
`LivenessThreshold` are not on chain — see `fibre-assign`. They come from the
pinned `assign.ParamsV10BlobV0` and are valid only for **blob version 0** on a
celestia-app build matching `assign.PinnedCelestiaAppCommit`. Other blob
versions are recorded with an explicit `assignment.error`, never a wrong table;
every record embeds the fingerprint so it is self-describing.

---

## sentinel-probe

Each cycle it plans every publication's reading and hands the due ones to a
dispatcher (`internal/probe/prober.go`, `blobread.go`):

**1. When.** `must_serve_until - 10 min` (`-end-read-offset`), half way
through a shorter window. A reading that cannot start by `must_serve_until -
3 min` (`-read-deadline`) is not made: its endorsing validators get a
`NOT_PROBED` row, this observer's gap. Publications settled before
`-end-read-since` were read on the earlier schedule and are left alone.

**2. Who, in what order.** The validators whose signature the settled promise
carries, in the order celestia-app's client uses (`ClientOrder`, which calls
`validator.Set.Select` over the set at the promise height), each expected to
return its assigned rows.

**3. Each request**, layer by layer, each timed and judged on its own, 15 s in
all (`-download-timeout`, the client's `RPCTimeout`):

| layer | check |
|---|---|
| L1 DNS | resolve the registered host (skipped for a literal IP) |
| L2 TCP | connect |
| L3 TLS | TLS 1.3 handshake (raw), record version / cipher / peer-cert fingerprint + validity |
| L3 identity | `fibre-tlsverify` — the peer cert's extension must be endorsed by the validator's consensus key for this chain ID |
| L4 retrievability | `DownloadShard`, then verify the returned rows against the commitment with the reading's shared `rsema1d` Reconstructor **and** against the recomputed `fibre-assign` assignment |

A failed dial, an unreachable peer or a timeout is asked again at once, as the
client re-dials. Hosts come from `x/valaddr` `AllBondedFibreProviders`
(latest height, cached); consensus keys from `/validators` at the promise
height. The assignment is **recomputed** here and cross-checked against the
row counts in the scan record — a mismatch is a hard error, not a silent
divergence.

**4. Enough.** The reading stops at `original_rows` distinct verified rows,
or when every validator has been asked.

**5. The record.** One raw `Measurement` per validator asked, all of a
reading's rows in one write: vantage, scheduled/started/finished times, each
layer's duration and result, the identity verdict, rows returned and both
verification results, the raw error text, and `read` (the place in the
order, the rows this answer added, what the reading came to and the
client's error). **No scores** — the observer derives the blob's status and
the obligation verdicts from these records (`observer/verdict`,
`observer/rollup`).

### Error-class taxonomy

Classification is a fact about one request (given whether the validator is
assigned this shard, whether the settled promise *proves* it stored the shard,
and which phase the request's *actual start time* falls in), never an
aggregate. Whether it counts against the validator is decided from the whole
reading (below the table). The grace, post and unassigned rows come from the
earlier schedule only:

| assigned | attested | phase | outcome | classification |
|---|---|---|---|---|
| yes | no | any | any | **UNATTESTED** (no proof this validator ever stored the shard; outside every rate, in both directions) |
| yes | yes | in-window (`t < must_serve_until`) | `NOT_FOUND` / `INVALID_ROWS` | **FAULT** (identity verified, and it did not serve what the chain proves it holds; a `NOT_FOUND` within 30 s of the deadline is `TOLERATED`) |
| yes | yes | in-window | `SERVER_ERROR` | **SERVER_ERROR** (reached, answered with an error or an answer no client accepts) |
| yes | yes | in-window | `RPC_THROTTLED` | **THROTTLED** (reached, refused with a rate limit: at the reading, no rows, as the client meets it) |
| any | any | any | certificate not endorsed by this validator's consensus key | **IDENTITY_MISMATCH** (an unusable endpoint, shown as its status; not a fault) |
| yes | yes | in-window | `DNS_FAIL` / `TCP_*` / `TLS_HANDSHAKE_FAIL` / `RPC_UNAVAILABLE` / `RPC_TIMEOUT` / `RPC_ERROR` | **UNREACHABLE** (from one vantage this is our path too) |
| yes | yes | any | `NO_REGISTERED_HOST` | **NOT_REGISTERED** (jailing and unbonding drop the bonded entry) |
| yes | yes | any | `WRONG_ROWS` / `PARTIAL` whose rows verify against the commitment | **SHADOWED_SHARD** (another promise over the same blob answered) |
| yes | yes | any | lapsed but correctly signed certificate | **IDENTITY_EXPIRED** (a late renewal, not impersonation) |
| yes | yes | in-window | `SERVED_OK` | **HEALTHY** |
| yes | yes | grace (`msu` … `msu + prune-tolerance`) | `NOT_FOUND` / unreachable | **TOLERATED** |
| yes | yes | post (`> msu + prune-tolerance`) | `NOT_FOUND` | **EXPECTED_GONE** |
| yes | yes | post | unreachable | **UNREACHABLE_POST_WINDOW** (not a fault; obligation over) |
| yes | yes | post | `WRONG_ROWS` | **SERVED_PAST_WINDOW** (`DownloadShard` enforces no assignment) |
| yes | yes | post | `SERVED_OK` | **SERVED_PAST_WINDOW** (fine; affects disk accounting) |
| no | — | any | `NOT_FOUND` | **EXPECTED_UNASSIGNED** |
| no | — | any | `SERVED_OK` | **SERVING_UNASSIGNED** (flagged for review) |
| any | — | any | request could not run / reading not made in time | **PROBE_ERROR** / **NOT_PROBED** |

**Not served** is the only thing said against a validator: the chain *proves*
it stored rows of a blob, the blob could *not be reconstructed* from what the
reading brought back, and its rows did not come back — not found, bad rows,
no answer, a rejected certificate, an error, a rate limit or no registered
host, as a reader using the client meets them. On an available blob none of
those counts either way. Only served and not served enter the service rate.

"Attested" means the observer verified a signature from that validator over the
settled promise against its consensus key. A Fibre server writes the shard to
its store before it signs, so a verified signature proves storage. The absence
of one does not prove the opposite: the publisher stops collecting signatures at
the safety threshold and keeps delivering in the background, so absence means
*unproven*. `internal/scan/attest.go` explains why the observer verifies rather
than counting the entries the transaction carries.

### Load shape

A reading asks only as many validators as it needs, so a validator is asked
for some blobs, not all of them. At most 16 blobs (`-blob-concurrency`) and
64 requests (`-concurrency`) are in flight at once, with 512 MiB of shards
(`-in-flight-mib`). These only delay a request, never drop one, and there is
no limit per validator, as the client has none. `publications.jsonl` is
tailed incrementally and a publication is
forgotten once its reading is on record. On a (re)start every reading that
was not made gets its `NOT_PROBED` rows, however old;
`-backfill-missed` (default 0, unbounded) caps how far back that goes. A
publication whose settlement tx failed, or whose promise names another chain
than the RPC's, is skipped with one log line.

The gRPC receive limit is the protocol's message bound
(`ProtocolParams.MaxMessageSize()`, about 139 MB), as the client's; an
answer over it, or one the client cannot parse, is `MALFORMED_SHARD`. Every
resolved address of a host is tried, IPv4 first, and the download talks to
the address the TLS check passed on. A validator with no `x/valaddr` host
is `NO_REGISTERED_HOST`.

`-read-now` reads every publication whose window still leaves room for a
request at once and exits: a dry run, for a scratch data directory.

Every measurement records `clock_offset_ms` (the observer's clock minus the
chain's latest block time). Phase boundaries are seconds to minutes wide and
are judged against the local clock, so a drifted vantage would mislabel
probes silently; past 30 seconds of offset the prober logs a warning.

### Restart / no hangs

The queue of readings is **never persisted** — it is re-derived every cycle
from `publications.jsonl` + `measurements.jsonl`, so a restart resumes
exactly; a reading under way when the process stopped is made again if its
window allows. Each measurement's dedupe key is `(vantage, promise_hash,
validator, scheduled_at)`. Every wait is bounded: the loop sleeps at most
`-max-sleep` (30s) between cycles, every request has its own 15 s, and
SIGINT/SIGTERM stops cleanly. `-once` reads everything currently due and
exits; `-drain` runs until every known reading is in the past; `-deadline`
caps the run.

Run one prober per vantage point (`-vantage <name>` is recorded on every
measurement); several probers writing to independent data dirs give you
multi-vantage coverage.

```
<data-dir>/measurements.jsonl   one raw Measurement per line, append-only + fsync
<data-dir>/archive/measurements.jsonl/   its older lines, gzip, once observer-archive has run
                                (internal/record; readers see one file at the same offsets)
```

---

## Tests

**Unit** (`go test ./...`): param-history ordering incl. same-block updates,
`must_serve_until` derivation, `EventUpdateFibreParams` JSON parse, the
assignment-table builder, store dedupe/resume, the reading (against real
Fibre servers on loopback with encoded shards: the stop at K, the three
outcomes, the re-dial, a reading no request left, limits that only delay),
`PhaseAt` boundaries, the
full taxonomy table, and the observer's store,
rollup, API and export packages.

**End-to-end, scanner** — `./devtest.sh 4 4`: fresh `multi-node-fibre.sh` devnet,
`sentinel-scan` following, 4 blobs published; every commitment recorded with
`must_serve_until == creation + 10m` and a non-degenerate assignment
(`sigma == distinct == 12291`, all 4 validators hold rows), `sentinel-verify`
green.

**End-to-end, prober + fault injection** — `./probe-devtest.sh 4 3` (**~10 min**;
the retention floor is 10 min): scanner + prober against a fresh devnet, one
validator's fibre server killed before the blobs are read, and each blob read
once, half way through its window. `sentinel-measure-check` confirms every
blob was available from the live validators and the killed one was never
served and never a `FAULT`.

An earlier run, on the earlier schedule (3 blobs × 5 schedule points × 4
validators = 60 measurements), held the taxonomy with **zero
misclassifications**:

| classification | count | meaning |
|---|---:|---|
| `HEALTHY` | 27 | live validators, in-window, rows verified against commitment + assignment |
| `FAULT` | 9 | the killed validator, in-window (3 blobs × 3 in-window points) — caught at L2 in <1 ms |
| `TOLERATED` | 12 | grace phase: blob already pruned + the killed validator unreachable |
| `EXPECTED_GONE` | 9 | post phase, live validators, blob pruned as expected |
| `UNREACHABLE_POST_WINDOW` | 3 | post phase, the killed validator — obligation over, not faulted |

Sample outputs from that run are committed under [`sample/`](sample/).

---

## Layout

```
cmd/sentinel-scan          the scanner CLI
cmd/sentinel-probe         the reader: one reading per blob, the Fibre client's way
cmd/sentinel-pub           devnet publish helper (fibre.Client.Upload + MsgPayForFibre; -abandon / -timeout / -withdraw for the escrow side)
cmd/sentinel-verify        checks publications.jsonl against expected commitments
cmd/sentinel-verify-export checks a downloaded daily export offline: sidecar, members vs manifest, ed25519 signature (docs/exports-signing.md)
cmd/sentinel-anchor        builds and prints (never broadcasts) the PayForBlobs that would anchor an export's manifest digest on Celestia
cmd/sentinel-measure-check checks the readings in measurements.jsonl against the rule
cmd/sentinel-recompute     re-derives every verdict and figure from the record or a daily export, optionally against the live API
cmd/sentinel-synth         writes a synthetic network-scale record for load and correctness tests
cmd/observer-collector     tails the record files into SQLite and polls the endpoint registry
cmd/observer-heartbeat     dials every registered endpoint (DNS, TCP, TLS, identity) every few minutes
cmd/observer-api           the read-only JSON API
internal/scan              scanner, param history, record schema, store, CometBFT RPC client
internal/probe             reading, client order, layered request, measurement store, classifier, prober loop
internal/uploadprobe       upload-side measurement groundwork: per-validator results from the fibre client's spans
observer/                  store, ingest, verdicts, rollups, exports, API (policy: the earlier sampling's master secret)
sample/                    outputs from a real devtest / probe-devtest run
```

## Build

Go 1.23+ with `GOTOOLCHAIN=auto` (celestia-app pins `go 1.26.5` and the toolchain
auto-downloads). `fibre-assign` and `fibre-tlsverify` are sibling modules in this
repo, resolved by relative `replace` directives — build from a full checkout of
the repo, not from this subdirectory alone.

```
cd fibre-sentinel && go test ./... && go build -o bin/ ./cmd/...
```

## License

Apache-2.0. See [`../LICENSE`](../LICENSE) and [`../NOTICE`](../NOTICE).
