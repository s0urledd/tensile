# How the observer works

A map of the running system: what each process owns, how a chain event
becomes a published number, and where the joints are. Written from the code
rather than from intent, so it can be checked against the code.

`docs/verdicts.md` is the methodology — what the numbers mean and what they
may claim. This is the machine. Where the two disagree, one of them is a bug.

---

## 1. In one paragraph

Celestia Fibre (CIP-51) has a validator sign for a shard of a blob at upload
time and hold it until a deadline the chain computes. The chain then checks
none of it: no serving proof, no challenge protocol, no slashing. This
observer watches the chain for those promises, works out who owes what,
reads each blob once near the end of its retention window the way
celestia-app's own client downloads it, and publishes what came back — as an
append-only record anyone can download and recompute.

---

## 2. Five processes, one data directory

Everything runs out of `DATA_DIR` (live: `/var/lib/fibre-observer/mocha`).
The processes share files, not memory, and only one of them writes SQLite.

| process | binary | writes | reads |
|---|---|---|---|
| scanner | `sentinel-scan` | `publications.jsonl`, `payments.jsonl`, `host_history.jsonl`, `state.json` | chain RPC |
| prober | `sentinel-probe` | `measurements.jsonl`, `vantage-requests.jsonl`, `sampling-secrets.jsonl` (reveals of the earlier draws) | `publications.jsonl`, `state.json`, `registry.jsonl`, `sampling-master.key`, chain RPC |
| heartbeat | `observer-heartbeat` | `reachability.jsonl` | chain RPC (registry) |
| collector | `observer-collector` | `observer.db`, `registry.jsonl`, `amendments.jsonl`, `exports/` | every `.jsonl`, `state.json`, chain RPC |
| API | `observer-api` | `snapshots/*.json` | `observer.db` (read-only), status files |

Each writes a status file (`internal/status`) that `/v1/health` reads
**without going through the database** — deliberately, because the collector
is one of the four and "is the observer working" must not depend on it.

Every process also appends a `RunEvent` to `runs.jsonl` on start and stop,
carrying its flags and `status.BuildRevision()`. That is how a published row
is tied back to the code and configuration that produced it.

---

## 3. The path from a block to a number

```
chain block
  └─ scanner: single-message MsgPayForFibre txs (TryParseFibreTx)
       ├─ param history  (EventUpdateFibreParams, + a state reconcile every 60 blocks)
       ├─ host history   (set_fibre_provider_info events, seeded from the bonded registry)
       ├─ validator set at the PROMISE height
       ├─ verify every validator signature itself (ed25519 over the promise sign bytes)
       ├─ fibre-assign shard table over that set
       └─→ publications.jsonl  (append-only, fsynced before the cursor advances)

  prober: re-derives the queue of readings every cycle from publications.jsonl
          + measurements.jsonl (never stored)
       ├─ ReadPoint(publication)  → must_serve_until - 10 min
       ├─ ClientOrder  → every validator with rows, in validator.Set.Select order
       ├─ per request, probe.Run  → DNS · TCP · TLS 1.3 · consensus-key identity
       │                            · DownloadShard, 15 s in all, one re-dial
       ├─ rows verified by the blob's shared Reconstructor; stop at K distinct rows
       ├─ short after the whole set: the rest asked again a minute later
       ├─ Classify(Evidence) → one classification + a reason per validator asked
       └─→ measurements.jsonl (a reading's rows together)

  heartbeat: every 5 min, every bonded provider's endpoint, layers 1-3 only
       └─→ reachability.jsonl

  collector: tails all of it into SQLite with byte-offset cursors
       ├─ endpoint history from AllBondedFibreProviders
       ├─ escrow balances, validator identities, Keybase avatars
       ├─ deferred shadow verdicts (amendments) once the scan frontier passes
       ├─ daily rollups + retention prune
       └─ daily export tarballs with digests

  API (read-only): windows, aggregates, snapshots
  web: static export, fetches the API in the browser
```

---

## 4. The record files

Append-only JSONL, one record per line. These are the product; SQLite is a
derived index of them.

| file | one line per | dated by |
|---|---|---|
| `publications.jsonl` | settled `MsgPayForFibre` | `settlement_time` |
| `measurements.jsonl` | validator asked in a reading (earlier: probe) | `started_at` |
| `sampling_decisions.jsonl` | publication the load policy sampled out before 27 September 2026, standing for a NOT_PROBED row per assigned validator per point; no longer written | `decided_at` |
| `reachability.jsonl` | heartbeat | `started_at` |
| `payments.jsonl` | escrow movement | `time` |
| `registry.jsonl` | endpoint open/close | `at` |
| `runs.jsonl` | process start/stop | `at` |
| `sampling-secrets.jsonl` | revealed day secret | `revealed_at` |
| `amendments.jsonl` | late shadow verdict | `judged_at` |
| `host_history.jsonl` | host registration | `time` |
| `param_uncertainty.jsonl` | a height range whose `x/fibre` params the observer cannot vouch for, and what came of closing it | `detected_at` |
| `corrections.jsonl` | a deadline or verdict a verified range moved, and the `range_corrected` line that closes the range | `judged_at` |
| `vantages/<name>/measurements.jsonl` | the second location's answer to a confirmation request, copied in; every export carries it, since a not-served row counts only once it is confirmed | `started_at` |

`state.json` is **not** a record file: it is the scanner's current param
history, scan gaps, host seed and frontier. Every export carries it as a
snapshot because `sentinel-recompute` needs all four to redraw a verdict the
way the observer drew it.

Dedupe keys make re-ingest a no-op: a publication by `settlement_tx_hash`, a
probe by `(vantage, promise_hash, validator_address, scheduled_at)`, a params
range by its id (`chain:kind:from-to`) plus its resolution, a correction by
`(target, range id)`.

---

## 5. The store

SQLite, WAL, `auto_vacuum(incremental)` (set at creation — changing it later
needs a full VACUUM, which is why an existing DB has to be deleted). The
collector owns the schema; the API opens `query_only` and refuses a database
older *or* newer than the binary expects.

**Schema version 20.** Base tables from `schema.sql`: `schema_migrations`,
`observer_runs`, `ingest_cursors`, `params_history`, `publications`,
`assignments`, `endpoints`, `probes`, `meta`, `reachability`. Migrations add:

| v | what |
|---|---|
| 2 | verified attestation (`attested` nullable — NULL is *unknown*, not *did not attest*) |
| 3 | endpoints say why they closed |
| 4 | `validator_identities` from the staking module |
| 5 | `probes_window` covering index (network aggregates) |
| 6 | `probes_validator_window` (the per-validator page) |
| 7 | `payments`, `escrow_accounts` |
| 8 | bytes per probe → transfer rate over the download alone |
| 9 | evidence on the row: indices, digest, gRPC code, `shadowed_by`, observer build |
| 10 | `observer_runs` config, `sampling_secrets` |
| 11 | retention: columns out of `raw_json`, `obligation_daily`, `probe_daily` |
| 12 | deferred shadow verdicts: `shadow_gap`, `classification_at_probe`, `probe_amendments` |
| 13 | host at settlement on the obligation and the row |
| 14 | `host_events` |
| 15 | `validator_avatars` |
| 16 | `probe_daily.identity_up` |
| 17 | `probe_daily` attestation split |
| 18 | indexes for `/v1/probes?at=` and `/v1/sampling` |
| 20 | `param_uncertainty.corrected_at`: verifying a range and having applied what it proves are two different facts, and `holds` is derived from both |
| 19 | params uncertainty: `param_uncertainty`, `publication_corrections`, `probe_corrections`, the `retention_unverified` hold and the `*_at_scan` / `*_at_probe` originals, `obligation_daily.held_param_unverified`, and `publications.must_serve_until_ambiguous` (written to the record since it was added and read by nothing until now) |
| 24 | `sampling_decisions` and its points: a sampled-out publication stored once; `probe_rows` derives its NOT_PROBED rows for every figure; the rows already stored for one are collapsed into it |

The store is append-only **in its inserts** (`ON CONFLICT DO NOTHING`) but not
in its verdicts: `ApplyAmendment` updates a row's classification in place when
a deferred shadow verdict settles, and `ApplyProbeCorrection` /
`ApplyPublicationCorrection` move a deadline and a verdict when a params range
is verified. Every one of them keeps what the row was stamped with in a
sibling column (`classification_at_probe`, `phase_at_probe`,
`must_serve_until_at_probe`, `must_serve_until_at_scan`) and logs the move to
its own append-only table and record file. `probe_corrections` carries **no**
foreign key to `probes`, unlike `probe_amendments`, whose `ON DELETE CASCADE`
lets the retention prune delete the log of amendments it once published.

Anything that caches per-publication results has to account for all of it —
see `blobcache.go`'s fingerprint, which carries `amended_at`, `corrected_at`
and `retention_unverified`, none of which adds a row or moves `MAX(rowid)`.
The API's window snapshots run to a thirty-minute TTL and key on
`meta.param_holds_rev`, which every path that raises or lifts a hold moves;
without that a withheld fault would stay on the front page for half an hour
after the hold landed. It is a counter incremented inside SQLite, not a
timestamp: the collector stamps one `time.Now()` at the top of a pass and
threads it through every record it ingests, so two ranges landing in the
same pass wrote the same nanosecond and a snapshot computed between them
stayed valid across the second one. The API treats the value as an opaque
token and only compares it for equality.

---

## 6. The verdict pipeline

Three levels, and they are not the same thing:

**Outcome** — what happened on the wire. `SERVED_OK`, `NOT_FOUND`,
`WRONG_ROWS`, `INVALID_ROWS`, `PARTIAL`, the transport failures,
`SERVER_ERROR`, `RPC_THROTTLED`, `NO_REGISTERED_HOST`, `UNROUTABLE_HOST`,
`MISSED`, `REACHABLE`. 21 of them; `AllOutcomes` exists so a new one cannot
reach a default arm unnoticed.

**Classification** — the verdict on one probe, from
`Classify(Evidence)`. Evidence carries: assigned, attested,
attestation-unknown, phase (from the **actual** start time, not the scheduled
one), outcome, commitment-verified, shadowed, shadow-uncertain, identity-stale,
rows-subset-of-own, pin-stale. Order matters and is deliberate:

1. observer-side first (`PROBE_ERROR`, `NOT_PROBED`, deadline, policy skip)
2. identity (a property of the endpoint, not of a shard)
3. registry state (`NOT_REGISTERED` — no host, or an unroutable one)
4. stale assignment pin → `PROBE_ERROR` (never a network-wide false accusation)
5. unattested → `UNATTESTED`, counted neither way
6. unassigned → `EXPECTED_UNASSIGNED` / `SERVING_UNASSIGNED`
7. shadowing → `SHADOWED_SHARD`, or deferred
8. then the phase arms

`FAULT` is the only class that counts against a validator:
`CountsAgainst()` is `c == ClassFault`, and every rate is built from that
predicate rather than from a list repeated per call site. Which rows are
`FAULT` for counting is decided from the blob's reading
(`verdict.CountedClass`): on an unavailable blob every answer that left the
reader without the validator's rows; on an available one none.

**Blob reading** — `verdict.BlobReading`, one per publication: **available**
when the distinct verified rows reach `original_rows`; **unavailable** when
the verified rows plus the rows of every validator with no answer of its own
(`probe.OwnAnswer`: not asked, or this observer's own failure; a validator
whose verified rows are in hand has answered) still fall short, over the
whole set the client asks, endorsing or not; `pending` while the window is
open; `not_read` after. A reading of the earlier schedule is judged at the newest point every
endorsing validator answered at.

**Obligation bucket** — one per `(validator, promise)` pair, in
`observer/verdict` and in SQL in `observer/rollup`:

```
pending      must_serve_until > as_of
served       its rows came back verified at the blob's reading
broken       not served: the blob was unavailable, its rows did not come
             back, and the second location confirmed it (confirmed_by)
not_counted  the rest: not asked (the rows were enough before its turn), a
             failure on a blob that was available, a failure the second
             location did not confirm, a reading the guard set aside, or no
             reading that decides the blob
```

Rate is `served / (served + broken)`. Everything else is printed beside it.

**Two implementations, on purpose.** `observer/rollup` holds the SQL;
`observer/verdict` is the Go twin `sentinel-recompute` runs; the API's tests
run both over the same rows. A constant shared between them (the end-segment
divisor) is held to the same value by a test, because a query fragment cannot
reference a Go constant.

---

## 7. The reading

`internal/probe/blobread.go`. Each blob is read once, the way celestia-app's
Fibre client downloads it:

- at `must_serve_until - 10 min` (`ReadPoint`; half way through a shorter
  window); a reading that cannot start by `must_serve_until - 3 min` is not
  made, and its endorsing validators get a `NOT_PROBED` row
- every validator the assignment gives rows, endorsing or not, in
  `validator.Set.Select` order (`ClientOrder`, celestia-app's own code),
  the next one asked while the rows still wanted outnumber the rows on their
  way; a validator that did not endorse is asked like the rest, its rows
  count toward the blob, and it is never counted (`UNATTESTED`)
- each request, connect, TLS and `DownloadShard` together, gets 15 s (the
  client's `RPCTimeout`; only the DNS lookup has a bound of its own), and
  is made again at once after a failed dial, an unreachable peer or a
  timeout, even past the cutoff; no request starts after
  `must_serve_until - 60 s`
- every row is verified against the commitment by one Reconstructor the
  reading shares; the reading stops at `original_rows` distinct rows
- short after every validator was asked: the ones that did not serve are
  asked again 60 s later, if that pass can start by
  `must_serve_until - 90 s`; otherwise, or when that pass cannot ask every
  one of them again before the cutoff, the reading is incomplete and its
  failures are `PROBE_ERROR`, this observer's gap
- a rate limit (`RPC_THROTTLED`) is the validator's answer, as the client
  takes it: no rows, never re-asked in the same pass
- pacing, never dropping: 16 blobs and 64 requests at once, 512 MiB of shards
  in flight, one request per validator from this observer at a time (a busy
  validator is passed over and come back to; one the pass that decides an
  unavailable reading never gets back to is recorded `PASSED_OVER`, this
  observer's gap, and the correlated-failure guard counts it as failed)

The rows of a reading are appended together in one write and one fsync
(`MeasurementStore.AppendReading`), one per validator asked and, on an
unavailable reading, one per validator passed over, with `read` saying
where each sat in it. Blobs settled before `-end-read-since` were
read on the earlier schedule (four in-window points, grace and post) and are
not read again; their rows are judged by the same rule.

`must_serve_until = creation_timestamp + max(payment_promise_timeout,
shard_retention)`, from the params in force at settlement — and where the
params moved between the promise height and settlement, the **earliest**
candidate over every value in force in that interval, because the server
computes its prune time from the params at *upload*, which the observer
cannot see.

Phase boundaries are 30s and 150s wide, so the prober measures its own clock
against chain time every cycle and stamps the offset on every row;
`clockSkewWarn` is 30s.

---

## 8. The earlier sampling

Until 27 September 2026 a load policy (`observer/policy`) sampled
publications against byte and request budgets, with a commit-and-reveal draw:
a blob was probed iff `H(promise_hash ‖ day_secret) < p·2^64`,
`day_secret = HMAC(master, date)`. Nothing is sampled or budgeted now. The
policy keeps only the master secret (`<data-dir>/sampling-master.key`), so
the prober can keep publishing each day's secret seven days after it ends
(`sampling-secrets.jsonl`, `/v1/sampling`) and the draws already made stay
auditable (`sentinel-recompute -sampling`). Once the last day with a draw is
revealed (2026-10-04), the key can be deleted; `probe-budget.json` is not
read any more. deploy/README.md lists every stored file and table only the
earlier model needed, with its size and how to remove it.

---

## 9. The API

`observer-api`, read-only, `Access-Control-Allow-Origin: *` on every response.

```
GET /v1/meta                  chain, counts, components, vantage
GET /v1/network               the window summary
GET /v1/validators            one row per validator
GET /v1/validators/{addr}     one validator, four windows (addr: consensus hex or
                              valcons1…, operator valoper1…, account address)
GET /v1/blobs                 publication list (?limit=, ?offset=, ?namespace=; total)
GET /v1/blobs/{hash}          one blob: its reading, each assigned validator's service word, the rows
GET /v1/probes                raw rows (?blob=, ?at=, ?validator=)
GET /v1/runs                  every process start/stop with its config
GET /v1/sampling              the earlier sampling: day commitments, and secrets once revealed
GET /v1/exports[/{name}]      daily tarballs + digests
GET /v1/avatars/{identity}    Keybase picture
GET /v1/health                machine-readable liveness (200 / 503)
GET /v1/market                the publisher side
GET /v1/publishers[/{addr}]   incl. the escrow withdrawal queue read from state
GET /v1/params                x/fibre params + change log (heights, block times), pinned protocol constants
```

**Windows**: `24h`, `7d`, `30d`, `all`.

**Snapshots.** `/v1/network`, `/v1/validators` and `/v1/market` are aggregates
over hundreds of thousands of rows, so they are computed on a schedule and
served from `snapshotCache` — with the moment they were taken and how long
they took published, rather than implied. TTLs: 1m / 5m / 15m / 30m, scaled
down while the cache is young. Persisted to `<data-dir>/snapshots/` so a
restart serves the last figures at once; the warm-up then replaces them.

**Two paths bypass the cache and are rationed** (4-burst, then one per 2s;
429 with `Retry-After`):

- `?as_of=<RFC3339>` — what the observer would have published at that moment
- `?exclude=<addr>` — the summary without named validators

Both are `Cache-Control: no-store`. The cache is keyed by window alone, so
writing either into it would publish one reader's view as everyone's
headline. Tests hold both paths off the cache.

**The correlated-failure guard** (the owner's decision until a control read
exists). A reading of a blob that could not be reconstructed, where ≥3
validators were asked and ≥50% were unreachable, or ≥50% failed (any answer
without rows), is *suspect* — the likeliest explanation is this observer's
own network, a stale pin or a broken coder, not that many independent
operators at one minute. Every count leaves those rows out and the blob
reads `not_read`; the readings are published at `vantage_health.suspect`.
A reading whose blob was available is never set aside: nothing in it counts
against anyone. `rollup.Point.Available` is where a control reading of a
blob this observer uploaded itself would plug in; none is made yet.

Its limit, stated plainly: the shares count validators, not the rows they
hold. When the validators that did not endorse have no rows to give, a mocha
blob is unavailable once the endorsers holding the most rows fail: 36 % to
55 % of them (median 48 %; 161 of the newest 400 publications of 28
September need half or more). Where it takes at least half of those asked,
the guard sets the reading aside and nobody is counted not served; where it
takes fewer, each failure counts once the second location confirms it.
Until a control read can tell this observer's side apart, a mass failure
counts neither way.

"Asked" is the denominator, and it holds only the rows that carry a
reachability verdict — the rows that could themselves have been in a
numerator. `verdict.GuardSilentClasses` names the four that cannot
(`NOT_PROBED`, `PROBE_ERROR`, `NOT_REGISTERED`, `UNATTESTED`) and
`rollup.GuardSilentSQL` is the same list spelled for the SQL twin, held to
it by `TestTheSQLAndTheGoTwinExcludeTheSameClassesFromTheGuard`. One
`PROBE_ERROR` is in it: an endorser the deciding pass passed over
(`PASSED_OVER`, `probe.GuardPassedOver`, `rollup.PassedOverSQL`), counted
as failed. The validators a busy pass passes over are the ones this
observer's own requests to still hold, which a slower pace asks and finds
unreachable; left out, they made whether a reading was set aside depend on
how busy this observer was.
`UNATTESTED` mattered at scale under the earlier schedule, which read every
assigned validator: a publisher stops collecting signatures at two thirds of
stake, so about a third of every point's rows were `UNATTESTED`, and in the
denominator they held the guard under 50% through a real outage.

---

## 10. The web app

Next.js `output: "export"` — plain files, all data fetched in the browser from
`NEXT_PUBLIC_API_BASE` (default `/api`, which Caddy proxies same-origin).

| route | reads |
|---|---|
| `/` | `/v1/meta`, `/v1/network`, `/v1/validators`, `/v1/market` |
| `/validator/?addr=` | `/v1/validators/{addr}` |
| `/blobs/` | `/v1/blobs`, `/v1/namespaces`, `/v1/market`, `/v1/network` |
| `/blob/?hash=` | `/v1/blobs/{hash}` |
| `/publishers/` | `/v1/market`, `/v1/publishers` |
| `/publisher/?addr=` | `/v1/publishers/{addr}` |
| `/methodology/` | `/v1/params` (the protocol-parameters section; the rest is static) |

`MIN_RATED` (20) gates every *ranked rate* — service, reachability, throughput:
below it the figure prints without a gauge and does not sort in either
direction. It deliberately does **not** gate the fault count: a ratio needs a
denominator, a fault is not a ratio, and a floor there would hide the finding
this observer exists to publish.

`NEXT_PUBLIC_SELF_VALIDATOR` marks the row belonging to this observer's own
operator. It marks and nothing else — no filter, no exclusion, no adjustment.
Default empty.

---

## 11. Deployment

Systemd templates, instance = network (`fibre-scan@mocha`). All
`Restart=always`, `RestartSec=5`, `User=fibre-observer`,
`EnvironmentFile=/etc/fibre-observer/%i.env`, and `ProtectSystem=strict` with
`ReadWritePaths=/var/lib/fibre-observer/%i` — **the data directory is the only
path a unit can write**, which is why the sampling secret lives there.

Units: `fibre-scan@`, `fibre-probe@`, `fibre-heartbeat@`, `fibre-collector@`,
`fibre-api@`, plus timers for `fibre-backup@` (rclone **copy**, never sync)
and `fibre-healthwatch@` (polls `/v1/health`, posts to a webhook), and
optional `fibre-litestream@`.

**Upgrade order matters**: install binaries → **stop the API** → restart the
collector (it owns migrations) → start the API. `store.OpenReadOnly` refuses a
database whose schema version is not exactly the binary's, in either
direction, so an API left running against a database the collector has just
migrated will fail its next open rather than serve columns it does not know.
The scanner shares no schema with the store and can be rolled at any point.

Caddy serves the static export from `/var/www/fibre-observer` and proxies
`/api/*` to the API port.

---

## 12. Retention and rollup

Defaults: raw rows 90 days, `raw_json` 30 days, roll a day up 14 days after it
ends. A day is **rolled before it is pruned**, oldest first, whole days at a
time — the record is never thinner than the rollup behind it.

Past the raw retention the `all` window is `obligation_daily` + `probe_daily`
for the pruned days plus the raw rows, and the response says so
(`rolled_up`). `Rolled.Without` applies `?exclude=` to the rolled days too,
or the `all` window would answer half the question.

---

## 13. Reproducibility

- **daily exports**: one tarball per UTC day, every record file plus
  `state.json`, with a manifest of line counts and SHA-256 digests. Records
  are assigned to a day by their own timestamp; late arrivals are included
  and *counted as late*
- **`sentinel-recompute`**: re-derives every row's phase and classification,
  each blob's reading, the obligation buckets with the guard, and (with
  `-sampling`) the earlier admission draws of every revealed day — from an export alone. With `-api` it compares
  against the live answer pinned to the same moment. Exit 1 when anything
  differs
- **`?as_of=`**: any window as of any past moment, so a figure cannot be
  quietly restated
- **`/v1/runs`**: the build and flags behind every row

What recompute is *not* trusted for: `assigned` and `attested` are the
prober's own conclusions, so it checks them against `publications.jsonl` —
the scanner's derivation. That catches scanner-vs-prober drift, not drift
from an on-chain answer; **the chain stores no assignment table.**

---

## 14. The two pinned modules

**`fibre-assign`** — a dependency-free reimplementation of
`fibre/validator.Set.Assign`, pinned by a differential test against the real
thing. The ChaCha8 stream and the shuffle are *not* reimplemented (same stdlib
primitives); the protocol logic around them is. Two inputs
(`MinRowsPerValidator` = 148, `LivenessThreshold` = 1/3) are neither on chain
nor observable, so `ParamsV10BlobV0` carries the exact celestia-app commit and
passing it is an explicit assertion by the caller.

**`fibre-tlsverify`** — verifies the validator-endorsed TLS identity
(`celestia-fibre-tls-v1`, OID 1.3.6.1.4.1.66463.1.1). No CA, no SAN chain:
trust comes from a consensus-key signature over the ephemeral TLS key.
Installed as `VerifyConnection` (not `VerifyPeerCertificate`) so it also runs
on resumed TLS 1.3 sessions. Pinned by the 21 golden vectors copied verbatim
from upstream.

Both are re-implementations because the upstream packages cannot be imported
from outside the celestia-app module.

---

## 15. Invariants — violating any of these is a defect

1. **A gap is never a zero.** A reading the observer did not make, or could
   not finish, is `NOT_PROBED`/`PROBE_ERROR`, counted and shown, never in a
   rate — per row *and per obligation*.
2. **A validator is not served only when the blob could not be
   reconstructed and its rows did not come back.** On an available blob no
   failure counts against anyone.
3. **Absence of a signature is "unproven", never "absent".** The publisher
   stops collecting at the safety threshold.
4. **Nothing is judged across the observer's own blindness**: a scan gap
   overlapping a shard's possible lifetime defers or permanently withholds
   the verdict.
5. **A stale assignment pin produces `PROBE_ERROR`, never a fault** — the one
   failure that would accuse the whole set at once.
6. **The filtered and pinned paths never touch the shared snapshot.**
7. **The two obligation implementations move together**, and a test holds
   their shared constant.
8. **Published figures carry their own age** (`computed_at`, `compute_ms`).
9. **The build revision on every row is a real commit.** A `-dirty` build is
   a row nobody can tie back to code.
10. **The sampling master secret never leaves the host.**
11. **A probe row's deadline equals its publication's, or the row is
    withheld.** `publications.must_serve_until` only moves through a
    correction, so a disagreement means the row was graded against a
    deadline this observer has withdrawn. The prober schedules from the
    append-only record and keeps producing such rows long after the range
    closed, so the predicate is the disagreement, not the range. The hold
    is stamped by `InsertProbe` itself (`store.ProbeHeldAtInsert`), in the
    statement that writes the row, over three arms: the publication is
    already withheld, the deadline disagrees, or a range that still
    withholds covers the publication. A publication settling into a range
    already on record is born withheld the same way. Rows already stored
    when a range lands are withheld in the transaction that records it,
    together with the cache revision, so neither the store nor a warm
    snapshot can publish a verdict the range has withdrawn. The collector
    ingests the ranges **before** the measurements, because a range says
    how to read the rows it covers.
12. **A range withholds until its corrections have landed, not until it is
    verified.** Reading every height says what the deadline should have
    been; applying that is what releases a row. The two facts are
    `param_uncertainty.resolution` and `param_uncertainty.corrected_at`,
    and `holds` is derived from both. Conflating them released the rows
    with the old deadline and the old fault still on them.
13. **No verdict is published against a deadline the observer cannot vouch
    for.** A publication whose upload interval overlaps an unclosed
    `param_uncertainty` range publishes `RETENTION_UNVERIFIED` in place of
    both `HEALTHY` and `FAULT` — withholding only the accusations would
    raise every rate it touched.
14. **A correction only moves a deadline earlier.** Verifying a params range
    can withdraw an accusation; it may never create one.

---

## 16. What the measurement cannot do

Stated here because they are properties of the machine, not of any validator.

- **Attestation is not proof of retention.** The server writes the shard
  before it signs, and the chain's own signature check short-circuits at
  quorum, so a verified signature proves the shard existed *at upload*.
- **Quorum selection bias.** Publishers stop collecting signatures at 2/3
  stake, so the measured population is selected for speed.
- **One vantage.** A failed request means *this* path failed; half of every
  reachability verdict is the observer's own network. That is why a
  not-served reading counts only once the second location, asked the same
  rows before `must_serve_until`, did not get them either.
- **A power cut looks exactly like an early prune.** The Fibre server commits
  its shard markers with `pebbledb.NoSync`, so a validator that lost power can
  answer `NotFound` for a shard still on its disk. The shape that separates
  them is in the record, not in one row: a machine event puts a validator's
  faults at one moment across many promises.
- **An eventless params change that reverts inside one check period is
  invisible.** `reconcileParams` compares chain state against the event
  history every 60 blocks. A change that lands and reverts between two
  checks produces no disagreement, so no range opens, nothing is held, and a
  validator pruning on the real deadline is published as a fault. Detection
  is endpoint sampling at 60-block granularity; `paramReconcileEvery` is the
  only lever.
- **A range no node can answer for stays held forever.** The scanner reads
  every height in a range to close it. A node that has pruned that state
  leaves the range `unresolvable`, and its obligations are withheld
  permanently — visible at `/v1/meta.param_uncertainty`, never silently
  dropped, but never judged either. An archive node can close it later; the
  resolution latch allows `unresolvable` → `verified`, only not the reverse.
- **A held day does not roll, so it does not prune.** `dayFinal` refuses a
  day with a held promise, because rolling freezes the buckets and the prune
  then deletes the rows a correction would re-grade. `rollup.Run` walks days
  in order, so one held day holds every later one. That is why the scanner
  closes a range in the pass that opens it, and why a range it cannot read
  is recorded `unresolvable` rather than left open.
- **Unavailable needs a finished reading.** A blob is unavailable only after
  every validator whose rows could have made it whole was asked, twice; a reading this
  observer could not finish is its own gap, and nothing in it counts. While the observer is
  blind it can withhold credit, never manufacture an accusation.
- **The rows a reading did not need say nothing.** A reading stops at enough
  rows, so a validator later in the order is often not asked at all, and an
  available blob says nothing about the validators that failed in it.

---

## 17. Where to look when something is wrong

| symptom | look at |
|---|---|
| a figure is stale | `computed_at` on the response; `snapshots/` on disk; the warm-up log |
| a validator reads 0 obligations | `attested` NULL vs 0; `assignment_error` on the publication |
| the service rate moved with no new readings | an amendment settled a deferred verdict (`probe_amendments`) |
| every validator failed in one reading | `vantage_health.suspect` — that reading is already out of every count |
| the scanner stopped | scan gaps in `state.json`; `scanner_lag` and `chain_liveness` in `/v1/health` |
| the prober records nothing | the prober's status `reads` block (queued, in progress, missed and retried in the last hour; `/v1/meta` components), `BackfillMissed` horizon |
| the build says `-dirty` | an untracked file in the working tree at build time |
| the API refuses to start | schema older or newer than the binary; run the collector once |
