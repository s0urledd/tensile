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
reads each blob once near the end of its retention window, asking every
validator that endorsed it for its own rows the way celestia-app's own
client asks for a shard, and publishes what came back — as an append-only
record anyone can download and recompute.

---

## 2. Five processes, one data directory

Everything runs out of `DATA_DIR` (live: `/var/lib/fibre-observer/mocha`).
The processes share files, not memory, and only one of them writes SQLite.

| process | binary | writes | reads |
|---|---|---|---|
| scanner | `sentinel-scan` | `publications.jsonl`, `payments.jsonl`, `host_history.jsonl`, `state.json` | chain RPC, and its websocket's new block headers |
| prober | `sentinel-probe` | `measurements.jsonl`, `sampling-secrets.jsonl` (reveals of the earlier draws) | `publications.jsonl`, `state.json`, `registry.jsonl`, `sampling-master.key`, chain RPC |
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
       ├─ each block read as soon as the node announces its header
       │  (NewBlockHeader on the RPC websocket, -subscribe); the tip still
       │  polled behind it, every 5 s (-subscribed-poll), and every second
       │  (-poll) while the subscription is down
       ├─ param history  (EventUpdateFibreParams, + a state reconcile every 60 blocks)
       ├─ host history   (set_fibre_provider_info events, seeded from the bonded registry)
       ├─ validator set at the PROMISE height
       ├─ verify every validator signature itself (ed25519 over the promise sign bytes)
       ├─ fibre-assign shard table over that set
       └─→ publications.jsonl  (append-only, fsynced before the cursor advances)

  prober: re-derives the queue of readings every cycle from publications.jsonl
          + measurements.jsonl (never stored)
       ├─ ReadPoint(publication)  → must_serve_until - 10 min, label full
       │                            (enough with -end-read-all=false)
       ├─ ClientOrder  → every validator that endorsed the promise, in validator.Set.Select order
       ├─ per request, probe.Run  → DNS · TCP · TLS 1.3 · consensus-key identity
       │                            · DownloadShard, 15 s in all, one re-dial
       ├─ rows verified by the blob's shared Reconstructor; every endorser asked
       ├─ result: available, or unavailable with the client's error
       ├─ Classify(Evidence) → one classification + a reason per validator asked
       ├─→ measurements.jsonl (a reading's rows together)
       └─ a validator whose answer did not serve: asked again up to twice,
          90 s after its last answer, when that is more than 60 s before
          must_serve_until; one request each, no re-dial; one lane per
          validator, a later attempt's endpoint failure answering every
          attempt waiting in it
          └─→ measurements.jsonl (a row per attempt, attempt 1 and 2)

  heartbeat: every 5 min, every bonded provider's endpoint, layers 1-3 only
       └─→ reachability.jsonl

  collector: tails all of it into SQLite with byte-offset cursors, every 10 s
       ├─ between those passes: state.json, publications and payments only,
       │  within a tenth of a second of the scanner writing one of them (their
       │  directory's file events, -fast-watch) and every second besides
       │  (-fast-every; nothing opened while they stand still)
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
| `measurements.jsonl` | request of a reading: each validator asked, and each later attempt (earlier: probe) | `started_at` |
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

`state.json` is **not** a record file: it is the scanner's current param
history, scan gaps, host seed and frontier. Every export carries it as a
snapshot because `sentinel-recompute` needs all four to redraw a verdict the
way the observer drew it.

Dedupe keys make re-ingest a no-op: a publication by `settlement_tx_hash`, a
probe by `(vantage, promise_hash, validator_address, scheduled_at)` (a later
attempt of a full reading adds its number), a params range by its id
(`chain:kind:from-to`) plus its resolution, a correction by
`(target, range id)`.

**Where a line is.** Every byte of a record file keeps one logical offset
for good (`internal/record`), and a line is held in up to five places:

| copy | where | written by | removed |
|---|---|---|---|
| live file | `<DATA_DIR>/<file>`; another vantage's heartbeats at `vantages/<name>/reachability.jsonl` | its writer, appending under a shared `flock` (`vantage-pull` for the other vantages) | never; rotation moves its older lines into a segment |
| archive segment | `archive/<file>/<seq>-<day>.jsonl.gz`, listed in `archive/<file>/index.json` (another vantage's under `vantages/<name>/archive/`) | `observer-archive`, daily | only by retirement, below |
| daily export | `exports/<tensile-…-day>.tar.gz`, its `.sha256`, its entry in `exports/index.json` | the collector, once a UTC day | never |
| store | `observer.db`: a row per publication, reading, endpoint check and payment, from which the line is written back byte for byte (the slim record, section 5) | the collector | never (section 12) |
| remote backup | `BACKUP_REMOTE/<network>/`: live files, segments, exports | `fibre-backup`, nightly `rclone copy` | never: copy does not delete |

The segments hold logical bytes `[0, base)` in order and the live file
`[base, …)`; a file's member in consecutive exports holds consecutive
ranges `[source_from, source_to)` of it. observer-archive rotates the
files the store keeps line by line (measurements, reachability and the
other vantages' heartbeats, publications, payments) and sampling
decisions, which it does not.

**Retiring a local copy.** `observer-archive -retire -db observer.db`, the
second step of `fibre-archive@`, removes an archived segment's gzip file,
and nothing else, when every byte of it is proven in three other places:

1. in daily exports intact on this disk: each export whose member of the
   file overlaps the segment's range, tarball and member against
   `exports/index.json`;
2. in the store: `observer/recordcheck` writes every line of each of those
   members back from the store and gets the member byte for byte. The
   answer is kept per tarball digest in `exports/verified.json`
   (`record-verify -ledger`); a day not in it is checked first, against the
   store opened read-only;
3. on the remote backup: `exports/remote.jsonl` holds an `"ok": true` line
   for the tarball's current digest, which `fibre-backup` writes after
   reading the remote copy back whole and hashing it (the archive unit has
   no network; the newest line per tarball counts).

`record.Retire` then, under the file's archive lock, reads the segment back
from those exports once more and requires its length, lines and SHA-256,
saves the index with the segment's `retired` record (the exports, the
member name, the exports directory relative to the archive directory, the
proof in words), and only then removes the file and syncs the directory.
Anything not proven is kept, with the reason in the run's output and in
`archive/retire-report.json`.

Never retired: the live files; the files the store does not keep line by
line (`registry`, `runs`, `sampling-secrets`, `sampling_decisions`,
`amendments`, `host_history`, `param_uncertainty`, `corrections`), whose
export no second copy can be checked against; the exports, `state.json` and
the store, since a retired range is read back from the exports. A day the
store does not give back whole (a sampled-out publication's NOT_PROBED rows,
which it keeps as one decision; a repeated key, of which it keeps the first
line) keeps its segments until a later check finds the day reproducible.

**Reading a retired range.** A segment whose file is there is read from the
file, retired or not (a crash between saving the index and removing the
file leaves both). One whose file is gone is read from the exports its
`retired` record names, relative to its own archive directory, so a
restored copy of the data directory reads its own `exports/`. The read is
two passes: the first checks every tarball and member against
`exports/index.json` and the range against the segment (length, lines,
SHA-256) and hands out nothing; the second hands out each 1 MiB block once
its digest is the first pass's. A reader gets the exact bytes or an error,
never fewer or other lines. Every reader of the whole record goes through
it unchanged: a rebuild from zero (the collector's ingest from offset 0),
`sentinel-recompute -data-dir <DATA_DIR>` and `sentinel-measure-check`
(`record.OpenAll`), the restore drill and `fibre-backup-manifest`
(`verify`, `cat`, `snapshot`, which carries the exports a retired segment
names), and the schema rollback's collector reading on from its cursors. A
build from before retirement does not know the `retired` record and stops
at the missing file; going back to one needs the segment files back from
the remote backup first, unless its cursors are past every retired range
(`deploy/README.md`, "Going back past schema 27"). The export builder reads
only bytes not yet exported, which are never retired, and the scanner and
the prober read nothing retired at start: the live files, and the whole of
`sampling_decisions.jsonl`, which is never retired.

---

## 5. The store

SQLite, WAL, `auto_vacuum(incremental)` (set at creation — changing it later
needs a full VACUUM, which is why an existing DB has to be deleted). The
collector owns the schema; the API opens `query_only` and refuses a database
older *or* newer than the binary expects.

**Schema version 28.** Base tables from `schema.sql`: `schema_migrations`,
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
| 26 | `publications_tx` and `publications_commitment`: a blob looked up by its settlement transaction or its commitment (`/v1/blobs?tx=`, `?commitment=`) is a seek, not a walk |
| 27 | the slim record: `publications.original_rows` / `total_rows` (the two values queries read out of `raw_json`), `slim_entries`, `reading_rows` |
| 28 | the slim endpoint check record in `reachability.raw_json`; no table change (the version keeps an older build, which would read the column as JSON, off the store) |

**The slim record (migration 27, `store/slim.go`, `internal/slim`).** A row this
build writes keeps in `raw_json` the slim form of its record: every field of the
JSONL line but those computed again from the others, from which the line is
written back byte for byte (`Store.Record`, `Store.ProbeRecord`). What is
computed again: each validator's rows (fibre-assign over the validator set and
the commitment), the copies a reading carries of its publication, and the
totals of the assignment. A computed field is dropped only when the computation
gives back exactly what the record holds; anything else is kept as it is. What
every slim record shares (dictionary, object shapes, validator sets, host
vectors) is in `slim_entries`, written in the same transaction as the record
that first needed it. A row an earlier build wrote keeps its line (it starts
with `{`; a slim record never does) and is read as it is. The copies the slim
record replaces are marked, not stored: `probes.row_indices` and
`assignments.rows_json` hold `=` where the list is the validator's own
assignment in its order (`Store.RowIndices`, `Store.AssignedRows`), and
`reading_rows.exact` keeps each reading's count of distinct verified rows, which
the rollup used to count with `json_each` over the lists. A row's record in
`raw_json` is therefore not JSON: read it through those functions, never with
`json_extract`.

An endpoint check row (`reachability`, migration 28) keeps its record the same
way, with nothing computed again: each value by the format's tags against the
same shared tables (`Store.ReachRecord`), and only when it reads back to the
line byte for byte; otherwise the line itself. 155 bytes of a 1,362-byte line on
the golden records, 760 bytes a row with its columns and indexes where it took
2,311.

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
The API's window snapshots run to a fifteen-minute TTL and key on
`meta.param_holds_rev`, which every path that raises or lifts a hold moves;
without that a withheld fault would stay on the front page for up to a
quarter of an hour after the hold landed. It is a counter incremented
inside SQLite, not a timestamp: the collector stamps one `time.Now()` at
the top of a pass and threads it through every record it ingests, so two
ranges landing in the same pass wrote the same nanosecond and a snapshot
computed between them stayed valid across the second one. The API treats
the value as an opaque token and only compares it for equality.

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
`FAULT` for counting is decided in `verdict.CountedClass`. At a full reading
(`probe.FullReading`: label `full`, or `end` started from
2026-10-02T16:09:49Z on) each endorser is judged on its own answers,
whatever the blob came to: its last answer, when none served, none was this
observer's gap (`NOT_PROBED`, `PROBE_ERROR`, or rows of the blob that are
not the validator's own, `probe.FullForeign`), it was owed no attempt that
is missing from the record (`next_attempt_due`), and some request of the
reading reached a server. At a reading before it, and at one labelled
`enough`, on an unavailable blob every answer that left the reader without
the validator's rows; on an available one none.

**Blob reading** — `verdict.BlobReading`, one per publication, the Fibre
client's result: **available** when the distinct verified rows reach
`original_rows`; **unavailable** with the client's error, `no shards
retrieved` or `not enough shards to reconstruct blob`, when they do not. A
reading that did not happen (the prober missed it, `NOT_PROBED`, or not a
single request reached a server, `probe.Reached` for none) is `pending`
while the window is open and `not_read` after, and no one is not served on it
(rows that did come back still count as served); at a full reading missed only
in part, the validators that were asked are still judged on their own answers. A
reading is judged from all of its rows, whatever phase each carries. A blob
of the earlier schedule shows the reading at the newest point in the window
every endorsing validator was reached at; each of its points counts as a
reading of its own.

**Obligation bucket** — one per `(validator, promise)` pair, in
`observer/verdict` and in SQL in `observer/rollup`:

```
pending      must_serve_until > as_of
served       its own rows came back verified at the blob's reading, at the
             reading's own request or a later attempt
broken       not served: at a full reading, its last answer did not serve
             and none of its answers was this observer's gap; before it, the
             blob was unavailable and its rows did not come back
not_counted  the rest: this observer's gap among its answers (its network,
             resolver or clock, and rows of the blob not its own, included),
             an attempt it was owed that is not on record, a reading in
             which no request reached a server, a blob not read; before full
             readings also a failure on a blob that was available (a
             validator such a reading did not ask has no row and no bucket)
```

Rate is `served / (served + broken)`. Everything else is printed beside it.

**Two implementations, on purpose.** `observer/rollup` holds the SQL;
`observer/verdict` is the Go twin `sentinel-recompute` runs; the API's tests
run both over the same rows. A constant shared between them (the end-segment
divisor) is held to the same value by a test, because a query fragment cannot
reference a Go constant.

---

## 7. The reading

`internal/probe/blobread.go`, `fullread.go`, `retry.go` and `ownside.go`.
Each blob is read once, and every validator that endorsed it is asked for
its own rows,
the way celestia-app's Fibre client asks for a shard (a full reading, label
`full`, `probe.FullReading`):

- at `must_serve_until - 10 min` (`ReadPoint`; half way through a shorter
  window); a reading that cannot start by `must_serve_until - 3 min` is not
  made, and its endorsing validators get a `NOT_PROBED` row
- every validator that endorsed the promise, in `validator.Set.Select` order
  (`ClientOrder`, celestia-app's own code), whatever the rows already held;
  a validator that did not endorse owes nothing and is not asked
- each request, lookup, connect, TLS and `DownloadShard` together, gets
  15 s (the client's `RPCTimeout`), and is made again at once after it
  failed before a server answered (a failed lookup or dial, whatever the
  cause), an unreachable peer or a timeout
- every row is verified against the commitment by one Reconstructor the
  reading shares, which gives the blob's result
- a validator whose answer did not serve is asked again, up to twice
  (`FullReadRetries`), 90 s after its last answer (`-retry-spacing`), when
  that is more than a minute before `must_serve_until`; an attempt the
  validator's own time leaves no room for is not owed, no row is written
  for it, and the answer before it is the validator's last. This
  observer's own delays (a late reading, a wait for room, a lane, a
  restart) are taken out first: an attempt only they push past that point
  stays owed, and is written `NOT_PROBED` when its cutoff comes. A row that
  owes an attempt says when it is due (`next_attempt_due`). Each attempt
  is one request with no re-dial, runs on its own (no blob slot, no shared
  Reconstructor), goes to
  the host the registry names then, and waits in its validator's lane (one
  worker each, so one attempt in flight to a validator, the one whose
  cutoff comes first); it writes a row of its own (`attempt` 1 and 2), and
  a restart resolves each blob once for the attempts the record still
  owes, then makes them or records them as abandoned
- a later attempt that fails before the validator's identity is verified
  (no such host, a connect refused, timed out or unroutable, a failed
  handshake or certificate) answers every other attempt of that validator
  due and waiting at the same host when it began (for a certificate, under
  the same key): each gets its own row,
  `shared_from` naming the request and `raw_error` saying so
- no request of the reading, nor a later attempt, starts later than
  `must_serve_until - 1 min` (`-request-start-margin`); one that was owed
  and cannot is not made, and its validator's row says so (`NOT_PROBED`):
  this observer's gap. The client's re-dial is part of its request and
  follows it, past that point or not
- a rate limit, a `CANCELLED` the server sends, a timeout or "no route to
  host" from a validator is that validator's rows not coming back, as the
  client sees it, unless it rests on this observer's own side: at a full
  reading a connect that timed out or found no route while no request
  reached any server from 30 s before it until it ended and none of up to
  three other validators' endpoints reached last accepts a connect now, a
  timeout after a lookup that took more than 5 s, or a certificate read as
  outside its window within the clock offset and a minute of its edge, is
  rewritten to `PROBE_ERROR` with the wire outcome in `raw_error`
- load: 16 blobs (`-blob-concurrency`) and 256 requests (`-concurrency`)
  at once, 512 MiB of shards in flight (`-in-flight-mib`), and with
  `-link-mbps` set, no more shard bytes than the link moves in half a
  request's time; shard bytes let go no faster than 400 Mbit/s
  (`-max-read-mbps`: a token bucket charged each request's whole expected
  shard, with a quarter of a second of burst, so the readings leave room
  on the observer's port); a request waits for room until its last
  start, its time starts once it is let go, and it carries the phase the
  reading started in, so the wait changes nothing. Every row records `observer_load`. The
  reading's own requests have no limit per validator, as the client has
  none; a later attempt waits while the same validator's previous attempt
  is in flight

The reading ends as the client's `Download` does: available, or
unavailable with the client's error. When not a single request reached a
server (no connection to any validator opened, none refused: this
observer's own network was down) the reading did not happen, and the blob
was not read by Tensile.

The rows of a reading are appended together in one write and one fsync
(`MeasurementStore.AppendReading`), one per validator asked, with `read`
saying where each sat in it and what the reading came to; each later
attempt appends its own. Readings started before 2026-10-02T16:09:49Z
(`probe.FullReadSince`) were labelled `end`; most asked the whole set in the
client's order until the rows reconstructed the blob (the first ones, from 27
September 2026, asked each endorsing validator once). An `end` row started
from then on belongs to a full reading; the first full readings, labelled
`end`, asked each validator once. No build writes `end` any more: a
reading with `-end-read-all=false` stops once the rows reconstruct the
blob, is labelled `enough`, and is judged by the earlier rule; `end`,
`full` and `enough` are each the end of the window's one reading
(`probe.EndOfWindowLabel`). Blobs settled before
`-end-read-since` were read on the earlier schedule (four in-window points,
grace and post) and are not read again. Both keep the rule of their time.

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
GET /v1/meta                  what the site's header, banners and footer read: chain, app
                              versions, health and its checks, scan gaps, counts (the site's own;
                              not on the API page)
GET /v1/network               the window summary
GET /v1/validators            one row per validator (last_served_at: the newest reading whose rows
                              came back verified, which the overview map names)
GET /v1/validators/{addr}     one validator, four windows (addr: consensus hex or
                              valcons1…, operator valoper1…, account address)
GET /v1/validators/{addr}/status
                              the few figures an alert needs, from the same snapshot row
                              (?window=; ?as_of= is refused)
GET /v1/validators/{addr}/feed.atom, /v1/feed.atom
                              endpoint and registration events as Atom
GET /v1/blobs                 publication list (?limit=, ?offset=, ?before_height= and
                              ?before_tx_index=, ?namespace=, ?commitment=, ?tx= the settlement
                              transaction hash, ?publisher= the paying account; total); each row
                              carries its settlement_tx_hash and blob_version
GET /v1/blobs/{hash}          one blob: its reading, each assigned validator's service word, the rows
                              (?rows=1 adds each reading's row_indices and rows_sha256)
GET /v1/namespaces            namespaces by newest settlement
GET /v1/probes                raw rows (?blob=, ?validator=, ?at=, ?class=, ?served=no, ?since=,
                              ?before=; up to 1000 a page, next_before continues; ?rows=1 adds
                              each reading's row_indices and rows_sha256, up to 200 a page)
GET /v1/sampling              the earlier sampling: day commitments, and secrets once revealed
GET /v1/exports[/{name}]      daily tarballs + digests; /v1/exports/pubkey the signing keys
GET /v1/avatars/{identity}    Keybase picture
GET /v1/health                machine-readable liveness (200 / 503), each process's status
GET /v1/tip                   the newest block read, and the newest blob stored (latest_blob);
                              one answer kept for 250 ms, so the node and the store are asked
                              at most four times a second however many pages poll it
GET /v1/market                the publisher side
GET /v1/publishers[/{addr}]   incl. the escrow withdrawal queue read from state
GET /v1/params                x/fibre params + change log (heights, block times), pinned protocol
                              constants, the fee formula (price_formula)
GET /v1/signing               endorsements per settled promise against the ⅔ quorum
GET /v1/hosting               where the registered endpoints are hosted, and how concentrated
```

**HTTP caching.** A successful answer is `Cache-Control: public, max-age=15`
unless its route sets its own; an error is `no-store`. A blob lookup that
finds nothing is `no-store` too: a 404 from `/v1/blobs/{hash}`, and
`/v1/blobs?commitment=` or `?tx=` with no blob to list. A reader looks a
blob up by what it holds, often a second after submitting it, and a miss a
cache kept would say "not indexed yet" for 15 seconds after the blob was on
record.

**Validator addresses.** Every row is keyed by the consensus address in
lower-case hex, and the answers keep naming validators by it (`address`,
`validator_address`), which is what the site keys on. A reader knows a
validator by its operator address, so every answer that names one also
carries `operator_address` (`celestiavaloper1…`) beside it: the validator
list, page and status, the rows of `/v1/probes`, a blob's `assignments` and
`probes`, and `/v1/network?exclude=` (`excluded_operator_addresses`, keyed by
the address in `excluded`). It comes from `validator_identities`, read once
per answer (`operatorAddrs`), and is absent for a validator the staking set
this observer read does not name. The table is only ever upserted, so a
validator removed and created again under the same operator with a new
consensus key leaves two rows with one operator address; the operator
address then goes only to the newest row, the one it resolves to, and the
older consensus key carries none, so its page stays linked by the
consensus address. Every route that takes a validator takes
any of its spellings: the consensus address in hex or `celestiavalcons1…`,
the operator address, or the operator's account address (`resolveAddr`).
The feeds link each validator's page by its operator address too, falling
back to the consensus address, and a validator with no moniker is named in
their titles by its shortened operator address, as the site names it
(`feedName`); an entry's ID keeps the consensus address it was minted
with, so a feed reader never sees an entry twice.

The public documentation is the site's API page (`web/src/app/api`, served at `/api/`):
every documented route in order, with its parameters, a Try it and an
example answer (`endpoints.ts`), then what every route shares. `/v1/meta`
and `/v1/avatars` are the site's own, and `/v1/sampling` serves the earlier
sampling's audit; none is on it. A
response carries what some reader uses: a field nothing reads is dropped
from the answer, never from the store (the snapshot rows keep their
internal figures; `shapes.go` projects them).

**Windows**: `24h`, `7d`, `30d`, `all`.

**Snapshots.** `/v1/network`, `/v1/validators`, `/v1/market` and the
`/v1/publishers` list are aggregates over hundreds of thousands of rows, so
they are computed on a schedule and served from `snapshotCache` — with the
moment they were taken published (`computed_at`), rather than implied.
The market and the publisher list are one snapshot, so the publisher page's
board and table describe the same moment. Keepers refresh every window as its
TTL runs out, read or not (`newKeepers` in `snapshot.go`): the 24h validator
list and every market window at 10 s (the live lane), network 24h at 1 min,
7d at 5 min, 30d and `all` at 15 min except network `all` at 5 min. The 24h
validator list, the market and network 24h each have a keeper of their own; the
longer windows share one and take turns. A TTL is a floor, not a promise: a computation longer than its TTL
waits twice its cost (the 24h validator list took 15–22 s in September 2026,
so it refreshes about every 35–45 s), and the windows of one cache are taken
at different moments, so a longer window can count less than a shorter one
until its next refresh. Persisted to `<data-dir>/snapshots/` (`-snapshot-dir`)
so a restart serves the last figures at once; the warm-up then replaces them.
Two files beside them keep what the snapshots derive from the whole record,
`original-rows.json` (each publication's `original_rows`, read once from its
record) and `endorsement-ledger.json` (each validator's newest
endorsements), so a restart catches them up instead of rebuilding them
(`derived.go`). Each is used only by a build that computes it the same way
(a digest of its SQL and of a version of the Go that folds it), and only
for the store it was computed from while that store still holds everything
it was computed from: the store's creation time (`schema_migrations` version
1), chain id and schema version (so a migration costs one rebuild), the
newest row it read and that row's key, and its newest entries read again.
The identity a file names is the one the store had when the memo or ledger
began to be read, not when the file is written: an API still running when
the collector migrates writes neither under the new schema, but drops both
and builds them again from the migrated store.
Each also carries a sha256 of its own body, so an edit or damage below the
entries read again is caught too. Anything else, a file whose digest is not
its body's, or one that does not parse, is refused and rebuilt from the
store, and left for the next write to replace rather than removed, since
another process may have written a good one in its place meanwhile. Each
write goes to a temporary file of its own, synced and renamed into place.
An older build does not read them.
A file is served only under the revision it was computed under (holds,
activation, `verdict.MethodologyVersion`) and for the vantage it was computed
for. A window with nothing to serve makes a reader wait at most 8 s, then
answers 503 with `Retry-After` and `"computing": true`, which the site shows
as figures being computed; `observer-api -warm-only` computes a new build's
snapshots beside the running API (into `<data-dir>/snapshots.next`; it
refuses the live directory) so a switch does not start cold
(deploy/README.md, "Upgrading a running observer").

**Day partials.** The 7d, 30d and `all` windows are summed from per-UTC-day
partials (`dayparts*.go`): each figure that is a sum, a maximum, a set union
or an exact histogram is kept per day once the day is final (sealed), and a
window is the days it covers wholly plus raw reads of the shipped statements
over what it covers partly or cannot use a sealed day for. Every write that
can move a sealed day is found by the catch-up each computation makes in its
own read transaction, and drops the day: rows past each table's mark, the
holds by a counter the collector moves in the transaction that moves a flag
(`meta.held_flags_rev`) and then an exact digest of each promise's held rows,
corrections by a fingerprint of what they write, a collapse and the prune of
an older build by the promises and spans they reach; a seal read before such
a write is not published (the journal). The sealer works one unit at a time
at a pace (`-day-partials-pace`, default 3: after each unit it rests three
times as long as the unit took, live and with `-warm-only` alike), backs off
a unit that fails (a minute, doubling, at most six hours), and logs one line
per burst. The partials are kept beside the snapshots (`day-partials.json`
and `day-partials/`) under a definition that holds their statements, their
Go as tokens and the definitions of the tables they read, for the store named
by its creation, chain and count of migrations that rewrote rows
(`meta.migration_rewrites`): a migration that only adds what they do not read
leaves them. Every hour the next sealed row day, settlement day and ledger day
in turn are read again raw and compared field by field; a sealed day that
differs is dropped, and a ledger day or a window (compared both ways a few
times after a start) that differs puts the process on raw reads until it is
restarted, as `-day-partials=false` would, the files left for inspection.
`/v1/health` carries a `day_partials` block (state, origin, days sealed,
oldest day due and not sealed, last audit, rebuilds, last write error) and a
`day_partials` check that fails on raw reads, on an audit that found a
difference, and on a day due and unsealed for over two days.

*The histograms on disk.* A sealed row day's service-time and transfer-rate
histograms are exact, one bin per distinct value of each validator's
readings (a transfer rate is nearly always a value of its own), and they
were most of what the partials held: about 14 bytes a probe row on the
September fixtures, every sealed day in memory from the start, gigabytes
for a year of a mainnet ten times Mocha's. They stay in the day's seal
file now (`dayparts_hist.go`): the sealer writes the day whole before it
publishes it, the epoch and the index keep the rest of the day (a few
counts per validator), and a start reads each row seal file through for
its digest without parsing it. A window reads the histograms of each day
it sums from that day's file, checked by the digest the epoch names, and
the newest days' are kept in memory up to `-day-partials-cache-mb` (64 MB;
0 keeps none). A seal file is written once and never changed, and a day
sealed again is another file, so a computation sums the seal its own epoch
names or, when that file is gone, damaged or another's, reads the day raw
in its own snapshot, which is what the seal held; the day is then dropped
and sealed again. A window of more than 31 sealed days (`all`, once the
record is that long) ranks the histograms in bounded memory
(`dayparts_rank.go`): it counts each population in buckets of 1/64 of an
octave, then reads only the bins of the bucket each rank falls in, never
the record's distinct values at once. The live `all` window keeps, for each
rank, a band of values around it a few days' readings wide and each day's
part of it (how many values fall below, how many above, the bins inside),
so a refresh reads only the days sealed since its last and builds a band
again only when its rank leaves it. On a 32-day synthetic record (311,000
probe rows) the partials hold 0.9 MB after a start where they held 4.4 MB
(3 bytes a probe row, not 15), and every window refreshes as fast as
before. On synthetic seal files of a mainnet ten times Mocha's (100
validators, 2,000 served readings each a day: 3 MB of seal file a day,
1.1 GB a year), a start checks a year's files in seconds where it parsed
them in about 40, the `all` window's bands are built in about 40 seconds
(once after a start, or when a rank leaves its band) and hold 26 MB, a
refresh from them takes a quarter of a second, reading at most the day
sealed since, and a pinned or filtered `all` (`?as_of=`, `?exclude=`),
ranked by the two reads, takes about half a minute and 0.1 GB; the build
before held about 1.5 GB for that year and summed every histogram whole on
each refresh of `all`, 8 seconds and 0.9 GB more at the peak for 90 days of
it.

*Still open.* A band keeps a part per day, so it still grows with the days,
by about 20 KB a day at that scale; folding the days older than a few weeks
into one part, and building a band again when one of them is dropped, would
stop that. A seal file is JSON, read at about 60 MB/s (50 ms a day at that
scale), which is most of the work of a window that misses the cache and of
a band built again; a binary layout of the histograms would cut it. And the
`all` window's attestation is still read raw over the whole window on every
refresh, as it always was (a pair counts once at the MAX of its rows, which
is not a sum over days), so its cost still grows with the record.

**Two paths bypass the cache and are rationed** (4-burst, then one per 2s;
429 with `Retry-After`):

- `?as_of=<RFC3339>` — what the observer would have published at that moment
- `?exclude=<addr>` — the summary without named validators (any spelling of
  each; `excluded` echoes their consensus addresses)

Both are `Cache-Control: no-store`. The cache is keyed by window alone, so
writing either into it would publish one reader's view as everyone's
headline. Tests hold both paths off the cache.

---

## 10. The web app

Next.js `output: "export"` — plain files, all data fetched in the browser from
`NEXT_PUBLIC_API_BASE` (default `/api`, which Caddy proxies same-origin).

Every page's header and footer read `/v1/meta` and `/v1/tip`. The header's
search asks for a blob identifier that found nothing again each time
`/v1/tip`'s `latest_blob` changes, while its panel is open, for two minutes
after the identifier was first asked.

| route | reads |
|---|---|
| `/` | `/v1/network` (the period), `/v1/publishers?window=all` (Available: the readings of every publisher summed, available over available plus unavailable), `/v1/validators` (the map's "served last" line is the rows' `last_served_at`), `/v1/blobs` (the recent blobs: as soon as `/v1/tip`'s `latest_blob` names a blob the grid does not hold, and every 30 s besides) |
| `/validator/?addr=` | `/v1/validators/{addr}` |
| `/blobs/` | `/v1/blobs` (the first page again as the chain moves), `/v1/namespaces`, `/v1/market` (the period), `/v1/publishers` (once its filter opens); its search (`?blob=`) asks 64 hex as `/v1/blobs/{hash}`, `?commitment=` and `?tx=`, and a blob ID as `?commitment=` |
| `/blob/?hash=`, `?id=`, `?tx=` | `/v1/blobs/{hash}`; a blob ID (`?id=`) or a settlement transaction (`?tx=`) is found first with `/v1/blobs?commitment=` or `?tx=`, and several matches open the Blobs list of them; a blob not on record yet is asked for again each time `/v1/tip`'s `latest_blob` changes, and every 30 s |
| `/publishers/` | `/v1/market`, `/v1/publishers` |
| `/publisher/?addr=` | `/v1/publishers/{addr}` |
| `/methodology/` | `/v1/params` (the protocol-parameters section; the rest is static) |
| `/api/` | `/v1/health` for its status dot, and each route when its Try it is sent; its example answers are fixed text (`endpoints.ts`) |

Every link to a validator's page — the overview's table and map, the map's
line of events from `/v1/feed.atom`, a blob's assignments — carries the
operator address (`/validator/?addr=celestiavaloper1…`, `validatorHref` in
`lib/addr.ts`), and the consensus address only for a validator with none on
record. The page names the validator by its operator address and shows the
consensus address under it; a link with the hex or `celestiavalcons1…`
address opens the same page, as the API resolves every spelling.

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
`fibre-api@`, plus timers for `fibre-backup@` (rclone **copy**, never sync,
then the remote proof of the exports), `fibre-archive@` (rotation, then
retirement; `PrivateNetwork=true`), `fibre-vantage-pull@` (each other
vantage's heartbeats, every minute) and `fibre-healthwatch@` (polls
`/v1/health`, posts to a webhook), and optional `fibre-litestream@`.

**The nightly order** (UTC). 03:00: the collector builds the previous day's
export (`-export-hour`). 03:17 plus up to 20 minutes (by about 03:36):
`fibre-backup` takes the manifest's cut, copies segments, then the live
files and the exports, then reads back from the remote each export not yet
proven and appends the result to `exports/remote.jsonl`. 04:40:
`fibre-archive` rotates, then retires what the three proofs of section 4 cover.
The backup holds every archive lock shared from its cut to its last check
and a run holds a file's lock exclusively, so the two never overlap; an
export proven a night late retires its segments a night late.

**A copy of the store** for a schema rollback (`sqlite3 .backup`, which
keeps every page and rowid; never `VACUUM INTO`) reads and writes the
whole store on the disk the validator shares. It runs at idle I/O priority
(`ionice -c3`) and is paused (`SIGSTOP`, then `SIGCONT`) while the disk's
I/O pressure (`some avg10` in `/proc/pressure/io`) is high: on NVMe with the
`none` scheduler the priority class alone does not hold it back
(`deploy/README.md`, "Going back past schema 27", has the loop).

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

Every row is kept for good: nothing deletes a probe or heartbeat row or
strips its `raw_json` (decided 2026-10-04; the 90-day prune and the 30-day
strip of 2026-09-18 were retired before either first ran). A day is rolled
up 14 days after it ends, for speed only.

The API keeps its rolled-up path for a database pruned before then: the
`all` window would be `obligation_daily` + `probe_daily` for the pruned
days plus the raw rows, and the response would say so (`rolled_up`).
`Rolled.Without` applies `?exclude=` to the rolled days too, or the `all`
window would answer half the question. With every row kept, `raw_from` is
never set and nothing is labelled.

---

## 13. Reproducibility

- **daily exports**: one tarball per UTC day, every record file plus
  `state.json`, with a manifest of line counts and SHA-256 digests. Records
  are assigned to a day by their own timestamp; late arrivals are included
  and *counted as late*. They are also the copy on this disk of every
  retired range (section 4), so they are never removed
- **`sentinel-recompute`**: re-derives every row's phase and classification,
  each blob's reading, the obligation buckets, and (with
  `-sampling`) the earlier admission draws of every revealed day — from an export alone. With `-api` it compares
  against the live answer pinned to the same moment. Exit 1 when anything
  differs
- **`?as_of=`**: any window as of any past moment, so a figure cannot be
  quietly restated
- **`runs.jsonl`** in every export: the build and flags behind every row

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
2. **A validator is judged only on its own answers.** At a full reading
   its last answer did not serve, none of its answers was this observer's
   gap (rows of the blob that are not its own are one), no attempt it was
   owed is missing from the record, and some request of the reading
   reached a server; at a reading before it, the blob could not be
   reconstructed and its rows did not come back.
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
8. **Published figures carry their own age** (`computed_at`; `/v1/network`
   adds `compute_ms`).
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
- **One vantage.** A reading is one client's requests from one place, as a
  reader's would be. A validator is judged on its own answers, so trouble on
  the path between this observer and one validator reads as that validator
  not serving; asking it again, twice, 90 s apart, keeps a passing failure
  from counting. A reading in which no request reached any server counts
  nothing.
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
- **Unavailable needs a reading that happened.** A blob is unavailable only
  after every endorser was asked (before full readings, the whole set, as
  the client asks it); a reading this observer missed, or one in which not
  a single request reached a server, is its gap, and no one is not served
  on it (at a full reading missed in part, only the validators it could not
  ask). While the observer is blind it can withhold credit, never
  manufacture an accusation.
- **Readings before 2026-10-02T16:09:49Z did not ask everyone.** Most of
  them stopped at enough rows, as a reading labelled `enough` does, so a
  validator later in the order was often not asked at all, and an
  available blob said nothing about the validators that failed in it; the
  failures are `not_counted`, and a validator not asked has no obligation
  on that blob.

---

## 17. Where to look when something is wrong

| symptom | look at |
|---|---|
| a figure is stale | `computed_at` on the response; `snapshots/` on disk; the warm-up log |
| the 7d, 30d or all windows are slow, or the disk busy | `day_partials` in `/v1/health` (state, oldest day not sealed, last audit); the API's `day partials:` log lines (one per sealer burst and per audit, a unit's failure and its recovery) |
| a validator reads 0 obligations | `attested` NULL vs 0; `assignment_error` on the publication |
| the service rate moved with no new readings | an amendment settled a deferred verdict (`probe_amendments`) |
| every validator failed in one reading | the blob's rows (`/v1/probes?blob=`): if no request reached a server (`PROBE_ERROR`, or a failed lookup or dial, everywhere) the blob reads not read and no one counts; otherwise it is unavailable, and at a full reading each validator is not served by its last answer |
| the scanner stopped | scan gaps in `state.json`; `scanner_lag` and `chain_liveness` in `/v1/health` |
| the prober records nothing | the prober's status `reads` block (queued, in progress, started late and missed in the last hour; `/v1/health` components), `BackfillMissed` horizon |
| a validator is often counted neither way | the status `reads` block: `requests_not_started_last_hour`, `admit_wait_p95_ms` and the reading-rate ceiling's part of it (`rate_wait_p95_ms`), the `retries_*` counts and `retries_not_made_by_validator_last_hour`; `observer_load` on its rows |
| the build says `-dirty` | an untracked file in the working tree at build time |
| a segment is not retired | its reason in `archive/retire-report.json`; the day in `exports/verified.json` (the store's answer) and in `exports/remote.jsonl` (the remote's) |
| a read of the whole record stops at a segment | its file is gone and `index.json` names no exports for it: it was moved by hand, not retired; put it back (the remote backup keeps every segment it was sent) |
| the API refuses to start | schema older or newer than the binary; run the collector once |
