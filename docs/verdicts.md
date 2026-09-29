# Verdict taxonomy

Every row the observer stores and every number the dashboard shows is built
from two fields that `fibre-sentinel` records per request: the
mechanism-level **outcome** (what happened on the wire) and the
**classification** (what that means given the phase of the retention window
and whether the validator was assigned the shard), and from one fact about
the whole blob: whether the rows its reading brought back reconstruct it. The
dashboard adds no new verdict classes. It only groups and counts the
existing ones.

Source of truth:

- outcomes and classifications: `fibre-sentinel/internal/probe/classify.go`
  (`Classify`), tested by the taxonomy table in
  `fibre-sentinel/internal/probe/probe_test.go`
- served / wrong rows / partial: `fibre-assign/verify.go` (`ShardMap.Verify`),
  tested in `fibre-assign/assign_test.go` (translated from celestia-app's
  `fibre/validator/set_test.go`)
- phases: `fibre-sentinel/internal/probe/schedule.go` (`PhaseAtWindow`)
- the reading: `fibre-sentinel/internal/probe/blobread.go`; what it comes to
  and what each row then counts as: `fibre-sentinel/observer/verdict/blobread.go`
  (`BlobReading`) and `verdict.CountedClass`, with their SQL twins in
  `fibre-sentinel/observer/rollup`

## Phases

| phase | span | what a reader should conclude |
|---|---|---|
| `in_window` | settlement to `must_serve_until` | the validator is under its retention obligation |
| `grace` | `must_serve_until` to `must_serve_until + prune tolerance` (default 2m30s) | an honest server prunes late by anything from a second to just under two minutes (a one-minute prune loop over a minute-granularity key); nothing here counts against a validator |
| `post` | after the grace span | the obligation is over; the blob is expected to be gone |

A blob is read in window, 10 minutes before `must_serve_until`; rows in the
grace and post phases exist only in the earlier schedule's record.

`must_serve_until = creation_timestamp + max(payment_promise_timeout, shard_retention)`,
using the on-chain params. The server itself reads the params when the shard
is uploaded, which happens somewhere between the promise height and the
settlement tx; the scanner evaluates every params entry in force anywhere in
that interval and, if they do not all agree, records the earliest bound and
sets `must_serve_until_ambiguous` on the publication. The interval starts
one block before the promise height, because the chain accepts a promise
one block ahead of the validating node's latest state, so a server one
block behind validated the upload against the state before the promise
block. The earliest bound is the one no server could have undershot, so a
later window on the server can only produce `SERVED_PAST_WINDOW` or
`EXPECTED_GONE`, never a fault. The scanner also re-reads the params from
state every 60 blocks: a governance change announces itself with an event,
an upgrade handler does not, and a change that arrived silently is recorded
as in force from the block after the check. Publications settled between
the change and the check keep the window computed from the old params (the
record is append-only); when that change shortened the window their
recorded deadline is later than the server's prune time, and an in-window
`NOT_FOUND` between the two would be a fault.

That range is written down (`param_uncertainty.jsonl`, published at
`/v1/meta.param_uncertainty`) and the scanner tries to close it in the same
pass, by reading `x/fibre` params at **every height in it**. Not a
bisection: a bisection locates a transition but cannot prove there was no
third value between the endpoints, and a third value whose window dipped
shorter is the entire reason the two endpoints are not a proof. Sixty
heights is about a second against the node the scanner already follows, so
the common case closes immediately and the values that were really in force
go into the param history at the heights they were in force from.

While a range is open — the node could not answer, or the range is wider
than one pass will read — every obligation it covers is **held**: the rows
publish `RETENTION_UNVERIFIED` instead of the `HEALTHY` or `FAULT` they were
stamped with, and neither the fault nor the credit reaches a rate. A
publication is covered when its upload interval, from the block before the
promise height to the settlement tx, overlaps the range.

**A row is held while its deadline disagrees with its publication's**, not
while its range is open. Those are different sets, and the difference is the
whole steady state: the prober schedules from `publications.jsonl`, which is
append-only and still carries the deadline the scanner stamped, so it keeps
producing measurements against a withdrawn deadline for as long as the *old*
window runs — hours after the range that corrected it was closed. A rule
keyed on the range cannot see those rows at all. A rule keyed on the
disagreement cannot miss them, whenever they arrive.

The hold is stamped in the same statement that writes the row, so a
measurement that should be withheld is born withheld: there is no moment at
which it exists and reads `FAULT`. Three things put it there, and each
covers a case the others cannot — the publication is already withheld
(the normal state while a range is open and nothing has been corrected),
the row's deadline disagrees with its publication's (every row the prober
produces after a correction), or the publication is covered by a range that
still withholds (the pass where the range itself has only just arrived).
The collector ingests the ranges before the measurements for the same
reason: a range says how to read the rows it covers.

Rows **already stored** when a range lands are withheld in the same
transaction that records it. A range is a statement that the verdicts of
the publications it covers cannot be published, and the rows carrying
those verdicts are in the store before it arrives; leaving them to the
hold sync at the end of the pass left every one of them readable as a
fault in the meantime, and for as long as the collector stayed down if it
stopped in between. The cache revision moves in that transaction too, so
a snapshot holding the withdrawn fault cannot outlive it. The collector's next pass re-grades it against the deadline
its publication now carries and the hold lifts. A row whose own record the
retention pass has stripped cannot be re-graded, so it stays withheld.

**Verifying a range is not what lifts the hold.** Reading every height says
what the deadline should have been; it does not move the deadlines already
stamped on the publications, or re-grade the rows drawn against them. Until
those corrections have actually landed, the store still holds the wrong
deadline and the rows still carry the verdicts drawn from it, so a verified
range keeps withholding. The hold lifts only when the correction pass has
re-derived **every** publication the range covers and **every** row of those
publications, and it says so in the record with a `range_corrected` line. A
row whose own record the retention pass has already stripped cannot be
re-derived, so its range stays open and its verdicts stay withheld — a row
this observer cannot re-derive is one it must not publish a verdict for.

When a range is verified, the deadlines it covers are recomputed against the
proven values and every row re-graded, as append-only corrections
(`corrections.jsonl`, `publication_corrections`, `probe_corrections`). The
row keeps what it was stamped with beside the corrected value
(`must_serve_until_at_probe`, `phase_at_probe`, `classification_at_probe`,
`corrected_at`), and the correction log carries no foreign key to the probe
row, so the retention prune cannot delete the record of a verdict this
observer withdrew.

**A correction only ever moves a deadline earlier.** Verifying a range can
make the recomputed deadline *later* — a value proven to have started before
the promise height replaces what the history had there rather than joining
it, and a longer replacement raises the earliest bound — and that would turn
a validator that read clean into a `FAULT` on evidence this observer did not
hold when it published the clean reading. So the correction is clamped: it
can withdraw an accusation, never make one. The cost is real and is taken
deliberately: a window that was silently *lengthened* leaves obligations
under-claimed, and a validator that pruned on the old shorter deadline keeps
a verdict this observer will not revisit.

What this still does not catch: a change that lands and reverts inside one
60-block period produces no disagreement at the check, so no range opens.
Detection is endpoint sampling at 60-block granularity, and that is the
bound.

## Who is actually obliged

A validator is assigned rows by `fibre-assign`, but assignment is the
publisher's arithmetic, not the validator's consent. The validator becomes
obliged only once it has the shard. The only on-chain evidence of that is a
signature from the validator on the settled `MsgPayForFibre`, because the
Fibre server writes the shard to its store **before** it signs
(celestia-app `fibre/server_upload.go`).

The observer verifies those signatures itself rather than counting them.
Honest validators do check them, in `CheckTx` and `ProcessProposal`
(`x/fibre/ante/ante.go`; `FinalizeBlock` skips the check because a committed
block has already passed `ProcessProposal`). But the check stops at the
quorum: `validateValidatorSignatures` (`x/fibre/keeper/msg_server.go`) walks
the entries in validator-set order and returns as soon as the ones verified
so far carry two thirds of the stake (`if hasEnough { return nil }`). Entries
after that point are never verified by any node, so a settled transaction
proves that a quorum signed, not that every signature entry in it is valid.
Each entry is
tried first against the validator at the same position in the set at the
promise height (the reference implementation builds the list positionally,
with nil entries for non-signers) and then, if that fails, against every
other member. Verification decides; position is only a hint.

The absence of a signature is **not** evidence the validator did not store
the shard. The reference client snapshots the signature list the moment the
safety threshold is reached and keeps delivering to the rest in the
background (`fibre/client_upload.go`), so a validator can hold a shard whose
signature never reached the chain. Absence means *unproven*, never *absent*.

### The selection bias this creates, stated plainly

This is the single largest limit on every serve rate published here, and it
does not average out.

A publisher stops collecting signatures the moment it has two thirds of
stake, so the validators that end up *proven* obliged for a blob are, by
construction, the ones that answered the upload first. Speed of response to
an upload decides who is in the denominator, and the serve rate is therefore
computed over a population selected for having been fast — which is not the
population an operator or a delegator has in mind when they read it.

Neither the size nor the direction of the bias is measured here. The
expectation is that it flatters: a validator slow to accept uploads is
under-represented in the denominator, and if slow-to-accept and
poor-at-retaining are the same operators, the published rate reads better
than the network's true retention. But that is an assumption about a
correlation this observer has never measured — it probes retention, not
upload latency, and it holds no reading of the two together. Two things
could turn it the other way: completion order depends on shard size, which
scales with stake, so the quorum leans toward smaller validators, and
nothing here establishes that smaller validators retain better; and a host
fast to accept an upload is not thereby a host that still has it thirteen
hours later.

So the direction is stated as what it is — the likelier of the two, not a
property of the measurement — and it is not the reason the rate goes
uncorrected. The reason is that any correction would have to model the
missing population, and a model is not an observation.

What is published instead is the size of the bias's input:
`attestation.blob_coverage`, the proven share of assigned (validator, blob)
pairs. A reader who wants the pessimistic bound can read the unproven count
beside the rate; this observer will not turn it into a number of its own.

That is why only the validators whose endorsement the settled promise
carries are counted. The reading asks the whole set, as the client does,
and a row from an assigned but unattested validator is `UNATTESTED`
whatever the wire outcome was, outside every count in both directions
(its verified rows still count toward the blob): a failure the validator was
never proven to owe cannot count against it, and a success it was never
proven to owe cannot count for it. The outcome field still records exactly
what happened.

Source: `fibre-sentinel/internal/scan/attest.go`, tested in
`attest_test.go`; the classification in `internal/probe/classify.go`, tested
by `TestClassify_UnattestedIsNeverAFault`.

## Outcomes (one per request, mechanism level)

| outcome | meaning |
|---|---|
| `SERVED_OK` | shard returned, every row verifies against the commitment and the returned indices equal the assigned set |
| `SERVER_ERROR` | the endpoint was reached, completed TLS, proved its identity and answered the RPC with an application error (gRPC `Internal`, `Unknown`, `DataLoss`, `Aborted`, or a `Canceled` the server sends while the request is still live on this side: the client takes it as the server's error and skips the shard; only a cancel of this observer's own is `PROBE_ERROR`). Deliberately not a reachability failure: calling it "unreachable" would be false about a server the observer just talked to. Deliberately not a fault either: the server did not say it lacks the shard |
| `RPC_THROTTLED` | the endpoint was reached, proved its identity and refused the request with `ResourceExhausted` that is neither the observer's own receive bound (`received message larger than max`, a `PROBE_ERROR`; the bound is sized from the blob and recorded as `download.recv_limit`) nor the server's own send bound (`trying to send message larger than max`, a `SERVER_ERROR`: the validator's gRPC refuses to deliver a shard it holds): a server-side limit. Celestia has said a per-peer rate limiter is coming to the Fibre server. The client takes it as the server's answer and skips the shard, and so does the reading (`THROTTLED`) |
| `PARTIAL` | fewer rows than assigned, the ones returned are valid |
| `WRONG_ROWS` | rows returned but not the assigned set (`ShardMap.Verify` failed) |
| `INVALID_ROWS` | rows returned but they fail commitment verification |
| `NOT_FOUND` | the server answered cleanly that it has no such shard |
| `DNS_FAIL` | registered host name did not resolve |
| `TCP_REFUSED` | connection actively refused |
| `TCP_TIMEOUT` | TCP connect timed out |
| `TCP_UNREACHABLE` | network or host unreachable, DNS fine |
| `TLS_HANDSHAKE_FAIL` | TCP fine, TLS 1.3 handshake failed |
| `IDENTITY_FAIL` | handshake fine, certificate extension is not endorsed by this validator's consensus key |
| `RPC_UNAVAILABLE` | gRPC Unavailable after a good handshake |
| `RPC_DEADLINE` | earlier rows: the download did not finish within the observer's own deadline (base plus shard size at 1 MiB/s); the observer gave up, the validator was not judged |
| `RPC_TIMEOUT` | the request ran out of the Fibre client's 15 s after the connection was made: the validator's slowness, as the client meets it (`UNREACHABLE`) |
| `MALFORMED_SHARD` | an answer no client accepts: over the protocol's message bound, or one the client cannot parse (`SERVER_ERROR` in window) |
| `RPC_ERROR` | any other gRPC error |
| `NO_REGISTERED_HOST` | the validator has no fibre host in `x/valaddr`, so nobody can fetch its rows; treated as unreachable |
| `PROBE_ERROR` (no coder) | the observer could not build the verifier for this blob's `(original_rows, total_rows)`, so no download was attempted |
| `REACHABLE` | TCP, TLS and identity passed and the download was deliberately skipped (the reachability heartbeat); no retention verdict |
| `PROBE_ERROR` | the observer's own probe failed (bug or config), not the target |
| `MISSED` | the reading could not start before its deadline (earlier rows: the scheduled point elapsed before the prober ran it) |

## Classifications (the verdict)

One sentence each, and what a reader should conclude.

| classification | when | conclude |
|---|---|---|
| `HEALTHY` | assigned validator returned `SERVED_OK` in window or in grace | the validator kept its promise at this point in time |
| `FAULT` | an identity-verified endpoint, for a shard the chain proves it stored, **said it has no such shard** (`NOT_FOUND` in window) or **returned bytes that do not verify against the commitment** (`INVALID_ROWS`, any phase). A third arm, rows outside this promise's assignment that verify against nothing (`WRONG_ROWS`/`PARTIAL` without `commitment_verified`, in window or grace), exists in the code as a guard but cannot be produced by the prober, which files rows that fail the commitment as `INVALID_ROWS` | the validator did not serve a shard the chain records it as obliged to hold, at a moment inside that obligation. That is what the row says, and it is the most it says: `x/fibre` calls this parameter a *minimum local retention* — upstream's own words are "the minimum local duration validators keep uploaded shards" (`x/fibre/types/params.go`) and "the on-chain local retention floor for uploaded shards" (`celestia/fibre/v1/query.proto`) — and the chain neither checks it nor penalises missing it. "Broke its promise" is a heavier sentence than the protocol supports; this row is the observation, not a verdict about intent. It counts against the validator only when the blob could not be reconstructed (see "When a failure counts"): on a blob that was available the reader was never left without the rows, and the row counts neither way. Two conditions, each reproducible by anyone who repeats the probe. At the reading, an answer no client can use (an empty shard, an RLC vector of the wrong length, a reply over the protocol's message bound) is `MALFORMED_SHARD`, filed `SERVER_ERROR`: the client skips such a shard, so it is not served on a blob that could not be reconstructed. On rows of the earlier schedule the same parse failure was the observer's gap (`PROBE_ERROR`, `shard shape:` on the row). Neither is `INVALID_ROWS`: the commitment check is the only thing that turns bytes into that fault. Every request of a reading carries the phase the reading started in, so a request this observer's own limits held back is judged as if it had been made at once. One margin: a `NOT_FOUND` whose answer arrives within 30 s of `must_serve_until` is graded as grace (`TOLERATED`, counted neither way) and the row says `phase_note: not_found_at_deadline`, because the server prunes on a minute tick against its own clock; only a request held back to the deadline itself can meet it. The count beside a validator's name is `obligations.broken`: one per shard signed for and not handed over from a blob that could not be reconstructed |
| `UNREACHABLE` | assigned and attested, in window, and the observer could not complete a conversation at all: `DNS_FAIL`, `TCP_REFUSED`, `TCP_TIMEOUT`, `TCP_UNREACHABLE`, `TLS_HANDSHAKE_FAIL`, `RPC_UNAVAILABLE`, `RPC_TIMEOUT`, `RPC_ERROR` | no answer within the client's 15 s (connect and TLS included), asked twice (the client's re-dial). On a blob that could not be reconstructed it is not served, as a reader using the client meets it; on one that was available it counts neither way. At the reading, "no route to host" is that validator's host not answering (`TCP_UNREACHABLE`); only a failure that never left this machine (no route out, no local address, a local socket error) is filed as the observer's own, `PROBE_ERROR` |
| `NOT_REGISTERED` | assigned validator with no Fibre host in `x/valaddr` at the time of the probe (`NO_REGISTERED_HOST`) | a registry state, not a refusal. Jailing and unbonding remove a provider from `AllBondedFibreProviders` while the chain keeps the entry: it is garbage-collected only once the validator is gone from staking state, or jailed and unbonded for longer than the unbonding time plus seven days |
| `SHADOWED_SHARD` | assigned validator returned rows that **verify against the blob commitment**, are not this promise's assignment (`WRONG_ROWS` or `PARTIAL` with `commitment_verified`), and are **exactly the row set another settled promise over the same commitment assigns to this validator** (`shadowed_by` names it) | that promise answered in this one's place. `DownloadShard` is addressed by the commitment alone; the Fibre store keeps every promise's shard side by side (`Put` "stored independently without deduplication") and `Get(commitment)` returns the first readable one in promise-hash order, so the validator has no way to tell the two apart. Its rows came back verified, so at the reading it is served. Without a matching promise the same wire result is `UNMATCHED_GENUINE`, and that verdict is drawn late (see "Deferred verdicts"): the order is by hash, not by time, so a promise settled after the probe can be the one that answered |
| `UNMATCHED_GENUINE` | assigned validator returned rows that verify against the blob commitment but match no settled promise's assignment for it, judged once every promise that could own them is on record | a shard uploaded for a promise that never settled is on disk until its prune and never on chain, and answers whenever its hash sorts first; the validator is serving genuine data of the blob. Its rows came back verified, so at the reading it is served, indices on the row |
| `IDENTITY_EXPIRED` | certificate endorsed by the right consensus key, but its signed validity window has lapsed or has not started | a renewal running late. Endpoint hygiene, not impersonation and not a retention failure. The client rejects the certificate all the same, so it is not served on a blob that could not be reconstructed, otherwise counted neither way |
| `IDENTITY_MISMATCH` | certificate not endorsed by this validator's consensus key, any validator, any phase (judged before attestation: a certificate is a property of the endpoint) | no client will download from this endpoint, so it is as unusable as one that does not answer. Shown as the endpoint's status; not served on a blob that could not be reconstructed, otherwise counted neither way |
| `SERVER_ERROR` | assigned and attested, in window, and the endpoint answered with an application error instead of the shard (`SERVER_ERROR` outcome) | the server was reached and did not hand over the shard. Not served on a blob that could not be reconstructed, otherwise counted neither way; a server that errors at every reading is visible as such on its own page. On earlier rows, in grace it is `TOLERATED`, after the window `UNREACHABLE_POST_WINDOW` |
| `THROTTLED` | assigned and attested, in window, and the endpoint refused the download with a rate limit (`RPC_THROTTLED` outcome) | the server was reached and declined to serve. The client takes a rate limit as the server's answer and moves on without the rows, and so does the reading, so it is the validator not serving: not served on a blob that could not be reconstructed, otherwise counted neither way |
| `TOLERATED` | assigned validator, grace phase: `NOT_FOUND` or unreachable | honest pruning lag; do not read anything into it |
| `EXPECTED_GONE` | assigned validator, post phase: `NOT_FOUND` | correct behaviour after the window |
| `SERVED_PAST_WINDOW` | assigned validator, post phase: still serving (`SERVED_OK`, `PARTIAL`, or `WRONG_ROWS`) | not a fault; the validator keeps data longer than it must. `WRONG_ROWS` is here rather than under FAULT because `DownloadShard` performs no assignment check at all — assignment is enforced only at upload — so rows outside an assignment, after the obligation ended, are not a rule the validator broke |
| `UNREACHABLE_POST_WINDOW` | assigned validator, post phase: unreachable | not a retention fault; the obligation was over. It still feeds the reachability view |
| `EXPECTED_UNASSIGNED` | validator not assigned this shard answered `NOT_FOUND` or was unreachable | normal; earlier records only, when `-probe-unassigned` was on |
| `SERVING_UNASSIGNED` | validator not assigned this shard returned data for it (`SERVED_OK`, `PARTIAL`, `WRONG_ROWS` or `INVALID_ROWS`) | unexpected; either the observer's assignment is wrong or the validator over-serves. Shown for review, never as a fault |
| `UNATTESTED` | assigned validator, any phase, where no verified signature from that validator appears on the settled promise and the probe reached the question of the shard at all (an observer-side outcome, a stale assignment pin, a missing registry entry or an unusable certificate is named first, as `PROBE_ERROR`, `NOT_REGISTERED` or `IDENTITY_MISMATCH`, none of which enters a rate) | nothing on chain proves this validator ever stored the shard, so no verdict is owed either way. Outside every rate, in both directions. The reading asks such a validator like the rest, as the client does, and its verified rows count toward the blob |
| `PROBE_ERROR` | the request failed on this observer's side (a local resolver, no route out, no coder, an answer this build could not handle) or the observer gave up on it (`RPC_DEADLINE`, earlier rows) | an observer problem, shown as a gap. When no request of a reading reached a server, the reading did not happen and the blob was not read by Tensile; when one did, the reading happened and this validator's rows did not come back |
| `NOT_PROBED` | the reading could not start in time (observer down or behind; `MISSED`), or on earlier records a publication the load policy sampled out, or a heartbeat (`REACHABLE`) | a gap in observation, never a zero |
| `RETENTION_UNVERIFIED` | the publication's upload interval overlaps a range of heights over which an `x/fibre` params change landed with no event and this observer has not read the params at every height (see "Phases"). Applied over the stored row rather than returned by `Classify`: the measurement record says what happened on the wire and is append-only, while whether this observer trusts its own deadline is a judgement that has to be revisable | this observer cannot say when the obligation ended, so it publishes no serve verdict — **neither the fault nor the credit**. Withholding only the accusations would raise every rate it touched, which is the same argument this document makes for `UNATTESTED`. Replaces exactly `HEALTHY` and `FAULT`; `INVALID_ROWS` is carved out, because bytes that fail the commitment are a fault in every phase and no deadline rescues them. Never a statement about the validator |

## How the dashboard derives its numbers

- **The reading.** Each blob is read once, 10 minutes before
  `must_serve_until`, exactly as celestia-app's Fibre client downloads it
  (`internal/probe/blobread.go`): the whole validator set in the client's
  own order (`validator.Set.Select`, called rather than copied), the next
  one asked while the rows still wanted outnumber the rows already on
  their way; each request, lookup, connect, TLS and `DownloadShard`
  together, gets the client's 15 s and is made once more at once after it
  failed before a server answered (a failed lookup or dial, whatever the
  cause) or after an unreachable or timed-out peer; every row is verified
  against the commitment by one Reconstructor the whole reading shares;
  and the reading stops at `original_rows` (4096 for blob v0) distinct
  verified rows. This observer's limits on requests and bytes in flight
  only delay a request, never drop it, and every request carries the
  phase the reading started in, so a delay changes nothing; there is no
  limit per validator, as the client has none. The rows of a reading are
  written together, one per validator asked, each with where it sat in the
  reading and what the reading came to (`read`).
- **The result** (`verdict.BlobReading`) is the client's, three ways only:
  **available** when the distinct verified rows reach `original_rows`;
  **unavailable** with the client's error, `no shards retrieved` when no
  verified row came back, `not enough shards to reconstruct blob` when
  some did, fewer than that. A reading is judged from all of its rows,
  whatever phase each carries. The only other state is a reading that did
  not happen: the prober was down, restarting or late (`NOT_PROBED`, a
  reading missed in part and short of the rows included), or not a single
  request reached a server, because this observer's own network was down:
  no connection to any validator was opened and none was refused
  (`probe.Reached` holds for none; a validator with no host, a connect that
  timed out or found no route reached no one). Blobs settled before 27
  September 2026 that the load policy of the time did not draw have no
  reading either. Then the blob is `not_read`, or `pending` while its
  window is open, and no one is not served on it; rows that came back
  verified still count as served.
- **What a validator's row counts as** (`verdict.CountedClass`, one per
  endorsed (validator, blob)):
  - `served` — its rows came back verified at the reading;
  - `broken` (not served) — the blob was unavailable and its rows did not
    come back, whatever the client met: not found, a timeout, a rate limit,
    a server error, a certificate the client rejects, no registered host;
  - `not_counted` — anything else once the window has closed: a validator
    the reading did not need to ask, one that failed while the blob was
    available all the same, or a blob not read by Tensile;
  - `pending` — the window has not closed.

  Only `served` and `broken` enter the rate, `served / (served +
  broken)`. The rule reads stored rows as they are; nothing is rewritten.
  A blob of the earlier schedule was read at several points in the window,
  and each point is a reading of its own: a validator's row counts by the
  reading at its own point, while the blob's status shown is the reading at
  the newest point in the window every endorsing validator was reached at
  (or failing that, any validator).
- **The network's Available figure** (`reconstructable` on `/v1/network`)
  is `recoverable = yes / (yes + no)` over the newest publications with a
  reading on record, at most `sample_limit` of them
  (`publications_examined` says how many). Beside it: `pending`,
  `not_yet_read` (the window is open and the reading is still to come),
  `not_read` (the window closed without a reading that decides it) and
  `unknown` (no assignment). The batch decides each blob from two bounds on
  the distinct rows (the verified row counts less the assignment's overlaps,
  and their sum) and reads the row lists only where the bounds straddle the
  threshold.
- Below **20** rated observations the percentage is printed without a gauge
  and the validator is not ranked by it in either direction (it sorts with
  the rows that have no rate at all). This holds for every ranked figure on
  the validator table — serve rate, reachability and throughput.
- **Stale assignment pin.** The prober polls `abci_info` and stamps the
  chain's `app_version` on every row. When it is above the celestia-app
  major the assignment constants are pinned to
  (`fibre-assign.PinnedCelestiaAppMajor`), every probe that depends on
  assignment is `PROBE_ERROR` ("assignment pin stale"), never `HEALTHY` or
  `FAULT`; identity and registry verdicts, which do not depend on
  assignment, are still drawn. The row says `observer.pin_stale`, so the
  gap is attributable after the fact.
- **Evidence on the row.** Every measurement records, beside the verdict:
  `download.row_indices` (the indices returned, in returned order),
  `download.rows_sha256` (SHA-256 over the returned row payloads in that
  order), `download.rpc_code` (the gRPC status code of a failed download),
  `download.rpc` (the read method called: `DownloadShard`; once upstream
  ships `DownloadShardStream` the prober tries both and records which one
  answered, and `Unimplemented` on either is an observer error, never a
  verdict),
  `download.shadowed_by` (the promise whose assignment the returned rows
  match), `read` (the validator's place in the reading's order, its rows
  the reading had not had, the distinct rows held after it, and what the
  reading came to with the client's error), `retry` (the first attempt
  before the client's re-dial), `host_at_settlement` (the host registered when the promise
  settled, where the upload went, derived from the chain's own
  `set_fibre_provider_info` events as the scanner reads them in the same
  `block_results` pass as everything else, seeded once from the bonded
  registry at the scan's start, and read once per validator the bonded
  seed missed (jailed or unbonding then; `seed_lazy`, a single
  `FibreProviderInfo` query at the settlement height, in force from the
  seed height since every later change is an event on record) or, when
  that state is pruned, at the tip (`seed_current`, in force from the tip
  only), and read again after a scan gap whose blocks' events were not
  read (`reseed`: the registry at the height before the scan's position,
  in force from the gap's end, for each validator with no event of its own
  since; settlements inside or before the gap stay unknown). A gap where
  only a publication's validator set was pruned read the block's events
  and does not make hosts unknown; `host_at_settlement_source` says event,
  seed, seed_lazy, seed_current, reseed, none (the chain's explicit answer
  that nothing is registered, never inferred from absence), or unknown
  because a scan gap or a missing seed leaves the question open; `host_history.jsonl` is the record and the export carries
  it), on rows of the earlier schedule
  `settlement_host_probe` (what that host answered when the current one
  did not serve and differs from it), `observer.build` (the observer's VCS revision), and
  `observer.assign_pin` / `observer.app_version`. The store keeps them as
  columns (`row_indices`, `rows_sha256`, `rpc_code`, `shadowed_by`,
  `observer_build`, `app_version`) and `/v1/probes` publishes the first four
  (the row indices and their digest with `?rows=1`, being most of a row's
  bytes); the build and app version are on every row of the record and of
  the daily export. A
  classification is a function of the wire result and the code; with these
  fields both halves are on the row.
- **Reachability** (`reachability_window`) is heartbeats that completed TLS
  over heartbeats sent, per validator and network-wide. The numerator is
  `tcp_ok = 1 AND tls_ok = 1`; whether the certificate was the right one is
  the separate `identity_rate_window` ("Endorsed"). Heartbeats exist only
  while the validator is in `AllBondedFibreProviders`, so a jailed or
  unbonded validator's denominator stops growing and the table prints no
  percentage for it. The table's status word is liveness only: the chain's
  own `jailed` and `bond_status` first, then `host`, then the latest
  handshake; a fault never appears there, it has its own column. "Reachable
  now" (`reachable`) is the newest heartbeat or probe (any phase, assigned or
  not, gaps excluded) with TCP and TLS both successful. `last_reachable_at`
  and `last_unreachable_at` say how long the current state has held.
- **A closed endpoint row** (`closed_reason = left_bonded_provider_list`)
  no longer lends its host to the validator row. `host` is empty, and
  `last_host` / `endpoint_closed_at` say what was registered and when it left
  the list. The host used to be back-filled from the newest probe row, which
  made the word for a jailed validator depend on whether the prober had
  restarted since it left: "down" while the prober's in-memory last-known
  host kept being dialled, "no host" after a restart. The prober now
  replays the collector's `registry.jsonl` on every boot and tails it from
  then on, so the last host a validator registered survives a restart and
  a jailed validator's remaining obligations keep being probed at it
  (`host_source = last_known` on the row); `NOT_REGISTERED` is reserved for
  a validator this observer never saw register a host.
- **Throughput** (`serve_bytes_per_second`) is the median of
  `bytes_returned * 1000 / download_ms` over `HEALTHY` in-window probes that
  carry a byte count (`serve_throughput_sample`); records from before schema
  8 have no byte count and are outside the sample, never zero. It is over the
  download step alone because the dial, handshake and identity check cost
  the same for a 148-row shard as for a 4,096-row one, so a whole-probe
  figure rises with stake by construction; and it is bytes rather than rows
  because a row is as wide as its blob's square. `serve_latency_p50_ms` and
  `_p95_ms` remain the whole probe, dial to verified rows.
- **TLS identity status** = the latest identity result: verified; expired
  (`IDENTITY_FAIL` with a stale reason: the right key, a lapsed window);
  mismatch (any other `IDENTITY_FAIL`); unverified (TLS completed, no
  identity verdict recorded); no TLS (`TLS_HANDSHAKE_FAIL`); or unreachable.
  Observer-side `PROBE_ERROR` heartbeats are left out of every reachability
  figure, as their probe-side twins are.
- `PROBE_ERROR`, `NOT_PROBED` and `MISSED` are excluded from every rate and
  rendered as gaps. `UNATTESTED` is also excluded, but it is not a gap: the
  probe ran and its outcome is recorded. It is excluded because no obligation
  was proven, which is a different statement and is labelled differently.
- Records written before the observer verified signatures carry no
  attestation at all. Their `attested` column is NULL, not 0, and they are
  counted under the older taxonomy and reported separately as
  `attestation.unknown_blobs`. "Not recorded" is never rendered as "did not
  attest".
- Every measurement carries `clock_offset_ms`, the observer's clock minus the
  chain's latest block time when the probe ran. Phases are decided against the
  local clock and the phase boundaries are minutes wide, so a vantage
  whose offset is large can be discounted after the fact. The prober logs a
  warning past 30 seconds.

## What a fault is, and what it is not

The only thing this site says against a validator is that the chain
**proves** it stored rows of a blob, the blob could **not be reconstructed**
from what came back, and its rows did not come back. A row's class says what
happened on the wire; whether it counts is decided from the whole reading
(below). These are the classes that are never a fault on their own:

| the observer saw | class | why it is not a fault |
|---|---|---|
| no signature from this validator on the settled promise | `UNATTESTED` | nothing proves it was ever sent the shard |
| no answer from the endpoint at all | `UNREACHABLE` | from one vantage, indistinguishable from the observer's own path failing |
| no Fibre host in the registry | `NOT_REGISTERED` | jailing and unbonding remove the provider from the bonded list; the chain keeps the entry until the validator leaves staking state or has been jailed and unbonded a week past its unbonding time |
| exactly another settled promise's rows for the same blob | `SHADOWED_SHARD` | `DownloadShard` takes a commitment, not a promise hash, and the store serves the first shard by promise-hash order; the validator cannot tell them apart |
| genuine rows of the blob that match no settled promise | `UNMATCHED_GENUINE` | an upload whose promise never settled can answer under hash-order serving; held out, beside the rate |
| genuine rows matching no promise the observer has scanned | `PROBE_ERROR` at the probe, then the late verdict | the owning promise may settle after the probe; `download.shadow_gap` says so, and the collector judges the row once the scanner has read past probe time + `payment_promise_timeout`: `SHADOWED_SHARD` if a promise on record assigns the rows, `UNMATCHED_GENUINE` if none does, `PROBE_ERROR` for good if a scan gap covers the interval |
| genuine rows matching no settled promise, judged late | `UNMATCHED_GENUINE` | a shard uploaded for a promise that never settled is on disk until its prune and never on chain, and answers when its hash sorts first; a validator serving it is serving a genuine piece of the blob. Not a fault the evidence supports: held out of the rate, counted beside it, indices on the row |
| a lapsed but correctly signed certificate | `IDENTITY_EXPIRED` | a late renewal, not someone else answering |
| a certificate signed by the wrong consensus key | `IDENTITY_MISMATCH` | an unusable endpoint, which is a statement about the endpoint (its status says so), not about a shard |
| an application error instead of the shard | `SERVER_ERROR` | the server did not say it lacks the shard; from one probe a hiccup and a loss look the same |
| a rate limit instead of the shard | `THROTTLED` | the server declined this request; like every answer without rows it counts only on a blob that could not be reconstructed |
| an outcome the taxonomy does not recognise | `PROBE_ERROR` | "we have not taught the observer about this" is not evidence |
| a local socket error, a probe this observer cancelled, a verification that timed out | `PROBE_ERROR` | the packets never left this machine |
| a `CANCELLED` status the server sends while the request is still live | `SERVER_ERROR` | the client takes it as the server's error and skips the shard; like every answer without rows it counts only on a blob that could not be reconstructed |

### When a failure counts

A validator whose rows did not come back counts as **not served** only when
it endorsed the promise and the blob was unavailable: the reading asked the
whole set, as the client does, and the rows that came back could not
reconstruct it. Then a reader using celestia-app's client is left without
the blob, and any answer that left it without the validator's rows counts:
`FAULT` (not found, bad rows), `UNREACHABLE`, `IDENTITY_MISMATCH`,
`IDENTITY_EXPIRED`, `SERVER_ERROR`, `THROTTLED`, `NOT_REGISTERED`, and a
request of this observer's that failed while another reached a server.
On an available blob the same answers count neither way: the
blob was there for any reader, and a validator the reading did not need to
ask is no gap either. Rows that came back verified are served, whatever
their class (`SHADOWED_SHARD`, `UNMATCHED_GENUINE`). A validator that did
not endorse is never counted. On a reading that did not happen nothing
counts.
The row keeps its class; `verdict.CountedClass` and `rollup.CountedClass`
say what it counts as.

Readings stored by earlier builds are read by the same rule, with nothing
rewritten: a reading with a `NOT_PROBED` row whose rows are short was
missed in part, so it did not happen, and no one is not served on it (the
rows that did come back still count as served); one in which not a single
request reached a server did not happen either.

## Known limits of a probe

These are properties of how the observer measures, not of any validator. They
are written down because a reader comparing two validators deserves to know
what the measurement cannot separate.

- **A power cut can look exactly like an early prune.** The Fibre server
  writes the shard file first and then commits the metadata that makes it
  discoverable — the promise record, the `/shard/` marker and the prune index
  — in one pebble batch with `pebbledb.NoSync` (celestia-app
  `fibre/store.go`). NoSync hands the batch to the operating system without
  waiting for it to reach the disk, so a power loss between the file's rename
  and that flush leaves the shard on disk with no marker pointing at it.
  `Get(commitment)` iterates markers, finds none, and the server answers
  `NotFound` for data it still physically holds; upstream's own `Get` doc
  names "crash leftover or pebble.NoSync power loss" as an expected case and
  cleans up the opposite kind of orphan inline. From outside, that is
  indistinguishable from a server that pruned early: the same wire answer, the
  same row, the same `FAULT`. This observer records the fault, because the
  obligation was not met at that moment and that is all the class asserts —
  it is not evidence that an operator deleted anything. The shape to look for
  is faults clustering at one moment across many promises for one validator,
  which is a machine event rather than a retention policy; the rows carry the
  time and the promise hashes, so an operator can point at it on the dispute
  route and the amendment is on the record beside the original.
- **One address per probe.** Every resolved address is tried at the TCP layer
  and the first that connects is the endpoint every later layer talks to. If
  that address accepts TCP and then fails at the RPC layer, the probe does not
  fall back to the next one, so a host whose backends differ can be recorded
  as unreachable on the strength of one of them. The alternative, letting
  gRPC re-resolve as the reference client does, would let the download land on
  a different peer from the one whose certificate was checked, and the record
  could no longer say which endpoint it describes. The result is `UNREACHABLE`
  either way, which counts only on a blob that could not be reconstructed.
- **One connection per probe.** The TLS handshake, the identity check and
  the download share one TCP session: the identity check runs inside the
  handshake's `VerifyConnection` callback on the connection the download
  then uses, so the certificate a row records (`tls.peer_cert_sha256`) is
  the one that served the rows (`tls.shared_with_download`). A Fibre server
  admits a bounded number of connections, and this observer holds one slot,
  as the reference client does. Rows from builds before this one opened a
  second connection for the download and carry no `shared_with_download`;
  on those, a second backend behind the same host:port could answer the
  download with a different certificate from the one recorded.
- **Shadowing needs the other promise, and the other promise may come
  later.** `SHADOWED_SHARD` requires the observer to know the promise that
  answered. The Fibre store keeps every (commitment, promise) shard side
  by side and `Get(commitment)` returns the first readable one in
  promise-hash order (celestia-app `fibre/store.go`), so which promise
  answers a download is decided by hash order, not by time: a promise
  uploaded before the probe and settled after it, up to
  `payment_promise_timeout` after its creation, can own the rows the probe
  got back and is not in the feed yet. The candidate set is therefore
  never complete at the probe. The prober matches what it holds (every
  publication until its post-deadline probe, past the store's prune) and,
  when nothing matches, files `PROBE_ERROR` with `download.shadow_gap` =
  `shadow_pending: ...` naming the bound; when a scan gap overlaps
  `(probe time - (max(shard_retention, payment_promise_timeout) + prune
  tolerance), probe time]` it names the gap instead, which is permanent.
- **Deferred verdicts.** The collector draws the deferred verdict once the
  scanner's frontier (`state.json` `last_scanned_time`, the block time of
  `last_scanned_height`) has passed `started_at + payment_promise_timeout`:
  every promise that could have answered is on record by then. Candidates
  are the promises over the same commitment settled by that bound whose
  window was still open at the probe (`must_serve_until` plus the store's
  prune lag); if one assigns this validator exactly the returned indices
  the row becomes `SHADOWED_SHARD` with `shadowed_by`, otherwise
  `UNMATCHED_GENUINE`; a scan-gap row, or a candidate whose assignment
  rows were not recorded, stays `PROBE_ERROR` for good. The row
  keeps the verdict it was stamped with (`classification_at_probe`) beside
  the amended one and `amended_at`; every amendment is appended to
  `amendments.jsonl`, replayed on a rebuild and shipped in the daily
  export, and `sentinel-recompute` draws the same verdict from the record
  and compares. Every rate reads the amended classification; the rollup
  of a day waits until none of its rows is still deferred.
- **A validator that moves.** The obligation is served at the endpoint
  clients are sent to, which is the registry now, so the probe goes to
  the current host and the verdict is that host's. The scanner records
  the host registered at the settlement height on every assignment
  (`host_at_settlement`, read from the chain at that height; null when
  the node had pruned it, which is not "no host"), and the prober carries
  it on the row; rows of the earlier schedule also carry what the old host
  answered when the current one did not serve (`settlement_host_probe`). With no host in the live registry and none this observer ever
  saw (a fresh vantage), the settlement host is the last fallback
  (`host_source = settlement`).
- **Why unmatched genuine rows are not a fault.** A shard uploaded for a
  promise that never settles (abandoned before `MsgPayForFibre`) is on
  disk until its prune and never on chain, so it can never be a
  candidate, and under hash-order serving it answers whenever its hash
  sorts first. A validator returning it is returning a genuine piece of
  the blob it holds; nobody, the validator included, holds the abandoned
  upload's assignment to contest with. So the late verdict for genuine
  rows that no settled promise assigns is `UNMATCHED_GENUINE`: held out of
  the rate, counted beside it, indices on the row. `FAULT` is reserved for
  what hash order cannot excuse: no shard of the blob at all
  (`NOT_FOUND` in window) or bytes that do not verify (`INVALID_ROWS`,
  rows failing the commitment). A validator that wanted to hide behind
  this would have to hold another shard of the same blob to serve, which
  is genuine data of the blob; the reading already counts the rows it
  returned toward the blob.
- **A protocol finding.** `DownloadShard` is addressed by commitment
  alone, and the store answers with the first shard in promise-hash
  order, so with several promises over one blob no client, the reference
  client included, can ask for a particular promise's shard. A
  per-promise retention obligation is therefore not checkable inside the
  protocol, not only from this vantage. An optional `promise_hash` on
  `DownloadShardRequest` would make it so.
- **A lost reading is a gap, an amendment for a missing row waits.** A
  prober outage leaves no row for the readings it slept through; on restart
  it writes a `NOT_PROBED` row per endorsing validator for every reading it
  did not make, however old (`-backfill-missed` caps that only when an
  operator sets it), so the blob reads `not_read` rather than vanishing
  from the record. A late verdict in `amendments.jsonl`
  whose probe row has not been ingested yet is retried on the next passes
  before being stepped over, so a measurements file that is merely behind
  keeps its verdict.
- **One vantage.** Every reading comes from a single network path, as one
  client's download does. A second location runs the heartbeat, which
  says whether an endpoint this observer calls down answers from
  elsewhere; it adds no reading to any rate.

- **Retention and the rollup.** Raw probe and heartbeat rows are kept for
  90 days and their `raw_json` (the bulk of a row) for 30; every typed
  column stays, including the evidence columns. Fourteen days after a UTC
  day ends, while its rows are all still present, the collector computes
  the day's rollup with the same SQL the API runs live: the obligation
  buckets per validator for the promises settled that day, and the row
  counts (in-window classes, gaps, heartbeats) for the rows started that
  day. A day is
  pruned only after it is rolled, whole days at a time, oldest first, so
  the record is never thinner than the rollup behind it. From the first
  prune on, the "all" window is the rollup for every day before `raw_from`
  plus the raw record from `raw_from` on, and the answer carries
  `rolled_up` (`raw_from`, the days folded in, and which figures rest on
  the rollup); the 24h, 7d and 30d windows never touch it. The two parts
  partition cleanly because they cut along the same lines the rollup was
  computed on: obligations by the day their promise settled (raw counts
  those settled from `raw_from` on), rows by the day they started (raw
  counts those started from `raw_from` on). A promise settled late on a
  rolled day may still have rows that started on a retained day; those
  rows stay until their own day is pruned and count in the row figures,
  while the obligation they belong to is the rollup's alone. Figures the
  rollup does not hold, latency percentiles, attestation and throughput, cover the raw record only, and the label
  says so. A pinned `as_of` before `raw_from` takes whole rolled days up
  to its own. Fourteen days is a floor, not the rule: a day rolls only
  once every promise settled on it has left its window and no probe row
  of theirs is still deferred, so a chain whose retention outruns the
  flag holds the rollup rather than rolling a pending obligation. The JSONL record and the daily exports are untouched by any
  of this: pruning is the database's, never the record's.

## Reproducing the figures

Every snapshot response — `/v1/network`, `/v1/validators`, `/v1/market` and
`/v1/validators/<addr>` — carries `record_through`: the scanner's checkpoint
height and block time as they stood when the figures were computed, beside
the chain tip the collector had last seen (`height`, `block_time`,
`chain_height`, `chain_tip_time`).
`computed_at` says when the figures were taken; `record_through` says over
which part of the record, which is what a reader needs to check them against
the chain. The site tags each headline figure with the kind of evidence it
rests on: a chain record, a verified response, or an observation from
Tensile's own network (`EVIDENCE` in `web/src/components/Panel.tsx`).

Every figure on the site is a function of the record and the code, and the
pieces needed to re-run that function are published:

- **The record.** The JSONL files are the record; the daily export at
  `/v1/exports` is one tarball per UTC day holding every record file's
  lines for that day (`publications`, `payments`, `measurements`,
  `sampling_decisions`, `reachability`, `registry`, `runs`, `sampling-secrets`,
  `amendments`), with a `manifest.json` of line counts, byte ranges and SHA-256
  digests, and a `.sha256` sidecar for the tarball. A record is assigned to a
  day by its own timestamp; one that reached the file after its day's export
  was built is in the next export, counted as late. Every line of every file
  is in exactly one export.
- **The code's configuration.** Each component appends its starts and
  stops to `runs.jsonl` with its flags (`status.RunEvent`), which every
  daily export carries, so a row can be traced to the prune tolerance,
  schedule and timeouts that produced it, and to the build
  (`observer.build` on the row itself since schema 9).
- **A pinned window.** `?as_of=<RFC 3339>` on `/v1/network` and
  `/v1/validators` answers what the observer would have published at that
  moment from the rows it had by then: rows started after `as_of` are
  left out, an obligation whose deadline is after it is pending. What the
  chain says now (jailed, bonded, the current registry) is not rewound,
  and the answer says so (`as_of_note`). Pinned answers bypass the
  snapshot cache and are rationed (a burst of four, then one every two
  seconds; `429` with `Retry-After` past that).
- **The headline without us.** The people who run this observer run a
  validator on the network it measures. Nothing about that row is filtered,
  excluded or adjusted — it is produced by the same code from the same
  record as every other — but a reader should not have to take that on
  trust, so `?exclude=<address>` on `/v1/network` recomputes the summary
  without named validators. It takes either address form, repeated or
  comma-separated, up to eight, and every per-validator population moves
  with it: the class counts, the obligations, attestation, latency, both
  reachability figures, the probe and gap counts, the previous window the
  deltas compare against, and
  the rolled-up days behind the "all" window. The answer echoes what it
  excluded (`excluded`, `exclude_note`), bypasses the snapshot cache and is
  rationed like a pinned window, so one reader's filter can never become
  everyone's headline.

  One figure stays whole, and the note says so. Reconstructability asks
  whether a blob could still be rebuilt from the rows that came back, and
  removing a validator's rows lowers that for real — "the network without
  you" is not the network's actual recoverability, and printing it as such
  would understate the thing the figure exists to measure.
- **The earlier sampling draw.** Until 27 September 2026 not every
  publication was probed: when the projected load exceeded a budget, a
  publication was admitted if `H(promise_hash || secret_of_its_day) <
  p · 2^64`, and a sampled-out one was recorded once
  (`sampling_decisions.jsonl`), standing for a `NOT_PROBED` row per
  assigned validator per point. Nothing is sampled now. The draws already
  made stay checkable: every row of that time carries `sampling.p`,
  `sampling.binding` and `sampling.day_commitment`, the prober still
  publishes each day's secret seven days after the day ends
  (`sampling-secrets.jsonl`, served beside its commitment at
  `/v1/sampling`), and `sentinel-recompute -sampling` recomputes the draws
  and exits 1 on a mismatch.
- **The tool.** `sentinel-recompute -data-dir <record or untarred export>`
  re-derives every row's phase and classification from the row's own
  fields and the run's recorded tolerance (`Measurement.Recompute`), each
  blob's result and the obligation buckets for a window
  (`observer/verdict`, a second implementation of the rules the API
  evaluates in SQL, kept apart from it; the API's tests run both over the
  same rows), and with `-api <base>` compares them to
  `/v1/validators?window=&as_of=`; `-sampling` checks the draws of every
  revealed day. Exit status 1 when anything differs.
- **What the row is not trusted for.** `Measurement.Recompute` re-derives
  the phase and re-runs the taxonomy, but two of the inputs it hands
  `Classify` are the prober's own conclusions: `assigned` and `attested`,
  the pair that turns an in-window NOT_FOUND into a FAULT rather than an
  UNATTESTED. Replaying those would check the taxonomy against itself, so
  the tool checks them against `publications.jsonl` instead — the assignment
  this observer computed at scan time from the validator set at the promise
  height, and the signature verification recorded beside it — and reports any
  drift under its own `assign|` line. The chain stores no assignment table:
  `x/fibre` neither computes nor keeps one, and every party derives the same
  table independently from the validator set (upstream `fibre/validator/set.go`).
  So this check catches a drift between the scanner and the prober; it is
  not a check against an on-chain answer, because there is no on-chain
  answer to check against. A row claiming an assignment for a promise the
  record does not carry is counted there too: its verdict rests on a claim
  nothing in the export can check.
- **What an export has to contain.** Every tarball carries `state.json`
  beside the day's lines, with its own digest in the manifest. It is not a
  record file but a snapshot, and the tool needs all four things in it: the
  param history, the scan gaps, the host seed and the scan frontier. Without
  the gaps a late shadow verdict the record deliberately defers is redrawn
  as a decided one, and the tool reports a difference where the observer was
  honestly blind. Feed it the union of the exports covering the window you
  are checking (each member appended to its path), or the live record
  directory.

What a difference means. A row whose recomputed class differs from its
stored one was classified by a different build: the class is stamped at
probe time and the taxonomy has changed since (the sample record in
`observer/testdata`, from before `UNREACHABLE` was held out, shows exactly
this). The row's evidence is unchanged; the site publishes the stored
class, and the export lets a reader apply today's rules to yesterday's
rows. A NOT_FOUND graded at the phase boundary can differ by the
microseconds between the RPC's return and the row's finish time. An
obligation figure that differs from the API's with the same rows and the
same `as_of` is a bug in one of the two implementations, and is why there
are two.

## Provisional faults

A `FAULT` younger than **30 minutes** (`verdict.FaultSettling`, measured from
the probe's `started_at`) is *provisional*: `provisional: true` on the probe
row, `provisional_faults` (`obligations`, `until`, `settling_seconds`) on the
network row, on every validator row and on each span of the validator page,
and "Not served · provisional" on the site. A broken obligation is provisional
when **every** FAULT behind it is that young; one settled fault makes it final.
After `until` the label is gone, whatever snapshot is being served.

**Why 30 minutes.** The period covers the evidence that can withdraw a
FAULT without a human, and nothing else:

| path | how long after the fault | what it does |
|---|---|---|
| silent params change | the scanner re-reads `x/fibre` params every 60 blocks (~6 min) | the range withholds its publications' rows (`RETENTION_UNVERIFIED`) in the transaction that records it |
| ingest | collector tail every 10 s, scanner catch-up in minutes | the row, or the evidence against it, reaches the store |

Thirty minutes was twice the longest of these under the earlier schedule
(12 min), rounded up; reading each blob once, its rows written together
when it ends, only shortened them. A dispute has no time bound, so it does
not set the period; an amendment is on the record whenever it lands.

**Why provisional faults are in the headline.** Three options:

- *Hold provisional faults out of the rate.* Rejected. It withholds the
  accusation and not the credit, which this document refuses for
  `RETENTION_UNVERIFIED` and `UNATTESTED` for the same reason: it
  raises every rate it touches. A validator failing now would read cleaner
  for half an hour than one that failed yesterday.
- *Hold every obligation decided in the last 30 minutes, served and broken
  alike.* Symmetric, but it is only a longer `pending`: every figure is half
  an hour older, and the thing a reader should know — this fault is fresh
  and can still move — is not shown at all.
- *Count it and flag it.* Chosen. A not-served count rests on the whole
  reading: the blob could not be rebuilt from what came back, and the
  shard did not come back at that minute (see "When a failure counts").
  The automatic withdrawal paths above
  already act on the store and move the snapshot revision, so a withdrawn
  fault leaves the headline by itself. The flag tells a reader which part
  of the figure can still move and until when.

Nothing about it changes a count, which is why it did not move
`MethodologyVersion`.

## Network reference on the validator page

The validator page's service rate is printed beside the network's over the
same window from the same vantage (`network_reference` on
`/v1/validators/{addr}`): `median_rate`, the median of the validators' own
obligation rates over those with at least 20 decided obligations (each
validator once, so one large validator cannot move it), and `pooled_rate`,
every obligation together. It is read from the same validators snapshot
`/v1/validators` serves, so the page and the table agree; a pinned
(`as_of`) window omits it rather than computing every validator's row per
page view.

Beside them, `in_retention_window` is the validator's endorsed shards whose
retention window has not ended at the answer's moment, from the chain's
record alone: a blob is read 10 minutes before its window ends, so these are
mostly not read yet, and none counts either way. (`obligations.pending` is
narrower: the obligations already read whose window is still open.)

## Disputing a verdict

A FAULT is a statement about a named operator, and an observer that makes
those without a correction path is asking to be trusted rather than checked.
There is one, and it does not depend on this project's goodwill.

**Check it yourself first.** Every FAULT row carries what produced it: the
promise hash, the reading's time, the phase, the wire outcome, the row
indices returned, a digest of the returned bytes, the gRPC status code, the
observer build and the chain's app version at the time. `/v1/probes?blob=…&rows=1` and
the day's export tarball both give the row in full, and `sentinel-recompute`
re-derives the verdict from it. The three most common reasons a verdict is
wrong are all visible in the row: the deadline was computed from params the
server did not have (`must_serve_until_ambiguous`), the observer's path was
the problem rather than the endpoint (the blob's other rows show whether
the reading reached anyone), or the shard was served from a different
promise (`shadowed_by`).

**Then say so, in public, on the record.** Open an issue on this repository
with the promise hash and the reading's time. The record is append-only, so a
verdict is never rewritten in place: a correction is an *amendment*, which
carries its own timestamp, the classification at probe time and the reason —
the same mechanism the deferred shadow verdicts use, visible on the row as
`classification_at_probe` and `amended_at`. Anyone reading the row later sees
both what was said and what it became.

**What this observer will not do.** It will not remove a row because an
operator asked. It will not amend a verdict without saying what changed and
why. And it will not claim that this path makes the figures correct: it makes
them *contestable*, which is the most a single-vantage observer can honestly
offer.

## Adding a class

A new classification must map to a new case in the taxonomy table in
`fibre-sentinel/internal/probe/probe_test.go`, and if it depends on what rows
came back, to a case in `fibre-assign/assign_test.go`. The dashboard must not
introduce verdicts of its own.
