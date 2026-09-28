# Verdict taxonomy

Every row the observer stores and every number the dashboard shows is built
from two fields that `fibre-sentinel` already records per probe: the
mechanism-level **outcome** (what happened on the wire) and the
**classification** (what that means given the phase of the retention window
and whether the validator was assigned the shard). The dashboard adds no new
verdict classes. It only groups and counts the existing ones.

Source of truth:

- outcomes and classifications: `fibre-sentinel/internal/probe/classify.go`
  (`Classify`), tested by the taxonomy table in
  `fibre-sentinel/internal/probe/probe_test.go`
- served / wrong rows / partial: `fibre-assign/verify.go` (`ShardMap.Verify`),
  tested in `fibre-assign/assign_test.go` (translated from celestia-app's
  `fibre/validator/set_test.go`)
- phases: `fibre-sentinel/internal/probe/schedule.go` (`PhaseAtWindow`)

## Phases

| phase | span | what a reader should conclude |
|---|---|---|
| `in_window` | settlement to `must_serve_until` | the validator is under its retention obligation |
| `grace` | `must_serve_until` to `must_serve_until + prune tolerance` (default 2m30s) | an honest server prunes late by anything from a second to just under two minutes (a one-minute prune loop over a minute-granularity key); nothing here counts against a validator |
| `post` | after the grace span | the obligation is over; the blob is expected to be gone |

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

The correlated-failure guard is *not* the protection here and never was: it
needs three faulting validators and half the point, so two affected
validators, or a share under the threshold, walks straight through it. The
guard is for a correlated outage; this is for the observer being wrong about
the deadline, which is a different fact and needs a different mechanism.

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
`attestation.coverage` (the proven share of assigned probes),
`serve_rate_held_out.UNATTESTED` (the rows this removes) and
`attestation.blob_coverage` (the same per blob). A reader who wants the
pessimistic bound can read the unproven count beside the rate; this observer
will not turn it into a number of its own.

That is why an assigned but unattested probe becomes `UNATTESTED` whatever
the wire outcome was, and sits outside the serve rate in both directions: a
failure the validator was never proven to owe cannot count against it, and a
success it was never proven to owe cannot count for it. The outcome field
still records exactly what happened.

Source: `fibre-sentinel/internal/scan/attest.go`, tested in
`attest_test.go`; the classification in `internal/probe/classify.go`, tested
by `TestClassify_UnattestedIsNeverAFault`.

## Outcomes (one per probe, mechanism level)

| outcome | meaning |
|---|---|
| `SERVED_OK` | shard returned, every row verifies against the commitment and the returned indices equal the assigned set |
| `SERVER_ERROR` | the endpoint was reached, completed TLS, proved its identity and answered the RPC with an application error (gRPC `Internal`, `Unknown`, `DataLoss`, `Aborted`). Deliberately not a reachability failure: calling it "unreachable" would be false about a server the observer just talked to. Deliberately not a fault either: the server did not say it lacks the shard |
| `RPC_THROTTLED` | the endpoint was reached, proved its identity and refused the request with `ResourceExhausted` that is neither the observer's own receive bound (`received message larger than max`, a `PROBE_ERROR`; the bound is sized from the blob and recorded as `download.recv_limit`) nor the server's own send bound (`trying to send message larger than max`, a `SERVER_ERROR`: the validator's gRPC refuses to deliver a shard it holds): a server-side limit. Celestia has said a per-peer rate limiter is coming to the Fibre server. Never a fault, and not the observer's error either; the probe policy counts it as a failure for backoff |
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
| `RPC_DEADLINE` | the download did not finish within the observer's own deadline (base plus shard size at 1 MiB/s); the observer gave up, the validator was not judged |
| `RPC_ERROR` | any other gRPC error |
| `NO_REGISTERED_HOST` | the validator has no fibre host in `x/valaddr`, so nobody can fetch its rows; treated as unreachable |
| `PROBE_ERROR` (no coder) | the observer could not build the verifier for this blob's `(original_rows, total_rows)`, so no download was attempted |
| `REACHABLE` | TCP, TLS and identity passed and the download was deliberately skipped (the reachability heartbeat); no retention verdict |
| `PROBE_ERROR` | the observer's own probe failed (bug or config), not the target |
| `MISSED` | the scheduled point elapsed before the prober ran it |

## Classifications (the verdict)

One sentence each, and what a reader should conclude.

| classification | when | conclude |
|---|---|---|
| `HEALTHY` | assigned validator returned `SERVED_OK` in window or in grace | the validator kept its promise at this point in time |
| `FAULT` | an identity-verified endpoint, for a shard the chain proves it stored, **said it has no such shard** (`NOT_FOUND` in window) or **returned bytes that do not verify against the commitment** (`INVALID_ROWS`, any phase). A third arm, rows outside this promise's assignment that verify against nothing (`WRONG_ROWS`/`PARTIAL` without `commitment_verified`, in window or grace), exists in the code as a guard but cannot be produced by the prober, which files rows that fail the commitment as `INVALID_ROWS` | the validator did not serve a shard the chain records it as obliged to hold, at a moment inside that obligation. That is what the row says, and it is the most it says: `x/fibre` calls this parameter a *minimum local retention* — upstream's own words are "the minimum local duration validators keep uploaded shards" (`x/fibre/types/params.go`) and "the on-chain local retention floor for uploaded shards" (`celestia/fibre/v1/query.proto`) — and the chain neither checks it nor penalises missing it. "Broke its promise" is a heavier sentence than the protocol supports; this row is the observation, not a verdict about intent. It is still the only class that counts against a validator here. Two conditions, each reproducible by anyone who repeats the probe. A response the observer cannot parse at all (an empty shard, an RLC vector whose length does not match the observer's own protocol params) is the observer's gap (`PROBE_ERROR`, `shard shape:` on the row), never `INVALID_ROWS`: the commitment check is the only thing that turns bytes into a fault. One margin: a `NOT_FOUND` whose answer arrives within 30 s of `must_serve_until` is graded as grace (`TOLERATED`) and the row says `phase_note: not_found_at_deadline`, because the server prunes on a minute tick against its own clock and the RPC reaches it tens of seconds after the probe's phase was fixed. The fault count beside a validator's name on the overview and on its own page is `obligations.broken`: one per shard signed for and not handed over, the same population the serve rate is drawn from. The probe-level count — every `FAULT` row of an assigned shard, in any phase, which is several rows per shard and also covers faults past the deadline, where there is no obligation to break — is `faults` in the API, and on the site it is on the tooltip and the detail line rather than in the number. The serve rate's population is in-window only |
| `UNREACHABLE` | assigned and attested, in window, and the observer could not complete a conversation at all: `DNS_FAIL`, `TCP_REFUSED`, `TCP_TIMEOUT`, `TCP_UNREACHABLE`, `TLS_HANDSHAKE_FAIL`, `RPC_UNAVAILABLE`, `RPC_ERROR` | we could not get to it. From one vantage that is not distinguishable from a route, firewall or peering problem on the observer's own path, so it is published in full beside the serve rate and kept out of it |
| `NOT_REGISTERED` | assigned validator with no Fibre host in `x/valaddr` at the time of the probe (`NO_REGISTERED_HOST`) | a registry state, not a refusal. Jailing and unbonding remove a provider from `AllBondedFibreProviders` while the chain keeps the entry: it is garbage-collected only once the validator is gone from staking state, or jailed and unbonded for longer than the unbonding time plus seven days |
| `SHADOWED_SHARD` | assigned validator returned rows that **verify against the blob commitment**, are not this promise's assignment (`WRONG_ROWS` or `PARTIAL` with `commitment_verified`), and are **exactly the row set another settled promise over the same commitment assigns to this validator** (`shadowed_by` names it) | that promise answered in this one's place. `DownloadShard` is addressed by the commitment alone; the Fibre store keeps every promise's shard side by side (`Put` "stored independently without deduplication") and `Get(commitment)` returns the first readable one in promise-hash order, so the validator has no way to tell the two apart. Never a fault. Without a matching promise the same wire result is `UNMATCHED_GENUINE`, and that verdict is drawn late (see "Deferred verdicts"): the order is by hash, not by time, so a promise settled after the probe can be the one that answered |
| `UNMATCHED_GENUINE` | assigned validator returned rows that verify against the blob commitment but match no settled promise's assignment for it, judged once every promise that could own them is on record | a shard uploaded for a promise that never settled is on disk until its prune and never on chain, and answers whenever its hash sorts first; the validator is serving genuine data of the blob. Held out of the rate, counted beside it, indices on the row. Never a fault |
| `IDENTITY_EXPIRED` | certificate endorsed by the right consensus key, but its signed validity window has lapsed or has not started | a renewal running late. Endpoint hygiene, not impersonation and not a retention failure |
| `IDENTITY_MISMATCH` | certificate not endorsed by this validator's consensus key, any validator, any phase (judged before attestation: a certificate is a property of the endpoint) | no client will download from this endpoint, so it is as unusable as one that does not answer. Shown as the endpoint's status and in the endorsement rate, held out of the serve rate: a wrong certificate proves nothing about any shard |
| `SERVER_ERROR` | assigned and attested, in window, and the endpoint answered with an application error instead of the shard (`SERVER_ERROR` outcome) | the server was reached and did not say it lacks the shard. From one probe this is not distinguishable from a transient fault (an overloaded process, a disk hiccup), so it is shown beside the rate and never inside it; a server that errors at every point is visible as such on its own page. In grace it is `TOLERATED`, after the window `UNREACHABLE_POST_WINDOW` |
| `THROTTLED` | assigned and attested, in window, and the endpoint refused the download with a rate limit (`RPC_THROTTLED` outcome) | the server was reached and declined to serve this request. That says nothing about the shard, so it is shown beside the rate and never inside it; the prober backs off from a validator that says so, and a limit set tight enough to turn away real clients is visible as such on the validator's own page. In grace it is `TOLERATED`, after the window `UNREACHABLE_POST_WINDOW` |
| `TOLERATED` | assigned validator, grace phase: `NOT_FOUND` or unreachable | honest pruning lag; do not read anything into it |
| `EXPECTED_GONE` | assigned validator, post phase: `NOT_FOUND` | correct behaviour after the window |
| `SERVED_PAST_WINDOW` | assigned validator, post phase: still serving (`SERVED_OK`, `PARTIAL`, or `WRONG_ROWS`) | not a fault; the validator keeps data longer than it must. `WRONG_ROWS` is here rather than under FAULT because `DownloadShard` performs no assignment check at all — assignment is enforced only at upload — so rows outside an assignment, after the obligation ended, are not a rule the validator broke |
| `UNREACHABLE_POST_WINDOW` | assigned validator, post phase: unreachable | not a retention fault; the obligation was over. It still feeds the reachability view |
| `EXPECTED_UNASSIGNED` | validator not assigned this shard answered `NOT_FOUND` or was unreachable | normal; only probed when `-probe-unassigned` is on |
| `SERVING_UNASSIGNED` | validator not assigned this shard returned data for it (`SERVED_OK`, `PARTIAL`, `WRONG_ROWS` or `INVALID_ROWS`) | unexpected; either the observer's assignment is wrong or the validator over-serves. Shown for review, never as a fault |
| `UNATTESTED` | assigned validator, any phase, where no verified signature from that validator appears on the settled promise and the probe reached the question of the shard at all (an observer-side outcome, a stale assignment pin, a missing registry entry or an unusable certificate is named first, as `PROBE_ERROR`, `NOT_REGISTERED` or `IDENTITY_MISMATCH`, none of which enters a rate) | nothing on chain proves this validator ever stored the shard, so no verdict is owed either way. Outside every rate, in both directions |
| `PROBE_ERROR` | the observer could not carry out the probe, or gave up on it (`PROBE_ERROR`, `RPC_DEADLINE`) | an observer problem, shown as a gap |
| `NOT_PROBED` | the slot elapsed unprobed (observer down or late), or the download was skipped by policy (`MISSED`, `REACHABLE`) | a gap in observation, never a zero |
| `RETENTION_UNVERIFIED` | the publication's upload interval overlaps a range of heights over which an `x/fibre` params change landed with no event and this observer has not read the params at every height (see "Phases"). Applied over the stored row rather than returned by `Classify`: the measurement record says what happened on the wire and is append-only, while whether this observer trusts its own deadline is a judgement that has to be revisable | this observer cannot say when the obligation ended, so it publishes no serve verdict — **neither the fault nor the credit**. Withholding only the accusations would raise every rate it touched, which is the same argument this document makes for `UNATTESTED` and for grace probes. Replaces exactly `HEALTHY` and `FAULT`; `INVALID_ROWS` is carved out, because bytes that fail the commitment are a fault in every phase and no deadline rescues them. Never a statement about the validator |

## How the dashboard derives its numbers

- **The rate's population** is probes of an assigned shard while the validator
  was *under obligation*: `assigned = 1 AND phase = 'in_window'`. The grace
  phase is deliberately outside it. A grace probe can only ever add `HEALTHY`,
  because `NOT_FOUND` and unreachability there are `TOLERATED` by design, so
  counting grace gave a validator that prunes promptly a **lower** rate than
  one that over-retains with identical in-window behaviour — the opposite of
  what the number claims to measure, on exactly the axis the "worst first"
  table sorts by.
- **Serve rate per probe** over that population = `HEALTHY / (HEALTHY + FAULT)`
  (`serve_rate`). Every other class is published beside it under its own
  name, never folded in, and the API carries both the counts
  (`serve_rate_held_out`) and the reason each class is out
  (`serve_rate_excluded_classes`) so the dashboard cannot describe the
  exclusions differently from the API. It is published, not headlined: see
  the next two entries.
- **Verdict coverage** (`serve_rate_coverage`) = `(HEALTHY + FAULT)` over every
  probe in that population. A high rate over low coverage is a statement about
  a handful of probes, and without this figure a reader cannot tell the two
  apart. The `probe_count` and `probe_gaps` fields are over *all* probes, a
  different population; they are not this.
- **Obligations** (`obligations`, whose `rate` is repeated as
  `serve_rate_by_obligation`) count one observation per (validator, blob)
  the settled promise proves (`attested = 1`), and this is the
  headline on every page. The schedule visits the same validator and blob
  four times in window, and the minimum-rows floor assigns every bonded
  validator every blob, so the probes inside one obligation are
  near-perfectly correlated — one lapsed certificate produces four `FAULT`
  rows for one event. Any confidence interval is drawn around the obligation
  count, never around the probe count, and the dashboard states it as an
  **upper bound on the fault rate**, because that is the direction an
  accusation is made in.
- **Which obligations.** `attested = 1` only: an obligation the settled
  promise proves. `attested = 0` is nothing to keep or break; `attested`
  NULL (a record from before signatures were verified) is not evidence
  either way and is outside the count, reported as
  `attestation.unknown_probes`. An obligation belongs to a window by its
  publication's `settlement_time`, not by each probe's `started_at`, so it
  is judged whole or not at all. The window's end is the moment the verdict
  is drawn; an obligation whose `must_serve_until` is later is `pending` and
  in no rate.
- **The last in-window reading is held to an absolute margin.** The four
  in-window points are fractions of the publication's own window (0.12, 0.45,
  0.72, 0.92), and the last one is additionally moved forward to sit no more
  than 2m30s before `must_serve_until`. A fraction alone does not survive a
  change of scale: 0.92 of a ten-minute devnet window is 48 seconds out, and
  0.92 of mocha's four-hour `shard_retention` is nineteen minutes out. Since
  NOT_FOUND at the grace point is TOLERATED by construction, nothing after
  that last reading can produce a verdict, so a validator that pruned inside
  those nineteen minutes served every probe it was given and bucketed as
  `served` — the one failure this observer exists to catch, passed silently.
  The margin never moves a point **earlier** than its fraction already puts
  it, so a short window keeps its tighter reading.
  The margin is on the *scheduled* time, and the prober may run a slot late —
  up to a twentieth of the publication's own window, twelve minutes on mocha's
  four hours — with the phase taken from the actual start. So an in-window
  slot is additionally never run past its own deadline: its allowance is
  whatever is left before `must_serve_until`, and a slot that cannot run in
  time is recorded `NOT_PROBED`. An obligation nobody observed is unobserved,
  which is published as such; it is not graded in a phase where a missing
  shard is tolerated by construction, and it is not `served`. It cannot usefully be smaller than
  the chain's own prune granularity: the Fibre server's prune loop runs once a
  minute against a minute-resolution key, so a reading closer than that would
  be accusing an operator of the clock.
- **An obligation is served only if this observer saw the shard near the end
  of the window the promise covers.** The newest in-window probe decides the
  verdict, the same rule the per-blob reconstructability verdict uses, and a
  `NOT_PROBED` or `PROBE_ERROR` row is never the newest while a real probe
  exists — but a newest probe taken at 12% of the window is not evidence
  about the other 88%, so it is not enough on its own. The buckets:
  - `served` — newest probe `HEALTHY`, no `FAULT` anywhere, **and** one of
    the `HEALTHY` readings taken in the last quarter of the retention
    window. The quarter is measured from the promise's own `settlement_time`
    and `must_serve_until`, both in every export, so anyone can redraw the
    line from the rows; the probe schedule puts its last in-window point at
    92% of the window, so a validator that answers it clears the cut with
    room to spare;
  - `broken` — any probe `FAULT`;
  - `end_unobserved` — a `HEALTHY` probe, but none that speaks for the end
    of the window: either the newest probe produced no verdict, or every
    reading was taken too early to say the shard survived;
  - `unobserved` — no `HEALTHY` and no `FAULT` at all, split by what the
    probes did see: `unobserved_reachable` (at least one download attempt
    with `tls_ok = 1`: the endpoint completed a handshake and answered with
    `SERVER_ERROR`, `THROTTLED`, an `RPC_*` failure or an unusable
    certificate), `unobserved_unreachable` (attempts, none of which
    completed TLS), `unobserved_not_probed` (no attempt: a load
    cap, a slot that elapsed).

  Only `served` and `broken` enter the rate. The first rule, "kept when no
  probe of it faulted", let a validator that served at the first point and
  answered 500 at the next three count as fully kept — the profile of a
  server that pruned early, which is the finding this observer exists to
  make. That obligation is `end_unobserved`, and a validator that never
  hands anything over is `unobserved_reachable`: not a
  fault, but not a clean record either, and counted on its own line beside
  the rate. The split uses each probe row's own `tls_ok`, not the heartbeat,
  because the probe made its own handshake at the moment that matters.

  The end-of-window requirement closes the same hole from the other side,
  and it is this observer's own failure mode rather than a validator's. When
  this site is down inside a retention window, the obligation keeps whatever
  verdict it had before the outage and the remaining slots are recorded as
  gaps — or, once the outage outruns the prober's backfill horizon, are never
  written at all. Under the earlier rule an obligation seen `HEALTHY` once
  and then never again published as **kept**, so the serve rate *rose* while
  nobody was watching, and an operator was credited for hours this observer
  did not see. Those obligations are now `end_unobserved`: they leave the
  rate rather than pad it.

  `served` and `broken` are deliberately not symmetric. A `FAULT` is
  conclusive from a single reading — the shard was gone at that minute. A
  serve is a claim about a whole window, so it needs a reading near the end
  of one. The consequence is worth stating rather than burying: while this
  observer is blind, the obligations it can still judge are enriched for
  faults, because a fault takes less evidence than a serve does. What it
  cannot do is invent one. Nothing in this rule can move an obligation into
  `broken`; the worst this observer's own downtime can do to an operator is
  decline to vouch for them, and the count of times that happened is printed
  beside the rate.
- Below **20** rated observations the percentage is printed without a gauge
  and the validator is not ranked by it in either direction (it sorts with
  the rows that have no rate at all). This holds for every ranked figure on
  the validator table — serve rate, reachability and throughput — not only
  for the serve rate: a validator that registered its endpoint an hour ago
  has a handful of handshakes, and the reachability column sorts worst
  first. A single unlucky probe used to
  render as "0.0%" beside a named validator and sort it above one with a
  hundred real faults.
- **Suspect points** (`vantage_health.suspect`). At any in-window schedule
  point where at least `min_validators` (3) and at least `threshold` (50%)
  of the distinct validators probed were `UNREACHABLE`, or at least
  `fault_threshold` (50%) and three were `FAULT`, every probe row at that
  `scheduled_at` is left out of the per-probe rate, the coverage and
  held-out counts, the obligation buckets, the per-point breakdown, the
  attestation counts and the fault count, network-wide and per validator
  alike. The attestation counts are in that list so a reader can reconcile
  the response against itself: `serve_rate_coverage.den` equals
  `attested_probes + unattested_probes + unknown_probes`, and
  `serve_rate_held_out.UNATTESTED` equals `attestation.unattested_probes`. That validators fail
  independently while one observer's network does not is the working
  assumption behind this guard, not a measurement; on a day when many
  endpoints really are down at once it errs toward publishing nothing.
  Both reasons are shown with their share and a link to the rows
  (`/v1/probes?at=<scheduled_at>`), and neither is given a cause. An
  `unreachable` point cannot be told, from one location, from a network
  problem on the observer's own side. A `fault` point has the observer
  errors this site can check for ruled out — a stale assignment pin is
  caught separately (every probe under a stale pin is `PROBE_ERROR` before
  it can be a `FAULT`), and a params-uncertainty range on record withholds
  its rows before they can be faults — and the ones it cannot check for
  are not ruled out, which is why the point is not called an incident. "Probed" is a
  row that carries a reachability verdict for the endpoint, which is the
  only kind of row that could land in either numerator. A row that could
  not have been `UNREACHABLE` or `FAULT` whatever happened at that point is
  not in the share's denominator either, or it would drag the share down by
  its mere presence: a validator the load cap turned away or a slot that
  elapsed (`NOT_PROBED`, `PROBE_ERROR`), one with no reachable Fibre host so
  that no connection was attempted (`NOT_REGISTERED`), and one whose
  assignment carries no signature on the settled promise (`UNATTESTED`),
  which the taxonomy decides before it looks at reachability at all. That
  last one is not a rare case: a publisher stops collecting at two thirds of
  stake, so a third of the assigned rows at a typical point are
  `UNATTESTED`, and leaving them in the denominator held the guard below its
  threshold through outages it exists to catch. Everything kept in the
  denominator means a connection was attempted and the endpoint answered or
  refused — an identity failure, a throttle or a server error is positive
  evidence that the network was up. So a burst among the validators that
  actually answered is caught however many did not. The points, the shares and the number of rows
  removed (gaps included) are published so the exclusion is visible, and
  the rows keep their classification in the store: a verifier sees what
  was excluded and why. The points are judged over every vantage together
  and, for a day's row figures in the rollup, over the rows started that
  day: with more than one vantage an outage on one marks the point for
  all, and a point whose rows straddle midnight can be suspect on one day
  and not the other. Both only ever remove rows from a rate, never add a
  verdict.
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
  match), `host_at_settlement` (the host registered when the promise
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
  it) with
  `settlement_host_probe` (what that host answered when the current one
  did not serve and differs from it), `observer.build` (the observer's VCS revision), and
  `observer.assign_pin` / `observer.app_version`. The store keeps them as
  columns (`row_indices`, `rows_sha256`, `rpc_code`, `shadowed_by`,
  `observer_build`, `app_version`) and `/v1/probes` publishes them. A
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
- **Reconstructable** for a blob is judged at the latest **complete**
  in-window probe point: the newest point at which every assigned validator
  has a real result (a verdict, not a gap). Grace and post points are never
  used, because "not found" is tolerated or expected there. The distinct row
  indices held by validators whose probe at that point was `SERVED_OK` are
  compared with `OriginalRows` (4096 for blob v0). Reconstructable means at
  least that many with every assigned validator serving; degraded means fewer
  than the full assignment answered but still at least `OriginalRows`; not
  reconstructable means fewer than `OriginalRows` distinct rows were observed
  served. "Every assigned validator" means every **attested** assigned
  validator: a validator with no proof of storage cannot demote a blob by
  staying quiet. A row that was served counts toward reconstruction whether
  or not the server's storage was proven, because the rows came back either
  way; attestation decides blame, never availability. While no in-window point is complete (a sweep still running, or
  validators skipped by the policy) the status is `pending` and the blob is
  left out of the network rate: an absent row is a gap, never a zero. The
  threshold is the row count from `fibre-assign`'s pinned protocol params,
  not a hard-coded fraction of validators. Rows from several vantages count a
  validator once.
- **Reconstructable** is published as four numbers, not one: `yes` (every
  validator proven to owe the blob served), `degraded` (the rows were all
  there but someone stayed quiet), `no`, and separately `pending`/`unknown`.
  "Degraded" is never folded into the numerator, because "the blob can be
  rebuilt" and "everyone kept their promise" are different statements. The
  response also carries `publications_in_window`, `publications_examined` and
  `sample_limit`, so a rate over the newest 2000 of 50000 cannot be read as a
  rate over the window.
- `PROBE_ERROR`, `NOT_PROBED` and `MISSED` are excluded from every rate and
  rendered as gaps. `UNATTESTED` is also excluded, but it is not a gap: the
  probe ran and its outcome is recorded. It is excluded because no obligation
  was proven, which is a different statement and is labelled differently.
- Records written before the observer verified signatures carry no
  attestation at all. Their `attested` column is NULL, not 0, and they are
  counted under the older taxonomy and reported separately as
  `attestation.unknown_probes`. "Not recorded" is never rendered as "did not
  attest".
- Every measurement carries `clock_offset_ms`, the observer's clock minus the
  chain's latest block time when the probe ran. Phases are decided against the
  local clock and the grace span is only a few minutes wide, so a vantage
  whose offset is large can be discounted after the fact. The prober logs a
  warning past 30 seconds.

## What a fault is, and what it is not

The only thing this site says against a validator is that it was **reached**
and failed to hand over a shard the chain **proves** it stored. Everything
that falls short of both halves of that sentence has its own class and its own
column:

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
| a rate limit instead of the shard | `THROTTLED` | the server declined this request; it said nothing about the shard |
| an outcome the taxonomy does not recognise | `PROBE_ERROR` | "we have not taught the observer about this" is not evidence |
| a local socket error, a cancelled probe, a verification that timed out | `PROBE_ERROR` | the packets never left this machine |
| a fault the second location re-fetched within 20 minutes, rows verified | `PROBE_ERROR`, with `cleared_by` | the shard was there; what failed was this observer's reading of it (see "Faults re-checked from a second location") |

This is why the serve rate moved after the audit. It did not get more
forgiving; it stopped making claims the evidence did not support.

### At the end reading

From 2026-09-27 16:20 UTC an endorsed shard is read once, 10 minutes before
`must_serve_until`, and its obligation counts as a reader using
celestia-app's client meets it: no rows back is not served (`UNREACHABLE`,
`IDENTITY_MISMATCH`, `IDENTITY_EXPIRED`, `SERVER_ERROR`, `THROTTLED` and
`NOT_REGISTERED` count as `FAULT`), and genuine rows of the blob are served
(`SHADOWED_SHARD`, `UNMATCHED_GENUINE`). The row keeps its class
(`probe.EndReadClass`, `rollup.ObligationClass`); the table above holds for
the earlier schedule's points.

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
  either way, which is outside the serve rate.
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
  the node had pruned it, which is not "no host"), the prober carries it
  on the row, and when the current host differs and did not serve, the
  prober asks the old host as evidence in the same slot
  (`settlement_host_probe`): a `FAULT` whose old host still serves the
  exact rows is data left behind on a move, not data lost, and the row
  says so. With no host in the live registry and none this observer ever
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
  is genuine data of the blob; the reconstructable verdict already counts
  the rows it returned.
- **A protocol finding.** `DownloadShard` is addressed by commitment
  alone, and the store answers with the first shard in promise-hash
  order, so with several promises over one blob no client, the reference
  client included, can ask for a particular promise's shard. A
  per-promise retention obligation is therefore not checkable inside the
  protocol, not only from this vantage. An optional `promise_hash` on
  `DownloadShardRequest` would make it so.
- **A lost probe row is a gap, an amendment for a missing row waits.** A
  prober outage leaves no row for the slots it slept through; on restart
  it writes a `NOT_PROBED` row for every elapsed slot of every publication
  still on record, however old (`-backfill-missed` caps that only when an
  operator sets it), so the obligation is counted as `unobserved_not_probed`
  rather than missing from the total. A late verdict in `amendments.jsonl`
  whose probe row has not been ingested yet is retried on the next passes
  before being stepped over, so a measurements file that is merely behind
  keeps its verdict.
- **One vantage.** Every reachability observation comes from a single network
  path. `/v1/network` publishes the worst schedule point in the window by how
  many validators were unreachable at once, because validators fail
  independently and one network does not. A second location re-checks
  endpoint failures and retention faults, never routine probes, and adds no
  reading to any rate (see "Faults re-checked from a second location").

- **Retention and the rollup.** Raw probe and heartbeat rows are kept for
  90 days and their `raw_json` (the bulk of a row) for 30; every typed
  column stays, including the evidence columns. Fourteen days after a UTC
  day ends, while its rows are all still present, the collector computes
  the day's rollup with the same SQL the API runs live: the obligation
  buckets per validator for the promises settled that day, and the row
  counts (in-window classes, faults, gaps, heartbeats) for the rows
  started that day, suspect points left out as they are live. A day is
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
  rollup does not hold, latency percentiles, the by-point breakdown,
  attestation and throughput, cover the raw record only, and the label
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
the chain tip the collector had last seen (`last_scanned_height`,
`last_scanned_time`, `chain_height`, `chain_tip_time` in `meta`).
`computed_at` says when the figures were taken; `record_through` says over
which part of the record, which is what a reader needs to check them against
the chain. `/v1/meta` also publishes `evidence`, the kind of evidence each
headline figure rests on — `chain_record`, `verified_response` or
`vantage_observation`, defined under `evidence_kinds` — and the site prints
the same three as tags beside the figures.

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
  stops to `runs.jsonl` with its flags (`status.RunEvent`); the collector
  replays them into `/v1/runs`, so a row can be traced to the prune
  tolerance, schedule and timeouts that produced it, and to the build
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
  with it: the class counts, the faults, the obligations, attestation
  coverage, latency, both reachability figures, the probe and gap counts,
  the per-point rate, the previous window the deltas compare against, and
  the rolled-up days behind the "all" window. The answer echoes what it
  excluded (`excluded`, `exclude_note`), bypasses the snapshot cache and is
  rationed like a pinned window, so one reader's filter can never become
  everyone's headline.

  Two figures stay whole, and the note says so. The correlated-failure
  guard is a statement about this observer's own minute rather than about
  any validator, so dropping one from the share would change which points
  this observer distrusts itself at. Reconstructability asks whether a blob
  could still be rebuilt from the rows that came back, and removing a
  validator's rows lowers that for real — "the network without you" is not
  the network's actual recoverability, and printing it as such would
  understate the thing the figure exists to measure.
- **The sampling draw.** Not every publication is probed. A vantage has a
  finite budget — bytes per hour and per day, globally and per validator —
  and when the projected load exceeds it the observer probes a random
  subset rather than probing everything badly or probing the cheap
  publications preferentially. Which subset is decided by a draw a reader
  can check afterwards, because a sample nobody can verify is indistinguishable
  from a sample chosen to flatter.

  Each publication is admitted if `H(promise_hash || secret_of_its_day) <
  p · 2^64`, where `p` is the admission probability at the moment it was
  first seen. The decision is sticky for the life of the publication's
  schedule: a publication is probed at every point or at none, never at some,
  because a partial schedule would pull the serve rate toward whichever points
  happened to run. A sampled-out publication is recorded once, with its
  probability (`sampling_decisions.jsonl`), and counts as a NOT_PROBED row
  for every assigned validator at every point, as before.

  Every probe row carries the three things needed to check it — `sampling.p`
  (that publication's own probability, not the process's current one),
  `sampling.binding` (which cap set it) and `sampling.day_commitment`
  (SHA-256 of the day's secret). Seven days after a UTC day ends the prober
  publishes that day's secret (`sampling-secrets.jsonl`, served beside its
  commitment at `/v1/sampling`), and from then on the inequality above can be
  recomputed by anyone, for every promise settled that day, against the rows
  in the export. `sentinel-recompute -sampling` does exactly that and exits 1
  on a mismatch. The seven days clear any retention window this observer
  schedules, so the draw stays unpredictable while it matters, and the master
  secret the day secrets derive from is never published and never leaves the
  vantage.

  What this cannot prove: that the publication *list* was complete. The draw
  is verifiable over the publications in the record; a publication the scanner
  never saw is in neither. That is what the scan gaps are for, and they are
  published beside it.
- **The tool.** `sentinel-recompute -data-dir <record or untarred export>`
  re-derives every row's phase and classification from the row's own
  fields and the run's recorded tolerance (`Measurement.Recompute`), the
  correlated-failure guard and the obligation buckets for a window
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
  are checking, or the live record directory.

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

## Faults re-checked from a second location

A `FAULT` is the one class held against a validator, and it rests on one
reading from one place. So every `FAULT` is asked once more from the second
vantage before it counts, and only a `FAULT`: routine probes are never
repeated, so the second vantage adds one request per fault and nothing else.

- The prober appends a confirmation request for every row it classifies
  `FAULT` to `<data-dir>/vantage-requests.jsonl`: the promise, the blob's
  commitment and code parameters, the validator and the host that failed,
  the rows it owes, the schedule point, and a deadline (the fault's start
  plus `ConfirmWindow`, **20 minutes**, or the end of the grace phase if that
  is sooner). The pull timer copies new lines to the second vantage.
- The second vantage (`sentinel-probe -confirm-requests`) fetches exactly
  those rows from that validator once, with the same probe: DNS, TCP, TLS,
  the consensus-key identity check, `DownloadShard`, and row verification
  against the commitment and the assignment. It keeps none of the
  observer's state: the consensus key comes from the validator set at the
  promise height on its own RPC, and the assignment is recomputed from it
  with `fibre-assign`; a request the chain does not bear out is refused
  (`PROBE_ERROR`, no connection made), and an RPC that does not answer is
  asked again until the deadline. It writes the answer to its own
  `measurements.jsonl` under its own vantage name and the fault's schedule
  point, and the observer copies that file back into
  `<data-dir>/vantages/<name>/measurements.jsonl`.
- The collector ingests those rows into `probe_confirmations`, never into
  `probes`, and applies the rule (`verdict.ConfirmFault`), with the window
  measured between the two probes' own `started_at`:

| the second vantage, started within 20 minutes of the fault | the fault | on the row |
|---|---|---|
| got the exact rows back, verified (`HEALTHY`) | **withdrawn**: filed `PROBE_ERROR`, an observer-side failure, outside the rate and never counted as served | `cleared_by`, `classification_at_probe: FAULT`, an amendment in `amendments.jsonl` with `cleared_by` and `confirm_key` |
| reached a verdict that is not `HEALTHY` (not found, unreachable, an error, rows that do not verify) | stands | `confirmed_by` |
| could not run the probe (`PROBE_ERROR`, `NOT_PROBED`), started later, or sent nothing | stands, as it did before there was a second vantage | nothing |

**Why a cleared fault is not served.** Every rate is this observer's own
readings. A second vantage's row adds no obligation, no reading and no
credit anywhere; it can only take back an accusation this observer made.
Crediting the second location's fetch as served would let the observer's
own blind spots be filled in from elsewhere, which is a different rate.
So a cleared fault leaves `broken` and lands where the obligation's other
readings put it (usually `end_unobserved`, when the cleared probe was the
last point).

**Why only `HEALTHY` clears.** It is the one answer that proves the rows were
there: the exact assigned set, verified against the commitment, from the
endpoint that proved the validator's consensus key. A shard that answers with
genuine rows of another promise, or an endpoint the second location cannot
reach either, says nothing in the validator's favour.

**The window.** Twenty minutes, so an answer that takes a few minutes to come
back (the vantage's poll, the pull timer each way, the collector's pass)
still lands inside the 30-minute settling period: a fault that is cleared is
withdrawn while it is still labelled provisional, and a settled fault stays
settled. The second vantage caps itself at 60 confirming probes an hour
(`-confirm-max-per-hour`); a request that waits past its deadline lapses and
its fault stands.

The rule is one implementation (`observer/verdict/confirm.go`), used by the
collector over the store and by `sentinel-recompute` over the record: with
`vantages/<name>/measurements.jsonl` beside the record it redraws every
clearing and compares it with `amendments.jsonl`; without them it applies the
recorded clearings and counts them as unchecked.

The API carries `cleared_by` / `confirmed_by` on probe rows and
`faults_cleared` on each validator row (over the raw rows; a rolled day keeps
its counts, not who cleared what).

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
| correlated-failure guard | the rest of the schedule point's rows can land up to `MaxLatenessFraction` (5%) of the window after it: 12 min on mocha's 4 h `shard_retention` | a point where half the set, and at least three, faulted or were unreachable leaves every count |
| silent params change | the scanner re-reads `x/fibre` params every 60 blocks (~6 min) | the range withholds its publications' rows (`RETENTION_UNVERIFIED`) in the transaction that records it |
| ingest | collector tail every 10 s, scanner catch-up in minutes | the row, or the evidence against it, reaches the store |
| second-vantage re-check | the confirming probe must start within 20 min; its answer is back a few minutes later | verified rows from the second location withdraw the fault (see "Faults re-checked from a second location") |

Twice the longest of the first three (12 min), rounded up to the half hour;
the re-check's window was then chosen to fit inside it. Deferred shadow
verdicts (`amendments.jsonl`) never touch a `FAULT` — they re-judge
`PROBE_ERROR` rows — and a dispute has no time bound, so neither sets the
period; an amendment is on the record whenever it lands. The one amendment
that does touch a `FAULT` is a second-vantage clearing, which is bounded by
its window.

**Why provisional faults are in the headline.** Three options:

- *Hold provisional faults out of the rate.* Rejected. It withholds the
  accusation and not the credit, which this document refuses for
  `RETENTION_UNVERIFIED`, `UNATTESTED` and grace for the same reason: it
  raises every rate it touches. A validator failing now would read cleaner
  for half an hour than one that failed yesterday.
- *Hold every obligation decided in the last 30 minutes, served and broken
  alike.* Symmetric, but it is only a longer `pending`: every figure is half
  an hour older, and the thing a reader should know — this fault is fresh
  and can still move — is not shown at all.
- *Count it and flag it.* Chosen. A FAULT is conclusive from one reading
  (the shard was not there at that minute; see "`served` and `broken` are
  deliberately not symmetric"), and the automatic withdrawal paths above
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

## Disputing a verdict

A FAULT is a statement about a named operator, and an observer that makes
those without a correction path is asking to be trusted rather than checked.
There is one, and it does not depend on this project's goodwill.

**Check it yourself first.** Every FAULT row carries what produced it: the
promise hash, the schedule point, the phase, the wire outcome, the row
indices returned, a digest of the returned bytes, the gRPC status code, the
observer build and the chain's app version at the time. `/v1/probes?blob=` and
the day's export tarball both give the row in full, and `sentinel-recompute`
re-derives the verdict from it. The three most common reasons a verdict is
wrong are all visible in the row: the deadline was computed from params the
server did not have (`must_serve_until_ambiguous`), the observer's path was
the problem rather than the endpoint (the point is in
`vantage_health.suspect`), or the shard was served from a different promise
(`shadowed_by`).

**Then say so, in public, on the record.** Open an issue on this repository
with the promise hash and the schedule point. The record is append-only, so a
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
