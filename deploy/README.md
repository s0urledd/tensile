# Deploying the observer

One VM runs everything. Five processes, one SQLite file, Caddy in front.

```
sentinel-scan ──► publications.jsonl, payments.jsonl, state.json ─┐
sentinel-probe ─► measurements.jsonl ─────────────────────────────┼─► observer-collector ─► observer.db ─► observer-api ─► Caddy ─► web/out
observer-heartbeat ─► reachability.jsonl ─────────────────────────┘                         (also polls x/valaddr and escrow balances)
```

The JSONL files are the append-only raw record; the SQLite database is
derived from them and can be rebuilt by deleting it and restarting the
collector. The collector's own `registry.jsonl` carries the endpoint
history (which validator registered which host, when), the one derived
table that has no other source; the prober reads it too, so a validator
that left the bonded list keeps being probed at its last registered host
across prober restarts. Back up the JSONL files and the database
(see "Backups"). Every process keeps a status file under `status/`, and
`/v1/health` turns them into one answer (see "Health and alerting").

One host can run one instance per network: the systemd units are
templates that take the network name as their instance (`fibre-scan@mocha`,
`fibre-api@mainnet`), each with its own env file, data directory and API
port, behind one Caddy with a site per network (see "Two networks").

## 1. Prerequisites

- A Linux VM with Go 1.23+ (the celestia-app pin needs 1.26.6; `GOTOOLCHAIN=auto` downloads it), Node 22, Caddy 2.
- `rclone` 1.60 or later, `python3` and util-linux's `flock`: the nightly
  backup, its manifest and its remote proof of the exports, and the second
  vantage's pull use them.
- A CometBFT RPC endpoint for the chain you observe. Use your own full node
  (default pruning is fine; no archive node is needed) with
  `storage.discard_abci_responses = false` in `config.toml`: the scanner
  reads `block_results` for every block, and a node that discards ABCI
  responses answers "node is not persisting finalize block responses"
  (rpc-mocha.pops.one did on 8 September 2026). Public RPCs on
  celestia-core v0.41.0 also cap heavy requests at 20 in flight. The
  scanner also subscribes to the node's new block headers on the same
  address's websocket (`/websocket`, which CometBFT serves on the RPC
  port), so it reads each block the moment the node has it. A proxy in
  front of the node has to pass websocket upgrades for that; without them
  the scanner follows by polling the tip every second, as before
  (`-subscribe=false` turns the subscription off).
- Before the chain runs app version 10, `x/fibre` and `x/valaddr` do not
  exist. The scanner logs "x/fibre is not active on this chain yet" and
  keeps following blocks, retrying the params query every 100 heights;
  the collector and heartbeat log the missing registry and carry on. This
  is the expected state on mocha-5 until the v10 upgrade.
- Outbound TCP to validators' Fibre ports (default 7980). Nothing more is
  needed to observe them: `DownloadShard` performs no caller authorization
  (celestia-app `fibre/server_download.go`), and the Fibre server's TLS config
  sets no `ClientAuth`, so it never asks for a client certificate
  (`fibre/server.go`). This observer is an ordinary client and can watch every
  validator that registers a Fibre endpoint, not only its operator's own.
- The prober and the heartbeat dial public addresses only. A registered
  host that is, or resolves only to, an address no client on the internet
  could reach is recorded as `UNROUTABLE_HOST` (the validator's
  `NOT_REGISTERED`) with no connection made: loopback, the private and
  link-local ranges, and the special-purpose ranges, among them
  `100.64.0.0/10` (carrier-grade NAT, and the addresses an overlay such as
  Tailscale hands out), `0.0.0.0/8`, `192.0.0.0/24`, `198.18.0.0/15`,
  `240.0.0.0/4`, the documentation ranges, `fec0::/10` and the NAT64 prefix
  `64:ff9b::/96` when the IPv4 address in it is one of these. An observer
  host on such an overlay therefore cannot be pointed into it from the
  chain.
- If the same host also runs your own validator and Fibre server: celestia-app
  main (#7848, 15 Sep 2026) recommends separate disks for Fibre shards and
  the node's data; the observer's data directory should not share the Fibre
  shard disk either.

## 1a. Describe the vantage

Every reachability verdict on the site is a statement about a network path,
and half that path is yours. The env file has two settings to describe it,
`VANTAGE_LOCATION` and `VANTAGE_PROVIDER`, and the API logs a warning at
startup if a public vantage leaves them blank. The API does not publish
them: `/v1/meta` lists the vantages by name only (`vantages`), and what a
reader is told about where the observer measures from is the methodology
page's text. The observer's own addresses and autonomous system are not
published either.

## 2. Build

```bash
git clone https://github.com/s0urledd/tensile && cd tensile
make build            # fibre-sentinel/bin/* and web/out/
```

`make build` stamps the commit into every binary (`REVISION`, defaulting to
`git rev-parse` with `-dirty` for a modified tree). Every measurement and
status file carries it; a binary built without it says `unknown`, and then
no one can say which code produced a verdict. Building from a tarball, pass
it: `make build REVISION=<commit>`.

## 3. Configure

Everything is per network. The examples below set up `mocha`; repeat with
`mainnet` (and its own RPC, data directory and API port) for the second.

```bash
sudo useradd --system --home /var/lib/fibre-observer --create-home fibre-observer   # the service user
sudo install -d -m 0755 /etc/fibre-observer
sudo install -m 0640 -o root -g fibre-observer deploy/observer.env.example /etc/fibre-observer/mocha.env
sudo cp deploy/publishers.yaml.example /etc/fibre-observer/publishers-mocha.yaml                  # publisher labels; optional, the API runs without it
```

Edit `mocha.env` (`sudo -e /etc/fibre-observer/mocha.env`, which keeps
its mode): set `NETWORK`, `RPC`, `VANTAGE`, `DATA_DIR`
(`/var/lib/fibre-observer/mocha`), `API_LISTEN` (a port of its
own), and, when you have them, `ALERT_WEBHOOK` or `TELEGRAM_BOT_TOKEN`
and `TELEGRAM_CHAT_ID`, and `BACKUP_REMOTE`. Leave `POLICY` empty.

The env file holds the alert token, the webhook and the backup remote,
so no other account on the host may read it: `0640 root:fibre-observer`.
A plain `cp` under root's umask leaves it `0644`, readable by every
account on a host it may share with a validator. systemd reads
`EnvironmentFile=` as root, the commands below that source it run under
`sudo`, and `deploy/test/exposure.sh` fails a file other accounts can
read. `rclone.conf` (section 7) is installed the same way.

The prober reads every blob; nothing is sampled or budgeted. `POLICY` is
left over from the earlier sampling, and the prober's unit no longer
passes it (`-policy`): the prober reveals no day secret. With a policy
and the master key it names, the prober reveals a secret for every past
day, draw or not, into the record for good; a new network never sampled
and never gets either. Mocha's draws (before 27 September 2026) are
revealed in `sampling-secrets.jsonl`, which stays in the record and the
exports; how its host drops the key is in "Stored data the reading no
longer needs".

`START_HEIGHT` is where a fresh scan starts (0, the default, is the tip).
The node must hold the blocks a scan reads: the state before the start and
the validator sets a promise window (1000 blocks) below it. A
`START_HEIGHT` closer than that to the node's oldest block is refused at
start, with the lowest height the node can serve. A scan from the tip on a
node that does not hold a promise window of blocks yet, one state-synced
a moment ago, waits until it does. The `scanner` health check fails
meanwhile, and its journal says which height the tip has to reach.

`host_at_settlement` on every assignment comes from the chain's
`set_fibre_provider_info` events, read in the same `block_results` pass
as the promises, seeded once from the bonded registry when the scan
starts and, per validator the bonded seed missed, by one
`FibreProviderInfo` query the first time it appears in an assignment
(`host_history.jsonl`). No state query at past heights is made, so
the node's state pruning does not matter to the observer; what must be
available is `block` and `block_results` over the scanner's lag behind
the tip, which the scan needs anyway (a block the node cannot serve is a
recorded gap, and a registration inside a gap makes the hosts of later
settlements unknown until the gap is re-scanned).

The prober asks a validator only for a blob it endorsed, in window, for its
own rows, as the client asks for a shard, and asks it again, up to twice,
only when its answer did not serve. The
read-path rate limiting Celestia is designing (forum
topic 2295) treats requests for shards a validator was never assigned as
illegitimate; reading only real, in-window, assigned commitments keeps the
observer's traffic on the right side of it.

**When the host is the validator's fault** (the methodology's own rule
is that this observer's trouble never counts against a validator). Under
the client's rules a host's addresses are tried as the client tries them,
the next one 250 ms after the one before unless that one has connected or
failed, IPv4 and IPv6 taking turns, so one dead address in a validator's
DNS is no longer a counted "not served". A DNS failure other than "no such
host", and an ICMP network or host unreachable, count against the validator only
when this observer's own resolver and network are shown working in the
same minutes; otherwise they stay this observer's gap, as before, and the
row says which way it went. The change moved the methodology version
(`methodology_version` on `/v1/meta`), so it applies to readings made from
the upgrade on, never to past ones, and the API's snapshot and day-partial
files are computed again: warm them before switching (`observer-api
-warm-only`, "Upgrading a running observer").

Text a row keeps from the wire or the resolver (a server's gRPC error
message, the addresses a name resolved to) is cut at 4 KiB, at a
character boundary, and ends with how long it was (`… (N bytes in all)`),
in `raw_error` and in each step's error and detail; the headers a server
may send on the download connection are bounded at 64 KiB, and its answer
by what that validator's shard of the blob can weigh. No validator can
fill the disk the record shares with the node, write a line too long for
the prober's restart to read back, or make the prober hold more than its
byte budget says.

**What the prober sustains.** On 28 September mocha settled about 20 blobs
a minute (1,200 an hour from 14:00 to 20:00 UTC, 22 in the busiest minute),
nearly all of 16 MiB. A reading asks every validator that endorsed the blob
for its own rows, so it moves the endorsers' share of the blob's encoded
rows, which for blob version 0 are four times the blob's size: two thirds
or more of them, by stake. For a 16 MiB blob that is about 43 to 64 MiB
of rows, and at 20 blobs a minute roughly 120 to 180 Mbit/s: arithmetic,
not a measurement, and about three times what the earlier reading moved
when it stopped at enough rows (12 to 20 validators and about 19 MiB a
blob, about 50 Mbit/s). Every endorser is asked for every blob it
endorsed; the reading's own requests have no limit per validator, as the
client has none, and at most one later attempt is in flight to a
validator. A scheduler run on the observer (82 shared fake validators with
mocha's row shape scaled to 1/16, 1.0 to 1.8 s per shard, production
timeouts, loopback) read 60 of 60 blobs at 20 a minute (reading p50 1.9 s,
max 3.0 s, no start lag) and 180 of 180 at 60 a minute (p50 2.5 s, max
4.3 s, no start lag) under the earlier reading, which stopped at enough
rows, and the earlier one-request-per-validator pacing, which only slowed
it; at 120 a minute every blob was still read but the start lag grew to
30 s in two minutes. The limits it keeps, 16 blobs and 256 requests at
once, 512 MiB of shards in flight and a reading rate of 400 Mbit/s
(`-blob-concurrency`, `-concurrency`, `-in-flight-mib`, `-max-read-mbps`),
only delay a request: its 15 s start once it is let go, and it carries the
phase its reading started in, so a request held back past
`must_serve_until` counts as the client, which asks at once, would have
made it. A request of a full reading, or a later attempt it owes, that
then cannot start a minute before `must_serve_until`
(`-request-start-margin`) is not made and is this observer's gap
(`NOT_PROBED`). An attempt is owed unless the validator's own time leaves
no room for it: this observer's own delays (a late reading, a wait for
room, a lane, a restart) never cost a validator one. The prober's status `reads`
block counts them (`requests_not_started_last_hour`, the `retries_*`
counts, per validator) beside the admission wait (`admit_wait_p95_ms`).
Measure the observer's link and set `-link-mbps` from it (in `PROBE_ARGS`,
section 4) before blobs grow toward 128 MiB: it holds the shard bytes in flight to what the link moves
in half a request's time, so a timeout is never the observer's own full
link, and every row records the load it was let go under
(`observer_load`).

**The reading-rate ceiling.** A full reading of a large blob can fill the
observer's port. Unpaced, a 128 MiB blob's full reading let its 62 to 67
requests go at once and pulled 376 to 392 MB in about 3 s, nearly a
1 Gbit/s port's line rate. `-max-read-mbps` (default 400; 0 turns it off;
another value goes in `PROBE_ARGS`) paces what is let go. Each request,
the reading's and every later attempt's, is charged its whole expected
shard against a token bucket of that rate. The bucket holds a quarter of a
second of the rate, 12.5 MB at the default, so no second lets go more than
62.5 MB, half of a 1 Gbit/s port. A shard larger than that (mocha's largest
validator holds 1,463 of 16,384 rows, 48.7 MB of a 128 MiB blob) waits
until the bucket has refilled what it lacks, under a second. At 400 Mbit/s
a 16 MiB blob's reading is let go over about 0.85 s and a 128 MiB blob's
over about 7.5 s; today's load of 16 MiB blobs, about a third of the rate,
is spread out but not held back. The client's re-dial after a first try
that had its session is charged again; one after a failed dial moved no
shard and is not.

The ceiling's wait is like the other limits'. It only delays, it is never
part of the request's 15 s, and it is recorded on the row
(`observer_load.rate_wait_ms`) and in the status file (`rate_wait_p95_ms`).
A full reading's request or later attempt whose turn would come after its
start cutoff (`must_serve_until` less `-request-start-margin`) is not made:
`NOT_PROBED`, this observer's gap, never the validator's. A request waiting
for the ceiling keeps its request slot and its share of the byte budget, so
the ceiling's own wait is at most the budget over the rate, about 11 s at
the defaults. `-link-mbps` bounds the bytes in flight at once and the
ceiling the rate they are let go at; with both set, keep the ceiling below
the link.

A validator that times out holds a request for 30 s at the reading (the
request and the client's re-dial), and 15 s at a later attempt, which is
one request. The other readings go on beside it, as other clients'
would, so an unavailable blob is still read at 20 a minute beside a
validator that hangs (`TestAnUnavailableBlobIsReadWhileAValidatorTimesOut`).
Each blob being read also holds its verifier and the first shard it
verified, up to about 11 MiB, beside the `-in-flight-mib` budget; a later
attempt's own verifier, about 4 MiB at K = 4096, is charged to it.

## 4. systemd

With the service user from section 3:

```bash
sudo install -d -o fibre-observer -m 0750 /var/lib/fibre-observer/mocha
sudo install -m 0755 fibre-sentinel/bin/* /usr/local/bin/
sudo install -m 0755 deploy/vantage-pull.sh /usr/local/bin/fibre-vantage-pull  # when the script changed
sudo install -m 0755 deploy/healthwatch.sh /usr/local/bin/fibre-healthwatch
sudo install -m 0755 deploy/backup.sh /usr/local/bin/fibre-backup
sudo install -m 0755 deploy/backup-manifest.py /usr/local/bin/fibre-backup-manifest
sudo cp deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fibre-scan@mocha fibre-probe@mocha fibre-heartbeat@mocha fibre-collector@mocha fibre-api@mocha
sudo systemctl enable --now fibre-healthwatch@mocha.timer fibre-backup@mocha.timer fibre-archive@mocha.timer
```

The units are templates: the part after `@` is the network, and it selects
`/etc/fibre-observer/<network>.env`, `/var/lib/fibre-observer/<network>`
and `publishers-<network>.yaml`. They are hardened (`ProtectSystem=strict`,
`ProtectHome`, `PrivateTmp`, an empty `CapabilityBoundingSet`,
`SystemCallFilter=@system-service`) and can write only their own data
directory.

Each unit runs one binary with the flags from its env file. The prober
also takes any extra flags from `PROBE_ARGS` in it (for example
`PROBE_ARGS=-max-read-mbps 600 -link-mbps 900`); a change there takes a
`systemctl restart fibre-probe@mocha`. Order does not
matter: the collector tolerates missing files, the API waits for the
collector to create the database, and before Fibre is active on the chain
the prober and heartbeat have nothing to do and say so in their logs.

None of the four processes exits on an RPC outage. The scanner retries a
transient failure for as long as it takes, with a warning every five minutes,
and picks up where it was; the prober waits for the chain at startup instead
of failing; the collector and heartbeat log the failure and try again next
round. A height the node cannot serve at all (pruned, or ABCI responses
discarded) is retried for ten minutes and then recorded as a gap in
`state.json`, shown on the dashboard and reported by `/v1/health`, and the
scan moves on. That includes `block_results` answering "could not find
results for height" more than a block below the tip: the node never kept
those results. At the tip the same answer is the second between the node
storing a block and committing it; it is retried quietly and logged only
if it lasts past the third attempt. A chain halt is warned about every
five minutes and waited out; `Restart=always` in the units is for
crashes, not for outages. The
scanner's block subscription is no exception: when it drops (a node
restart) or the node cancels it, the scanner polls the tip every second and
subscribes again with backoff, and the journal says `block subscription
lost` and `block subscription up again`, one line each. The collector's
watch on the scanner's files works the same way: if it cannot start or is
lost, the collector reads them every second on its timer and says so once.

`sudo systemctl status 'fibre-*@mocha'` and `journalctl -u fibre-probe@mocha -f`.

### Health and alerting

Every process rewrites `<DATA_DIR>/status/<component>.json` on each unit of
work and at least every fifteen seconds: whether the last cycle succeeded,
the last error and when, its progress height, how often its loop completes a
cycle (`cadence_s`) and the free space of the data disk. `/v1/health` reads
those files straight from disk, adds what it reads from the store and from
the API itself, and answers 200 when every check passes and 503 with the
failing checks otherwise.

| check | fails when |
|---|---|
| `scanner`, `prober`, `heartbeat`, `collector` | the process is missing or dead; it keeps failing (`alive but failing: <stage>`); or its loop has completed no cycle for three of its cadences, and at least three minutes (`no completed cycle for <d>`). The last one is a stuck loop: its status file keeps being refreshed, so nothing else shows it. A scanner waiting at the tip of a chain that makes no block is not stuck; `chain_liveness` says why. The prober also fails when its readings failed in the last 15 minutes and none was made. |
| `work` | one of the collector's stages has failed for over ten minutes: `state`, `checkpoint`, `late verdicts`, `corrections`, `holds`, `retention`, `registry`, `export`, `heartbeat`, `hosting` or `avatars` (the store's side of the pictures: listing the identities due, storing one; a Keybase lookup that fails is not a stage's failure). The chain poll's success no longer hides it. |
| `ingest` | lines have waited in a record file for ten minutes with the collector's cursor unmoved (`collector has ingested nothing for <d>`): its loop is stuck, and the site's figures stop moving. Where the API cannot read the record files, it fails once no cursor has moved for 15 minutes. |
| `chain_polls` | a chain-side poll has not succeeded in time: the chain status and the endpoints for 15 minutes, the escrow for 15 minutes once Fibre is active, the validator identities for 26 hours. A poll never recorded fails once the collector has run longer than that. The last value stays served meanwhile, so this is the only sign. |
| `scanner_lag` | the scanner is more than 200 blocks behind the chain. |
| `chain_liveness` | the newest block the observer knows of, from the collector's poll or the scanner's own reading, is over ten minutes old: the chain or the node is halted. A stuck collector alone no longer reads as a halted chain. |
| `vantages` | another vantage seen in the last seven days has sent no endpoint check for 20 minutes: its heartbeat died or the pull fails. |
| `api_errors` | a route answered a 5xx, or its handler panicked, in the last ten minutes. A 503 is not counted: the API answers it on purpose, while a window is still being computed (which `snapshots` covers) and as `/v1/health`'s own verdict. The error is in the API's journal. |
| `snapshots` | a window's figures have not refreshed for twice their interval (at least 15 minutes), or three refreshes in a row failed. The site keeps showing the older figures. |
| `records` | the API met a stored row it cannot decode in the last 15 minutes. The row is published with that part marked (a blob's `reconstructable` status `unknown`, a reading's `row_indices` left out) and the rest of the answer stands; the API's journal names each row once. |
| `disk` | the data disk has under 15% free. |
| `scan_gaps` | a scan gap is recorded (see the Runbook). |
| `pin` | the chain upgraded past this build's pin (see the Runbook). |
| `unassignable_publications` | a publication settled in the last 24 hours could not be assigned. |
| `day_partials` | an audit found the day partials not what the store holds, or a day due stayed unsealed for two days. |
| `hosting_db` | with the hosting lookup on, the IP-to-ASN file is over 45 days old (7b). |

`/v1/health` is public, so it says what is wrong and not what the host
looks like. A check's detail names the stage that failed and for how long,
never an error's text, a path or the disk's size. Each component shows
only `component`, `present`, `alive`, `ok`, `age_s` and `started_at`. The
full errors are in the units' journals, and the status files on the host
keep the rest (the last error, the disk, the build). `scan_gaps` keeps
each range, its reason and its times, and no longer the node's error for
it, which names the node. `/v1/meta` no longer
carries the verdict or the checks. The site shows none of them: it says
something only when the API does not answer (a line above the page and a
dot on the network chip). Every failing check reaches the operator from
the health watch below instead.

`fibre-healthwatch@<network>.timer` asks `/v1/health` every five minutes as
the service user and posts when the verdict or the set of failing checks
changes, again every `ALERT_REPEAT_MIN` while it stays bad, and once on
recovery, with every failing check and its detail. It posts first and
records what it sent only once a destination took it, so a post that was
refused is sent again on the next run, and a full disk does not silence
the alerts after it. It also checks the jobs `/v1/health` cannot see, by
what they left behind and not only by their unit's state (a timer never
enabled leaves a unit that never failed). Each one that is wrong joins the
failing checks under its own name until it holds again:

- `fibre-backup@<network>` or `fibre-archive@<network>` whose last run
  failed (`systemctl is-failed`);
- `backup-copy`: with `BACKUP_REMOTE` set, no backup copy has finished
  for 26 hours (`exports/remote-copy.json`), or none ever while exports
  have been there that long;
- `export`: after 04:00 UTC, no daily export for yesterday;
- `vantage-pull`: with `VANTAGE_PULL_NAMES` set, nothing new from a
  vantage for 30 minutes;
- `fibre-vantage-pull@<network>`: its last run failed and nothing new
  came from a vantage for 10 minutes. One failed run that the next one
  fixes is not reported.

A vantage that never sent anything is not judged: before Fibre is live
there is nothing for it to check.

A backup that stops finishing stops the retirement of local copies after
its second failed night ("Retiring local copies" below), so it is heard of
the first night. The watcher's exit status is its own: 0 for a run that
did its job, whatever the observer's state (that is in the alert and the
log line), and 1 when an alert was refused or its state could not be
written. A failed `fibre-healthwatch@<network>` unit therefore means the
watcher itself is in trouble. It posts to
`ALERT_WEBHOOK`, any URL that accepts a JSON body with a `content` field
(Discord, Slack incoming webhooks, Matrix), and to Telegram when
`TELEGRAM_BOT_TOKEN` (from @BotFather) and `TELEGRAM_CHAT_ID` (a chat the
bot is in) are set in the env file, whichever are set. After setting them,
`sudo bash -c 'set -a; . /etc/fibre-observer/mocha.env; fibre-healthwatch mocha --test'`
sends one message and says whether each destination took it. With none
set it only logs; an external uptime monitor pointed at
`https://<site>/api/v1/health` is the same signal with somebody else's
timer. The webhook and the bot token are never printed, and never on
curl's command line either, where every account on the host could read
them in the process list while a post is in flight: curl reads the URL
from a config on its stdin.

### Upgrading a running observer

The collector owns the schema and the API opens the database read-only, so an
upgrade has an order: install the new binaries, restart **fibre-collector**
first, then the rest. The API refuses to start against a database older than
the binary expects and says so, which is the intended failure — it will not
serve numbers from a schema it does not understand.

```bash
sudo install -m 0755 fibre-sentinel/bin/* /usr/local/bin/
sudo install -m 0755 deploy/vantage-pull.sh /usr/local/bin/fibre-vantage-pull   # the timer runs it next minute
sudo systemctl restart fibre-collector@mocha       # applies migrations
sudo systemctl restart fibre-scan@mocha fibre-probe@mocha fibre-heartbeat@mocha fibre-api@mocha
```

The pull script is installed with the binaries because it changes with
them: one installed from before this change also fetches each vantage's
`measurements.jsonl` and pushes requests to it (`VANTAGE_PUSH`), neither of
which anything uses any more, and the current one resumes from the logical
end that `observer-archive -logical-end` of the same build reports.

A build that changes what `observer-archive` rotates, or the deploy
scripts and units beside it, as the build that retires local copies does,
is installed with the archive timer stopped, and its writers restarted
before the timer runs again ("Archive: bounded live files", upgrade
order).

**The env files and the site's own settings** (the build that gave
`fibre-site@` a file of its own). A host set up before it has its env
files as `cp` left them, `0644`; close them, then give site-server its two
settings *before* the new `fibre-site@.service` is installed, since the
unit no longer reads the network's env file and does not start without its
own:

```bash
sudo chown root:fibre-observer /etc/fibre-observer/*.env /etc/fibre-observer/rclone.conf
sudo chmod 0640 /etc/fibre-observer/*.env /etc/fibre-observer/rclone.conf
sudo grep -E '^(SITE_LISTEN|API_LISTEN)=' /etc/fibre-observer/mocha.env | sudo tee /etc/fibre-observer/site-mocha.env >/dev/null
sudo cat /etc/fibre-observer/site-mocha.env   # both lines, SITE_LISTEN and API_LISTEN, before going on
sudo cp deploy/systemd/fibre-site@.service /etc/systemd/system/ && sudo systemctl daemon-reload
sudo systemctl restart fibre-site@mocha
```

Once closed, `mocha.env` is read only by root and the service user, so
the line that copies the two settings out of it reads it as root too.
Read the new file back before the restart: with a line missing,
site-server starts anyway, on its own defaults (`127.0.0.1:3112`, the API
at `127.0.0.1:8081`), which are mocha's ports and no other network's.
A `mocha.env` that never had `SITE_LISTEN` gets the file written whole,
as in section 5.

`SITE_LISTEN` can stay in `mocha.env`, where nothing reads it any more.
`deploy/test/exposure.sh` checks the modes from then on.

The API also refuses a database **newer** than itself, so an API left on an
old build after the collector moved on says so rather than serving columns
it does not know. A migration can take a while on a large store: start the
new API only once `SELECT MAX(version) FROM schema_migrations` reads the
new version, and give it minutes, not seconds. The prober never opens the
database: restart it last, once, and never stop it for a copy of the data
directory, since its time down is readings and attempts not made.

**Going back.** Every binary refuses a database newer than itself, so an
older build needs the database's version taken back too. When the newer
migrations only added columns or indexes, as migrations 25 and 26 do, that
is one row each, not a copy of the data directory:

1. install the older prober and restart it;
2. wait until the newer collector has read everything the newer prober
   wrote: its cursor in `ingest_cursors` for `measurements.jsonl` equals
   the file's size;
3. stop the collector and the API;
4. `sudo -u fibre-observer sqlite3 /var/lib/fibre-observer/mocha/observer.db 'DELETE FROM schema_migrations WHERE version > 25'`,
   25 being the older build's `store.SchemaVersion` (or the same statement
   through Python's `sqlite3`);
5. remove any drop-in that passes the API a flag the older build does not
   know, `-day-partials`, `-day-partials-pace` or `-day-partials-cache-mb`
   among them (an unknown flag stops it at start):
   `sudo systemctl revert fibre-api@mocha`, or
   delete the file under `/etc/systemd/system/fibre-api@mocha.service.d/`,
   then `sudo systemctl daemon-reload`;
6. install the older collector and API, and start them.

The columns and indexes stay, unread, and the next upgrade runs the
migration again over them.

**Going back past schema 27 (or 28).** Migration 27 is not additive: the rows a
schema-27 collector writes hold the slim record in `raw_json` and `=` in
`probes.row_indices` and `assignments.rows_json`, which an older build
would read as JSON and as lists and answer wrongly; migration 28 does the same
for `reachability.raw_json`. Deleting the version row is therefore never the
way back from either, and the way back from 28 to 27 is the same as below. The store is derived, though,
and every observation is in the record files (the JSONL files with their
`archive/` segments), which the schema-27 build writes exactly as before. So
an older build gets a schema-26 store back, and its collector reads every
line written since that store was cut from its own cursors
(`ingest_cursors`, logical offsets, the archive segments included). No
observation made after the copy is lost:

1. **Before the upgrade**, with the collector and the API stopped, take a
   copy of the store that keeps every page as it is:
   `sqlite3 observer.db ".backup '/root/fibre-v/.deployment/observer-v26.db'"`
   (never `VACUUM INTO`, which can renumber rowids). Keep it, and the
   previous binaries, until the new build has run a week. The copy reads
   and writes the whole store on a disk it shares with the validator, so
   run it at idle I/O priority and pause it while the disk's I/O pressure
   is high. On an NVMe disk with the `none` scheduler the priority alone
   changes nothing (see `Nice=10` below); the pause is what holds:

   ```sh
   cd /var/lib/fibre-observer/mocha
   ionice -c3 nice -n10 sqlite3 observer.db ".backup '/root/fibre-v/.deployment/observer-v26.db'" &
   copy=$!
   trap 'kill -CONT "$copy" 2>/dev/null' EXIT
   # "some avg10" in /proc/pressure/io is the share of the last ten seconds
   # in which a task waited on I/O; at 10% or more the copy is stopped
   # until it falls back
   while kill -0 "$copy" 2>/dev/null; do
     p=$(awk '/^some/ { split($2, a, "="); print int(a[2]) }' /proc/pressure/io)
     if [ "$p" -ge 10 ]; then kill -STOP "$copy" 2>/dev/null; else kill -CONT "$copy" 2>/dev/null; fi
     sleep 5
   done
   wait "$copy" && echo "copy complete"
   ```

   A stopped copy holds only a read transaction on a store nothing else
   writes, so a pause costs time, not consistency.
2. **To go back:** stop the API and the collector, move `observer.db*`
   aside (keep it), put the copy in its place, owned by `fibre-observer`,
   and install the previous collector and API. Remove any drop-in that
   passes a flag the older build does not know.
3. **Start the collector.** It opens the copy at 26 and reads on from its
   cursors: every publication, reading, heartbeat, payment and late
   verdict written since the copy goes in the same way it would have the
   first time. Once its `measurements.jsonl` cursor equals the file's
   logical end, start the API: it finds the snapshots and day partials
   belong to another store and builds them again, once.
4. **Without a copy**, the same steps with no store: the older collector
   rebuilds it from the whole record ("Rebuild from the record" below).
   Slower, the same result, but only while nothing is retired: a build
   from before retirement cannot read a retired range (below).

What comes back from neither (it has no record line) is what a rebuild
never brings back: escrow balances, validator identities and pictures,
and the chain-side `meta` keys, all polled again within the hour.

This was tested on the golden records of 2–5 October. A schema-26 copy
was taken at the end of the 4th. The schema-27 build opened it, migrated it
and took in the 5th. The older build was then started from the copy and
its collector read the 5th from its cursors. All 340 API answers were byte
for byte those of a store the older build wrote from the four days at once.
Before going back, `record-verify` found every line of the four days in the
schema-27 store, byte for byte. The older build refuses the schema-27 store
at start ("database schema version 27 is newer than this binary's 26").

**Going back past schema 29.** Migration 29 removes what only the earlier
sampling and the second vantage's confirmations used: the index
`probes_sampling_started`, the index `probes_cleared`, and the table
`probe_confirmations` with its indexes. Each is an index or an empty
table, so no row is rewritten and none is lost. The table has always been
empty; the migration refuses to run, and the collector stops with the
reason, if it ever holds a row. The columns `probes.cleared_by` and
`probes.confirmed_by` stay, unread: dropping a column rewrites the whole
table. Migration 29 also sets `publications.must_serve_until_ambiguous`
where the record says so, which migration 19 did once and nothing did
since, and the collector writes it on every insert from then on. An older
build refuses the schema-29 store at start ("database schema version 29 is
newer than this binary's 28"), and deleting the version row is not the way
back: the older build reads the table and the index that are gone. The way
back is the one above. Take a `sqlite3 .backup` copy of the store before
the upgrade, with the collector and the API stopped (the loop above), and
keep it with the previous binaries until the new build has run a week. To
go back, stop the API and the collector, move `observer.db*` aside, put the
copy in its place, owned by `fibre-observer`, install the previous
binaries and start the collector: it reads on from its cursors. Without a
copy, the older collector rebuilds the store from the record. Migration 29
changes no record file.

The upgrade to schema 29, in order:

1. stop the API and the collector, take the copy above (`observer-v28.db`),
   and keep it;
2. install the binaries, `deploy/healthwatch.sh` as
   `/usr/local/bin/fibre-healthwatch`, and the units
   (`sudo cp deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/ && sudo systemctl daemon-reload`);
3. empty `POLICY` in the env file (the new prober unit no longer reads it);
4. start the collector, which applies migration 29, then the API;
5. restart the scanner and the heartbeat, then the prober, last and once;
6. `sudo rm /var/lib/fibre-observer/mocha/sampling-master.key`;
7. with the hosting lookup on, `sudo systemctl enable --now fibre-hosting-db@mocha.timer` (7b);
8. build the site for the network and copy it in place (section 5).

**Once anything is retired.** A collector that reads a retired range
("Retiring local copies" below) reads it from the exports; one from before
retirement does not know the `retired` record, opens the segment's file
and stops when it is gone. A segment is retired only once a nightly backup
has copied its file to the remote (it waits a night after it is archived),
so every retired segment's file is on the remote. So, going back to such a
build:

- copy the retired segments' files back from the remote first, as the
  service user and with the collector stopped:

  ```sh
  sudo -u fibre-observer env RCLONE_CONFIG=/etc/fibre-observer/rclone.conf \
    rclone copy '<BACKUP_REMOTE>/mocha/archive' /var/lib/fibre-observer/mocha/archive --include '*.jsonl.gz'   # BACKUP_REMOTE as in mocha.env
  ```

  and the same for each `vantages/<name>/archive`. The older build then
  reads every range from its files again, from any cursor, a rebuild from
  zero included;
- its `observer-archive` must never run. It saves `index.json` without
  the `retired` records it does not know, after which this build finds
  them only in `archive/<file>/retired.json` (which the older build never
  touches, and from which this build's next run writes them back into the
  index), and the older build's readers stop at the first. Stop the timer
  before installing the older binaries, and keep it off for as long as
  they run: `sudo systemctl disable --now fibre-archive@mocha.timer`, and
  `sudo systemctl enable --now fibre-archive@mocha.timer` once this build
  is back. (This build's `-retire` would also remove a retired segment's
  file wherever it finds one, as a retirement a crash cut short.)
- its `sentinel-recompute` and `sentinel-verify` read `publications.jsonl`
  as a plain file, not through the archive: once this build has rotated
  it, they see only the live lines and report every archived publication
  missing. Check the record with this build's tools, which read the
  archive and the exports. The older collector and API read the archive
  and are not affected.

Once a segment is retired, the exports are the copy of its lines on this
disk: each member is a contiguous byte range of the source file, so the
exports, kept and backed up, give the record back for a rebuild or a
rollback that reaches past the live files, byte for byte and at the same
offsets. They carry every file this observer writes, and each
second vantage's heartbeats as a member named by its path,
`vantages/<name>/reachability.jsonl`; the first export after that member
was added starts the file at its first byte, its older lines counted late.

Never let an older collector read rows a newer
prober wrote: it keys a row on the reading, not the attempt, so it keeps a
validator's first answer and drops the later ones. So do the two counters
the day partials follow in `meta`, `held_flags_rev` and
`migration_rewrites`: an older build neither reads nor writes them. Coming
forward again, the API reads every hold again when it loads its files,
since an older collector moved the flags without counting.

Schema 5 adds a covering index over `probes`. On a store with 700,000 probes it
takes a few seconds and about 200 bytes a probe; the collector logs it and the
restart is not otherwise different.

The API computes the network summary, the validator list and the market
figures for every window at startup rather than on demand, and keeps the
last computed copy of each under `<DATA_DIR>/snapshots/` (`-snapshot-dir`
moves it). A restarted API serves those copies at once, with their real age
shown on the page, while the warm-up recomputes them behind; the first
minute or two after a restart is busier than the steady state, but nobody
waits for it. The same directory keeps `endorsement-ledger.json`, which the
figures derive from the whole record, so a restart does not rebuild it; the
API checks that it belongs to this database as it now stands, at its schema
version, and was computed the way this build computes it, and rebuilds it
when it does not (once after a migration), and an older build ignores it.
An API still serving when the collector migrates does not write what it
read before the migration under the new schema: it drops the ledger and
builds it again from the migrated database. `-warm-only` writes it too, and
the copy below carries it over with the snapshots. Earlier builds also kept
`original-rows.json` there; this one reads each publication's
`original_rows` column (migration 27) instead.

The same directory also keeps the day partials the 7d, 30d and "all"
windows are summed from: `day-partials.json`, the index, and
`day-partials/`, one file per sealed day, which the index names with its
digest. They are checked the same way on start (a seal file that is
missing, damaged or not the one the index names leaves only its own day to
be read raw until it is sealed again; anything else wrong begins them again
from the database), are written as the sealer comes to rest and when the
API stops, whenever a day was sealed or dropped since the last write, and
an older build ignores them. A migration that changes a table they read, or
rewrites rows, begins them again; one that only adds what they do not read
(a table, an index, a column of a table they do not read) leaves them.
`-day-partials=false` turns them off and reads every window whole, each
statement on its own, as the build before them did; the files stay where
they are, and the next start with the flag on catches up from them.

A sealed day's service-time and transfer-rate histograms, most of what
the partials weigh, are not held in memory: they stay in the day's seal
file, written when the day is sealed, and a window reads them from it as
it sums the day. `-day-partials-cache-mb` (default 64) keeps the newest
days' in memory within that many megabytes, 0 none; an older build does
not know the flag and stops at start with it (see "Going back"). A seal
file that goes missing or is damaged while the API runs costs only its
own day, as on start: the window that meets it reads the day raw, and the
sealer seals it again under a new name.

Sealing reads the database a day at a time, and the disk it reads may be
the one other services write (the validator beside it): it is the
partials' one burst of reads. `-day-partials-pace` keeps it gentle: after
each unit of work the sealer rests that many times as long as the unit
took, live and with `-warm-only` alike. The default, 3, leaves the disk to
everything else at least three quarters of the time; 0 does not rest. A
unit that fails is tried again after a minute, then two, four, at most six
hours, while the sealer goes on with the others, and the API logs the first
failure and the recovery. Each burst of sealing ends with one line in the
journal (`day partials: sealer: sealed ...`), and every hourly audit with
one (`day partials: audit: ...`). `/v1/health` carries a `day_partials`
block (state, days sealed, the oldest day due and not sealed, the last
audit) and a `day_partials` check, which fails when an audit found the
partials not what the database holds (a window or a ledger day that
differs puts the process on raw reads, as `-day-partials=false` would,
until it is restarted, and leaves the files as they are for inspection)
and when a day due has stayed unsealed for two days.

That holds only while the copies are still valid. Each file carries the
revision it was computed under (the rules, `verdict.MethodologyVersion`,
the retention holds and whether Fibre is active), and the API serves a file
only under the same revision. A build that changes the rules therefore
starts with nothing it may serve, and its longer windows take minutes to
compute. A reader of such a window waits at most eight seconds and then gets
a 503 with `Retry-After` and `"computing": true`, which the site shows as
figures being computed and asks again for; nothing hangs, but the figures
are missing until the warm-up reaches them.

To avoid that, compute the new build's snapshots before switching to it.
`observer-api -warm-only` opens the database read-only, seals every day of
the day partials that is due and builds their publication ledger, computes
every window of every snapshot once under the current revision, writes the
files to `-snapshot-dir` and exits 0 (non-zero, with the reason, on any
failure). With `-warm-only` that directory defaults to
`<data-dir>/snapshots.next`, and the live `<data-dir>/snapshots` is
refused: the running API rewrites its files there under the same temporary
names, and an old API restarted meanwhile would load the new build's market
files. It must run as the service user and with the unit's own flags: the
snapshots depend on `-vantage` (the heartbeats counted are that vantage's)
and the market one on `-publishers`, and a file written for another
vantage is not loaded. `systemd-run` gives it both, from the unit's env
file, expanding `${…}` the way the unit's `ExecStart` does.

Seed `snapshots.next` with a copy of the whole live directory first: the
warm-up then begins from the live API's partials and its ledger,
and seals only the days the live API had not. A partial from a build that
folds days another way is refused as it is loaded (its definition holds the
Go that folds them), so a seed is never a stale figure, at worst a cold
start: the build that moved the histograms to the seal files is one, and
its warm-up seals every day again. Sealing is most of a warm-up's time,
and at the default pace takes four times its work. On a synthetic record
of 32 days (2.2 GB, 19,000 publications, 311,000 readings), the work of
sealing every day from nothing is about 20 seconds of reads (4.5 GB read
with the database's memory map off; 50 seconds from a cold page cache), 80
seconds at the default pace; seeded from the files of an API that had
sealed them, it seals nothing and takes the two seconds of loading them.
The four and a half days of Mocha on record in late September 2026 took
about three minutes unpaced; months of traffic ten times as busy take
hours, which is what the seed spares.

`Nice=10` below lowers the warm-up's CPU priority only. On an NVMe disk with
the `none` I/O scheduler (`cat /sys/block/nvme0n1/queue/scheduler`) neither
it nor `IOSchedulingClass=idle` lowers its reads: the pace is what does.

After the collector has migrated the database (the new binary refuses an
older schema), and while the old API keeps serving:

```bash
sudo install -m 0755 fibre-sentinel/bin/* /usr/local/bin/
sudo install -m 0755 deploy/vantage-pull.sh /usr/local/bin/fibre-vantage-pull   # the timer runs it next minute
sudo systemctl restart fibre-collector@mocha       # applies migrations
# the seed: a copy of the whole live directory, as the service user
sudo -u fibre-observer sh -c 'rm -rf /var/lib/fibre-observer/mocha/snapshots.next &&
  cp -r /var/lib/fibre-observer/mocha/snapshots /var/lib/fibre-observer/mocha/snapshots.next'
# seconds when seeded; minutes, hours on a long, busy record, when not
sudo systemd-run --wait --pipe --collect -p User=fibre-observer -p Nice=10 \
  -p EnvironmentFile=/etc/fibre-observer/mocha.env \
  /usr/local/bin/observer-api -warm-only -data-dir '${DATA_DIR}' -snapshot-dir '${DATA_DIR}/snapshots.next' \
  -vantage '${VANTAGE}' -vantage-location '${VANTAGE_LOCATION}' -vantage-provider '${VANTAGE_PROVIDER}' \
  -publishers /etc/fibre-observer/publishers-mocha.yaml
sudo systemctl stop fibre-api@mocha
sudo -u fibre-observer sh -c 'rm -rf /var/lib/fibre-observer/mocha/snapshots/day-partials &&
  cp -r /var/lib/fibre-observer/mocha/snapshots.next/. /var/lib/fibre-observer/mocha/snapshots/ &&
  rm -r /var/lib/fibre-observer/mocha/snapshots.next'
sudo systemctl start fibre-api@mocha
# only when this build changed them (not for the day partials, which are the
# collector's and the API's): every minute the prober is down is readings not made
sudo systemctl restart fibre-scan@mocha fibre-probe@mocha fibre-heartbeat@mocha
```

The seed and the switch each copy a whole directory, never its `*.json`
files alone: `day-partials.json` names the files under `day-partials/`, and
an index copied without them leaves its days to be sealed again. The seed
may copy while the live API writes; a day whose seal file it missed is
sealed again by the warm-up. At the switch the live directory's own
`day-partials/` goes first, so that no seal file of the old API's is left
under a name the new index uses. Both copies run as `fibre-observer` so the
files stay its own: the API rewrites them on every refresh. For the same
reason the warm-up does not run as root, which would also risk creating the
database's `-shm` file owned by root. A hold or the activation landing
between the warm-up and the start changes the revision: the files are then
dropped and recomputed rather than served, which is the cold start again
and never a stale figure. `journalctl -u fibre-api@mocha` shows
`snapshot(s) loaded from disk` and `day partials: loaded from ...` on
start. The scanner, the prober and the heartbeat need no restart for a
build that changed only the collector and the API, as one changing the day
partials does; restart them only when their own code changed, the prober
last.

This keeps the old API serving from the migrated database for the
minutes of the warm-up, where the plain upgrade above leaves it seconds, so
use it only when the build's schema change, if any, is additive: new tables,
columns or indexes the old API does not read. When a migration changes or
drops something the old API reads, stop `fibre-api@mocha` before restarting
the collector; the warm-up still spares the new build a cold start, but the
API is down for those minutes.

## 5. The site

The site is a static export of `web/`, and each network gets its own build.
Two settings are fixed when it is built, and a build made for one network
is wrong on the other:

- `NEXT_PUBLIC_API_URL`, that site's public API
  (`https://mocha.observer.example.org/api/v1`). The methodology page's
  links to the exports and the signing key, and the recompute command it
  prints, use it, and a shared link's preview takes the site's address
  from it. Unset, it is Mocha's live API.
- `NEXT_PUBLIC_SELF_VALIDATOR`, the consensus address
  (`celestiavalcons1…`) of the validator the operator runs on that
  network, which the site marks "runs Tensile". `web/.env.production`
  holds Mocha's; a value given on the command line wins over it. Each
  network has its own validator key, so each build needs its own value.

`NEXT_PUBLIC_NETWORKS` lists every network's site for the header's switch
(the current one is marked by origin) and is the same in every build.
Build one network at a time, since each build replaces `web/out`:

```bash
cd web && npm ci
NEXT_PUBLIC_API_URL=https://mocha.observer.example.org/api/v1 \
NEXT_PUBLIC_SELF_VALIDATOR=celestiavalcons1... \
NEXT_PUBLIC_NETWORKS="mainnet=https://observer.example.org,mocha=https://mocha.observer.example.org" \
  npm run build
sudo install -d /var/www/fibre-observer/mocha && sudo cp -r out/. /var/www/fibre-observer/mocha/
```

`make build` builds the site with none of them set, which is a Mocha build.

### Caddy

Put `deploy/Caddyfile` in place with your domains instead of
`observer.example.org` and `mocha.observer.example.org`:

```bash
sudo cp deploy/Caddyfile /etc/caddy/Caddyfile && sudo systemctl reload caddy
```

Each site block names its network's `observer-api` port and its build's
directory (`/var/www/fibre-observer/<network>`). Caddy serves the build
(its `/api/` page documents the API), proxies the site's `/api/v1/*` to
that port, and gets TLS certificates from Let's Encrypt. Delete the second
site block if you run one network.

### site-server, behind a shared proxy

Where the front proxy is shared with other projects and cannot be given a
file server for this site (the live Mocha host is one),
`fibre-site@<network>` serves the build and passes `/api/v1/*` to the API
itself (`deploy/site-server.cjs`, Node's standard library only, run by a
throwaway user):

```bash
sudo install -d -m 0755 /usr/local/lib/fibre-observer
sudo install -m 0644 deploy/site-server.cjs /usr/local/lib/fibre-observer/site-server.cjs
sudo install -d -m 0755 /srv/fibre-site/mocha && sudo cp -r web/out/. /srv/fibre-site/mocha/   # this network's build
sudo systemctl enable --now fibre-site@mocha
```

The unit reads `SITE_LISTEN` (a port of its own per network, for example
`127.0.0.1:3112` for mocha and `127.0.0.1:3113` for mainnet) and
`API_LISTEN` (that network's API, as in its env file) from
`/etc/fibre-observer/site-<network>.env`, which holds those two lines and
nothing else, and the site from `/srv/fibre-site/<network>`. It does not
load the network's env file: that one holds the alert and backup
credentials, and site-server faces the internet and needs none of them.

```bash
printf 'SITE_LISTEN=127.0.0.1:3112\nAPI_LISTEN=127.0.0.1:8081\n' | sudo tee /etc/fibre-observer/site-mocha.env >/dev/null
```

Point the front proxy's site for that network at `SITE_LISTEN`. A new
build is copied over the directory; the server picks up the changed files
without a restart.

site-server rations the API for the visitors. Each client, an IPv4
address or an IPv6 /64 (one host is given a whole /64), has a token bucket
(a burst of 120 requests, 10 a second; `/v1/tip` 60 and 30 a second) and
at most 16 requests in flight; the site as a whole has at most 48 requests
at the API at once. A request holds its place in those 48 only until the
API's answer begins, so a client reading an answer slowly (a day's export)
costs only its own 16. An answer with no byte moving for 60 s is given up,
and a client that goes away cancels its request to the API. Past 50,000
clients in ten minutes, new ones share one allowance, so the table of
clients stays bounded. An answer refused by these limits is a 429 with
`retry-after: 5`.

### Two networks

Mocha and mainnet are two instances of everything, side by side:

| | mocha | mainnet |
|---|---|---|
| env | `/etc/fibre-observer/mocha.env` | `/etc/fibre-observer/mainnet.env` |
| data | `/var/lib/fibre-observer/mocha` | `/var/lib/fibre-observer/mainnet` |
| `POLICY` | empty: the prober's unit passes no `-policy` | empty |
| API | `127.0.0.1:8081` | `127.0.0.1:8080` |
| units | `fibre-*@mocha`, `fibre-*@mocha.timer` | `fibre-*@mainnet`, `fibre-*@mainnet.timer` |
| site build | `NEXT_PUBLIC_API_URL` and `NEXT_PUBLIC_SELF_VALIDATOR` of mocha | the same, of mainnet |
| site files | `/var/www/fibre-observer/mocha` or `/srv/fibre-site/mocha` | `/var/www/fibre-observer/mainnet` or `/srv/fibre-site/mainnet` |
| site | `mocha.observer.example.org` | `observer.example.org` |
| litestream | `litestream-mocha.yml`, bucket path `mocha/observer.db` | `litestream-mainnet.yml`, bucket path `mainnet/observer.db` |

Each instance needs its own RPC node with `discard_abci_responses = false`.
Nothing is shared between them but the binaries; a data directory belongs
to one chain and the scanner refuses to resume it against another. Disk: a
mocha instance grows by a few GB a month, a mainnet instance by what its
publication rate makes it (see "Backups"). Two instances double the
reading traffic. Setting up mainnet from nothing is "7d. Mainnet: a fresh
install".

## 6. docker compose (alternative)

```bash
cp deploy/observer.env.example deploy/.env   # edit RPC, VANTAGE, DOMAIN
docker compose -f deploy/docker-compose.yml up -d --build
```

Same five processes plus Caddy, one image built from `deploy/Dockerfile`.
Data lives in the `observer-data` volume. Compose is one network per
project (it binds 80 and 443); for two networks on one host use systemd.
The image builds the site with the defaults, which are Mocha's (section
5): on another network, serve a build made for it in place of the image's
copy.

## 7. Backups, retention, rebuild

Budget for disk: one measurement is about 1.5 KB in `measurements.jsonl`
(about 3 KB when it carries the verified row indices) and about twice that
again in the database, one per validator a reading asks. At mocha's rate on
28 September (about 20 blobs a minute, 12 to 20 validators asked each) that
is about 1.2 to 1.8 GB a day of JSONL plus the database. The JSONL files are the record; the biggest are kept bounded
by moving their older lines into compressed segments under `archive/`
(below), never by deleting a line, and a segment's file leaves the disk
only once every line of it is proven to be in three other places
("Retiring local copies" below). `/v1/health` fails the `disk` check
under 15% free, so the alert arrives while there is still room to act; the
disk may be shared with other services, and nothing is deleted to make room. When a disk fills,
retirement ("Retiring local copies" below) is how a segment's file leaves
the disk with its lines still readable. Anything moved off the box by hand
(append-only or immutable; a copy is complete the moment it is taken) has
to come back before the database is rebuilt: a segment whose index entry
names no exports stops every reader of the whole record (a rebuild,
recompute, the manifest tool's `cat`). Nothing here deletes a row.

**Retention (decided 2026-10-04):** every row is kept for good. Probe and
heartbeat rows and their `raw_json` are never deleted or stripped, so an
old blob, a validator's history and every rate read the same years on as
they do today. The 90-day prune and the 30-day `raw_json` strip of
2026-09-18 were retired before either first ran; their flags
(`-retain-raw`, `-retain-raw-json`) are gone, and a unit that still passes
one fails at start. **14 days** after a UTC day ends (`-rollup-after`) the
collector computes the day's per-validator rollup (obligation buckets by
settlement day; classes, faults, gaps and heartbeats by start day) with the
API's own SQL, for speed only. `-rollup-after` is a floor, not the
rule: a day rolls only once every promise settled on it has left its
window (`must_serve_until` plus an hour) and no probe row of theirs still
awaits the late shadow verdict, so a chain whose retention is longer than
the flag holds the rollup rather than rolling a pending obligation; the
log says which day is waiting and why, once when it starts waiting. The
pass runs hourly (`-retention-every`); the status file shows
`rollup_through` and `rollup_waiting`. A warning in the log that obligations
were still pending at roll means `-rollup-after` is shorter than a
retention window on this chain: raise it. The
JSONL files (with their `archive/` segments) remain the record; the
daily export is what a verifier downloads. The collector builds it: one
tarball per UTC day under `<DATA_DIR>/exports` (`-exports-dir`), once the
grace hour has passed (`-export-hour`, default 03:00 UTC, so late rows
land in their own day), served at `/v1/exports` with a manifest of
digests. `sentinel-recompute` re-derives every verdict and every
obligation figure from an untarred export and compares them with the
API's `?as_of=` answer; see `docs/verdicts.md`, "Reproducing the
figures". At mainnet's 148 MB/s the
publication rate is many times mocha's, which is why the decision is
written down now: a rollup added later could not reconstruct the "all"
window it replaced.

Two copies, both shipped. The record copy carries `backup-manifest.json`,
written by `fibre-backup-manifest` before the copy starts: one consistent
cut — `state.json` read whole first (the scanner fsyncs its record files
before it replaces the state, so a record file read afterwards holds
everything the checkpoint covers), then every record file's length up to
its last complete line (dependents cut before what they refer to), then
the SHA-256 of exactly those bytes with every line parsed and counted as a
JSON record. The manifest carries the checkpoint and the bytes of
`state.json` as they were. The files keep growing while rclone reads them,
so the copy is at least the cut; `deploy/test/restore.sh` trims a restored
copy back to the cut, checks every hash, parses every line, and puts the
cut's own `state.json` in place of the copy's, which was read later and
points past the records the cut holds. A copy that came back missing,
short, altered or unparseable is a failed restore, not a surprise. The
master key is never in it.

A line in a record file that is not a JSON record (a writer that died
mid-line on a full disk, and its restart's first line glued to the
fragment; another vantage's line pulled as it was) stays in the file for
good, since the files are append-only and the collector steps over it. The
cut lists it by its line number (`bad_lines`, `bad_count`; `show` prints
them), copies it as it is, and `verify` requires exactly those lines and no
other. It no longer fails the cut: a cut that failed on it stopped every
copy from then on. A cut that cannot be taken at all (an archive index
that places no live file) does not stop the copy either: the files are
copied without a new manifest, the remote keeps the last one, and
`fibre-backup@` fails at the end, which the health watch reports.

The manifest is uploaded last, on its own, once every file it describes is
on the remote: a night that stops part-way (a reboot, the link, a full
bucket) leaves the previous manifest, never a new one beside older files.
The files it describes are all still there: grown since, which `verify`
trims back, or rotated since by `observer-archive` (below), the cut's
live lines then in the segments that rotation wrote, which go to the
remote before any live file. `verify` reads them back from there into the
live file's place and checks them against the cut, so a remote's copy
verifies after a failed night too.

`verify` takes the names from a manifest that came back from the remote
with the copy, and trims and writes the files they name; it touches only
the names a cut holds (the record files, and
`vantages/<name>/reachability.jsonl` under a name `vantage-pull` accepts),
segment and export names with no directory part, and nothing a link in
the copy leads out of it. A manifest altered on the remote cannot make the
restore drill, which runs as root, cut a file outside its copy.

A segment retired on the host is listed in the
cut with its `retired` record; `verify` checks it by its file when the
copy has one (the remote has every retired segment's file: a segment is
retired only after a backup has copied it) and otherwise reads it back
from the copy's `exports/`,
every tarball and member against `exports/index.json` and the range
against the segment's SHA-256. Each
other vantage's heartbeats (`vantages/<name>/reachability.jsonl`, with
their own `archive/` beside them) are cut and checked like the observer's
own files.


- **litestream** for the database: copy `deploy/litestream.yml` to
  `/etc/fibre-observer/litestream-mocha.yml` and set both lines marked
  `<network>` in it: the database's `path` (the instance's data directory)
  and the replica's `path` in the bucket (`mocha/observer.db`; on mainnet
  `mainnet/observer.db`, or mainnet's database replicates over Mocha's),
  put the bucket keys in
  `/etc/fibre-observer/litestream-mocha.env` (mode 0600), and enable
  `fibre-litestream@mocha`. It replicates the **derived** database only,
  continuously, with 72 h of history.
- **fibre-backup** for the record: `fibre-backup@mocha.timer` runs
  `rclone copy` of every `.jsonl` (the record, `registry.jsonl`,
  `runs.jsonl`, `sampling_decisions.jsonl`, `sampling-secrets.jsonl`, `amendments.jsonl`,
  each `vantages/<name>/reachability.jsonl`), the archived
  segments under `archive/` and `vantages/<name>/archive/` (first, see "Archive" below), `state.json`, the status files
  and the daily exports to `BACKUP_REMOTE/<network>` nightly (`deploy/backup.sh`),
  then `backup-manifest.json`, last,
  with the rclone remote configured once in `/etc/fibre-observer/rclone.conf`
  (`0640 root:fibre-observer`, like the env file: it holds the remote's
  credentials, and the backup reads it as the service user).
  It copies rather than mirrors, so moving old files off a full disk, or
  retiring a segment, can never delete them from the remote, and a
  segment is retired only after a backup has copied its file.
  It never copies `sampling-master.key`, which must not leave the host, nor
  the database, which litestream covers. A copy that finishes is recorded
  in `exports/remote-copy.json`: `{"copied_at", "remote"}`, `remote` being
  the remote's fingerprint (the first 16 hex digits of the SHA-256 of
  `BACKUP_REMOTE/<network>`, never the remote itself). After the copy it
  reads each export not yet proven back from the remote (`rclone cat`),
  hashes it, and appends one line to `exports/remote.jsonl`:
  `{"name", "sha256", "checked_at", "ok", "remote"}`, `sha256` being the
  local `.sha256` it was checked against. The newest line for a name
  counts; a tarball rebuilt under a new digest, one whose check failed, or
  one last proven on another remote (a new `BACKUP_REMOTE`) is read again
  the next night. A failed check is `"ok": false` and does not fail the
  unit, whose status is the copy's; it only keeps the segments that export
  holds on the disk a night longer. The first run reads every export back
  once, and so does the first run after `BACKUP_REMOTE` changes. A last
  line that a crash or a full disk cut short (NUL bytes included) is
  dropped before the next line is appended, since `observer-archive`
  refuses a complete line that is not a check; no complete line is ever
  changed. The remote is never printed: the script's lines call it
  `BACKUP_REMOTE`, and rclone's messages from the copies have it replaced.
  A remote given whole on the command line
  (`:s3,access_key_id=…,secret_access_key=…:bucket`) is still in rclone's
  command line while it runs, where anyone on the host can read it; a
  named remote in `rclone.conf` keeps the credentials out of both. With
  `BACKUP_REMOTE` empty the timer runs and does nothing, so enable it
  everywhere and arm it with one variable.

**Rebuild from the record.** Stop the instance's collector and API, move
`observer.db*` aside, start the collector: it recreates the schema, replays
`registry.jsonl` (endpoint history), then tails the JSONL files from zero:
each file's archived segments first (a retired one read back from the
daily exports, below), then the live file.
Every record has a natural key and every insert is `ON CONFLICT DO
NOTHING`, so a replay never duplicates. The run record (`observer_runs`) comes
back from `runs.jsonl`, which every component appends its starts, stops
and flags to, the revealed sampling secrets from
`sampling-secrets.jsonl`, and the late shadow verdicts from
`amendments.jsonl`, the collector's own log of them (replayed before
anything is re-judged, so a rebuild never draws a verdict twice). What a rebuild does **not** bring back, because
it has no JSONL source: the collector's own run row, the escrow balances
and validator identities (re-polled within minutes), the validators'
Keybase pictures (re-fetched within the hour, see below), and the
chain-side `meta` keys (re-polled at once). Litestream's copy is the backup
for those.

**Validator pictures.** The identity a validator sets in the staking module
is a Keybase key suffix. The collector resolves it through Keybase's public
lookup and keeps the picture in the store (`-avatars-every`, hourly;
`-avatar-max-age`, a day), one lookup per identity, spaced out; the API
serves it at `/v1/avatars/<identity>` and the site shows it in place of the
initials. Readers never contact Keybase or its CDN, so the site's `img-src`
stays `'self'`. An identity that is not a sixteen-character suffix, or that
Keybase has no picture for, is never looked up again before the max age.
`-avatars-every 0` turns the lookup off.

**Restore the database** from litestream: stop `fibre-collector@mocha` and
`fibre-api@mocha` (each holds the WAL), then
`litestream restore -config /etc/fibre-observer/litestream-mocha.yml /var/lib/fibre-observer/mocha/observer.db`,
delete any `observer.db-wal` / `-shm` left beside it, start both.

Test a restore and a rebuild before you need one: stop the collector, move
the database aside, restore or delete it, start the collector, and check
`/v1/meta` counts match.

### Stored data the reading no longer needs

No record line, no row and no column with data below is deleted, and
nothing is rewritten: the earlier schedule's rows (`w1` to `w4`, `grace`,
`post`, 8,000 each) and the `NOT_PROBED` end rows of readings the old
prober could not make are the record, and stay. Migration 29 drops only
indexes and a table that has never held a row. What only served the
earlier model, with its size on the mocha host on 29 September, and how to
remove it once the owner approves:

| data | size | still read by | how to remove |
|---|---|---|---|
| `probe-budget.json` | 1.3 MB | the prober before this change; the new prober never reads or writes it | after the new prober is running: `rm <DATA_DIR>/probe-budget.json` |
| `sampling-master.key` | 32 B | the prober only while its unit passes `-policy`: it reveals a secret a day, for days with no draw since 2026-09-26, the last day with one (revealed 2026-10-04) | install this build's units (the prober's passes no `-policy`), empty `POLICY` in `mocha.env`, restart the prober with the upgrade (its time down is readings not made), then `rm <DATA_DIR>/sampling-master.key`. The secrets revealed stay in `sampling-secrets.jsonl` |
| `sampling-secrets.jsonl` | 5.8 KB | the collector (`sampling_secrets` table), the daily export, `sentinel-recompute -sampling` | kept: the earlier draws' audit is part of what old blobs show. Without `-policy` nothing is appended to it |
| `sampling_decisions.jsonl` | 8.3 KB | the collector (`sampling_decisions` table), the daily export | kept, as above |
| table `sampling_decisions` (53 rows) and `sampling_decision_points` (318 rows, with its key index) | 0.2 MB | the obligation rows of sampled-out publications (`obligation_rows`) | kept: removing them would change those blobs' obligations |
| index `probes_sampling_started` | 68 MB | nothing: `/v1/sampling`, its only reader, is removed | dropped by migration 29 (an index, no row goes). Never `VACUUM` the store: it can renumber rowids that some figures read in order |
| columns `probe_daily.faults`, `attested`, `unattested`, `unknown_att` | none yet (no day rolled) | nothing: written as 0 | a migration that bumps the schema, whenever the table is next changed |
| columns `obligation_daily.end_unobserved`, `unobserved_reachable`, `unobserved_unreachable`, `unobserved_not_probed` | none yet | summed into `not_counted` | the same; one `not_counted` column would do |
| `snapshots/` | 1.0 MB, 12 files | the API, which rewrites every file on start and on each refresh | nothing to do, but for `original-rows.json` (below) |
| `snapshots/original-rows.json` | not measured; an entry per publication | nothing: this build reads each publication's `original_rows` column (migration 27) and neither reads nor writes the file | derived, not record: `rm <DATA_DIR>/snapshots/original-rows.json` once this build runs. An older build put back rebuilds it |
| table `probe_confirmations` and its indexes, index `probes_cleared` | empty (0 rows) | nothing: the second location's confirmation of failed readings is gone | dropped by migration 29, which refuses if the table holds a row |
| columns `probes.cleared_by` and `probes.confirmed_by` | every value NULL | nothing | kept: dropping a column rewrites `probes`, most of a 6 GB store on a disk the validator shares |
| rows stored before the slim record (migration 27, 2026-10-06): full lines in `publications.raw_json` and `probes.raw_json`, full lists in `assignments.rows_json` and `probes.row_indices`, the same row lists several times over | an estimate: about 0.75 GB in `assignments.rows_json` and as much again in `publications.raw_json` | the blob page, `/v1/probes`, the late verdicts, the corrector, the retirement's record check | the collector writes them in the slim forms a new row gets, a batch at a time after each pass (`-slim-backfill-budget`, 2 s of work a pass; `0` turns it off): only while no ingest is due and `/proc/pressure/io` says the disk is not busy, each value only where its new form reads back to the old one byte for byte, the rest left as they are and counted. It changes no record file, and gives the freed pages back a few megabytes at a time. Progress is in `meta` (`slim_backfill_<table>`, the last rowid done; `slim_backfill_done_at` once every table is done) and on the collector's status file (`slim_backfill`) |
| `vantages/de-1/measurements.jsonl` | 0 B | the pull script installed before this change, which still fetches it every minute | install the new one first (`sudo install -m 0755 deploy/vantage-pull.sh /usr/local/bin/fibre-vantage-pull`, as in "Upgrading a running observer"), then `rm` it |

### Archive: bounded live files

`measurements.jsonl` grows about 100 MB a day. `observer-archive`, run daily
by `fibre-archive@<network>.timer` at 04:40 UTC (after the export and the
backup), keeps it bounded without taking a line out of the record, and with
it the sampling decisions, the heartbeats, the publications, the payments
and each other vantage's heartbeats:

```
<DATA_DIR>/measurements.jsonl                         the live file: lines dated in the last -keep (default 7 days)
<DATA_DIR>/archive/measurements.jsonl/index.json      segments, their digests, where the live file starts
<DATA_DIR>/archive/measurements.jsonl/000001-2026-10-02.jsonl.gz
<DATA_DIR>/archive/measurements.jsonl/000002-2026-10-03.jsonl.gz   one per run: the lines dated before that day
<DATA_DIR>/archive/reachability.jsonl/...
<DATA_DIR>/archive/sampling_decisions.jsonl/...
<DATA_DIR>/archive/publications.jsonl/...
<DATA_DIR>/archive/payments.jsonl/...
<DATA_DIR>/archive/.lock                              held by a run (exclusive) and the backup (shared)
<DATA_DIR>/archive/retire-report.json                 the last retirement: what went, what was kept and why
<DATA_DIR>/vantages/de-1/archive/reachability.jsonl/...   another vantage's heartbeats, under vantages/de-1/archive/.lock
```

Every byte keeps its offset. Segment 1 holds bytes `[0, a)` of the file as
written, segment 2 `[a, b)`, and the live file starts at `b` (its *base*,
named in `index.json` by the SHA-256 of its first line). The collector's
ingest cursors and the export's `source_from`/`source_to` are these logical
offsets, so a rotation neither re-reads nor skips a line, a collector behind
it (or rebuilding from scratch) reads the segments first, and every export
is byte for byte what it would have been. `sentinel-recompute`,
`sentinel-measure-check` and a rebuild read segments then live file.

A run, per file: cut before the first line dated at or after the cutoff
(the start of the UTC day `-keep` ago; `scheduled_at` for measurements and
heartbeats, the other vantages' included, `decided_at` for sampling
decisions, `settlement_time` for publications, `time` for payments), never
past what the daily export has read, always leaving the last line; write
the segment, fsync it, read it back and match its digest; copy the rest to
a temp file; then, holding the file's exclusive `flock`, copy what was
appended since, write the index and rename the copy over the live file. The
writers (`sentinel-probe`, `observer-heartbeat`, `sentinel-scan` for
publications and payments, and `vantage-pull` for the other vantages'
heartbeats) append under a shared `flock` and reopen the path when it no
longer names the file they hold, so no line is lost or written twice. A
crash at any step leaves the record readable as before; the new segment
keeps a temp name until the index naming it is saved, and the next run
removes the leftovers. A second run the same day moves nothing. A live
file that ends inside a line at the swap is left as it is for the night
("ends inside a line ... a later run archives the file"), not a failure:
its writer finishes or cuts the line (the observer's own writers when
they restart; `vantage-pull`, which appends whole lines only, with its
next pull).

`publications.jsonl` and `payments.jsonl` are rotated only once the
newest scanner start in `runs.jsonl` says it follows a rotation
(`follows_rotation`, which `sentinel-scan` of this build and later
records): a scanner of an older build appends through descriptors it
opened once, and one not restarted since the upgrade would write on into
the file a rotation replaced, losing those publications and payments from
the record. Until then the run says `left as it is: ... restart fibre-scan
on this build first`.

A run reads each live file up to its cut, writes the segment and copies
the rest of the live file, on the disk the validator shares. `Nice=10` and
`IOSchedulingClass=idle` in the unit do not slow that on an NVMe disk with
the `none` scheduler, so the run paces itself: before each file it reads
`/proc/pressure/io` and, while `some avg10` is above 6 or `full avg10`
above 4, waits until `some avg10` has stayed below 2 for 20 seconds,
reading it every 5 seconds and never waiting longer than 30 minutes at a
time (`-pace-some`, `-pace-full`, `-pace-calm`, `-pace-calm-for`,
`-pace-max-wait` in `ARCHIVE_ARGS`; `-pace-some 0` turns it off). It never
waits inside a file's rotation, which holds `archive/.lock` (the backup
waits on it) and, at the swap, the live file's exclusive `flock` (the
writers wait on it). Each pause is a `pace|` line in the journal with the
pressure that started it, and another when it ends; one cut off at the 30
minutes says so, and the run goes on.

`-keep` must exceed the longest retention window in `state.json` plus a day
(the command refuses less): a restarted prober reads only the live files,
and treats a publication whose rows may have been archived as finished
instead of writing its slots again. With the default backfill
(`-backfill-missed 0`) that means a prober down for longer than `-keep`
does not write NOT_PROBED rows for slots older than the archive's cutoff.

```sh
sudo -u fibre-observer observer-archive -data-dir /var/lib/fibre-observer/mocha -dry-run   # what would move
sudo systemctl start fibre-archive@mocha                                                   # a run now
sudo -u fibre-observer observer-archive -data-dir /var/lib/fibre-observer/mocha -status    # base, live size, segments
sudo -u fibre-observer observer-archive -data-dir /var/lib/fibre-observer/mocha -verify    # every segment's digest
fibre-backup-manifest cat /var/lib/fibre-observer/mocha measurements.jsonl | wc -l         # the whole record
```

`-keep 336h` in `ARCHIVE_ARGS` in the env file keeps two weeks live. The
backup copies `archive/` and each `vantages/<name>/archive/` before the live
files, and its manifest names every segment and the live base; `restore.sh`
and `verify` check each segment. The small record files (host history, the
collector's own logs, runs) are not archived: their writers hold them open
without the lock. `vantage-pull` resumes each vantage's file from its
logical end (`observer-archive -logical-end vantages/<name>/reachability.jsonl`,
the live file's base plus its size), not from its size, so a rotated file
goes on where the record ends. Do not run `observer-archive` on the second
vantage's own host: the file there is what the pull reads its offsets from,
and a file shorter than the local record stops the pull ("Second vantage"
below).

Upgrade order, for the build that adds publications, payments and the
other vantages to the rotation and retires local copies (and any later
one that changes what is rotated). On the live observer the timer is
already enabled, and a 04:40 run between installing the binaries and
restarting the writers would rotate files under writers of the older
build, so it is stopped first and started again last:

```sh
sudo systemctl stop fibre-archive@mocha.timer
sudo install -m 0755 fibre-sentinel/bin/* /usr/local/bin/
sudo install -m 0755 deploy/vantage-pull.sh /usr/local/bin/fibre-vantage-pull
sudo install -m 0755 deploy/backup.sh /usr/local/bin/fibre-backup
sudo install -m 0755 deploy/backup-manifest.py /usr/local/bin/fibre-backup-manifest
sudo install -m 0755 deploy/healthwatch.sh /usr/local/bin/fibre-healthwatch
sudo cp deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart fibre-collector@mocha       # applies migrations, if any
sudo systemctl restart fibre-scan@mocha fibre-probe@mocha fibre-heartbeat@mocha fibre-api@mocha
sudo systemctl start fibre-archive@mocha.timer
```

The deploy scripts change with the binaries: an older pull resumes by
size and would fetch a rotated vantage file's older bytes again, an older
backup copies no vantage archive and proves no export on the remote, and
an older manifest tool cannot read a retired segment. The writers are
restarted at once, the scanner among them, not after a warm-up: the
archive run rotates `publications.jsonl` and `payments.jsonl` only once a
scanner of this build has started (above), but the timer stays off until
every writer follows a rotation. The first retirement runs that night,
once the backup has recorded a finished copy and its remote proofs; it
checks at most `-check-days` days against the store a night ("Retiring
local copies" below).

### Retiring local copies

A segment's gzip file is one of three copies of its lines on this disk:
they are also in the daily exports, and the store writes each
publication, reading, endpoint check and payment back to its line byte for
byte (`docs/SYSTEM.md`, "The slim record"). The second step of
`fibre-archive@<network>`,
`observer-archive -retire -db <DATA_DIR>/observer.db`, removes a segment's
file, and nothing else, once every byte of it is

1. in daily exports intact on this disk: each export whose member of the
   file overlaps the segment (`exports/index.json`), tarball and member
   matching their digests;
2. given back by the store, line by line, byte for byte, for each of those
   members: `record-verify`'s check, kept per tarball digest in
   `exports/verified.json`; a day not in it yet is checked first, against
   the store opened read-only;
3. on the remote backup with the same SHA-256: an `"ok": true` line for
   the tarball's current digest in `exports/remote.jsonl`, written by
   `fibre-backup` after its copy (the archive unit has no network), read
   from the remote `fibre-backup` copies to now; and that copy last
   finished within `-remote-max-age` (default 48 h,
   `exports/remote-copy.json`). A tarball is proven on the remote once,
   so the proof stands for the remote's copy only while the copies to that
   remote go on finishing: a remote lost, emptied or locked out shows as
   a backup that no longer finishes (one failed night is waited out, two
   are not), and a new `BACKUP_REMOTE` as a copy to another remote, whose
   proofs the backup then takes again. Until then every segment is kept,
   with that reason;
4. itself on the remote: archived before that last copy finished, which
   took its file (the backup copies the archive first, holding the archive
   lock shared). A segment archived since waits a night, so every retired
   segment's file is on the remote too, and a build from before retirement
   can have it copied back ("Going back past schema 27").

`record.Retire` then, under the file's archive lock, reads the segment
back from those exports once more (every tarball and member digest, the
range's length, lines and SHA-256 against the segment), saves the
segment's `retired` record (the exports, the member name, the exports
directory relative to the archive directory, the proof in words) first in
`archive/<file>/retired.json` and then in `index.json`, and only then
removes the file. A crash in between leaves the file, which readers go on
reading and the next run removes. `retired.json` is the copy an
`observer-archive` from before retirement never touches: it rewrites
`index.json` without the records it does not know, and every reader of
this build takes a missing record from `retired.json` (and this build's
next archive run writes it back into the index). A segment that misses
any of the four is kept, and the run says why: one line per file
(retired now, retired before, kept, bytes freed), one per kept segment
(`2026-09-25 measurements.jsonl: 22560 lines sampled out, not
reproducible from the store`, `2026-10-07 not yet proven on the remote`),
and the same in `archive/retire-report.json`. A kept segment is the
normal answer for a recent day, not a failure; the unit fails only on an
error.

Its reads are the heavy part, so what keeps a segment is found before
anything is read: a day not proven on the remote, one the ledger already
says the store does not give back, or a backup that has not finished
lately keeps the segment without a tarball read, night after night. Only
a segment that would go has each of its exports read whole for its
digest, and a day `exports/verified.json` does not hold yet read again
with every line looked up in the store; then `record.Retire` reads it
back from its exports. A run checks at most `-check-days` days (default
3) against the store; the segments of the days past that are kept (`not
checked against the store yet`) for the next nights, so the first run,
which finds the ledger empty and every exported day before it, spreads
that over several nights rather than keep the disk busy for hours. The
retirement paces itself as the archive step does (above: the same flags,
the same `pace|` lines): it waits before each export it reads, inside the
store check before each member of the tarball and every 10,000 lines (no
read transaction stays open between two lookups, and the ledger is
written only after the check), and before each segment it retires. It
never waits inside `record.Retire`, whose read back holds `archive/.lock`:
a pause there would hold back with it any backup waiting on that lock
(which gives up after two hours). The timer is not `Persistent`: a night
missed while the host was down is not made up at boot, when the
validator catches up and the disk is at its busiest, but by the next
04:40 run.

Never retired:

- the live files: their bytes move only by rotation, into a segment;
- `registry.jsonl`, `runs.jsonl`, `sampling-secrets.jsonl`,
  `sampling_decisions.jsonl`, `amendments.jsonl`, `host_history.jsonl`,
  `param_uncertainty.jsonl` and `corrections.jsonl`: the store does not
  keep them line by line, so nothing shows their exported lines to be the
  record a second way (sampling decisions are archived; their segments
  stay);
- the exports, `state.json` and the store: a retired segment is read back
  from the exports, which the backup copies and never deletes.

A segment whose day the store does not give back whole (the NOT_PROBED
rows of a publication it keeps as one sampling decision, a line it dropped
as a repeat) stays as long as that holds; once the store has caught up,
`record-verify -db <DATA_DIR>/observer.db -exports <DATA_DIR>/exports -day <day> -ledger <DATA_DIR>/exports/verified.json`
checks the day again and replaces its entry. It pauses as the retirement
does (the same `-pace-*` flags and defaults, `pace|` lines), so it can be
run by hand on the live host.

Every reader of the whole record reads a retired range from the exports
(`internal/record`), held to the same digests, in two passes: the first
proves every tarball, member and the range and hands out nothing, the
second hands out each block once it matches the first, so a reader gets
the exact bytes or an error, never fewer or other lines. That covers a
rebuild from zero, `sentinel-recompute -data-dir <DATA_DIR>`,
`sentinel-measure-check`, `restore.sh` and `persistence.sh` (a restored
copy reads its own `exports/`, which the backup carries;
`fibre-backup-manifest snapshot` carries the exports its retired segments
name) and `fibre-backup-manifest cat`. The export builder reads only bytes
it has not exported yet, which are never retired, and the scanner and the
prober read nothing retired at start. The schema rollback is in
"Going back past schema 27" above.

This was tested on a copy of the Mocha record of 24 September to 5
October (3.0 GB of record files, its 19 daily exports, a store built from
it, a local directory as the remote). Everything dated before 29 September
was archived into five segments. With no finished copy recorded, a copy
finished before the segments were archived, a copy to another remote, a
tarball read back from the remote with another digest, a failed or
missing remote check, or a copy 72 hours old, nothing was retired and
each segment said why; with one byte of a local tarball altered nothing
was retired and the run failed. With every condition met, three segments
went (publications 383.8 MB, endpoint checks 8.4 MB, payments 0.8 MB of
gzip) and two stayed: the readings, for the 22,560 sampled-out lines of 25
September, and the sampling decisions; a second run retired nothing more.
A store rebuilt from zero over the retired directory gave the same 18,274
API answers, byte for byte, as one built before anything was archived,
and `sentinel-recompute` the same output (24 h, 7 d, all: everything
matches). The build before retirement gave those 18,274 answers too, both
over the record as it was and, rebuilt from zero, over the retired
directory once the retired files were copied back from the remote.

The nightly order, all UTC:

| when | unit | what |
|---|---|---|
| 03:00 | `fibre-collector@` (`-export-hour`) | the previous day's export |
| 03:17, plus up to 20 min | `fibre-backup@` | the manifest's cut; the copy, segments first, then the live files and the exports; the finished copy recorded (`exports/remote-copy.json`); then the remote proof of each export not yet proven on that remote |
| 04:40 | `fibre-archive@` | rotation, then retirement (at most `-check-days` days checked against the store), each pausing while the disk is busy |

The backup holds `archive/.lock` (and each `vantages/<name>/archive/.lock`)
shared from the cut to its last check, and an archive run holds a file's
lock exclusively, so the two never overlap: a backup still running at 04:40
delays the archive run, and an export proven a night late retires its
segments a night late.

```sh
sudo systemctl start fibre-archive@mocha                                     # rotation and retirement now
jq . /var/lib/fibre-observer/mocha/archive/retire-report.json               # what went, what stayed and why
tail -n 3 /var/lib/fibre-observer/mocha/exports/remote.jsonl                # the newest remote proofs
cat /var/lib/fibre-observer/mocha/exports/remote-copy.json                  # the backup's last finished copy
```

### Runbook

- **Health is 503.** Read the `checks` list: it names the process or
  condition. The details are short on purpose; the errors themselves are
  in the journals. A dead process:
  `journalctl -u fibre-<name>@<network> -n 100`. A `scanner_lag`: the
  RPC node is behind or slow; the scanner catches up on its own. A
  `scan_gaps`: the node could not serve those heights (pruned, or results
  it never kept), or the
  operator skipped them (each range's `reason` says which, see below); point
  the scanner at a node that keeps them and delete `gaps` from `state.json`
  after re-scanning from the lowest gap height with `-start-height`, or
  accept the gap (the dashboard says which blocks). A `pin`: see below.
- **A process has `no completed cycle`, or `ingest` fails.** Its loop is
  stuck while the process lives: on 7 October 2026 the collector waited on
  its own store connection and stopped ingesting. Keep the evidence, then
  let it restart: `sudo systemctl kill -s QUIT fibre-<name>@<network>`
  writes every goroutine's stack to the journal and ends the process, and
  `Restart=always` starts it again. Save
  `journalctl -u fibre-<name>@<network> -n 2000 --no-pager` and report it.
  The collector reads on from its cursors; nothing is lost.
- **`work` fails.** One collector stage has failed for over ten minutes;
  the detail names it and how long (the stages are in the table above).
  `journalctl -u fibre-collector@<network> -n 500` has the error. An
  `export` that keeps failing holds the nightly backup's proof and the
  retirement back, so it is the one to look at first.
- **`chain_polls` fails.** The collector's polls of the node fail, and the
  site keeps serving the last values. The detail names the stale polls.
  Check the node (`deploy/test/rpc-check.sh`) and the collector's journal.
- **`vantages` fails.** Another vantage's endpoint checks stopped
  arriving: its heartbeat died on its host, or the pull fails
  (`journalctl -u fibre-vantage-pull@<network> -n 50`). Until it is back,
  an endpoint is no longer checked from the second location.
- **`api_errors` or `snapshots` fails.** A route answered a 5xx other
  than 503, or a window's figures stopped refreshing; the detail names the
  route or the window. `journalctl -u fibre-api@<network> -n 200` has the
  error.
- **`records` fails.** The API met a stored row it cannot decode. It no
  longer takes a page down: the row is published with that part marked (a
  blob's `reconstructable` status `unknown`, a reading's `row_indices` left
  out). `journalctl -u fibre-api@<network> | grep 'does not decode'` names
  each row once; keep those lines and report them. The stored row is not
  changed, so a build that reads it again clears the check 15 minutes after
  the last one met.
- **The collector says `has no index.json but holds N export tarball(s)`.**
  `exports/index.json` is gone while the tarballs are there. The export
  builder stops rather than start a new index listing one day: the API
  would stop listing every older export, every retired segment is read
  back by its export's entry there, and the next backup would copy the
  short index over the remote's good one. Put the last good copy back as
  the service user (`rclone copyto '<BACKUP_REMOTE>/mocha/exports/index.json'
  /var/lib/fibre-observer/mocha/exports/index.json`, `RCLONE_CONFIG` as in
  the unit); the next export run goes on. A day built after that copy was
  taken has no entry in it: each tarball carries its own `manifest.json`,
  from which the entry is made again (the entry is the manifest's fields
  with the tarball's `name`, `bytes` and `sha256`, and `signature` from
  `<name>.sig` when the export is signed).
- **The chain upgraded past the pin** (`pin_status: chain_ahead`). The row
  assignment this observer computes depends on constants pinned to a
  celestia-app commit (`fibre-assign/params.go`), and a new major may change
  them. Until the pin is bumped, verdicts about who holds which rows may be
  wrong, and the site shows a banner. To bump: diff `fibre`, `x/fibre`,
  `x/valaddr`, `proto` and `specs` between the pinned commit and the release
  tag, update `PinnedCelestiaAppCommit`, `PinnedCelestiaAppVersion` and the
  `celestia-app` line plus the copied `replace` block in
  `fibre-sentinel/go.mod`, re-run `fibre-assign/reftest`, rebuild, deploy.
  The bump also adds the pin it leaves to `assignPins` in
  `internal/slim` when reftest is bit-identical across the two: the slim
  record computes a validator's rows again only under a pin listed there,
  and refuses a record derived under any other rather than compute other
  rows (`docs/SYSTEM.md`, "The slim record").
  `pin_status` compares only the major version: a new release of the same
  major (v10.x) reads `matches` whatever it changed. So the same diff is
  done by hand for every release a network moves to, and before mainnet
  starts ("7d. Mainnet: a fresh install", step 1).
- **The scanner refuses `START_HEIGHT`, or waits for the node's history.**
  A fresh scan needs the node to hold the promise window (1000 blocks)
  below its start. A refused `START_HEIGHT` names the lowest height the
  node can serve: set that, or 0, or use a node with more history. A scan
  from the tip waiting on a freshly synced node starts by itself once the
  tip reaches the height its journal names.
- **A publication with no assignment** (`unassignable_publications` > 0;
  `/v1/health` fails while one settled in the last 24h, then lists it passing):
  a blob version this build does not know. Same bump; the scanner does not
  re-scan settled publications, so re-scan from that height afterwards.
- **A process crash-loops.** `journalctl` shows the reason at the top of
  each attempt; the crash dump is the last 300 log lines. A full disk, an
  unreadable data directory or a chain-id mismatch are the known causes;
  none of them is an RPC outage.
- **The scanner crash-loops on one block.** The scanner exits, rather than
  guess, on a block it cannot make sense of: a params event or provider
  registration it cannot parse, a publication it cannot build for a reason
  other than the node lacking the height, a block whose tx and result
  counts disagree. None is expected (the formats match upstream), but if one
  fires, `Restart=always` brings it back to the same height and it dies
  there again, forever, and the feed stops. Every such exit ends with
  `to move past this height, restart with -skip-heights N`. To do that:

  ```
  journalctl -u fibre-scan@mocha -n 300 --no-pager > /root/scan-crash-<N>.log   # keep the evidence
  echo 'SKIP_HEIGHTS=<N>' >> /etc/fibre-observer/mocha.env                       # or edit the existing line
  systemctl restart fibre-scan@mocha
  journalctl -u fibre-scan@mocha -n 50 --no-pager | grep -E "SKIP|skip-heights"
  curl -s localhost:${API_LISTEN}/v1/health | jq '.scan_gaps'
  ```

  The value is comma-separated heights and ranges (`1234,2000-2005`); one
  that does not parse stops the scanner at startup rather than skip the
  wrong thing. A listed height is not read at all — not its publications,
  params changes, host registrations or escrow movements — and is recorded
  as a scan gap with the reason `skipped by the operator (-skip-heights)`,
  exactly like a height the node could not serve: `state.json`, the API's
  `scan_gaps`, the `scan_gaps` health check (health reads degraded while it
  stands) and the site all show it, and a publication settled in it is
  unknown, never counted served or unserved. The flag value is also in
  `runs.jsonl` with the rest of each start's config. Nothing is skipped that
  is not listed. It is safe to leave set: once the scan is past the height
  it is inert (the scanner says so at every start) and a height already on
  record as a gap is never added twice. Clear `SKIP_HEIGHTS` at the next
  planned restart anyway, so it cannot bite a later re-scan. Skip only the
  height the exit names, and report the crash dump: the real fix is a build
  that reads the block. After that fix, re-scan the height the same way as
  any other gap (the `scan_gaps` item above) with `SKIP_HEIGHTS` empty.
- **Moving to a new host.** Copy the data directory (or restore from the
  two backups), install the binaries and units, copy `/etc/fibre-observer`,
  keep the same `VANTAGE` name if the egress addresses stay the same and a
  new one if they do not: the vantage is what a validator matches its logs
  against.

## 7a. Activation day

Fibre exists only from app version 10. A vantage started before the upgrade is
watching a chain where `x/fibre` and `x/valaddr` do not answer, and most of it
recovers on its own the moment they do.

What self-heals, without touching anything:

- The scanner's params seed. It retries every 60 heights while the module is
  inactive and seeds the first time the query succeeds.
- The scanner's host registry. Same cadence: it retries until the bonded
  registry can be read. (Before this was added, a scanner that lived through
  activation had no host history and no back-fill for the rest of its life,
  and only a restart fixed it.)
- `fibre_active` and `app_version` in the store, which the collector re-reads
  on every pass, and the "not active yet" banner the site draws from them.
- The prober's app-version poll, which lifts the stale-pin hold on verdicts.
- The heartbeat's inactive path, which logs once and reports OK rather than
  failing.

What to check once the upgrade lands, in this order:

```
curl -s localhost:${API_LISTEN}/v1/meta | jq '{fibre_active, app_version, chain_height}'
curl -s localhost:${API_LISTEN}/v1/health | jq '.status, (.checks[] | select(.ok == false))'
journalctl -u fibre-scan@mocha -n 50 --no-pager | grep -iE "seed|param|host history"
curl -s localhost:${API_LISTEN}/v1/network | jq '{registered_endpoints, reachability, reachability_window}'
```

Before that, the "not live yet" notice on the overview and the header chip
show the height x/signal scheduled the version that brings Fibre at and an
estimate of when the chain reaches it, once there is one, and the validator
table marks each bonded validator `signalled` or `not signalled`
(`upgrade_signal.{upgrade_height, eta_seconds}` on `/v1/meta`,
`signaled_upgrade` on each row; both disappear once the chain is on that
version). The height and the estimate are published only when the upgrade
x/signal scheduled is to app version 10, the one that brings Fibre (the
collector stores the scheduled version as `signal_upgrade_app_version`);
an upgrade to another version shows no Fibre countdown.

`registered_endpoints` moving off zero is the first sign the registry is being
read. `reachability` follows within a heartbeat interval, and
`reachability_window`, which pools every check in the window, with it. The
activation changes the snapshots' revision, so for the half minute or so the
API takes to recompute the 24h window, `/v1/network` answers 503 with
`"computing": true` and the last line prints `null` for all three: ask again.
Publications appear only once somebody actually pays for a blob, which may be
hours later; an empty publication feed on activation day is a quiet network,
not a broken observer, and the site says which.

What does not self-heal: a scanner that exits on the same block at every
restart (`systemctl status fibre-scan@mocha` shows it cycling; the last line
of each crash dump ends `restart with -skip-heights N`). The first real
Fibre blocks are when a format this build misreads would show up. Do not
wait for a build: follow the Runbook item "The scanner crash-loops on one
block", which moves past that height and publishes it as a scan gap.

## 7b. Hosting and concentration (optional)

The site can show which network provider and country each registered Fibre
host resolves into (a "Hosting" column in the validators table, a
concentration panel on the overview, `hosting` on every row of
`/v1/validators`, and `/v1/hosting`). The Foundation Delegation Program asks
recipients not to run on Hetzner or OVH, and delegators care how
concentrated the set is; nothing else on the site answers either.

It is **off until you download two database files**. The collector makes no
network call for it, ever: the addresses are the ones the heartbeat already
resolved and stored on its reachability rows, and the IP-to-network mapping
is read from local files.

| file | source | licence |
| --- | --- | --- |
| `ip2asn-combined.tsv.gz` (required) | [iptoasn.com](https://iptoasn.com/) — IPv4+IPv6 range → origin AS, AS name, AS registry country | Public Domain, [ODC PDDL v1.0](https://opendatacommons.org/licenses/pddl/1-0/) |
| `dbip-country-lite.csv.gz` (optional) | [DB-IP IP to Country Lite](https://db-ip.com/db/download/ip-to-country-lite) — range → country (geolocation estimate) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/); the site prints the required "IP Geolocation by DB-IP" credit |
| `dbip-city-lite.csv.gz` (optional, ~85 MB) | [DB-IP IP to City Lite](https://db-ip.com/db/download/ip-to-city-lite) — range → city, region, coordinates (adds `city`/`region`/`lat`/`lon` to each validator's `hosting`, which places it on the overview map; absent file = country only; `HOSTING_CITY_DB` / `-hosting-city-db` to move it, `HOSTING_SKIP_CITY=1` to skip it) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), same credit |

Considered and not used: CAIDA's AS-to-Organization dataset (and the
RouteViews pfx2as files usually paired with it) is under CAIDA's acceptable
use agreement, which is not a licence to republish; MaxMind GeoLite needs an
account key and has its own EULA.

Enable it, per network, as the service user:

```
sudo install -m 0755 deploy/hosting-db.sh /usr/local/bin/fibre-hosting-db
sudo -u fibre-observer fibre-hosting-db /var/lib/fibre-observer/mocha/hosting
```

(the path is `<DATA_DIR>/hosting`; the script downloads to a temp name,
checks that it gunzips whole (`gzip -t`), its format and its line count,
and renames, so a failed download never replaces a good file, nor does
one the server sent complete but whose gzip stream is cut short or fails
its CRC, which would pass the line count and then fail every lookup until
the next month's refresh). Within one endpoint poll (a minute) the collector
logs `hosting: N open endpoint(s), N resolved, N with an origin AS`, and
`/v1/hosting` answers `"enabled": true`. No restart, no unit change. To keep
the files elsewhere, set `HOSTING_ASN_DB=` and `HOSTING_COUNTRY_DB=` in the
network's env file (or pass `-hosting-asn-db` / `-hosting-country-db`), then
restart the collector.

Refresh it monthly (DB-IP publishes monthly; iptoasn hourly) with the
timer that runs the same script as the service user:

```
sudo systemctl enable --now fibre-hosting-db@mocha.timer
```

It is `Persistent`, so a month missed while the host was down is made up
after boot. It refreshes `<DATA_DIR>/hosting` only: files kept elsewhere
(`HOSTING_ASN_DB=`) are refreshed by whoever put them there, and
`HOSTING_SKIP_COUNTRY=1` / `HOSTING_SKIP_CITY=1` in the env file skip the
DB-IP files. `/v1/hosting` says when each file was last changed, and the
`hosting_db` health check fails once the IP-to-ASN file is over 45 days
old: the refresh has stopped. The collector re-runs the lookup when a file's
size or mtime changes. Removing `ip2asn-combined.tsv.gz` turns the feature
off again, and the next pass clears the stored lookups so the API never
serves one that can no longer be reproduced.

What it is and is not: the provider is a mapping from the **origin AS
number** (the list, with sources, is in `observer/hosting/providers.go` and
published as `provider_asns` on `/v1/hosting`); anything not on the list is
"Other", which the concentration counts never treat as a single entity.
Everything is "as resolved from this vantage": GeoDNS, proxies, tunnels and
anycast can hide where a host really runs, and a geolocated country is an
estimate. The site shows it without a verdict. The table the lookups live in
(`endpoint_hosting`) is derived state, created by the collector with
`CREATE TABLE IF NOT EXISTS` rather than a schema migration; it is not in the
exports and needs no backup.

### Atom feeds

Nothing to deploy. `/api/v1/feed.atom` (network: registrations, host
changes, bonded-list changes, first faults, observer incidents) and
`/api/v1/validators/<address>/feed.atom` (one validator's endpoint state
changes) are derived per request from the store, cached five minutes, and
carry `ETag`/`Last-Modified`. Entry IDs are `tag:` URIs built from the host
name the feed is requested at, the chain id, the validator and the moment
the change happened, so they stay stable across restarts: do not change
the public host name casually, or every subscriber sees the last 30 days
again once.

## 7c. Second vantage (endpoint confirmation)

An endpoint that fails from the observer is checked again from a second
location before it is called unreachable. The second vantage is only
`observer-heartbeat` on another host, under its own `-vantage` name; it
needs outbound access alone (no open port) and reads the bonded provider list
from any Mocha RPC:

```
# on the second host (here the backup server, sftp account tensile-backup)
groupadd --system tensile-vantage
useradd --system --no-create-home --shell /usr/sbin/nologin --gid tensile-vantage tensile-vantage
usermod -aG tensile-vantage tensile-backup      # the sftp account may read the record
install -d -o tensile-vantage -g tensile-vantage -m 0750 /srv/tensile-vantage /srv/tensile-vantage/de-1
ln -s /srv/tensile-vantage /srv/tensile-backup/vantage   # sftp path vantage/de-1/...
install -m 0755 observer-heartbeat /usr/local/bin/tensile-heartbeat
# unit: User=tensile-vantage, Group=tensile-vantage, UMask=0027, ProtectSystem=strict,
#   ReadWritePaths=/srv/tensile-vantage/de-1, ExecStart=/usr/local/bin/tensile-heartbeat
#   -rpc https://<mocha rpc>:443 -data-dir /srv/tensile-vantage/de-1 -vantage de-1 -interval 5m
```

The vantage service has its own group on purpose: it parses TLS answers from
every registered host, so it must not be able to write the backups kept under
the same account.

On the observer, `fibre-vantage-pull@<net>.timer` fetches each vantage's
record once a minute over the backup account's sftp-only key, appending only
new bytes, into `<data-dir>/vantages/<name>/reachability.jsonl`; the collector
ingests every file there, and the daily export, the archive and the
retirement treat it as they treat the observer's own heartbeats. The pull
is `rclone` (1.60 or later) with the sftp remote given whole on its command
line, so it needs no rclone config: it asks for the file's size
(`rclone lsf`), then appends what lies past the local file's logical end
(`rclone cat --offset`, the end from `observer-archive -logical-end`),
holding the local file's shared `flock` as the observer's own writers do.
One pull of a vantage runs at a time (`vantages/<name>/.pull.lock`, taken
exclusively without waiting): two would read the same end and append the
same bytes twice, so a pull run by hand beside the timer's leaves the
vantage to the one already running. A remote file shorter than that end was replaced or cut on the vantage:
the pull fetches nothing, says so and fails until someone has looked; the
local record is never cut to match. rclone checks the host key against
`VANTAGE_PULL_KNOWN` and may settle on another key type than OpenSSH did,
so that file should hold every key the host offers (`ssh-keyscan <host>`,
all types). Check it once as the service user:
`sudo -u fibre-observer rclone lsf --format s ':sftp,host=<host>,user=tensile-backup,key_file=/etc/fibre-observer/backup_ed25519,known_hosts_file=/etc/fibre-observer/backup_known_hosts,shell_type=none:vantage/de-1/reachability.jsonl'`
prints the remote file's size. Configure it in the network's env file:

```
VANTAGE_PULL_HOST=tensile-backup@85.10.211.222
VANTAGE_PULL_NAMES=de-1
```

```
install -m 0755 deploy/vantage-pull.sh /usr/local/bin/fibre-vantage-pull
cp deploy/systemd/fibre-vantage-pull@.{service,timer} /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now fibre-vantage-pull@mocha.timer
```

The `vantages` health check fails when a vantage seen in the last seven
days has sent no endpoint check for 20 minutes, whether its heartbeat died
or the pull fails.

Each network needs its own second vantage: another heartbeat instance on
the same host, against that network's RPC, under its own vantage name and
directory (for mainnet, for example, `de-1-mainnet` in
`/srv/tensile-vantage/de-1-mainnet`, with a unit of its own), and
`VANTAGE_PULL_NAMES` in that network's env file. A heartbeat row does not
name its chain, so the collector checks two things it does carry: its
validator must be one this chain knows, and its height one this chain
could have been at. A file pulled from the other network stops at its
first row: nothing of it reaches the record, and the collector names the
file and reports failing on every pass until the file is moved out of
`vantages/`.

The second vantage runs the heartbeat and nothing else. A blob's reading is
this observer's own, as one client's download is, and nothing asks the
second vantage to read it again. Earlier builds pushed confirmation requests
to `vantage/<name>/inbox/`; nothing writes or reads that inbox now, and
`VANTAGE_PUSH` in the env file is ignored.

`deploy/test/vantage-sync.sh` checks the pull against the fake rclone
(`deploy/test/fake-rclone.sh`) and a fake `observer-archive`: the first
pull, an append, a rotated local file resumed from its logical end, a
remote shorter than the local record, two pulls at once, and a failed or
missing fetch.

## 7d. Mainnet: a fresh install

Mainnet runs on a server of its own, with a node of its own. Everything
above holds for it with `mainnet` as the instance; these are the steps that
differ from Mocha's, in order.

1. **Check the celestia-app release.** The row assignment is pinned to the
   commit Mocha runs (`PinnedCelestiaAppCommit` in `fibre-assign/params.go`,
   `v10.4.0-mocha`). `pin_status` compares only the major version, so any
   v10 release reads `matches`, whatever it changed. Before mainnet starts,
   diff the release tag mainnet runs against the pinned commit, in a
   celestia-app checkout:

   ```bash
   git diff 5187d2fb5eb8bc4b534c74724882943c54253ae9 <mainnet tag> -- fibre x/fibre x/valaddr proto specs
   ```

   Then run the reference test against that tag: in
   `fibre-assign/reftest/go.mod`, require the tag and copy the `replace`
   block from celestia-app's `go.mod` at that tag (its README says how),
   and `cd fibre-assign/reftest && go test ./...` must pass. If the diff
   touches `fibre/protocol_params.go`, `fibre/blob.go`, the shard download
   or the `MsgPayForFibre` proto, bump the pin before anything else (Runbook,
   "The chain upgraded past the pin"). Do the same again for every release
   mainnet moves to.
2. **The node.** A full node with `storage.discard_abci_responses = false`.
   A node restored from a state-sync snapshot holds nothing below it: let
   it build history until
   `sudo RPC_CHAIN_ID=celestia deploy/test/rpc-check.sh "$RPC"` passes
   (6000 blocks behind the tip). The scanner refuses a `START_HEIGHT` the
   node cannot serve, and a scan from the tip waits for the 1000 blocks it
   needs (section 3).
3. **The env file.** `/etc/fibre-observer/mainnet.env` from
   `deploy/observer.env.example`, installed as in section 3
   (`sudo install -m 0640 -o root -g fibre-observer deploy/observer.env.example /etc/fibre-observer/mainnet.env`):
   `NETWORK=mainnet`, `RPC` (the mainnet
   node), a `VANTAGE` name of its own,
   `DATA_DIR=/var/lib/fibre-observer/mainnet`, `POLICY=` empty,
   `API_LISTEN=127.0.0.1:8080`, `START_HEIGHT=0` or a height the node holds,
   `END_READ_SINCE=` empty, `VANTAGE_LOCATION`, `VANTAGE_PROVIDER`, the
   alert settings and `BACKUP_REMOTE`. With site-server,
   `/etc/fibre-observer/site-mainnet.env` with `SITE_LISTEN` (a port of
   its own) and `API_LISTEN=127.0.0.1:8080` (section 5).
   `publishers-mainnet.yaml` only if you label publishers.
4. **The units.** Section 4 with `mainnet`: the data directory
   `/var/lib/fibre-observer/mainnet`, then
   `sudo deploy/test/smoke.sh "$RPC" mainnet`, then
   `fibre-scan@mainnet fibre-probe@mainnet fibre-heartbeat@mainnet fibre-collector@mainnet fibre-api@mainnet`
   and the timers `fibre-healthwatch@mainnet.timer
   fibre-backup@mainnet.timer fibre-archive@mainnet.timer` (and
   `fibre-hosting-db@mainnet.timer` with the hosting lookup, 7b).
5. **The site.** A build of its own (section 5):
   `NEXT_PUBLIC_API_URL=https://<mainnet site>/api/v1`,
   `NEXT_PUBLIC_SELF_VALIDATOR=<the operator's mainnet celestiavalcons1…>`
   and the same `NEXT_PUBLIC_NETWORKS` as Mocha's build. Serve it with
   Caddy from `/var/www/fibre-observer/mainnet`, or with site-server:
   `site-server.cjs` in `/usr/local/lib/fibre-observer/`, the build in
   `/srv/fibre-site/mainnet`, `fibre-site@mainnet` enabled, the front
   proxy pointed at `SITE_LISTEN`. When `NEXT_PUBLIC_NETWORKS` changes,
   build and copy Mocha's site again too, so its switch links to mainnet.
6. **Backups.** `litestream-mainnet.yml` with both `<network>` lines set to
   `mainnet`, and `fibre-litestream@mainnet`. The nightly copy writes under
   `BACKUP_REMOTE/mainnet` by itself.
7. **The second vantage.** A heartbeat instance of its own against a
   mainnet RPC, and `VANTAGE_PULL_NAMES` in `mainnet.env` (7c).
8. **The checks.** Section 8 with `mainnet`, and
   `sudo bash -c 'set -a; . /etc/fibre-observer/mainnet.env; fibre-healthwatch mainnet --test'`
   to prove the alerts arrive.

## 8. Checks after deploy

Run the smoke test first. It installs nothing and changes nothing: it parses
every unit, runs each one's own `ExecStart` with its own `EnvironmentFile` as
its own `User`, and checks that each reaches the chain and writes where it
should. That catches a binary in the wrong place, a flag that no longer
exists, a variable the env file never sets, and a directory the service user
cannot write, which is most of what actually goes wrong on a first deploy.

```bash
sudo deploy/test/smoke.sh http://127.0.0.1:26657 mocha
```

It does not test the sandboxing directives, which is what the
`systemd-analyze verify` step inside it is for, and it does not replace
starting the units for real:

```bash
sudo systemctl start fibre-scan@mocha fibre-heartbeat@mocha fibre-collector@mocha fibre-probe@mocha fibre-api@mocha
systemctl --no-pager status 'fibre-*@mocha' | grep -E 'Active|Loaded'
curl -s https://mocha.observer.example.org/api/v1/health | jq .status   # "ok" once every process has run a cycle
curl -s https://mocha.observer.example.org/api/v1/network | jq .registered_endpoints   # null while the API still answers "computing": true, its first half minute or so: ask again
```

Then check the vantage is described: with `VANTAGE_LOCATION` or
`VANTAGE_PROVIDER` blank the API logs a warning at startup.

```bash
journalctl -u fibre-api@mocha --no-pager | grep 'vantage not fully described'   # no output is right
```

Check that the scanner reads each block as the node announces it, and that
the collector reads the scanner's files as they change:

```bash
journalctl -u fibre-scan@mocha --no-pager | grep 'block subscription'      # "up", and no "unavailable" after it
journalctl -u fibre-collector@mocha --no-pager | grep 'fast tick:'        # "... and within 100ms of a change to one of them"
```

Confirm the provider you declared is the one your traffic actually carries:

```bash
whois -h whois.radb.net "$(curl -4 -s https://ifconfig.co)" | grep -i origin
```

Before Fibre activates on the chain, `publications` stays 0 and the site
shows the "0 Fibre publications" notice; that is the expected state.

### Acceptance tests

Six scripts under `deploy/test/` turn the questions an operator should be
able to answer before trusting the site into checks that pass or fail. Each
reads `/etc/fibre-observer/<instance>.env`, needs only `python3`, `curl` and
the installed binaries, and exits 0 only when every check passed. Run them in
this order the first time; the first two take seconds, the middle three take
minutes and touch the running services, the last one takes a day.

| script | question | touches | time |
|---|---|---|---|
| `rpc-check.sh <rpc> [rpc2]` | is the RPC node on the expected chain (`RPC_CHAIN_ID`, default `mocha-5`; `celestia` on mainnet), in sync, and keeping `block_results`, the validator set and historical state as far back as the observer reads (6000 blocks)? With a second node, do the two agree on a block hash? | nothing | seconds |
| `exposure.sh` | is only ssh/http/https reachable from outside, is every unit enabled for a reboot, does HTTPS reach the API through Caddy, does a test alert actually arrive at every destination set (the webhook, the Telegram chat; neither set fails), are the env file, `rclone.conf` and litestream's env file closed to other accounts, is a sampling master key, if one is left, `600`? | posts one test message | seconds |
| `persistence.sh` | live: do the checkpoints survive a restart, is the database sound? On one consistent cut of the record: no duplicate line, a rebuild from the cut alone holds exactly its records, and `sentinel-recompute` agrees with a second API serving that same cut, both as of the cut's timestamp | restarts collector + scanner; rebuilds into a temp dir; a throwaway API on `:18082` | minutes |
| `restore.sh` | does the nightly copy verify against its manifest (every file present, at least the cut, hash and record count equal, every line a JSON record but the ones the cut lists, a live file rotated since read back from the copy's segments, the cut's own `state.json` put in place of the copy's, no master key), rebuild to exactly the cut's records, and serve them from a second API on a spare port? | starts a throwaway API on `:18081` | minutes |
| `outage.sh` | when the chain source is cut, does the site say so within twelve minutes and keep serving its last figures; when it returns, does the scanner catch up with no gap, no lost row and no duplicate; when every process is stopped and started, is nothing lost? | edits the env file (restored on every exit path), restarts and stops units | ~30 min |
| `resource-watch.sh run` / `summarize` | over a day, what grows (memory per unit, data directory), what lags (scanner behind the chain, newest block age, collector behind `measurements.jsonl`, snapshot compute time) and what fails (RPC-shaped journal errors, health)? | nothing | 24 h |

```bash
sudo deploy/test/rpc-check.sh "$RPC" https://rpc.celestia-mocha.com
# on mainnet: sudo RPC_CHAIN_ID=celestia deploy/test/rpc-check.sh "$RPC" <a second mainnet RPC>
sudo deploy/test/exposure.sh mocha
sudo deploy/test/persistence.sh mocha
sudo deploy/test/restore.sh mocha
sudo deploy/test/outage.sh mocha
sudo nohup deploy/test/resource-watch.sh run mocha 300 86400 > /var/log/fibre-resource-watch-mocha.log 2>&1 &
# a day later
deploy/test/resource-watch.sh summarize /var/log/fibre-resource-watch-mocha.csv
```

The scripts have regression tests of their own: `deploy/test/selftest.sh`
(also `make test-deploy`, and CI) runs them against fake API and RPC servers
on loopback and a fake rclone over a local directory — a closed port,
healthy and degraded answers, env values with spaces and quotes, a backup
that grew, was truncated, altered or lost a file, a line that is not a
record, a copy rotated after its cut, a manifest that names a file outside
its copy, a retired segment read back from its exports, the remote proof
of the exports, the manifest uploaded last, alert URLs kept off curl's
command line, a hosting database cut short, and app version 9/10 against
the x/fibre query — with no root, no systemd and no rclone.

`rpc-check` is the one to run before anything else, and against any public
endpoint you consider: a node started with `storage.discard_abci_responses =
true` answers `/status` like any other and fails `block_results` at every
height, which the scanner needs at every height. The public mocha endpoints
differ on exactly this.

## 9. What has been exercised

The units, the environment file, the Caddyfile and the whole process chain
were run against a local four-validator devnet on 16 September 2026: six
units verified by `systemd-analyze`, scanner, heartbeat, collector, prober
and API each started from their own unit as `fibre-observer`, writing to
`/var/lib/fibre-observer/data`, with `/v1/meta` serving the vantage block.
Both Caddyfiles validate under Caddy 2.8.4.

What has **not** been exercised anywhere: a real domain, a real certificate,
systemd as PID 1 actually supervising and restarting these units, litestream
replicating to real object storage, the template units with two instances
side by side, and `fibre-backup` against a real rclone remote. Those need a
host. The health endpoint, the status files, the snapshot persistence and
the registry replay are covered by the Go tests.
