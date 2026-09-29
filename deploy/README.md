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

- A Linux VM with Go 1.23+ (the celestia-app pin needs 1.26.5; `GOTOOLCHAIN=auto` downloads it), Node 22, Caddy 2.
- A CometBFT RPC endpoint for the chain you observe. Use your own full node
  (default pruning is fine; no archive node is needed) with
  `storage.discard_abci_responses = false` in `config.toml`: the scanner
  reads `block_results` for every block, and a node that discards ABCI
  responses answers "node is not persisting finalize block responses"
  (rpc-mocha.pops.one did on 8 September 2026). Public RPCs on
  celestia-core v0.41.0 also cap heavy requests at 20 in flight.
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
- If the same host also runs your own validator and Fibre server: celestia-app
  main (#7848, 15 Sep 2026) recommends separate disks for Fibre shards and
  the node's data; the observer's data directory should not share the Fibre
  shard disk either.

## 1a. Describe the vantage before you publish anything

Every reachability verdict on the site is a statement about a network path,
and half that path is yours, so a reader cannot judge an `UNREACHABLE`
without knowing roughly where it was measured from. The env file has two
settings for that, `VANTAGE_LOCATION` and `VANTAGE_PROVIDER`, and the API
logs a warning at startup if a public vantage leaves them blank. Both are
your word, and `/v1/meta` says so per field. The observer's own addresses
and autonomous system are not published.

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
sudo install -d -m 0755 /etc/fibre-observer
sudo cp deploy/observer.env.example /etc/fibre-observer/mocha.env
sudo cp fibre-sentinel/observer/policy/policy.mocha.yaml /etc/fibre-observer/policy-mocha.yaml   # policy.example.yaml for mainnet
sudo cp deploy/publishers.yaml.example /etc/fibre-observer/publishers-mocha.yaml                  # publisher labels; optional, the API runs without it
```

Edit `mocha.env`: set `NETWORK`, `RPC`, `VANTAGE`, `DATA_DIR`
(`/var/lib/fibre-observer/mocha`), `POLICY`
(`/etc/fibre-observer/policy-mocha.yaml`), `API_LISTEN` (a port of its
own), and, when you have them, `ALERT_WEBHOOK` and `BACKUP_REMOTE`.

The prober reads every blob; nothing is sampled or budgeted. The policy file
is read only for the earlier sampling's master secret
(`<DATA_DIR>/sampling-master.key` unless the policy names another file),
so the prober can keep publishing each day's secret seven days after the day
ends (`-reveal-after`, to `<DATA_DIR>/sampling-secrets.jsonl`, served at
`/v1/sampling`) and the draws made before 27 September 2026 stay
auditable. A new vantage, which never sampled, can run without `-policy`.
Only the day secrets are ever revealed, never the master: keep it off
anything the publishers can read and out of any backup that leaves the host.
Once the last day that had a draw is revealed, the key can be deleted and
`-policy` dropped; the old `probe-budget.json` can go as soon as this
prober runs (see "Stored data the reading no longer needs").

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

The prober asks a validator only for a blob it was assigned rows of, in
window, as the client asks, and stops once a blob's rows are enough. The
read-path rate limiting Celestia is designing (forum
topic 2295) treats requests for shards a validator was never assigned as
illegitimate; reading only real, in-window, assigned commitments keeps the
observer's traffic on the right side of it.

**What the prober sustains.** On 28 September mocha settled about 20 blobs
a minute (1,200 an hour from 14:00 to 20:00 UTC, 22 in the busiest minute),
nearly all of 16 MiB. A reading asks 12 to 20 validators (12 when every
validator holds its rows, 20 when the third that did not endorse holds
nothing) and moves about 19 MiB, the rows needed plus the requests already
on their way: at 20 blobs a minute that is 240 to 400 requests and about
380 MiB a minute, about 50 Mbit/s. The validator with the most stake is
asked for nearly every blob; there is no limit per validator, as the client
has none. A scheduler run on the observer (82 shared fake validators with
mocha's row shape scaled to 1/16, 1.0 to 1.8 s per shard, production
timeouts, loopback) read 60 of 60 blobs at 20 a minute (reading p50 1.9 s,
max 3.0 s, no start lag) and 180 of 180 at 60 a minute (p50 2.5 s, max
4.3 s, no start lag) under the earlier one-request-per-validator pacing,
which only slowed it; at 120 a minute every blob was still read but the
start lag grew to 30 s in two minutes. So the prober keeps up at three times
today's rate with nothing queued. The limits it keeps, 16 blobs and 64
requests at once and 512 MiB of shards in flight (`-blob-concurrency`,
`-concurrency`, `-in-flight-mib`), only delay a request: its 15 s start once
it is let go, it is never dropped, and it carries the phase its reading
started in, so a request held back past `must_serve_until` counts as the
client, which asks at once, would have made it.

A validator that times out holds a request for 30 s (the request and the
client's re-dial). The other readings go on beside it, as other clients'
would, so an unavailable blob is still read at 20 a minute beside a
validator that hangs (`TestAnUnavailableBlobIsReadWhileAValidatorTimesOut`).
Each blob being read also holds its verifier and the first shard it
verified, up to about 11 MiB, beside the `-in-flight-mib` budget.

## 4. systemd

```bash
sudo useradd --system --home /var/lib/fibre-observer --create-home fibre-observer
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
directory. The sampling secret must therefore sit under the data directory,
not under `/etc`.

Each unit runs one binary with the flags from its env file. Order does not
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
scan moves on. A chain halt is warned about every five minutes and waited
out; `Restart=always` in the units is for crashes, not for outages.

`sudo systemctl status 'fibre-*@mocha'` and `journalctl -u fibre-probe@mocha -f`.

### Health and alerting

Every process rewrites `<DATA_DIR>/status/<component>.json` on each unit of
work and at least every fifteen seconds: whether the last cycle succeeded,
the last error and when, its progress height, and the free space of the
data disk. `/v1/health` reads those files straight from disk and answers
200 when every one of scanner, prober, heartbeat and collector is alive and
succeeding, the scanner is within 200 blocks of the chain, the disk has
over 5% free, no scan gap is recorded and the chain has not upgraded past
this build's pin, and 503 with the failing checks otherwise. `/v1/meta`
carries the same components and verdict, and the header chip on the site
reflects it: green, amber with the failing processes in its tooltip, red
when nothing is alive.

`fibre-healthwatch@<network>.timer` asks `/v1/health` every five minutes as
the service user and posts to `ALERT_WEBHOOK` when the verdict changes,
again every `ALERT_REPEAT_MIN` while it stays bad, and once on recovery. The
webhook is any URL that accepts a JSON body with a `content` field (Discord,
Slack incoming webhooks, a Matrix or Telegram bridge). With no webhook it
only logs; an external uptime monitor pointed at
`https://<site>/api/v1/health` is the same signal with somebody else's
timer.

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
which anything uses any more.

The API also refuses a database **newer** than itself, so an API left on an
old build after the collector moved on says so rather than serving columns
it does not know.

Schema 5 adds a covering index over `probes`. On a store with 700,000 probes it
takes a few seconds and about 200 bytes a probe; the collector logs it and the
restart is not otherwise different.

The API computes the network summary, the validator list and the market
figures for every window at startup rather than on demand, and keeps the
last computed copy of each under `<DATA_DIR>/snapshots/` (`-snapshot-dir`
moves it). A restarted API serves those copies at once, with their real age
shown on the page, while the warm-up recomputes them behind; the first
minute or two after a restart is busier than the steady state, but nobody
waits for it.

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
`observer-api -warm-only` opens the database read-only, computes every window
of every snapshot once under the current revision, writes the files to
`-snapshot-dir` and exits 0 (non-zero, with the reason, on any failure).
With `-warm-only` that directory defaults to `<data-dir>/snapshots.next`, and
the live `<data-dir>/snapshots` is refused: the running API rewrites its
files there under the same temporary names, and an old API restarted
meanwhile would load the new build's market files. It must run as the
service user and with the unit's own flags: the snapshots depend on
`-vantage` (the heartbeats counted are that vantage's) and the market one on
`-publishers`, and a file written for another vantage is not loaded.
`systemd-run` gives it both, from the unit's env file, expanding `${…}` the
way the unit's `ExecStart` does. After the collector has migrated the
database (the new binary refuses an older schema), and while the old API
keeps serving:

```bash
sudo install -m 0755 fibre-sentinel/bin/* /usr/local/bin/
sudo install -m 0755 deploy/vantage-pull.sh /usr/local/bin/fibre-vantage-pull   # the timer runs it next minute
sudo systemctl restart fibre-collector@mocha       # applies migrations
# a few minutes, reading the database only, beside the running API
sudo systemd-run --wait --pipe --collect -p User=fibre-observer -p Nice=10 \
  -p EnvironmentFile=/etc/fibre-observer/mocha.env \
  /usr/local/bin/observer-api -warm-only -data-dir '${DATA_DIR}' -snapshot-dir '${DATA_DIR}/snapshots.next' \
  -vantage '${VANTAGE}' -vantage-location '${VANTAGE_LOCATION}' -vantage-provider '${VANTAGE_PROVIDER}' \
  -publishers /etc/fibre-observer/publishers-mocha.yaml
sudo systemctl stop fibre-api@mocha
sudo -u fibre-observer sh -c 'cp /var/lib/fibre-observer/mocha/snapshots.next/*.json /var/lib/fibre-observer/mocha/snapshots/ &&
  rm -r /var/lib/fibre-observer/mocha/snapshots.next'
sudo systemctl start fibre-api@mocha
sudo systemctl restart fibre-scan@mocha fibre-probe@mocha fibre-heartbeat@mocha
```

The copy runs as `fibre-observer` so the files stay its own: the API
rewrites them on every refresh. For the same reason the warm-up does not run
as root, which would also risk creating the database's `-shm` file owned by
root. A hold or the activation landing between the warm-up and the start
changes the revision: the files are then dropped and recomputed rather than
served, which is the cold start again and never a stale figure.
`journalctl -u fibre-api@mocha` shows `snapshot(s) loaded from disk` on
start.

This keeps the old API serving from the migrated database for the few
minutes of the warm-up, where the plain upgrade above leaves it seconds, so
use it only when the build's schema change, if any, is additive: new tables,
columns or indexes the old API does not read. When a migration changes or
drops something the old API reads, stop `fibre-api@mocha` before restarting
the collector; the warm-up still spares the new build a cold start, but the
API is down for those minutes.

## 5. Caddy

Install the site: `sudo mkdir -p /var/www/fibre-observer && sudo cp -r web/out/. /var/www/fibre-observer/`.
Then `deploy/Caddyfile` with your domains in place of `observer.example.org`
and `mocha.observer.example.org`:

```bash
sudo cp deploy/Caddyfile /etc/caddy/Caddyfile && sudo systemctl reload caddy
```

Caddy serves the same static export on every site, proxies each site's
`/api/*` to that network's `observer-api` port, and gets TLS certificates
from Let's Encrypt. Delete the second site block if you run one network.

, and the header shows
a switch between them (the current one is marked by origin):

```bash
cd web && NEXT_PUBLIC_NETWORKS="mainnet=https://observer.example.org,mocha=https://mocha.observer.example.org" npm run build
```

### Two networks

Mocha and mainnet are two instances of everything, side by side:

| | mocha | mainnet |
|---|---|---|
| env | `/etc/fibre-observer/mocha.env` | `/etc/fibre-observer/mainnet.env` |
| data | `/var/lib/fibre-observer/mocha` | `/var/lib/fibre-observer/mainnet` |
| policy | `policy-mocha.yaml` (from `policy.mocha.yaml`) | `policy-mainnet.yaml` (from `policy.example.yaml`) |
| API | `127.0.0.1:8081` | `127.0.0.1:8080` |
| units | `fibre-*@mocha`, `fibre-*@mocha.timer` | `fibre-*@mainnet`, `fibre-*@mainnet.timer` |
| site | `mocha.observer.example.org` | `observer.example.org` |

Each instance needs its own RPC node with `discard_abci_responses = false`.
Nothing is shared between them but the binaries and the static export; a
data directory belongs to one chain and the scanner refuses to resume it
against another. Disk: a mocha instance grows by a few GB a month, a
mainnet instance by what its publication rate makes it (see "Backups"). Two
instances double the reading traffic.

## 6. docker compose (alternative)

```bash
cp deploy/observer.env.example deploy/.env   # edit RPC, VANTAGE, DOMAIN
docker compose -f deploy/docker-compose.yml up -d --build
```

Same five processes plus Caddy, one image built from `deploy/Dockerfile`.
Data lives in the `observer-data` volume. Compose is one network per
project (it binds 80 and 443); for two networks on one host use systemd.

## 7. Backups, retention, rebuild

Budget for disk: one measurement is about 1.5 KB in `measurements.jsonl`
(about 3 KB when it carries the verified row indices) and about twice that
again in the database, one per validator a reading asks. At mocha's rate on
28 September (about 20 blobs a minute, 12 to 20 validators asked each) that
is about 1.2 to 1.8 GB a day of JSONL plus the database, until `raw_json`
is dropped after 30 days. The JSONL files are the record; the three biggest are kept bounded
by moving their older lines into compressed segments under `archive/`
(below), never by deleting a line. `/v1/health` fails the `disk` check
under 5% free so the alert arrives before a write does. When a disk fills,
move the oldest archive segments or the small record files off the box
(append-only or immutable; a copy is complete the moment it is taken) and
rebuild the database from the rest if you want it smaller. Nothing here
deletes a probe row yet: the "all" window is exactly that.

**Retention (decided 2026-09-18, implemented in the collector):** raw
probe and heartbeat rows are kept for **90 days** (`-retain-raw`);
`raw_json` (the bulk of a row) is dropped after **30 days**
(`-retain-raw-json`) while every typed column, including the evidence
columns from schema 9, stays; **14 days** after a UTC day ends
(`-rollup-after`) the collector computes the day's per-validator rollup
(obligation buckets by settlement day; classes, faults, gaps and
heartbeats by start day) with the API's own SQL, and only a rolled day is
ever pruned, whole days at a time. `-rollup-after` is a floor, not the
rule: a day rolls only once every promise settled on it has left its
window (`must_serve_until` plus an hour) and no probe row of theirs still
awaits the late shadow verdict, so a chain whose retention is longer than
the flag holds the rollup rather than rolling a pending obligation; the
log says which day is waiting and why. From the first prune on the "all"
figures are the rollup plus the raw rows and carry a `rolled_up` label
(`raw_from`, days folded in); the shorter windows never touch it. The
retention pass runs hourly (`-retention-every`); the status file shows
`rollup_through` and `raw_from`. A warning in the log that obligations
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


- **litestream** for the database: copy `deploy/litestream.yml` to
  `/etc/fibre-observer/litestream-mocha.yml` (fix the `path` to the
  instance's data directory), put the bucket keys in
  `/etc/fibre-observer/litestream-mocha.env` (mode 0600), and enable
  `fibre-litestream@mocha`. It replicates the **derived** database only,
  continuously, with 72 h of history.
- **fibre-backup** for the record: `fibre-backup@mocha.timer` runs
  `rclone sync` of every `.jsonl` (the record, `registry.jsonl`,
  `runs.jsonl`, `sampling_decisions.jsonl`, `sampling-secrets.jsonl`, `amendments.jsonl`), the archived
  segments under `archive/` (first, see "Archive" below), `state.json`, the status files
  and the daily exports to `BACKUP_REMOTE/<network>` nightly (`deploy/backup.sh`),
  with the rclone remote configured once in `/etc/fibre-observer/rclone.conf`.
  It copies rather than mirrors, so moving old files off a full disk can
  never delete them from the remote.
  It never copies `sampling-master.key`, which must not leave the host, nor
  the database, which litestream covers. With `BACKUP_REMOTE` empty the
  timer runs and does nothing, so enable it everywhere and arm it with one
  variable.

**Rebuild from the record.** Stop the instance's collector and API, move
`observer.db*` aside, start the collector: it recreates the schema, replays
`registry.jsonl` (endpoint history), then tails the JSONL files from zero.
Every record has a natural key and every insert is `ON CONFLICT DO
NOTHING`, so a replay never duplicates. The run record (`/v1/runs`) comes
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

Nothing below is deleted by this change, and nothing is rewritten: the
earlier schedule's rows (`w1` to `w4`, `grace`, `post`, 8,000 each) and
the `NOT_PROBED` end rows of readings the old prober could not make are the
record, and stay. What only served the earlier model, with its size on the
mocha host on 29 September, and how to remove it once the owner approves:

| data | size | still read by | how to remove |
|---|---|---|---|
| `probe-budget.json` | 1.3 MB | the prober before this change; the new prober never reads or writes it | after the new prober is running: `rm <DATA_DIR>/probe-budget.json` |
| `sampling-master.key` | 32 B | the prober's reveal of the earlier draws' day secrets (`-policy`) | after the last draw day (2026-09-26) is revealed, on 2026-10-04 with `-reveal-after 7d`: `rm <DATA_DIR>/sampling-master.key` and drop `-policy` from the unit |
| `sampling-secrets.jsonl` | 5.8 KB | `/v1/sampling`, the daily export, `sentinel-recompute -sampling` | only with the sampling audit itself: then `rm`, and remove `/v1/sampling` in the same change |
| `sampling_decisions.jsonl` | 8.3 KB | the collector (`sampling_decisions` table), the daily export | the same: with the sampling audit |
| table `sampling_decisions` (53 rows) and `sampling_decision_points` (318 rows, with its key index) | 0.2 MB | `/v1/sampling`, the obligation rows of sampled-out publications (`obligation_rows`) | with the sampling audit, in a migration that bumps the schema: `DROP TABLE sampling_decision_points; DROP TABLE sampling_decisions;` and the views over them |
| index `probes_sampling_started` | 68 MB | `/v1/sampling` only | with the sampling audit, in the same migration: `DROP INDEX probes_sampling_started;` then `VACUUM` (hours on a 5 GB store; run it with the collector stopped) |
| columns `probe_daily.faults`, `attested`, `unattested`, `unknown_att` | none yet (no day rolled) | nothing: written as 0 | a migration that bumps the schema, whenever the table is next changed |
| columns `obligation_daily.end_unobserved`, `unobserved_reachable`, `unobserved_unreachable`, `unobserved_not_probed` | none yet | summed into `not_counted` | the same; one `not_counted` column would do |
| `snapshots/` | 1.0 MB, 12 files | the API, which rewrites every file on start and on each refresh | nothing to do: none is left from an earlier model |
| table `probe_confirmations`, columns `probes.cleared_by` and `probes.confirmed_by`, index `probes_cleared` | empty (0 rows; every value NULL) | nothing: the second location's confirmation of failed readings is gone | a migration that bumps the schema, whenever `probes` is next changed |
| `vantages/de-1/measurements.jsonl` | 0 B | the pull script installed before this change, which still fetches it every minute | install the new one first (`sudo install -m 0755 deploy/vantage-pull.sh /usr/local/bin/fibre-vantage-pull`, as in "Upgrading a running observer"), then `rm` it |

### Archive: bounded live files

`measurements.jsonl` grows about 100 MB a day. `observer-archive`, run daily
by `fibre-archive@<network>.timer` at 04:40 UTC (after the export and the
backup), keeps it and the other two biggest files bounded without taking a
line out of the record:

```
<DATA_DIR>/measurements.jsonl                         the live file: lines dated in the last -keep (default 7 days)
<DATA_DIR>/archive/measurements.jsonl/index.json      segments, their digests, where the live file starts
<DATA_DIR>/archive/measurements.jsonl/000001-2026-10-02.jsonl.gz
<DATA_DIR>/archive/measurements.jsonl/000002-2026-10-03.jsonl.gz   one per run: the lines dated before that day
<DATA_DIR>/archive/reachability.jsonl/...
<DATA_DIR>/archive/sampling_decisions.jsonl/...
<DATA_DIR>/archive/.lock                              held by a run (exclusive) and the backup (shared)
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
heartbeats, `decided_at` for sampling decisions), never past what the daily
export has read, always leaving the last line; write the segment, fsync it,
read it back and match its digest; copy the rest to a temp file; then,
holding the file's exclusive `flock`, copy what was appended since, write
the index and rename the copy over the live file. The writers
(`sentinel-probe`, `observer-heartbeat`) append under a shared `flock` and
reopen the path when it no longer names the file they hold, so no line is
lost or written twice. A crash at any step leaves the record readable as
before; the next run removes the leftovers. A second run the same day moves
nothing.

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
backup copies `archive/` before the live files, and its manifest names every
segment and the live base; `restore.sh` and `verify` check each segment. The
small record files (publications, payments, host history, the collector's
own logs, runs) are not archived: their writers hold them open without the
lock. Neither are the files under `vantages/`, which `vantage-pull` resumes
by size; do not run `observer-archive` on a second vantage, whose files the
observer pulls that way.

Upgrade order: install binaries, restart `fibre-probe` and
`fibre-heartbeat` (the writers must hold the lock before any rotation) and
`fibre-collector`, then enable the timer.

### Runbook

- **Health is 503.** Read the `checks` list: it names the process or
  condition. A dead process: `journalctl -u fibre-<name>@<network> -n 100`.
  A `scanner_lag`: the RPC node is behind or slow; the scanner catches up
  on its own. A `scan_gaps`: the node could not serve those heights, or the
  operator skipped them (each range's `reason` says which, see below); point
  the scanner at a node that keeps them and delete `gaps` from `state.json`
  after re-scanning from the lowest gap height with `-start-height`, or
  accept the gap (the dashboard says which blocks). A `pin`: see below.
- **The chain upgraded past the pin** (`pin_status: chain_ahead`). The row
  assignment this observer computes depends on constants pinned to a
  celestia-app commit (`fibre-assign/params.go`), and a new major may change
  them. Until the pin is bumped, verdicts about who holds which rows may be
  wrong, and the site shows a banner. To bump: diff `fibre`, `x/fibre`,
  `x/valaddr`, `proto` and `specs` between the pinned commit and the release
  tag, update `PinnedCelestiaAppCommit`, `PinnedCelestiaAppVersion` and the
  `celestia-app` line plus the copied `replace` block in
  `fibre-sentinel/go.mod`, re-run `fibre-assign/reftest`, rebuild, deploy.
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
curl -s localhost:${API_LISTEN}/v1/network | jq '{registered_endpoints, reachability, validators_probed}'
```

Before that, the "not live yet" notice on the overview and the header chip
carry x/signal's tally for the version that brings Fibre — how much voting
power has signalled, the threshold, how many bonded validators have not, and
the scheduled height once there is one — and the validator table marks each
bonded validator `signalled` or `not signalled` (`upgrade_signal` on
`/v1/meta`, `signaled_upgrade` on each row; both disappear once the chain is
on that version).

`registered_endpoints` moving off zero is the first sign the registry is being
read. `reachability` follows within a heartbeat interval. The activation
changes the snapshots' revision, so for the half minute or so the API takes
to recompute the 24h window, `/v1/network` answers 503 with
`"computing": true` and the last line prints `null` for all three: ask again. Publications appear
only once somebody actually pays for a blob, which may be hours later; an
empty publication feed on activation day is a quiet network, not a broken
observer, and the site says which.

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
| `dbip-city-lite.csv.gz` (optional, ~85 MB) | [DB-IP IP to City Lite](https://db-ip.com/db/download/ip-to-city-lite) — range → city, region, coordinates (adds `city`/`region`/`lat`/`lon` to `hosting` and `by_city` to `/v1/hosting`; absent file = country only; `HOSTING_CITY_DB` / `-hosting-city-db` to move it, `HOSTING_SKIP_CITY=1` to skip it) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), same credit |

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
checks the format and line count, and renames, so a failed download never
replaces a good file). Within one endpoint poll (a minute) the collector
logs `hosting: N open endpoint(s), N resolved, N with an origin AS`, and
`/v1/hosting` answers `"enabled": true`. No restart, no unit change. To keep
the files elsewhere, set `HOSTING_ASN_DB=` and `HOSTING_COUNTRY_DB=` in the
network's env file (or pass `-hosting-asn-db` / `-hosting-country-db`), then
restart the collector.

Refresh monthly (DB-IP publishes monthly; iptoasn hourly) by re-running the
same command, e.g. from cron. The collector re-runs the lookup when a file's
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
ingests every file there. Configure it in the network's env file:

```
VANTAGE_PULL_HOST=tensile-backup@85.10.211.222
VANTAGE_PULL_NAMES=de-1
```

```
install -m 0755 deploy/vantage-pull.sh /usr/local/bin/fibre-vantage-pull
cp deploy/systemd/fibre-vantage-pull@.{service,timer} /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now fibre-vantage-pull@mocha.timer
```

The second vantage runs the heartbeat and nothing else. A blob's reading is
this observer's own, as one client's download is, and nothing asks the
second vantage to read it again. Earlier builds pushed confirmation requests
to `vantage/<name>/inbox/`; nothing writes or reads that inbox now, and
`VANTAGE_PUSH` in the env file is ignored.

`deploy/test/vantage-sync.sh` checks the pull against a fake sftp.

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

Then check the site says where it watches from. If `complete` is false the
dashboard is publishing reachability verdicts without telling a reader which
network they were measured on, and the API will have logged a warning at
startup:

```bash
curl -s https://observer.example.org/api/v1/meta | jq .vantage_info
```

Confirm the ASN you declared is the one your traffic actually carries, since
that is the claim a reader will check:

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
| `rpc-check.sh <rpc> [rpc2]` | is the RPC node on `mocha-5`, in sync, and keeping `block_results`, the validator set and historical state as far back as the observer reads (6000 blocks)? With a second node, do the two agree on a block hash? | nothing | seconds |
| `exposure.sh` | is only ssh/http/https reachable from outside, is every unit enabled for a reboot, does HTTPS reach the API through Caddy, does a test alert actually arrive, is the master key `600`? | posts one test message | seconds |
| `persistence.sh` | live: do the checkpoints survive a restart, is the database sound? On one consistent cut of the record: no duplicate line, a rebuild from the cut alone holds exactly its records, and `sentinel-recompute` agrees with a second API serving that same cut, both as of the cut's timestamp | restarts collector + scanner; rebuilds into a temp dir; a throwaway API on `:18082` | minutes |
| `restore.sh` | does the nightly copy verify against its manifest (every file present, at least the cut, hash and record count equal, every line a JSON record, the cut's own `state.json` put in place of the copy's, no master key), rebuild to exactly the cut's records, and serve them from a second API on a spare port? | starts a throwaway API on `:18081` | minutes |
| `outage.sh` | when the chain source is cut, does the site say so within twelve minutes and keep serving its last figures; when it returns, does the scanner catch up with no gap, no lost row and no duplicate; when every process is stopped and started, is nothing lost? | edits the env file (restored on every exit path), restarts and stops units | ~30 min |
| `resource-watch.sh run` / `summarize` | over a day, what grows (memory per unit, data directory), what lags (scanner behind the chain, newest block age, collector behind `measurements.jsonl`, snapshot compute time) and what fails (RPC-shaped journal errors, health)? | nothing | 24 h |

```bash
sudo deploy/test/rpc-check.sh "$RPC" https://rpc.celestia-mocha.com
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
on loopback — a closed port, healthy and degraded answers, env values with
spaces and quotes, a backup that grew, was truncated, altered or lost a
file, and app version 9/10 against the x/fibre query — with no root, no
systemd and no rclone.

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
