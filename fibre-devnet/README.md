# fibre-devnet

A multi-validator local Celestia Fibre devnet, for exercising shard assignment
and retention end to end.

`multi-node-fibre.sh [N]` (default 4, minimum 2) brings up, on one host:

- **N `celestia-appd` validators**, full mesh, near-equal stake. Node 0 has a
  hair more so it is the stable block proposer; all N stay up for consensus.
- **One `fibre` server per validator.** Each fibre server signs payment
  promises by delegating `SignRawBytes` to its node's `priv_validator_grpc_laddr`
  (there is no standalone signer — so every validator in the set needs a running
  node process). During fault testing you kill a *fibre server*, never a node.
- Each fibre host registered on chain (`celestia-appd tx valaddr set-host`).
- A funded `uploader` account with escrow deposited, for driving a fibre client
  or a Sentinel probe.

## Stake shape, and why it matters more than validator count

By default every validator gets near-equal stake. That is the wrong shape for
most of what is worth testing, because almost everything a real network does
differently follows from the stake being skewed rather than from there being
more validators.

```bash
FIBRE_DEVNET_POWERS=fibre-devnet/powers/mocha-5.txt ./multi-node-fibre.sh 30
```

`fibre-devnet/powers/` holds voting-power snapshots of real networks, one power
per line, largest first, with the chain, height and time they were taken in the
header. `snapshot-powers.py <rpc>` refreshes one.

A smaller set is picked by spreading evenly down the curve, tail included, so it
keeps the shape of the real one. Taking the largest N instead — `FIBRE_DEVNET_POWER_PICK=head` —
drops the entire class of small validators, and that class is where the
interesting behaviour lives. Measured against the mocha-5 snapshot (79
validators, 322,778,683 total power):

| pick | share range | lifted to the 148-row floor |
|---|---|---|
| largest 20 | 2.33% – 13.1% | **0 of 20** |
| spread 20 | 0.0000003% – 26.1% | 2 of 20 |
| largest 30 | 1.89% – 10.6% | **0 of 30** |
| spread 30 | 0.0000003% – 18.9% | 9 of 30 |
| all 79 | 0.0000003% – 7.2% | **33 of 79** |

Two thirds of the real network's validators are small enough that the clamp
decides their row count, and on mocha the smallest has a voting power of **1**.
A run that never lifts anyone to the floor has not tested the branch 42% of the
network lives on.

Twelve validators spread down that curve give a 27-fold range in assigned rows
and trip both clamps at once — the largest is cut to the 4,096 ceiling, the
smallest lifted to the 148 floor:

```
  power        share      rows
  23110000     39.4225%   4096   <- ceiling (raw 4841)
  ...
  1             0.0000%    148   <- floor (raw 1)
```

Spreading preserves the shape but not the shares: sampling 12 of 79 pulls the
largest from 7.2% up to 39%. For mocha's actual shares you would run all 79,
and that does not fit an ordinary machine: after a day of blocks each
`celestia-appd` sits at about 1.9 GB resident (the fibre server adds little),
so 30 nodes filled a 62 GB host and pushed it into swap. Twelve nodes need
about 25 GB and produce every class the taxonomy has.

### Producing each verdict class on purpose

A devnet that just runs produces `HEALTHY`, and once windows close, `TOLERATED`
and `EXPECTED_GONE`. Everything else comes from doing something to it. The two
that the taxonomy exists to tell apart come from the same action at different
times:

```bash
# 1. start the devnet on a real curve, publish a few blobs
FIBRE_DEVNET_POWERS=powers/mocha-5.txt ./multi-node-fibre.sh 12
sentinel-pub -count 6 -gap 20s

# 2. stop two fibre servers and leave them down
awk '$1==10 || $1==11 {print $2}' ~/.fibre-devnet/logs/fibre-pids | xargs kill

# 3. keep publishing
sentinel-pub -count 6 -gap 20s
```

Blobs from step 1 were uploaded to those two validators and signed by them, so
the chain proves they held the shard: a probe that cannot reach them afterwards
is **UNREACHABLE**. Blobs from step 3 never reached them, so they never signed
and nothing proves they were ever sent anything: a probe is **UNATTESTED**,
whatever happened on the wire. Same two validators, same downtime, two different
classes, and neither is held against anyone.

The three classes that say something about a server that *is* up come from
three different layers of the same process, and each has a one-line trigger.
Nothing here needs a restart except the last one.

```bash
H=${FIBRE_DEVNET_HOME:-$HOME/.fibre-devnet}; O=${FIBRE_DEVNET_PORT_OFFSET:-0}

# FAULT: identity verifies, the chain proves it signed for the shard, and it
# answers NotFound. Delete the flat shard files under one server's store; the
# pebble marker stays, store.Get finds no file behind it and DownloadShard
# returns codes.NotFound.
rm "$H"/fibre3/shards/*

# SERVER_ERROR: same state, but the store fails for a reason other than
# "not there". Cut every shard file down to its 4-byte codec-version header;
# the decoder fails on the next field and DownloadShard returns codes.Internal.
truncate -s 4 "$H"/fibre5/shards/*

# IDENTITY_MISMATCH: the certificate is endorsed by a consensus key that is not
# this validator's. Restart fibre 7 against node 2's signer; the certificate it
# mints is endorsed by val2's key and a client expecting val7 refuses it.
# --unlimited-budget is what lets it boot: with a budget, start refuses a
# signer that is not in the active set. On a real network the same thing is a
# TLS-terminating proxy in front of the server, or a signer swapped without
# restarting Fibre.
kill "$(awk '$1==7{print $2}' "$H"/logs/fibre-pids)"
fibre start --home "$H"/fibre7 \
  --app-grpc-address 127.0.0.1:$((9090+O+70)) \
  --signer-grpc-address 127.0.0.1:$((26669+O+200)) \
  --server-listen-address 127.0.0.1:$((7980+O+7)) \
  --unlimited-budget >> "$H"/logs/fibre7.log 2>&1 &
sed -i "s/^7 .*/7 $! $((7980+O+7))/" "$H"/logs/fibre-pids
```

Only blobs a server signed for before the change are rated, and the probes
that rate them run at fixed points inside the ten-minute window, so each class
shows up within one retention window of the command:

```bash
curl -s 'http://127.0.0.1:8080/v1/probes?class=SERVER_ERROR&limit=5'
```

What happens next differs per server. The upload path never reads the shard
files, so val3 and val5 keep signing new uploads and their new blobs are
`HEALTHY` again until the command is repeated. val7's uploads are signed with
the wrong key, the publisher drops them, and its new blobs are `UNATTESTED`;
its 5-minute handshake reads `bad certificate` on the overview until it is
restarted against its own signer.

`IDENTITY_EXPIRED` needs a certificate whose signed validity window has lapsed;
`NOT_REGISTERED` needs a validator with no `x/valaddr` entry.

**`sentinel-pub` publishes at the protocol's safety threshold by default.** It
used to pass `WithAwaitAllSignatures()` unconditionally, which waits for every
validator rather than stopping at two thirds of voting power. That had two
consequences worth knowing about if you read older runs: every devnet blob came
back with a full signature set, so `UNATTESTED` — about a third of every real
assignment — was never produced at all; and a publish FAILED outright the moment
any validator was down, with `not enough voting power: collected X, required Y`
where Y is the whole set rather than two thirds. Pass `-await-all` if you
deliberately want every signature.

### Producing escrow events on purpose

A publisher that always pays produces settlements and nothing else, so the
Publishers page shows one column. The rest of the escrow side of `x/fibre`
comes from `sentinel-pub`'s other modes. The devnet sets
`payment_promise_timeout` to its floor of ten minutes, so an abandoned promise
becomes reportable ten minutes after it was created.

```bash
# 1. a publisher with a persistent key, so later runs act as the same account
sentinel-pub -key-file ~/pub-a.key -count 3 -gap 10s

# 2. obtain signatures for two blobs and never settle them. Validators hold
#    the shards; nothing about it reaches the chain yet.
sentinel-pub -key-file ~/pub-a.key -deposit 0 -abandon -count 2 -gap 10s
#    -> abandoned-<commitment8>.json, one per blob

# 3. ten minutes later, report the timeouts from a DIFFERENT account, the way
#    a validator that stored the shards for nothing would. The chain charges
#    pub-a's escrow the same fee a settlement would have cost and pays the
#    reporter nothing.
sentinel-pub -key-file ~/pub-b.key -deposit 0 -timeout abandoned-*.json

# 4. ask for some of the escrow back; the payout lands in a begin-block after
#    withdrawal_delay (43,800 s on the devnet: it cannot go below the
#    protocol's 12h10m floor).
sentinel-pub -key-file ~/pub-a.key -deposit 0 -withdraw 1000000000
```

Each step shows up on the Publishers tab within one collector pass: step 2
changes nothing (that is the point: an unsettled promise is invisible on
chain), step 3 raises *Timed out* and lowers pub-a's settlement rate, step 4
appears as a withdrawal request and, half a day later, a payout. A timeout
submitted from a key whose bytes match a validator's operator address is
counted on that validator's page as *timeouts enforced*; the devnet's
validator keys live in `$FIBRE_DEVNET_HOME/appN`, so `-key-file` a mnemonic of
one of them if you want to see that row.

The chain refuses a timeout before `creation + payment_promise_timeout`
(`payment promise has not yet timed out`) and after the escrow is gone; a
promise settled in the meantime is refused as already processed.

### Two thirds, and why most validators have no signature on chain

The publisher stops collecting signatures once **two thirds of voting power**
has answered (`fibre/validator/signature_set.go`), and the chain's own check
uses the same threshold. Validators past that point still receive the shard —
delivery continues in the background — but their signature never reaches the
chain, so nothing on chain proves they hold it.

It is a race, not a fixed list: membership follows upload-completion order, so
the same validator is in some quorums and not others. How large the quorum is
can be bounded: simulated against the mocha snapshot over 20,000 uniformly
random arrival orders it holds a median of 53 of 79 validators, and the floor
is 30, the case where the largest validators all answer first — a bound, not an
expectation. Who is in it cannot be bounded that way: completion order depends
on shard size (which scales with stake), on the network path between publisher
and validator, and on the validator's own write-and-sign latency, none of which
a uniform draw models. An earlier version of this section said inclusion was
flat across stake; that was the simulation's assumption read back as a result,
and it is withdrawn.

The observer classifies those probes `UNATTESTED` and holds them out of the
serve rate in both directions. A devnet with equal stake barely produces the
class; one on a real curve produces it the way the real network will.

## Why a multi-node devnet

`celestia-app/scripts/single-node-fibre.sh` brings up one validator — and with
one validator the shard assignment is degenerate: that validator gets all 4096
original rows, so retention and reconstruction behaviour can't be observed.
Assignment only becomes interesting (rows split, no single validator holding
enough to matter, wrap-around) with several validators of comparable stake.

## Short lifetimes

Genesis `x/fibre` params are lowered to the **protocol floor** — the same idea
as `setFibreShortLifetimes` in `celestia-app/fibre/internal/e2e/fibre_stack_test.go`:

| param | default | devnet | floor |
|---|---|---|---|
| `payment_promise_timeout` | 1h | **10m** | `MinPaymentPromiseTimeout` = 10m |
| `shard_retention` | **4h** | **10m** | `MinShardRetention` = 10m |
| `withdrawal_delay` | 24h | 12h10m | `MinWithdrawalDelay` = MaxPromiseTimeout + 10m |

So `pruneAt = creation_timestamp + max(10m, 10m) = creation + 10m`, and the
fibre server's prune loop ticks every 60s. **10 minutes is the hard protocol
minimum** — `Params.Validate()` rejects anything lower, on chain and via
`MsgUpdateFibreParams`. That is why the end-to-end tests take ~15 minutes.

Ports (node `i`, plus `FIBRE_DEVNET_PORT_OFFSET` if set — use it on a host that
already runs a chain on 26657/9090): RPC `26657+i·100`, P2P `26656+i·100`, SDK gRPC `9090+i·10`,
API `1317+i·10`, privval gRPC `26669+i·100`, core BlockAPI gRPC `19098+i·100`
(remapped from `:9098`), pprof `6060+i`, fibre listen `7980+i`.

## Prerequisites

`celestia-appd` and `fibre` on `PATH`, plus `curl` (`jq` optional, only for
prettier output). Build them from a celestia-app checkout at the pinned commit
`5187d2fb5eb8bc4b534c74724882943c54253ae9` (v10.4.0-mocha; `x/fibre` and `x/valaddr` are
live from block 1, no upgrade):

```
go build -tags ledger -o build/celestia-appd ./cmd/celestia-appd
go build             -o build/fibre           ./fibre/cmd
```

## Run

```
./multi-node-fibre.sh 4
#  ~1-3 min to genesis + READY (4-node gentx is slow on some platforms).
#  Prints every endpoint, the uploader address, per-validator consensus addrs.
#  Writes ${FIBRE_DEVNET_HOME:-~/.fibre-devnet}/READY when it is up.
#  Ctrl-C tears everything down; homes + logs stay under the workdir.
```

## Status

Smoke-tested at **N=2 and N=4** with `celestia-appd` + `fibre` built from the
pinned commit: nodes produce blocks, every fibre server comes up (signer +
app-gRPC connected), every host registers on chain, the uploader escrow funds.
The Fibre Sentinel repo's `fibre-sentinel/devtest.sh` and
`fibre-sentinel/probe-devtest.sh` drive full N=4 runs against this script,
including a mid-window fault injection, and both pass.

Portability notes (watch for these if you change the script):

- Genesis `gentx` needs `--fees` — celestia enforces a network minimum gas price
  even at InitChain, so a zero-fee gentx panics the node on startup.
- Per-process ports that collide on one host and are remapped:
  `priv_validator_grpc_laddr` (`:26669`), the celestia-core BlockAPI gRPC
  `[rpc] grpc_laddr` (`:9098`), `[rpc] pprof_laddr` (`:6060`), plus the obvious
  RPC / P2P / SDK-gRPC / API.
- `celestia-appd version` prints nothing without release ldflags — harmless.
- Cleanup helpers (`taskkill` fallback, `netstat -ano` PID parse) are
  Windows/git-bash specific; on Linux the `lsof`/`kill` path is used instead.

## What the end-to-end flow looks like

1. Upload a blob with the fibre client (funded `uploader` account); get its
   `BlobID` and promise height.
2. `fibre-assign`: compute the assignment for that commitment over the validator
   set at the promise height, with `assign.ParamsV10BlobV0`.
3. For each **assigned** validator, `DownloadShard` from its registered host
   (over a `fibre-tlsverify` TLS config), verify the row proofs / RLC against
   the commitment (`pkg/rsema1d`), and `ShardMap.Verify` the returned indices.
   An un-assigned validator returning `NotFound` is normal.
4. Repeat on a schedule until `creation + 10m`; expect a clean flip to
   `NotFound` shortly *after* that, not before.

`fibre-sentinel` implements exactly this, persistently.
