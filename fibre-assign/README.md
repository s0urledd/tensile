# fibre-assign

A standalone, dependency-free recompute of Celestia Fibre's **shard
assignment**.

```
import "github.com/plsgiveup/fibre/fibre-assign"
```

## What it does

When a blob is published to Fibre, its erasure-coded rows are split among the
validators: each validator is responsible for serving a specific set of row
indices for a retention window. Which rows go to which validator is a
deterministic function of the blob commitment, the consensus validator set at
the blob's promise height, and a handful of protocol parameters.

`Assign(commitment, validators, params)` returns that mapping —
`map[Address][]int`, one row-index slice per validator, in the exact assignment
order celestia-app produces. `ShardMap.Verify(addr, rows)` then classifies what
a probed validator actually returned: served its exact set / under-delivered /
served rows it was not assigned.

## Why it exists

The assignment lives in celestia-app's `fibre/validator.Set.Assign`, which is in
an `internal`-adjacent package you cannot import from outside the celestia-app
module. Anything that wants to check whether a validator is serving the *right*
rows — a monitor, a Fibre Sentinel, a retrieval client verifying a download —
needs this calculation without pulling in the whole DA node.

This is a from-scratch reproduction of the assignment logic, pinned bit-for-bit
to celestia-app by a differential test.

**Zero dependencies.** Standard library only. No `go.sum`. The differential test
(and its celestia-app dependency) lives in the nested `./reftest` module and is
never pulled by consumers of this package.

## What is / isn't reproduced

| part | how |
|---|---|
| ChaCha8 stream + Fisher-Yates shuffle | **not reimplemented** — calls the same `math/rand/v2` primitives (`rand.NewChaCha8(seed)` then `rng.Shuffle`). `rand/v2` guarantees a stable stream per seed across Go versions, so this half is identical by construction. |
| row-count formula | reproduced: `clamp(ceil(OriginalRows·power·ltDen / (totalPower·ltNum)), MinRowsPerValidator, OriginalRows)`, pure checked int64 |
| canonical validator ordering | reproduced: voting power desc, ties by address asc (`bytes.Compare`) — matches `core.ValidatorSet` |
| offset walk + modulo wrap-around | reproduced |

## How it was verified

Two commands, each self-contained.

**1. The package itself** (zero deps, ~1s):

```
cd fibre-assign && go test ./...
```

21 tests: the pinned ChaCha8 permutation, degenerate inputs, single-validator,
equal-stake exact cover, stake-proportional counts, the `MinRowsPerValidator`
floor, cross-validator wrap-around, equal-power ordering stability, the overflow
guard, the version guard, and `ShardMap.Verify` — translated from
celestia-app's `fibre/validator/set_test.go`.

**2. Differential test against the real implementation** (nested module — pulls
celestia-app from the module proxy, no local checkout):

```
cd fibre-assign/reftest && go test ./...
```

Runs **~890 randomised scenarios** — 1–140 validators, six voting-power
distributions, blob-v0 parameters and randomised `(K, N, MinRowsPerValidator,
LivenessThreshold)` — through **both** this package and celestia-app's real
`validator.Set.Assign`, asserting the two `ShardMap`s are **bit-identical**
(same validators, same row slices, same order). Plus the called-out edge shapes
and shuffled input order. `reftest/go.mod` pins celestia-app to `v10.4.0-mocha`, the
tag for commit `5187d2fb5eb8bc4b534c74724882943c54253ae9`, and repeats
its `replace` block (Go does not apply a dependency's replaces).

CI runs both on every push (`fibre-assign` with `-race`, `fibre-assign-reftest`
against the pinned celestia-app graph).

## Version pinning — no silent default

`MinRowsPerValidator` (148) and `LivenessThreshold` (1/3) are **not on chain and
not observable at runtime**. They are constants compiled into celestia-app
(`fibre/protocol_params.go`, marked `toml:"-"` so operators cannot change them
either). No node or Fibre-server RPC returns them.

So `Assign` **requires** a `ProtocolParams` value — no function fills one in. The
package ships exactly one pre-filled value, and passing it is an explicit
assertion:

```go
// "I have confirmed this network runs celestia-app at the pinned commit"
m, err := assign.Assign(commitment, vals, assign.ParamsV10BlobV0)
```

A version string cannot establish that (a v10 patch could change the constants),
so `ParamsV10BlobV0` carries the exact commit, and

```go
got.Fingerprint() == assign.ParamsV10BlobV0.Fingerprint()
```

cross-checks a `ProtocolParams` from any second source (a release tag read by
hand, a future node endpoint) before you trust it. If you cannot confirm the
pin, read the four values from the target build's `fibre/protocol_params.go` +
`fibre/blob.go` and construct your own `ProtocolParams`.

## Usage

```go
vals := make([]assign.Validator, len(set))
for i, v := range set { // set = /validators?height=<promise height>
    addr, _ := assign.AddressFromEd25519PubKey(v.ConsensusPubKey)
    vals[i] = assign.Validator{Address: addr, VotingPower: v.VotingPower}
}

m, err := assign.Assign(blobCommitment, vals, assign.ParamsV10BlobV0)

rows, assigned := m.Rows(targetAddr)
if !assigned {
    // NotFound from this validator is expected, not a fault
}
// after DownloadShard, classify what it actually served:
err = m.Verify(targetAddr, returnedRowIndices) // nil / "expected N" / "not assigned" / "duplicate"
```

`Assign` returns an empty map (nil error) for degenerate inputs (empty set,
`TotalRows == 0`, `MinRowsPerValidator == 0`), and a `*ParamsError` /
`*DuplicateAddressError` / `*OverflowError` otherwise. The reference wraps int64
overflow silently; this package refuses it, because a wrapped count blames the
wrong validator — unreachable with realistic staking-reduced voting power, but a
refusal is safer than a wrong answer.

## License

Apache-2.0. See [`../LICENSE`](../LICENSE) and [`../NOTICE`](../NOTICE).
