# reftest — differential test against celestia-app's real Set.Assign

A **nested Go module**. It is *not* part of the shippable `fibre-assign`
package: `cd fibre-assign && go test ./...` does not descend here, and consumers
of `fibre-assign` never pull celestia-app.

It runs `fibre-assign` and celestia-app's real `fibre/validator.Set.Assign`
side by side and requires **bit-identical** `ShardMap`s — same validators, same
row-index slices, same order.

## Running

```
cd fibre-assign/reftest && go test ./...
```

Needs only:

- **Go with toolchain auto-download.** celestia-app pins `go 1.26.6`; the default
  `GOTOOLCHAIN=auto` fetches it.
- **Network to the module proxy.** celestia-app is a normal pinned dependency —
  `require github.com/celestiaorg/celestia-app/v10 v10.4.0-mocha`,
  the tag for commit `5187d2fb5eb8bc4b534c74724882943c54253ae9`. No
  local checkout.

The `replace (...)` block in `go.mod` is copied verbatim from celestia-app's own
`go.mod` at that commit — Go does not apply a dependency's replace directives, so
a build here has to repeat them. Refresh the pin and the block together.

## What it covers

| test | scenarios | what it stresses |
|---|---:|---|
| `TestDifferential_V10BlobV0` | 400 | 1–140 validators, 6 voting-power distributions, blob-v0 parameters |
| `TestDifferential_RandomParams` | 300 | also randomised `(K, N, MinRowsPerValidator, LivenessThreshold)` |
| `TestDifferential_EdgeCases` | 9 shapes × 8 rounds | single / whale / equal-power / 200-equal wrap-around / whale+dust / 1-2-3 / liveness-ceil |
| `TestDifferential_ShuffledInputOrder` | 120 | `fibre-assign` must canonicalise unordered input to match `core.NewValidatorSet` |

~892 scenarios total. Validator keys are deterministic
(`GenPrivKeyFromSecret("reftest/<scenario>/<i>")`), so any failure is
reproducible from its scenario index. The test also asserts
`AddressFromEd25519PubKey == core.Validator.Address` for every validator.

CI runs this as the `fibre-assign-reftest` job on every push.

This test caught one real bug during development: `cmtmath.Fraction`'s fields are
`uint64`, not `int64` — `fibre-assign`'s `Fraction` was fixed to match.
