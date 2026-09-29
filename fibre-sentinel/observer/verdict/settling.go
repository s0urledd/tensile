package verdict

import "time"

// FaultSettling is how long a counted not-served reading stays provisional
// after the reading that found it started.
//
// A not-served reading can still be taken back automatically for a short
// while after it is written, by evidence that has not reached the store
// yet. Two paths are bounded, and the settling period covers both with
// margin:
//
//   - A silent x/fibre params change is noticed at the scanner's next
//     reconcile, at most paramReconcileEvery (60) blocks later — about six
//     minutes at mocha's block time — and the range it opens withholds the
//     rows it covers (RETENTION_UNVERIFIED) in the same transaction that
//     records it. A reading drawn against the stale deadline in between is
//     withdrawn then.
//   - Ingest: the collector tails the prober's files every 10 s, and a
//     scanner that fell behind the chain catches up in minutes on a healthy
//     node. A blob's reading is written whole, once it is final, so no part
//     of the reading that decides whether a failure counts can arrive after
//     the failure itself.
//
// Half an hour is several times the longer of the two. Longer would hold a
// reading away from its final word for no evidence that can still arrive: a
// dispute is a human process with no bound, which is what the amendment
// record is for, not a settling period.
//
// Provisional is a label, not a hold. A provisional reading is counted in
// every figure exactly as a final one is, and flagged so a reader knows the
// figure can still move; docs/verdicts.md ("Provisional faults") gives the
// reasons. The figures themselves do not depend on it, which is why
// changing this constant does not change MethodologyVersion.
const FaultSettling = 30 * time.Minute
