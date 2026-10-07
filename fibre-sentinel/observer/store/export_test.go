package store

// What the tests of package store_test reach of the backfill's insides.

// SetBatch bounds the backfill's batches to rows rows and bytes bytes, so that a store of a few rows takes several.
func (b *Backfill) SetBatch(rows, bytes int) { b.rows, b.bytes = rows, bytes }

// OnCommit hands f every value a batch converted, with the value it replaced, once the batch has committed.
func (b *Backfill) OnCommit(f func([]BackfillValue) error) { b.committed = f }
