package api

import (
	"database/sql"
	"testing"
)

// recordColumns gives fixture rows written straight into the tables what the store writes beside every publication
// and verified reading since migration 27, and what that migration backfills over a store's rows: a publication's
// original_rows and total_rows, and each reading's count of distinct verified rows. A fixture that goes through
// store.UpsertPublication and store.InsertProbe has them already.
func recordColumns(t testing.TB, db interface {
	Exec(string, ...any) (sql.Result, error)
}) {
	t.Helper()
	for _, q := range []string{
		`UPDATE publications SET original_rows = json_extract(raw_json, '$.assignment.protocol_params.original_rows'),
			total_rows = json_extract(raw_json, '$.assignment.protocol_params.total_rows')
		 WHERE original_rows IS NULL AND json_valid(raw_json)`,
		`INSERT OR REPLACE INTO reading_rows (promise_hash, scheduled_at, exact)
		 SELECT qx.promise_hash, qx.scheduled_at, COUNT(DISTINCT j.value) FROM probes qx, json_each(qx.row_indices) j
		 WHERE qx.commitment_verified = 1 AND qx.row_indices IS NOT NULL AND json_valid(qx.row_indices)
		 GROUP BY qx.promise_hash, qx.scheduled_at`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
}
