package api

import (
	"database/sql"
	"math"
	"math/big"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
)

// TestLoadSumIsExactlyRounded holds the bundled SQLite to what the day
// partials' load bytes rest on (loadPart.loadBytes): summed with SUM, terms
// that are each an integer times 2^-k come to the exact sum rounded to the
// nearest double, ties to even, in whatever order they are added. SQLite
// has summed REAL values with Kahan-Babuska-Neumaier compensation since
// 3.43; every term, partial sum and rounding error is then a multiple of
// 2^-K, and the error is carried exactly while n·S < 2^(106-K). A version
// of SQLite that sums otherwise fails here before it can change a figure.
//
// Each trial draws terms row_count * blob_size / original_rows with
// original_rows 2^11, 2^12 or 2^13 and products up to 2^52, so the sums
// reach 2^54 and round, sums them in several random orders with the
// statement's own expression, and compares CAST(SUM(...) AS INTEGER) with
// loadBytes over the same terms kept as exact integers.
// TENSILE_LOADSUM_TERMS sets the terms per trial (1.9 million reach 2^60).
func TestLoadSumIsExactlyRounded(t *testing.T) {
	terms := 40000
	if v, err := strconv.Atoi(os.Getenv("TENSILE_LOADSUM_TERMS")); err == nil && v > 0 {
		terms = v
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var version string
	if err := db.QueryRow(`SELECT sqlite_version()`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("SQLite %s, %d terms a trial", version, terms)
	if _, err := db.Exec(`CREATE TABLE t (rc INTEGER NOT NULL, bs INTEGER NOT NULL, orig INTEGER NOT NULL, k REAL NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(2026, 9))
	mismatches, compared := 0, 0
	for trial := 0; trial < 4; trial++ {
		if _, err := db.Exec(`DELETE FROM t`); err != nil {
			t.Fatal(err)
		}
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		ins, err := tx.Prepare(`INSERT INTO t (rc, bs, orig, k) VALUES (?, ?, ?, 0)`)
		if err != nil {
			t.Fatal(err)
		}
		l := &loadPart{Bytes: map[int]*big.Int{}}
		for i := 0; i < terms; i++ {
			k := 12
			if trial%2 == 1 {
				k = 11 + rng.IntN(3)
			}
			rc := int64(1 + rng.IntN(16384))
			bs := 1 + rng.Int64N((int64(1)<<52)/rc)
			if trial == 3 {
				bs = 1 + rng.Int64N(1<<32) // the range a blob size can have
			}
			if _, err := ins.Exec(rc, bs, int64(1)<<k); err != nil {
				t.Fatal(err)
			}
			p := (loadTerms{n: 1, rows: rc, hi: (rc * bs) >> 20, lo: rc * bs & (1<<20 - 1), maxTerm: rc * bs, maxSize: bs}).part(int64(1) << k)
			// part splits the product itself; here it is split as the SQL
			// splits blob_size, which comes to the same integer
			p.Bytes = map[int]*big.Int{k: new(big.Int).Mul(big.NewInt(rc), big.NewInt(bs))}
			l.add(p)
		}
		ins.Close()
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		want, ok := l.loadBytes()
		if !ok {
			t.Fatalf("trial %d: outside the exact class", trial)
		}
		exact := exactSum(l)
		for order := 0; order < 4; order++ {
			if _, err := db.Exec(`UPDATE t SET k = ?`, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE t SET k = abs(random()) / 9.2e18`); err != nil {
				t.Fatal(err)
			}
			var got int64
			var sum float64
			if err := db.QueryRow(`SELECT CAST(SUM(rc * (bs * 1.0 / NULLIF(orig, 0)) ORDER BY k) AS INTEGER),
					SUM(rc * (bs * 1.0 / NULLIF(orig, 0)) ORDER BY k) FROM t`).Scan(&got, &sum); err != nil {
				t.Fatal(err)
			}
			compared++
			if got != want || sum != exact {
				mismatches++
				t.Errorf("trial %d order %d: SQLite %d (%v), exact sum rounded %d (%v)", trial, order, got, sum, want, exact)
			}
		}
		t.Logf("trial %d: sum %.6g (2^%.1f) in 4 orders", trial, exact, math.Log2(exact))
	}
	t.Logf("%d sums compared, %d mismatches", compared, mismatches)
}

// exactSum is the exact sum of a partial's terms, rounded to the nearest
// double, ties to even.
func exactSum(l *loadPart) float64 {
	sum := new(big.Rat)
	for k, v := range l.Bytes {
		sum.Add(sum, new(big.Rat).SetFrac(v, new(big.Int).Lsh(big.NewInt(1), uint(k))))
	}
	f, _ := sum.Float64() // the nearest float64, ties to even
	return f
}
