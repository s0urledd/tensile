package assign

import (
	"math"
	"testing"
)

// mkVals builds a validator set with deterministic addresses: validator i gets
// address 0x00..00(i+1). Canonical order for equal powers is therefore input
// order; for distinct powers it is power-descending.
func mkVals(powers ...int64) []Validator {
	vs := make([]Validator, len(powers))
	for i, p := range powers {
		var a Address
		a[19] = byte(i + 1)
		vs[i] = Validator{Address: a, VotingPower: p}
	}
	return vs
}

var testCommitment = [32]byte{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
	17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
}

func mustAssign(t *testing.T, c [32]byte, vals []Validator, p ProtocolParams) ShardMap {
	t.Helper()
	m, err := Assign(c, vals, p)
	if err != nil {
		t.Fatalf("Assign: %v", err)
	}
	return m
}

func allDistinct(rows []int) bool {
	seen := make(map[int]struct{}, len(rows))
	for _, r := range rows {
		if _, dup := seen[r]; dup {
			return false
		}
		seen[r] = struct{}{}
	}
	return true
}

// ---- ported from celestia-app fibre/validator/set_test.go TestSet_Assign ----

func TestAssign_Degenerate(t *testing.T) {
	cases := []struct {
		name string
		vals []Validator
		p    ProtocolParams
	}{
		{"empty set", nil, ParamsV10BlobV0},
		{"zero total rows", mkVals(1, 1, 1), ProtocolParams{OriginalRows: 4096, TotalRows: 0, MinRowsPerValidator: 148, LivenessThreshold: Fraction{1, 3}}},
		{"zero min rows", mkVals(1, 1, 1), ProtocolParams{OriginalRows: 4096, TotalRows: 16384, MinRowsPerValidator: 0, LivenessThreshold: Fraction{1, 3}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := Assign(testCommitment, tc.vals, tc.p)
			if err != nil {
				t.Fatalf("want nil err, got %v", err)
			}
			if len(m) != 0 {
				t.Fatalf("want empty ShardMap, got %d entries", len(m))
			}
		})
	}
}

func TestAssign_SingleValidatorGetsCappedRows(t *testing.T) {
	p := ProtocolParams{OriginalRows: 10, TotalRows: 30, MinRowsPerValidator: 1, LivenessThreshold: Fraction{1, 3}}
	m := mustAssign(t, testCommitment, mkVals(1), p)
	if len(m) != 1 {
		t.Fatalf("want 1 validator, got %d", len(m))
	}
	for _, rows := range m {
		if len(rows) != 10 { // ceil(10*1*3 / 1*1) = 30, capped at originalRows
			t.Fatalf("want 10 rows, got %d", len(rows))
		}
		if !allDistinct(rows) {
			t.Fatalf("rows not distinct: %v", rows)
		}
	}
}

func TestAssign_EqualStakesExactCover(t *testing.T) {
	// 100 equal validators, Σ counts == totalRows, no wrap, every row once.
	p := ProtocolParams{OriginalRows: 1000, TotalRows: 3000, MinRowsPerValidator: 1, LivenessThreshold: Fraction{1, 3}}
	powers := make([]int64, 100)
	for i := range powers {
		powers[i] = 1
	}
	m := mustAssign(t, testCommitment, mkVals(powers...), p)
	seen := make(map[int]int)
	for _, rows := range m {
		if len(rows) != 30 { // 100 vals * 1% * 3 = 30
			t.Fatalf("want 30 rows/validator, got %d", len(rows))
		}
		for _, r := range rows {
			seen[r]++
		}
	}
	if len(seen) != 3000 {
		t.Fatalf("want 3000 distinct rows, got %d", len(seen))
	}
	for r, n := range seen {
		if n != 1 {
			t.Fatalf("row %d assigned %d times, want 1 (no wrap expected)", r, n)
		}
	}
}

func TestAssign_StakeProportional(t *testing.T) {
	p := ProtocolParams{OriginalRows: 18, TotalRows: 54, MinRowsPerValidator: 1, LivenessThreshold: Fraction{1, 3}}
	vals := mkVals(1, 2, 3)
	m := mustAssign(t, testCommitment, vals, p)

	total := int64(6)
	seen := make(map[int]struct{})
	for _, v := range vals {
		rows := m[v.Address]
		// expected = min(ceil(18 * VP * 3 / (6 * 1)), 18)
		want := int((18*v.VotingPower*3 + 5) / 6)
		if want > 18 {
			want = 18
		}
		if len(rows) != want {
			t.Fatalf("VP=%d: want %d rows, got %d", v.VotingPower, want, len(rows))
		}
		got, err := AssignedRows(v.VotingPower, total, p)
		if err != nil || got != want {
			t.Fatalf("AssignedRows(VP=%d) = %d, %v; want %d", v.VotingPower, got, err, want)
		}
		for _, r := range rows {
			if _, dup := seen[r]; dup {
				t.Fatalf("unexpected cross-validator duplicate row %d (Σ=45 < 54)", r)
			}
			seen[r] = struct{}{}
		}
	}
}

func TestAssign_MinRowsFloor(t *testing.T) {
	p := ProtocolParams{OriginalRows: 6, TotalRows: 18, MinRowsPerValidator: 5, LivenessThreshold: Fraction{1, 3}}
	m := mustAssign(t, testCommitment, mkVals(1, 2, 3), p)
	for a, rows := range m {
		if len(rows) < 5 {
			t.Fatalf("%s: %d rows, want >= minRows 5", a, len(rows))
		}
		for _, r := range rows {
			if r < 0 || r >= 18 {
				t.Fatalf("row index %d out of range [0,18)", r)
			}
		}
	}
}

func TestAssign_Deterministic(t *testing.T) {
	vals := mkVals(5, 3, 3, 1, 1)
	a := mustAssign(t, testCommitment, vals, ParamsV10BlobV0)
	b := mustAssign(t, testCommitment, vals, ParamsV10BlobV0)
	for addr, ra := range a {
		rb := b[addr]
		if len(ra) != len(rb) {
			t.Fatalf("%s: len %d vs %d", addr, len(ra), len(rb))
		}
		for i := range ra {
			if ra[i] != rb[i] {
				t.Fatalf("%s row %d: %d vs %d", addr, i, ra[i], rb[i])
			}
		}
	}
}

// ---- ported from TestSet_AssignedRows ----

func TestAssignedRows_ClampBounds(t *testing.T) {
	p := ParamsV10BlobV0
	for _, stakes := range [][]int64{{1, 2, 3}, {1, 1, 1, 1, 1}, {100, 1, 1}} {
		var total int64
		for _, s := range stakes {
			total += s
		}
		vals := mkVals(stakes...)
		m := mustAssign(t, testCommitment, vals, p)
		for _, v := range vals {
			got, err := AssignedRows(v.VotingPower, total, p)
			if err != nil {
				t.Fatal(err)
			}
			if len(m[v.Address]) != got {
				t.Fatalf("stakes=%v VP=%d: map has %d rows, AssignedRows=%d", stakes, v.VotingPower, len(m[v.Address]), got)
			}
			if got < p.MinRowsPerValidator || got > p.OriginalRows {
				t.Fatalf("VP=%d: count %d outside [%d, %d]", v.VotingPower, got, p.MinRowsPerValidator, p.OriginalRows)
			}
		}
	}
}

// ---- edge cases the task calls out ----

func TestAssign_WraparoundOnMinRowsFloor(t *testing.T) {
	// 200 equal validators at ParamsV10BlobV0: each floored to 148, Σ = 29600 > 16384.
	powers := make([]int64, 200)
	for i := range powers {
		powers[i] = 1
	}
	m := mustAssign(t, testCommitment, mkVals(powers...), ParamsV10BlobV0)

	rowUses := make(map[int]int)
	for _, rows := range m {
		if len(rows) != 148 {
			t.Fatalf("want 148 rows/validator, got %d", len(rows))
		}
		if !allDistinct(rows) {
			t.Fatalf("single validator got a duplicate row (impossible: 148 < 16384): %v", rows)
		}
		for _, r := range rows {
			rowUses[r]++
		}
	}
	if len(rowUses) != 16384 {
		t.Fatalf("want all 16384 rows covered, got %d", len(rowUses))
	}
	overlaps := 0
	for _, n := range rowUses {
		if n > 1 {
			overlaps++
		}
	}
	if overlaps == 0 {
		t.Fatal("expected wrap-around overlaps across validators, found none")
	}
	// Σ = 29600 assignments over 16384 rows, and 29600 < 2*16384, so every row
	// is used once or twice and exactly 29600-16384 rows are used twice.
	twice := 0
	for _, n := range rowUses {
		if n != 1 && n != 2 {
			t.Fatalf("row used %d times, want 1 or 2", n)
		}
		if n == 2 {
			twice++
		}
	}
	if twice != 29600-16384 {
		t.Fatalf("rows-used-twice = %d, want %d", twice, 29600-16384)
	}
}

func TestAssign_EqualPowerOrderingIsAddressStable(t *testing.T) {
	// Same validators, different input order, must yield identical assignment.
	base := mkVals(7, 7, 7, 7, 7)
	want := mustAssign(t, testCommitment, base, ParamsV10BlobV0)

	perms := [][]int{
		{4, 3, 2, 1, 0},
		{2, 0, 4, 1, 3},
		{1, 2, 3, 4, 0},
	}
	for _, perm := range perms {
		shuffled := make([]Validator, len(base))
		for i, src := range perm {
			shuffled[i] = base[src]
		}
		got := mustAssign(t, testCommitment, shuffled, ParamsV10BlobV0)
		for addr, wr := range want {
			gr := got[addr]
			if len(wr) != len(gr) {
				t.Fatalf("perm %v addr %s: len %d vs %d", perm, addr, len(wr), len(gr))
			}
			for i := range wr {
				if wr[i] != gr[i] {
					t.Fatalf("perm %v addr %s row %d: %d vs %d", perm, addr, i, wr[i], gr[i])
				}
			}
		}
	}
}

func TestAssign_SingleValidatorGetsAllOriginalRows(t *testing.T) {
	// Powers span from 1 to well above any real staking-reduced validator power
	// (Celestia total staked ~6e8 reduced units); none overflow the int64
	// row-count formula.
	for _, power := range []int64{1, 5, 1_000_000, 900_000_000} {
		m := mustAssign(t, testCommitment, mkVals(power), ParamsV10BlobV0)
		var rows []int
		for _, r := range m {
			rows = r
		}
		if len(rows) != ParamsV10BlobV0.OriginalRows {
			t.Fatalf("power %d: single validator got %d rows, want %d", power, len(rows), ParamsV10BlobV0.OriginalRows)
		}
		if !allDistinct(rows) {
			t.Fatalf("power %d: rows not distinct", power)
		}
		for _, r := range rows {
			if r < 0 || r >= ParamsV10BlobV0.TotalRows {
				t.Fatalf("row %d out of range", r)
			}
		}
	}
}

func TestAssign_LowPowerValidatorsPinnedToFloor(t *testing.T) {
	vals := mkVals(1000, 1, 1, 1)
	m := mustAssign(t, testCommitment, vals, ParamsV10BlobV0)
	if got := len(m[vals[0].Address]); got != ParamsV10BlobV0.OriginalRows {
		t.Fatalf("whale: %d rows, want %d", got, ParamsV10BlobV0.OriginalRows)
	}
	for _, v := range vals[1:] {
		if got := len(m[v.Address]); got != ParamsV10BlobV0.MinRowsPerValidator {
			t.Fatalf("VP=1 validator: %d rows, want floor %d", got, ParamsV10BlobV0.MinRowsPerValidator)
		}
	}
}

func TestAssignedRows_Overflow(t *testing.T) {
	maxPow := int64(math.MaxInt64 / 8) // cometbft MaxTotalVotingPower
	_, err := AssignedRows(maxPow, maxPow, ParamsV10BlobV0)
	if _, ok := err.(*OverflowError); !ok {
		t.Fatalf("want *OverflowError, got %v", err)
	}
	// Assign should surface it too.
	_, err = Assign(testCommitment, mkVals(maxPow), ParamsV10BlobV0)
	if _, ok := err.(*OverflowError); !ok {
		t.Fatalf("Assign: want *OverflowError, got %v", err)
	}
}

func TestAssign_DuplicateAddress(t *testing.T) {
	v := Validator{Address: MustAddressFromHex("0000000000000000000000000000000000000001"), VotingPower: 5}
	_, err := Assign(testCommitment, []Validator{v, v}, ParamsV10BlobV0)
	if _, ok := err.(*DuplicateAddressError); !ok {
		t.Fatalf("want *DuplicateAddressError, got %v", err)
	}
}

// Two entries for one address with different power are not neighbours once
// another validator's power sorts between them; the set is still refused.
func TestAssign_DuplicateAddressNotAdjacent(t *testing.T) {
	a := MustAddressFromHex("0000000000000000000000000000000000000001")
	b := MustAddressFromHex("0000000000000000000000000000000000000002")
	vals := []Validator{{Address: a, VotingPower: 10}, {Address: b, VotingPower: 5}, {Address: a, VotingPower: 1}}
	m, err := Assign(testCommitment, vals, ParamsV10BlobV0)
	dup, ok := err.(*DuplicateAddressError)
	if !ok {
		t.Fatalf("want *DuplicateAddressError, got %v (map of %d)", err, len(m))
	}
	if dup.Address != a {
		t.Fatalf("duplicate reported as %s, want %s", dup.Address, a)
	}
	if m != nil {
		t.Fatalf("a refused set must not return a map, got %d entries", len(m))
	}
}

func TestParams_Validate(t *testing.T) {
	if err := ParamsV10BlobV0.Validate(); err != nil {
		t.Fatalf("ParamsV10BlobV0 must validate: %v", err)
	}
	bad := []ProtocolParams{
		{OriginalRows: 0, TotalRows: 1, MinRowsPerValidator: 0, LivenessThreshold: Fraction{1, 3}},
		{OriginalRows: 100, TotalRows: 50, MinRowsPerValidator: 10, LivenessThreshold: Fraction{1, 3}},
		{OriginalRows: 100, TotalRows: 200, MinRowsPerValidator: 500, LivenessThreshold: Fraction{1, 3}},
		{OriginalRows: 100, TotalRows: 200, MinRowsPerValidator: 10, LivenessThreshold: Fraction{3, 3}},
		{OriginalRows: 100, TotalRows: 200, MinRowsPerValidator: 10, LivenessThreshold: Fraction{0, 3}},
	}
	for i, p := range bad {
		if err := p.Validate(); err == nil {
			t.Fatalf("bad params %d validated", i)
		}
	}
}

// ---- MinRowsPerValidator pinned-constant cross-check ----

func TestPinnedMinRowsMatchesFormula(t *testing.T) {
	// Re-derive ProtocolParams.MinRowsPerValidator() for v10 defaults
	// (fibre/protocol_params.go): max(uniqueDecodeSamples, reconstructionSamples).
	const (
		lambda            = 100  // UniqueDecodingSecurityBits
		encodingRatio     = 0.25 // EncodingRatio = K/(K+N)
		maxValidatorCount = 100
		rows              = 4096 // K
		ltNum, ltDen      = 1, 3
	)
	uniqueDecodeSamples := int(math.Ceil(lambda / (1 - math.Log2(1+encodingRatio))))
	validatorsForReconstruction := ceilDiv(maxValidatorCount*ltNum, ltDen)
	if validatorsForReconstruction < 1 {
		validatorsForReconstruction = 1
	}
	reconstructionSamples := ceilDiv(rows, validatorsForReconstruction)
	want := uniqueDecodeSamples
	if reconstructionSamples > want {
		want = reconstructionSamples
	}
	if want != ParamsV10BlobV0.MinRowsPerValidator {
		t.Fatalf("formula gives MinRowsPerValidator=%d, ParamsV10BlobV0 pins %d", want, ParamsV10BlobV0.MinRowsPerValidator)
	}
}

func ceilDiv(a, b int) int { return (a + b - 1) / b }
