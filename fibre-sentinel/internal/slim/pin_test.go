package slim

import (
	"bytes"
	"errors"
	"testing"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// a celestia-app pin no build of fibre-assign was ever made from
const foreignPin = "00000000000000000000000000000000000000aa"

// Every pin a record on a network was written with, newest last. A re-pin of fibre-assign lists none of the earlier
// pins (they are listed under the compiled pin they were checked against), which makes every slim record written under
// them undecodable, so it must fail here first: list each old pin under the new one when reftest is bit-identical
// across the two, and when the assignment changed, settle how the records written under it stay readable before the
// build ships.
var pinsOnRecord = []string{
	"3b77dc2f5b00e1a646a2e9dd98b5c024a0d9ad8a", // v10.2.0-mocha: mocha, 2026-09-21 to 2026-10-06
	"5187d2fb5eb8bc4b534c74724882943c54253ae9", // v10.4.0-mocha: mocha, from 2026-10-06
}

func TestEveryPinOnRecordIsReproduced(t *testing.T) {
	for _, p := range pinsOnRecord {
		if !assignPins[p] {
			t.Errorf("records were written under celestia-app %s and this build (pinned to %s) does not list it as one it reproduces", p, assign.PinnedCelestiaAppCommit)
		}
	}
	if !assignPins[assign.PinnedCelestiaAppCommit] {
		t.Error("the compiled pin is not listed")
	}
}

// An earlier pin is reproduced only under the compiled pin it was checked against: a build of fibre-assign pinned
// anywhere else (a re-pin whose assignment may have changed) reproduces its own pin and none of the earlier ones, so
// the records written under them are refused with ErrAssignmentPin until they are checked and listed again.
func TestARepinReproducesNoEarlierPin(t *testing.T) {
	pins := pinsReproducedBy(foreignPin)
	if len(pins) != 1 || !pins[foreignPin] {
		t.Fatalf("a build pinned to %s reproduces %v, want only its own pin", foreignPin, pins)
	}
	for compiled, earlier := range reproducedUnder {
		for _, p := range earlier {
			if p == compiled {
				t.Errorf("%s is listed as an earlier pin of itself", p)
			}
			if pins[p] {
				t.Errorf("%s, checked against %s, is reproduced by a build pinned to %s", p, compiled, foreignPin)
			}
		}
	}
}

// A publication whose assignment names a pin this build does not reproduce keeps its validator list whole: nothing
// of it depends on the compiled fibre-assign, so it reads back the same whatever build decodes it, and its readings
// take nothing from its rows.
func TestAForeignPinIsNotDerived(t *testing.T) {
	pub := testPublication(t, 0x61, testValidators(6))
	pub.Assignment.ProtocolParams.PinnedCelestiaApp = foreignPin
	l := line(t, pub)
	w := NewTables()
	body, info, err := w.EncodePublication(l)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(info.PinErr(), ErrAssignmentPin) {
		t.Fatalf("PinErr = %v, want ErrAssignmentPin", info.PinErr())
	}
	v := pub.Assignment.Validators[0]
	if rows, ok := info.Rows(v.Address); ok {
		t.Fatalf("the publication gives %d rows for %s under a pin this build does not reproduce", len(rows), v.Address)
	}
	if len(body) < len(l)/4 {
		t.Errorf("the slim publication is %d bytes of a %d-byte line: its validator list was derived", len(body), len(l))
	}
	m := testReading(pub, v, 0)
	mb, err := w.EncodeMeasurement(line(t, m), info, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := NewTables()
	for _, e := range w.Pending() {
		if err := r.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	got, rPub, err := r.DecodePublication(body)
	if err != nil || !bytes.Equal(got, l) {
		t.Fatalf("publication: %v", err)
	}
	if _, ok := rPub.Rows(v.Address); ok || !errors.Is(rPub.PinErr(), ErrAssignmentPin) {
		t.Fatalf("the decoded publication gives rows or no pin error (PinErr %v)", rPub.PinErr())
	}
	if got, err := r.DecodeMeasurement(mb, rPub, nil); err != nil || !bytes.Equal(got, line(t, m)) {
		t.Fatalf("reading: %v", err)
	}
	// the line form gives its readings the same
	fromLine, err := PubFromLine(l)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fromLine.Rows(v.Address); ok || !errors.Is(fromLine.PinErr(), ErrAssignmentPin) {
		t.Fatalf("PubFromLine gives rows under a foreign pin (PinErr %v)", fromLine.PinErr())
	}
}

// A slim publication whose rows were derived by a build that reproduced its pin, decoded by one that does not, is
// refused with ErrAssignmentPin rather than rebuilt with another assignment; so is a reading that takes its
// validator's rows from it.
func TestADerivedTableUnderAForeignPinIsRefused(t *testing.T) {
	pub := testPublication(t, 0x62, testValidators(6))
	l := line(t, pub)
	w := NewTables()
	body, info, err := w.EncodePublication(l)
	if err != nil {
		t.Fatal(err)
	}
	v := pub.Assignment.Validators[1]
	mb, err := w.EncodeMeasurement(line(t, testReading(pub, v, 0)), info, nil)
	if err != nil {
		t.Fatal(err)
	}
	// the store as a build whose fibre-assign is pinned elsewhere sees it: the record's pin is not one it lists
	r := NewTables()
	n := 0
	for _, e := range w.Pending() {
		if e.Kind == KindString && string(e.Body) == assign.PinnedCelestiaAppCommit {
			e.Body, n = []byte(foreignPin), n+1
		}
		if err := r.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if n != 1 {
		t.Fatalf("the pin is %d dictionary entries, want 1", n)
	}
	if _, _, err := r.DecodePublication(body); !errors.Is(err, ErrAssignmentPin) {
		t.Fatalf("decoding a derived table under a foreign pin: %v, want ErrAssignmentPin", err)
	}
	foreign := bytes.Replace(l, []byte(assign.PinnedCelestiaAppCommit), []byte(foreignPin), 1)
	fromLine, err := PubFromLine(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.DecodeMeasurement(mb, fromLine, nil); !errors.Is(err, ErrAssignmentPin) {
		t.Fatalf("decoding a reading derived from a foreign-pin publication: %v, want ErrAssignmentPin", err)
	}
	// with the pin it was written under, both read back
	if got, _, err := w.DecodePublication(body); err != nil || !bytes.Equal(got, l) {
		t.Fatalf("publication under its own pin: %v", err)
	}
}

// A publication whose assignment failed (no pin, no validator list) has no rows to reproduce: whatever form it is read
// in, it is not reported as another pin's.
func TestAFailedAssignmentIsNotAPinError(t *testing.T) {
	pub := testPublication(t, 0x64, testValidators(3))
	pub.Assignment = scan.AssignmentTable{Error: "no pinned protocol params for blob version 9", ValidatorSetHeight: 999}
	l := line(t, pub)
	w := NewTables()
	body, info, err := w.EncodePublication(l)
	if err != nil {
		t.Fatal(err)
	}
	_, decoded, err := w.DecodePublication(body)
	if err != nil {
		t.Fatal(err)
	}
	fromLine, err := PubFromLine(l)
	if err != nil {
		t.Fatal(err)
	}
	for name, p := range map[string]*Pub{"encoded": info, "decoded": decoded, "line": fromLine} {
		if p.PinErr() != nil {
			t.Errorf("%s: PinErr = %v for a failed assignment", name, p.PinErr())
		}
	}
}
