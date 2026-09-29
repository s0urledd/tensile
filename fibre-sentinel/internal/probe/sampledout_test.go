package probe

import (
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

const drawnOut = "budget:p=0.286:validator_bytes_per_day:day_commitment=c0ffee"

// A sampled-out decision on record expands to the NOT_PROBED rows it stood
// for: one per assigned validator per point, with the draw on each.
func TestSampledOutExpandsToItsRows(t *testing.T) {
	now := time.Now().UTC()
	pb := pub(now.Add(-2*time.Minute), now.Add(30*time.Minute))
	pb.SchemaVersion = scan.AttestationSchemaVersion
	pb.Assignment.Validators = []scan.ValidatorAssignment{
		{Address: "aa01", RowCount: 12, Attested: true},
		{Address: "aa02", RowCount: 7, Attested: false},
		{Address: "aa03", RowCount: 3, Attested: true},
		{Address: "aa04"}, // not assigned
	}
	d := SampledOut{SchemaVersion: SampledOutSchemaVersion, Kind: SampledOutKind, Vantage: "v1",
		PromiseHash: pb.PromiseHash, MustServeUntil: pb.MustServeUntil, DecidedAt: now,
		Sampling: SamplingDecision{P: 0.286, Binding: "validator_bytes_per_day", DayCommitment: "c0ffee"},
		Reason:   drawnOut, Validators: 3,
		Points: []SampledOutPoint{{Label: "w1", At: now, Phase: PhaseInWindow}, {Label: "grace", At: pb.MustServeUntil.Add(30 * time.Second), Phase: PhaseGrace}}}
	ms := d.Expand(pb)
	if len(ms) != 6 {
		t.Fatalf("%d rows, want 3 validators x 2 points", len(ms))
	}
	for _, m := range ms {
		if m.Classification != ClassNotProbed || m.ClassificationReason != drawnOut || m.Sampling == nil || m.Sampling.P != 0.286 {
			t.Fatalf("row = %+v", m)
		}
		if !IsSampledOutRow(m) {
			t.Fatal("an expanded row is not recognised as one")
		}
	}
}

func TestIsSampledOutRow(t *testing.T) {
	for _, c := range []struct {
		cls    Classification
		reason string
		want   bool
	}{
		{ClassNotProbed, drawnOut, true},
		{ClassNotProbed, "budget:validator_bytes_per_day", false},
		{ClassNotProbed, "scheduled point elapsed before the prober ran it", false},
		{ClassHealthy, drawnOut, false},
	} {
		if got := IsSampledOutRow(Measurement{Classification: c.cls, ClassificationReason: c.reason}); got != c.want {
			t.Errorf("%s %q: got %v", c.cls, c.reason, got)
		}
	}
}
