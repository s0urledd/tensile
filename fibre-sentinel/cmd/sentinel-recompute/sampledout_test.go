package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/policy"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

func writeLines(t *testing.T, path string, vs ...any) {
	t.Helper()
	var b []byte
	for _, v := range vs {
		j, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		b = append(append(b, j...), '\n')
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A sampled-out publication recorded once is expanded into the rows it
// stands for: the obligations come out unobserved_not_probed for every
// attested validator, and -sampling checks the draw from the decision.
func TestSampledOutDecisionIsExpandedAndItsDrawChecked(t *testing.T) {
	dir := t.TempDir()
	settle := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	pub := scan.Publication{
		SchemaVersion:  scan.AttestationSchemaVersion,
		PromiseHash:    "9f2c1e0b6a5d4c3b2a1908f7e6d5c4b3a29180f7e6d5c4b3a29180f7e6d5c4b3",
		SettlementTime: settle, MustServeUntil: settle.Add(4 * time.Hour),
		Promise: scan.PromiseFields{Commitment: "cc"},
		Assignment: scan.AssignmentTable{ValidatorSetHeight: 9, Validators: []scan.ValidatorAssignment{
			{Address: "aa", RowCount: 3, Attested: true}, {Address: "bb", RowCount: 2, Attested: true}, {Address: "cc", RowCount: 1},
		}},
	}
	secret := sha256.Sum256([]byte("day secret"))
	h := sha256.New()
	hb, _ := hex.DecodeString(pub.PromiseHash)
	h.Write(hb)
	h.Write(secret[:])
	draw := float64(binary.BigEndian.Uint64(h.Sum(nil)[:8])) / math.Exp2(64)

	decision := func(p float64) probe.SampledOut {
		d := probe.SampledOut{SchemaVersion: probe.SampledOutSchemaVersion, Kind: probe.SampledOutKind, Vantage: "ut-1",
			PromiseHash: pub.PromiseHash, MustServeUntil: pub.MustServeUntil, DecidedAt: settle.Add(5 * time.Second),
			Sampling: probe.SamplingDecision{P: p, Binding: "validator_bytes_per_day", DayCommitment: "x"},
			Reason:   "budget:p=0.100:validator_bytes_per_day:day_commitment=x", Validators: 3}
		for _, pt := range earlierSchedule(pub) {
			d.Points = append(d.Points, probe.SampledOutPoint{Label: pt.Label, At: pt.At, Phase: probe.PhaseAt(pt.At, pub, probe.ScheduleConfig{})})
		}
		return d
	}
	commit := sha256.Sum256(secret[:])
	writeLines(t, filepath.Join(dir, policy.SecretsFile), policy.Reveal{Day: "2026-09-20", Commitment: hex.EncodeToString(commit[:]),
		Secret: hex.EncodeToString(secret[:]), RevealedAt: settle.Add(8 * 24 * time.Hour)})

	for _, c := range []struct {
		name  string
		p     float64
		diffs int
	}{
		{"drawn out, recorded out", draw / 2, 0},
		{"drawn in, recorded out", math.Min(1, draw*1.5+1e-9), 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(dir, probe.SampledOutFile)
			writeLines(t, path, decision(c.p))
			rows, n, bad := expandSampledOut(path, []scan.Publication{pub}, 10)
			if n != 1 || bad != 0 || len(rows) != 3*len(decision(c.p).Points) {
				t.Fatalf("expanded %d decision(s) into %d rows, %d bad", n, len(rows), bad)
			}
			checked, diffs, err := checkSampling(filepath.Join(dir, policy.SecretsFile), []scan.Publication{pub}, rows, 10)
			if err != nil || checked != 1 || diffs != c.diffs {
				t.Fatalf("sampling check: checked %d, diffs %d (want %d), %v", checked, diffs, c.diffs, err)
			}
		})
	}

	// The obligations the rows make: the two attested validators not read,
	// so not counted; the unattested one is no obligation.
	rows, _, _ := expandSampledOut(filepath.Join(dir, probe.SampledOutFile), []scan.Publication{pub}, 10)
	vrows := make([]verdict.Row, 0, len(rows))
	for _, m := range rows {
		vrows = append(vrows, verdict.FromMeasurement(m))
	}
	win := verdict.Window{End: settle.Add(24 * time.Hour), All: true}
	blobs := verdict.Blobs{pub.PromiseHash: verdict.FactsOf(pub)}
	net, _ := verdict.ComputeObligations(vrows, map[string]time.Time{pub.PromiseHash: settle}, win, blobs)
	if net.Total != 2 || net.NotCounted != 2 {
		t.Fatalf("obligations: %+v", net)
	}

	// A decision the record cannot back is a difference.
	other := pub
	other.PromiseHash = "00"
	path := filepath.Join(dir, probe.SampledOutFile)
	writeLines(t, path, decision(0.1))
	if _, _, bad := expandSampledOut(path, []scan.Publication{other}, 10); bad != 1 {
		t.Fatal("a decision for a promise not on record passed")
	}
}

// earlierSchedule is the schedule the sampled-out decisions on record were
// made under: four in-window points, a grace point and a post point.
func earlierSchedule(pub scan.Publication) []probe.SchedulePoint {
	span := pub.MustServeUntil.Sub(pub.SettlementTime)
	var pts []probe.SchedulePoint
	for i, f := range []float64{0.12, 0.45, 0.72, 0.92} {
		pts = append(pts, probe.SchedulePoint{At: pub.SettlementTime.Add(time.Duration(float64(span) * f)), Phase: probe.PhaseInWindow, Label: fmt.Sprintf("w%d", i+1)})
	}
	return append(pts,
		probe.SchedulePoint{At: pub.MustServeUntil.Add(30 * time.Second), Phase: probe.PhaseGrace, Label: "grace"},
		probe.SchedulePoint{At: pub.MustServeUntil.Add(210 * time.Second), Phase: probe.PhasePost, Label: "post"})
}
