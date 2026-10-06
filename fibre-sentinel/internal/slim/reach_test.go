package slim

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

// heartbeat is an endpoint check record as observer-heartbeat writes it.
func heartbeat(validator, host string, at time.Time, ok bool) []byte {
	m := probe.Measurement{SchemaVersion: 2, Vantage: "ut-1", ValidatorSetHeight: 1389418, ValidatorAddress: validator, ValidatorHost: host,
		ScheduleLabel: "heartbeat", ScheduledAt: at, StartedAt: at.Add(291025838), FinishedAt: at.Add(362448261), LatenessMS: 291,
		Phase: probe.PhasePost, Classification: probe.ClassNotProbed, ClassificationReason: "reachable; download skipped by policy", TotalDurationMS: 71}
	m.DNS = probe.StepResult{OK: true, Detail: "literal IP 173.201.36.182"}
	m.TCP = probe.StepResult{Attempted: true, OK: ok, DurationMS: 35, Detail: "-> 173.201.36.182:7980"}
	m.Outcome = probe.OutcomeReachable
	if !ok {
		m.Outcome, m.RawError = probe.OutcomeTCPTimeout, "dial tcp 173.201.36.182:7980: i/o timeout"
	}
	b, _ := json.Marshal(m)
	return b
}

// Every endpoint check record comes back byte for byte, a failed one too, through tables loaded by another process
// from the entries the writer kept.
func TestEndpointChecksComeBackByteForByte(t *testing.T) {
	w := NewTables()
	at := time.Date(2026, 10, 5, 0, 2, 11, 0, time.UTC)
	var lines, bodies [][]byte
	for i := 0; i < 50; i++ {
		l := heartbeat("ac2a961260b80fc88ddb9e52f1a957e2f5d60d64", "173.201.36.182:7980", at.Add(time.Duration(i)*5*time.Minute), i%7 != 3)
		b, err := w.EncodeReachability(l)
		if err != nil {
			t.Fatal(err)
		}
		lines, bodies = append(lines, l), append(bodies, b)
	}
	r := NewTables()
	for _, e := range w.Pending() {
		if err := r.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	for i, b := range bodies {
		got, err := r.DecodeReachability(b)
		if err != nil || !bytes.Equal(got, lines[i]) {
			t.Fatalf("record %d: %v\n got %s\nwant %s", i, err, got, lines[i])
		}
	}
	if n := len(bodies[1]); n > len(lines[1])/5 {
		t.Errorf("a record of %d bytes kept in %d", len(lines[1]), n)
	}
	// a record that names an entry the reader has not loaded asks for it
	if _, err := NewTables().DecodeReachability(bodies[0]); err == nil {
		t.Error("a record decoded without its tables")
	}
	// a line Go's encoder would not write is refused rather than written back otherwise
	if _, err := NewTables().EncodeReachability([]byte(`{"a": 1}`)); err == nil {
		t.Error("a line with a space after the colon was encoded")
	}
}

// TestEndpointChecksOfARecord encodes every line of the endpoint check files named in TENSILE_SLIM_REACH
// (comma-separated, in order, as the collector would take them in), decodes each with tables loaded only from what
// was kept, compares byte for byte, and says how many bytes a line takes, the tables included.
func TestEndpointChecksOfARecord(t *testing.T) {
	files := os.Getenv("TENSILE_SLIM_REACH")
	if files == "" {
		t.Skip("TENSILE_SLIM_REACH not set")
	}
	w := NewTables()
	var n, lineBytes, bodyBytes, refused int64
	var lines, bodies [][]byte
	// the reader loads only what the writer kept, as it is kept: entries in batches, as transactions commit
	r := NewTables()
	var tableBytes int64
	kinds := map[int]int{}
	check := func() {
		for _, e := range w.Pending() {
			if err := r.Add(e); err != nil {
				t.Fatal(err)
			}
			tableBytes += int64(len(e.Body))
			kinds[e.Kind]++
		}
		for i, b := range bodies {
			got, err := r.DecodeReachability(b)
			if err != nil || !bytes.Equal(got, lines[i]) {
				t.Fatalf("a record does not come back: %v\n got %s\nwant %s", err, got, lines[i])
			}
		}
		lines, bodies = lines[:0], bodies[:0]
	}
	for _, path := range strings.Split(files, ",") {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<26)
		for sc.Scan() {
			l := append([]byte(nil), sc.Bytes()...)
			n++
			lineBytes += int64(len(l))
			b, err := w.EncodeReachability(l)
			if err != nil {
				refused++
				continue
			}
			bodyBytes += int64(len(b))
			lines, bodies = append(lines, l), append(bodies, b)
			if len(lines) == 20000 {
				check()
			}
		}
		f.Close()
		if err := sc.Err(); err != nil {
			t.Fatal(err)
		}
	}
	check()
	t.Logf("%d records, every one back byte for byte; %d kept as their line (not Go's encoding)", n, refused)
	t.Logf("lines %.0f B each; slim %.1f B each, %.1f B with the tables (%d B: %d strings, %d shapes)",
		float64(lineBytes)/float64(n), float64(bodyBytes)/float64(n), float64(bodyBytes+tableBytes)/float64(n), tableBytes, kinds[0], kinds[1])
}
