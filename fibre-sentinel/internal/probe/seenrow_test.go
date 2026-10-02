package probe

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// Every field a restart decodes (seenRow) is Measurement's own: the same
// name, type and JSON key, so the lean decode reads what the row says.
func TestSeenRowMatchesMeasurement(t *testing.T) {
	st, mt := reflect.TypeOf(seenRow{}), reflect.TypeOf(Measurement{})
	for i := 0; i < st.NumField(); i++ {
		f := st.Field(i)
		g, ok := mt.FieldByName(f.Name)
		if !ok {
			t.Fatalf("seenRow.%s is not a field of Measurement", f.Name)
		}
		if f.Type != g.Type || f.Tag.Get("json") != g.Tag.Get("json") {
			t.Fatalf("seenRow.%s is %s %q, Measurement's is %s %q", f.Name, f.Type, f.Tag.Get("json"), g.Type, g.Tag.Get("json"))
		}
	}
}

// A row read back by the lean decode is the row remember was given: every
// field remember reads survives the trip.
func TestSeenRowKeepsWhatRememberReads(t *testing.T) {
	at := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	due := at.Add(2 * time.Minute)
	m := Measurement{SchemaVersion: MeasurementSchemaVersion, Vantage: "v1", PromiseHash: "p1", ValidatorAddress: "a1", ValidatorHost: "h:1",
		HostAtSettlement: "h:0", Assigned: true, Attested: true, AttestationUnknown: true, AssignedRowCount: 7, ScheduleLabel: FullReadLabel,
		ScheduledAt: at, StartedAt: at, FinishedAt: at.Add(time.Second), Attempt: 1, NextAttemptDue: &due,
		Read:    &ReadInfo{Order: 4, NovelRows: 2, BlobHaveAfter: 9, BlobResult: ReadUnavailable, BlobError: ClientErrNotEnoughShards},
		Outcome: OutcomeNotFound, Classification: ClassFault}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var r seenRow
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	got := r.measurement()
	if got.DedupeKey() != m.DedupeKey() || got.ValidatorHost != m.ValidatorHost || got.HostAtSettlement != m.HostAtSettlement ||
		got.Assigned != m.Assigned || got.Attested != m.Attested || got.AttestationUnknown != m.AttestationUnknown ||
		got.AssignedRowCount != m.AssignedRowCount || got.ScheduleLabel != m.ScheduleLabel || !retryOpen(got) ||
		!got.NextAttemptDue.Equal(due) || *got.Read != *m.Read {
		t.Fatalf("read back %+v, want %+v", got, m)
	}
}
