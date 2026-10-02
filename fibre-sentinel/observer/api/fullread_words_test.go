package api

import (
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The words of a not-served reading say the rule it was judged by: at a
// full reading the validator's own answers, asked again; at an end reading
// from probe.FullReadSince on, asked once; before it, rows on a blob that
// could not be reconstructed. A short answer of its own rows says so.
func TestNotServedIsWordedByTheRuleItWasJudgedBy(t *testing.T) {
	after, before := probe.FullReadSince.Add(time.Hour), probe.FullReadSince.Add(-time.Hour)
	for _, c := range []struct {
		name, label   string
		at            time.Time
		outcome       string
		want, wantNot []string
	}{
		{"full", probe.FullReadLabel, after, string(probe.OutcomeNotFound),
			[]string{"own rows did not come back", "every time it was asked again", "on the wire: wire"}, []string{"Unavailable", "asked once"}},
		{"full, short", probe.FullReadLabel, after, string(probe.OutcomePartial),
			[]string{"fewer of this validator's own rows came back than it holds", "asked again"}, []string{"did not come back"}},
		{"end, asked once", probe.EndReadLabel, after, string(probe.OutcomeNotFound),
			[]string{"own rows did not come back", "which asked once"}, []string{"asked again", "Unavailable"}},
		{"end, before", probe.EndReadLabel, before, string(probe.OutcomeNotFound),
			[]string{"the blob was Unavailable"}, []string{"asked"}},
	} {
		got := serviceReason(probeRow{Service: "not_served", ScheduleLabel: c.label, StartedAt: store.TS(c.at), Outcome: c.outcome, Reason: "wire"})
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: %q lacks %q", c.name, got, w)
			}
		}
		for _, w := range c.wantNot {
			if strings.Contains(got, w) {
				t.Errorf("%s: %q says %q", c.name, got, w)
			}
		}
	}

	for _, c := range []struct {
		name, label   string
		at            time.Time
		want, wantNot string
	}{
		{"full", probe.FullReadLabel, after, "at the reading or when it was asked again", "could not be reconstructed"},
		{"end, asked once", probe.EndReadLabel, after, "for its own rows, once", "could not be reconstructed"},
		{"end, before", probe.EndReadLabel, before, "could not be reconstructed", "own rows"},
	} {
		got := firstFaultSummary("h1", c.label, "2026-10-03T00:00:00Z", c.at)
		if !strings.Contains(got, c.want) || strings.Contains(got, c.wantNot) {
			t.Errorf("first fault, %s: %q", c.name, got)
		}
	}
}
