package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// Fibre is live when the chain is on its version and x/fibre answers, not
// when the version alone says so: at the upgrade height the old binary
// reports v10 with no module behind it.
func TestFibreIsActiveOnlyOnceTheModuleAnswers(t *testing.T) {
	unknownPath := &scan.ABCIError{Code: 6, Codespace: "sdk", Log: "unknown query path"}
	calls := 0
	for _, c := range []struct {
		name    string
		av      uint64
		err     error
		verdict string
		known   bool
		asks    bool
	}{
		{"below the Fibre version", 9, nil, "no", true, false},
		{"v10 reported, module not there (halted old binary)", 10, unknownPath, "no", true, true},
		{"v10 and x/fibre answers", 10, nil, "yes", true, true},
		{"v10, node busy", 10, errors.New("server busy"), "", false, true},
	} {
		calls = 0
		v, known := fibreActive(c.av, func() error { calls++; return c.err })
		if v != c.verdict || known != c.known || (calls > 0) != c.asks {
			t.Errorf("%s: verdict=%q known=%v asked=%d", c.name, v, known, calls)
		}
	}
}

// While a vantage file holds rows from another chain the export is not
// built (it would copy them whole, signed, for good), and the hold says
// which vantage; with no such file the builder runs as before.
func TestTheExportIsHeldWhileAVantageFileIsFromAnotherChain(t *testing.T) {
	now := time.Date(2026, 10, 8, 4, 0, 0, 0, time.UTC)
	runs := 0
	run := func(time.Time) ([]string, error) { runs++; return []string{"2026-10-07.tar.gz"}, nil }
	built, held, err := exportStep(run, []string{"mo-1"}, now)
	if !held || err == nil || !strings.Contains(err.Error(), "mo-1") || runs != 0 || built != nil {
		t.Fatalf("held=%v err=%v runs=%d built=%v", held, err, runs, built)
	}
	built, held, err = exportStep(run, nil, now)
	if held || err != nil || runs != 1 || len(built) != 1 {
		t.Fatalf("not held: held=%v err=%v runs=%d built=%v", held, err, runs, built)
	}
	fail := errors.New("index.json missing")
	if _, held, err = exportStep(func(time.Time) ([]string, error) { return nil, fail }, nil, now); held || !errors.Is(err, fail) {
		t.Fatalf("a builder failure: held=%v err=%v", held, err)
	}
}
