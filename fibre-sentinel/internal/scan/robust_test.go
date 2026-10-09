package scan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

func TestLoadPublications_TornTailIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "publications.jsonl")
	os.WriteFile(path, []byte(`{"promise_hash":"aa"}`+"\n"+`{"promise_hash":"bb","settle`), 0o644)
	pubs, err := LoadPublications(path)
	if err != nil || len(pubs) != 1 || pubs[0].PromiseHash != "aa" {
		t.Fatalf("pubs=%+v err=%v", pubs, err)
	}
	// a malformed COMPLETE line is still a hard error
	os.WriteFile(path, []byte(`{"promise_hash":"aa"}`+"\n"+`{bad}`+"\n"), 0o644)
	if _, err := LoadPublications(path); err == nil {
		t.Fatal("malformed complete line must error")
	}
}

func TestOpenStore_RepairsTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "publications.jsonl")
	os.WriteFile(path, []byte(`{"promise_hash":"aa","settlement_tx_hash":"t1"}`+"\n"+`{"promise_hash":"bb","settlem`), 0o644)
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	if !st.Seen("t1") {
		t.Fatal("intact record not loaded")
	}
	b, _ := os.ReadFile(path)
	if string(b) != `{"promise_hash":"aa","settlement_tx_hash":"t1"}`+"\n" {
		t.Fatalf("torn tail not truncated: %q", b)
	}
	if err := st.AppendPublication(Publication{PromiseHash: "cc", SettlementTxHash: "t2"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	pubs, err := LoadPublications(path)
	if err != nil || len(pubs) != 2 {
		t.Fatalf("after append: %d pubs, err %v", len(pubs), err)
	}
	if err := st.SaveState(PersistState{ChainID: "x", LastScannedHeight: 7}); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadState()
	if err != nil || got == nil || got.LastScannedHeight != 7 {
		t.Fatalf("state: %+v err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state.json.tmp")); !os.IsNotExist(err) {
		t.Fatal("temp state file left behind")
	}
}

func TestIsModuleInactive_ByCode(t *testing.T) {
	if !IsModuleInactive(&ABCIError{Code: 6, Codespace: "sdk", Log: "unknown request: ..."}) {
		t.Fatal("sdk/6 should be inactive")
	}
	if IsModuleInactive(&ABCIError{Code: 3, Codespace: "sdk", Log: "invalid request"}) {
		t.Fatal("sdk/3 is not inactive")
	}
	if !IsModuleInactive(errors.New("rpc error: unknown query path")) {
		t.Fatal("text match lost")
	}
	if !IsResultsNotPersisted(errors.New("block_results 5: rpc: finalize block responses not persisted")) {
		t.Fatal("not-persisted detection")
	}
}

func TestRetryRPC(t *testing.T) {
	s := &Scanner{log: NewLogger(10)}
	calls := 0
	err := s.retryRPC(context.Background(), "test", func() error {
		calls++
		if calls < 2 {
			return errors.New("block_results 9: not found")
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
	// a height the node does not have is retried for the grace period and
	// then reported as unavailable, typed, so the scanner can record a gap
	// and move on instead of exiting.
	prev := unavailableGrace
	unavailableGrace = 0
	defer func() { unavailableGrace = prev }()
	calls = 0
	err = s.retryRPCAt(context.Background(), "test", 9, func() error {
		calls++
		return errors.New("finalize block responses not persisted")
	})
	var ue *ErrHeightUnavailable
	if !errors.As(err, &ue) || ue.Height != 9 || calls != 1 {
		t.Fatalf("not persisted: err=%v calls=%d", err, calls)
	}
	if !s.recordGap(9, err, time.Time{}) || !s.recordGap(10, err, time.Time{}) || len(s.gaps) != 1 || s.gaps[0].From != 9 || s.gaps[0].To != 10 {
		t.Fatalf("gaps not merged: %+v", s.gaps)
	}
	if s.recordGap(11, errors.New("boom"), time.Time{}) {
		t.Fatal("a transient error must never become a gap")
	}
	err = s.retryRPCAt(context.Background(), "test", 12, func() error {
		return errors.New("height 12 is not available, lowest height is 500")
	})
	if !errors.As(err, &ue) {
		t.Fatalf("pruned: %v", err)
	}
	// a cancelled context stops at once
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls = 0
	start := time.Now()
	err = s.retryRPC(ctx, "test", func() error { calls++; return errors.New("boom") })
	if err == nil || calls != 1 || time.Since(start) > time.Second {
		t.Fatalf("cancelled: err=%v calls=%d", err, calls)
	}
}

// A promise may be up to PaymentPromiseHeightWindow blocks older than the
// block that settles it, and the assignment table is built over the validator
// set at the promise height. A node whose state base sits inside that span —
// state-synced, or with a retention floor above it — cannot serve that set at
// all. CometBFT's wording for it matches none of the other phrases, so the
// retry loop had no exit: the scan stopped advancing on that block, forever,
// with no gap recorded and nothing on the site saying so.
func TestIsHeightUnavailable_PrunedValidatorSet(t *testing.T) {
	for _, s := range []string{
		"could not find validator set for height #1234",
		"RPC error -32603 - Internal error: could not find validator set for height 987",
		"Could Not Find Validator Set For Height #1",
	} {
		if !IsHeightUnavailable(errors.New(s)) {
			t.Errorf("a pruned validator set was not read as an unavailable height: %q", s)
		}
	}
	// and the phrases that must stay unrecognised, so a real defect is not
	// quietly filed as a gap
	for _, s := range []string{
		"connection refused",
		"context deadline exceeded",
		"invalid signature on promise",
	} {
		if IsHeightUnavailable(errors.New(s)) {
			t.Errorf("%q was read as an unavailable height; it is not one", s)
		}
	}
}

// wholeLines decodes every line of a JSONL file, failing on one that does
// not decode, and returns how many there are.
func wholeLines(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		var v map[string]any
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			t.Fatalf("%s line %d does not decode: %q", filepath.Base(path), n+1, line)
		}
		n++
	}
	return n
}

// host_history.jsonl and param_uncertainty.jsonl are repaired before the
// first append, as publications.jsonl and payments.jsonl are at open, and
// failed_txs.jsonl and tx_costs.jsonl are at open too (their seen-sets are
// read there). A crash or a full disk in the middle of a write leaves a
// partial last line; the next record used to be written straight after it,
// and the two came out as one line the collector cannot decode and skips: a
// range or a registration the scanner never writes again.
func TestTheSideRecordsAreRepairedBeforeTheFirstAppend(t *testing.T) {
	dir := t.TempDir()
	unc := filepath.Join(dir, "param_uncertainty.jsonl")
	hosts := filepath.Join(dir, "host_history.jsonl")
	failed := filepath.Join(dir, failedtx.FileName)
	costs := filepath.Join(dir, txcost.FileName)
	if err := os.WriteFile(unc, []byte(`{"schema_version":1,"id":"t:check_skipped:1-2"}`+"\n"+`{"schema_version":1,"id":"t:silent_chan`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hosts, []byte(`{"cons_address":"aa","source":"seed"}`+"\n"+`{"cons_addr`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(failed, []byte(`{"schema_version":1,"dedupe_key":"h1:0","height":1}`+"\n"+`{"schema_version":1,"dedupe_ke`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(costs, []byte(`{"schema_version":1,"dedupe_key":"h1:1","height":1}`+"\n"+`{"schema_version":1,"dedupe_ke`), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.AppendParamUncertainty(ParamUncertainty{ID: "t:silent_change:3-4", Kind: UncertaintySilentChange, FromHeight: 3, ToHeight: 4}); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendHostEvent(HostEvent{HostEntry: HostEntry{FromHeight: 5, ConsAddress: "bb", Host: "b.example:7980", Source: HostFromEvent}}); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendFailedTx(failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(2, 0), Height: 2, Code: 5, Codespace: "sdk"}); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendTxCost(txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(2, 1), Height: 2, TxIndex: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{unc, hosts, failed, costs} {
		if n := wholeLines(t, path); n != 2 {
			t.Fatalf("%s: %d lines, want the whole one before and the new one", filepath.Base(path), n)
		}
	}
}

// An append that fails leaves nothing for the retry to be glued onto: the
// file is cut back to where the line began and the handle is dropped, so
// the retry opens the file again through the repair. Here the write cannot
// be made (the handle is closed under the store) and a partial line sits
// at the end of the file, as a write that failed partway leaves it; the
// retry still writes a whole line. Before, the dead handle was kept and
// every later append failed, or, with a live one, landed on the partial
// bytes.
func TestAFailedSideRecordAppendIsRetriedOntoAWholeLine(t *testing.T) {
	for _, c := range []struct {
		name   string
		handle func(*Store) *os.File
		append func(*Store, int) error
	}{
		{"param_uncertainty.jsonl", func(st *Store) *os.File { return st.uncFile }, func(st *Store, i int) error {
			return st.AppendParamUncertainty(ParamUncertainty{ID: fmt.Sprintf("t:check_skipped:%d-%d", i, i), Kind: UncertaintyCheckSkipped, FromHeight: int64(i), ToHeight: int64(i)})
		}},
		{"host_history.jsonl", func(st *Store) *os.File { return st.hostFile }, func(st *Store, i int) error {
			return st.AppendHostEvent(HostEvent{HostEntry: HostEntry{FromHeight: int64(i), ConsAddress: "aa", Host: "a.example:7980", Source: HostFromEvent}})
		}},
		{failedtx.FileName, func(st *Store) *os.File { return st.failFile }, func(st *Store, i int) error {
			return st.AppendFailedTx(failedtx.Record{SchemaVersion: failedtx.SchemaVersion, DedupeKey: failedtx.Key(int64(i), 0), Height: int64(i), Code: 5, Codespace: "sdk"})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, c.name)
			st, err := OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if err := c.append(st, 1); err != nil {
				t.Fatal(err)
			}
			_ = c.handle(st).Close()
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteString(`{"schema_version":1,"from_hei`); err != nil {
				t.Fatal(err)
			}
			f.Close()

			if err := c.append(st, 2); err == nil {
				t.Fatal("an append on a closed handle reported success")
			}
			if err := c.append(st, 2); err != nil {
				t.Fatalf("the retry failed: %v", err)
			}
			if n := wholeLines(t, path); n != 2 {
				t.Fatalf("%d lines, want 2", n)
			}
		})
	}
}
