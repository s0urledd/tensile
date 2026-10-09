package api

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The proof that what a successful transaction cost changes no answer but
// the two keys that carry it. Two data dirs hold the same record, failures
// included, read by the real collector, and the same staking identity,
// which gives ctValidator the operator address its failed registration was
// signed by. The second also holds tx_costs.jsonl, its lines at the
// record's own positions: the first publication's settlement, the deposit,
// the withdrawal request, the timeout, and the registration of ctOther's
// event at (27, 1), the transaction at (27, 0). Every route the golden
// harness asks answers the same from both, but for two: the first
// publication's blob answer gains tx_cost, and ctOther's validator answers
// gain that registration's hash on its row.

// ctOtherHostTx is the registration's hash.
var ctOtherHostTx = strings.Repeat("d3", 32)

func TestTxCostsChangeNoAnswerButTheirOwnKeys(t *testing.T) {
	if _, ok := failedtx.OperatorForm(ctValoper); !ok {
		t.Fatalf("%s is no operator address", ctValoper)
	}
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	dirA, dirB := ctDataDir(t, true), ctDataDir(t, true)
	msg := func(kind, signer string) failedtx.Msg {
		return failedtx.Msg{Index: 0, TypeURL: kindTypeURL[kind], Signer: signer}
	}
	line := func(hash string, h int64, i int, m failedtx.Msg) txcost.Record {
		at := ctBlockTime(h)
		if h == 32 {
			at = time.Date(2026, 9, 6, 17, 0, 27, 494519078, time.UTC) // the settlement's block
		}
		return txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(h, i), Height: h, TxIndex: i, Time: at, TxHash: hash,
			GasWanted: 200000, GasUsed: 61234, Fee: "2000utia", FeePayer: ctPublisher, Messages: []failedtx.Msg{m}, RecordedAt: at.Add(time.Second)}
	}
	ctWriteLines(t, filepath.Join(dirB, txcost.FileName),
		line(ctSettledTx, 32, 0, msg(failedtx.KindSettlement, ctPublisher)),
		line(ctDepositTx, 20, 1, msg(failedtx.KindDeposit, ctPublisher)),
		line(ctWithdrawTx, 45, 0, msg(failedtx.KindWithdrawalRequest, ctPublisher)),
		line(ctTimeoutTx, 60, 0, msg(failedtx.KindTimeout, ctPublisher)),
		line(ctOtherHostTx, 27, 0, msg(failedtx.KindSetHost, tqBech("celestiavaloper", 0x57))))
	stA, stB := ctCollect(t, dirA, now), ctCollect(t, dirB, now)
	if a, b := ctCount(t, stA, "tx_costs"), ctCount(t, stB, "tx_costs"); a != 0 || b != 5 {
		t.Fatalf("cost rows: %d without the file, %d with", a, b)
	}
	for _, st := range []*store.Store{stA, stB} {
		at := store.TS(now.Add(-time.Hour))
		tqExec(t, st, `INSERT INTO validator_identities (cons_address, operator_address, moniker, status, first_seen_at, updated_at)
			VALUES (?, ?, 'ct', 'BOND_STATUS_BONDED', ?, ?)`, ctValidator, ctValoper, at, at)
	}

	// The counts, and the rows every figure is counted from.
	ctx := context.Background()
	ca, err := stA.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := stB.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ca != cb {
		t.Errorf("Count: %+v without cost lines, %+v with", ca, cb)
	}
	for _, table := range []string{"publications", "assignments", "payments", "host_events", "probes", "failed_txs", "failed_tx_msgs"} {
		if na, nb := ctCount(t, stA, table), ctCount(t, stB, table); na != nb || na == 0 {
			t.Errorf("%s: %d rows without cost lines, %d with", table, na, nb)
		}
	}

	srvA, srvB := ctServer(t, stA, now), ctServer(t, stB, now)
	reqs := goldenRequests(t, stA, now)
	if other := goldenRequests(t, stB, now); strings.Join(other, "\n") != strings.Join(reqs, "\n") {
		t.Fatalf("the two stores give different requests")
	}
	asked := map[string]bool{}
	for _, r := range reqs {
		asked[r] = true
	}
	blob := "/v1/blobs/" + ctPromise
	for _, r := range []string{"/v1/blobs?tx=" + ctSettledTx, "/v1/blobs?tx=" + ctDepositTx + "&limit=25", "/v1/hosting", blob,
		"/v1/validators/" + ctOther + "?window=24h", "/v1/validators/" + ctValidator + "?window=24h"} {
		if !asked[r] {
			reqs, asked[r] = append(reqs, r), true
		}
	}
	if len(reqs) < 50 {
		t.Fatalf("only %d requests", len(reqs))
	}
	other := "/v1/validators/" + ctOther + "?"
	var sawCost, sawOther int
	for _, r := range reqs {
		ra, rb := ctAsk(srvA, r), ctAsk(srvB, r)
		if r != blob && !strings.HasPrefix(r, other) {
			if a, b := ctWhole(t, ra), ctWhole(t, rb); a != b {
				t.Errorf("%s answers differently with cost lines on record:\n%s\n---\n%s", r, a, b)
			}
			continue
		}
		headA, a := bcSplit(t, ra)
		headB, b := bcSplit(t, rb)
		if r == blob {
			// tx_cost, and nothing else
			if _, ok := a["tx_cost"]; ok {
				t.Errorf("%s: tx_cost without cost lines", r)
			}
			if got, want := string(b["tx_cost"]), `{"gas_wanted":200000,"gas_used":61234,"fee":"2000utia","fee_payer":"`+ctPublisher+`","messages":1}`; got != want {
				t.Errorf("%s: tx_cost %s, want %s", r, got, want)
			}
			delete(b, "tx_cost")
			sawCost++
		} else {
			// the registration's hash on its row, and nothing else
			var ha, hb []map[string]json.RawMessage
			if err := json.Unmarshal(a["endpoint_history"], &ha); err != nil {
				t.Fatalf("%s: %v: %s", r, err, a["endpoint_history"])
			}
			if err := json.Unmarshal(b["endpoint_history"], &hb); err != nil {
				t.Fatalf("%s: %v: %s", r, err, b["endpoint_history"])
			}
			found := false
			for i := range hb {
				if string(hb[i]["height"]) != "27" {
					continue
				}
				if _, ok := ha[i]["tx_hash"]; ok || string(ha[i]["height"]) != "27" || string(hb[i]["tx_hash"]) != `"`+ctOtherHostTx+`"` {
					t.Errorf("%s: the row at 27 without cost lines %v, with %v", r, ha[i], hb[i])
				}
				delete(hb[i], "tx_hash")
				found = true
			}
			if !found || len(ha) != len(hb) {
				t.Fatalf("%s: endpoint_history %s without cost lines, %s with", r, a["endpoint_history"], b["endpoint_history"])
			}
			// both written back the same way
			for _, h := range []struct {
				rows []map[string]json.RawMessage
				body map[string]json.RawMessage
			}{{ha, a}, {hb, b}} {
				raw, err := json.Marshal(h.rows)
				if err != nil {
					t.Fatal(err)
				}
				h.body["endpoint_history"] = raw
			}
			sawOther++
		}
		ja, _ := json.Marshal(a)
		jb, _ := json.Marshal(b)
		if headA != headB || string(ja) != string(jb) {
			t.Errorf("%s: beside the cost, the answers differ:\n%s\n%s\n---\n%s\n%s", r, headA, ja, headB, jb)
		}
	}
	if sawCost != 1 || sawOther == 0 {
		t.Errorf("asked the blob %d time(s) and ctOther's answers %d", sawCost, sawOther)
	}

	// ctValidator's answers are the same in both, the failed registration
	// its operator signed listed in each.
	for _, srv := range []*Server{srvA, srvB} {
		_, rows, ok := ehHistory(t, tqAsk(t, srv, "/v1/validators/"+ctValidator+"?window=24h"))
		listed := false
		for _, e := range rows {
			listed = listed || (e.Outcome == endpointFailed && e.TxHash == ctHostTx)
		}
		if !ok || !listed {
			t.Errorf("%s's endpoint_history does not list %s: %v", ctValidator, ctHostTx, rows)
		}
	}
}
