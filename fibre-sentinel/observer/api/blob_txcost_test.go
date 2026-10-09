package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// bcSplit is an answer as compared: its status and headers, and its body
// with the computation's duration zeroed (ctWhole), the body's keys raw.
func bcSplit(t *testing.T, resp *http.Response) (head string, body map[string]json.RawMessage) {
	t.Helper()
	whole := ctWhole(t, resp)
	head, raw, ok := strings.Cut(whole, "\n\n")
	if !ok {
		t.Fatalf("no body: %s", whole)
	}
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
	return head, body
}

// The blob page's tx_cost: the settlement transaction's own gas and fee,
// when the scanner recorded them at the settlement's position under its
// hash. A blob with no such line answers byte for byte what it answered
// before the record existed, a line at its position of another
// transaction included.
func TestBlobAnswerCarriesItsSettlementCost(t *testing.T) {
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	dirA, dirB := ctDataDir(t, false), ctDataDir(t, false)
	pff := failedtx.Msg{Index: 0, TypeURL: kindTypeURL[failedtx.KindSettlement], Signer: ctPublisher}
	line := func(hash string, h int64, i int) txcost.Record {
		at := ctBlockTime(h)
		return txcost.Record{SchemaVersion: txcost.SchemaVersion, DedupeKey: txcost.Key(h, i), Height: h, TxIndex: i, Time: at, TxHash: hash,
			GasWanted: 400000, GasUsed: 219118, Fee: "8000utia", FeePayer: ctPublisher, Messages: []failedtx.Msg{pff}, RecordedAt: at.Add(time.Second)}
	}
	ctWriteLines(t, filepath.Join(dirB, txcost.FileName),
		line(ctSettledTx, 32, 0),              // the first publication's settlement
		line(strings.Repeat("e5", 32), 36, 0)) // at the second's position, another transaction's hash
	stA, stB := ctCollect(t, dirA, now), ctCollect(t, dirB, now)
	if n := ctCount(t, stB, "tx_costs"); n != 2 {
		t.Fatalf("%d cost rows", n)
	}
	srvA, srvB := ctServer(t, stA, now), ctServer(t, stB, now)

	rows, err := stA.DB().Query(`SELECT promise_hash FROM publications ORDER BY settlement_height`)
	if err != nil {
		t.Fatal(err)
	}
	var pubs []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			t.Fatal(err)
		}
		pubs = append(pubs, h)
	}
	rows.Close()
	if len(pubs) != 3 || pubs[0] != ctPromise {
		t.Fatalf("publications %v", pubs)
	}

	for i, p := range pubs {
		path := "/v1/blobs/" + p
		ra, rb := ctAsk(srvA, path), ctAsk(srvB, path)
		if i > 0 {
			if a, b := ctWhole(t, ra), ctWhole(t, rb); a != b {
				t.Errorf("%s answers differently with a cost line that is not its own:\n%s\n---\n%s", path, a, b)
			}
			continue
		}
		headA, a := bcSplit(t, ra)
		headB, b := bcSplit(t, rb)
		if _, ok := a["tx_cost"]; ok {
			t.Errorf("%s: tx_cost without a cost line: %s", path, a["tx_cost"])
		}
		if got, want := string(b["tx_cost"]), `{"gas_wanted":400000,"gas_used":219118,"fee":"8000utia","fee_payer":"`+ctPublisher+`","messages":1}`; got != want {
			t.Errorf("%s: tx_cost %s, want %s", path, got, want)
		}
		delete(b, "tx_cost")
		ja, _ := json.Marshal(a)
		jb, _ := json.Marshal(b)
		if headA != headB || string(ja) != string(jb) {
			t.Errorf("%s: beside tx_cost, the answers differ:\n%s\n%s\n---\n%s\n%s", path, headA, ja, headB, jb)
		}
	}
}
