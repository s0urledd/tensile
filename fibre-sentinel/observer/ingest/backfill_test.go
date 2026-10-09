package ingest_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/ingest"
)

// failedDeposit is a final failed MsgDepositToEscrow as sentinel-scan
// writes it: the failed_tx_msgs row it gives is listed under its signer.
func failedDeposit(height int64, txIndex int) failedtx.Record {
	r := failedTx(height, txIndex)
	r.Code, r.Codespace = 5, "sdk"
	r.Log = "failed to execute message; message index: 0: spendable balance 10utia is smaller than 50utia: insufficient funds"
	r.Messages = []failedtx.Msg{{Index: 0, TypeURL: "/celestia.fibre.v1.MsgDepositToEscrow", Signer: "celestia1pub",
		Detail: &failedtx.MsgDetail{Publisher: "celestia1pub", Amount: "50utia"}}}
	return r
}

func appendLines(t *testing.T, path, lines string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(lines); err != nil {
		t.Fatal(err)
	}
}

// A backfill's merge (cmd/sentinel-txbackfill) appends lines of heights far
// below the newest after them. The next pass reads them from the cursor like
// any line: every one stored, verbatim, each failure with its failed_tx_msgs
// row, the lists ordered by height whatever the file's order; a re-ingest
// from the first byte inserts nothing new.
func TestBackfilledLinesOfLowerHeightsAreIngested(t *testing.T) {
	st := openStore(t)
	dir := t.TempDir()
	failedPath, costsPath := filepath.Join(dir, failedtx.FileName), filepath.Join(dir, txcost.FileName)
	appendLines(t, failedPath, jsonLine(t, failedDeposit(1500, 0))+jsonLine(t, failedDeposit(1501, 2)))
	appendLines(t, costsPath, jsonLine(t, txCost(1500, 1))+jsonLine(t, txCost(1502, 0)))
	now := time.Now()
	if r, err := ingest.FailedTxs(st, failedPath, now); err != nil || r.Inserted != 2 {
		t.Fatalf("the scanner's failures: %+v %v", r, err)
	}
	if r, err := ingest.TxCosts(st, costsPath, now); err != nil || r.Inserted != 2 {
		t.Fatalf("the scanner's costs: %+v %v", r, err)
	}

	backFailed := []failedtx.Record{failedDeposit(20, 0), failedDeposit(700, 3), failedDeposit(19, 1)}
	backCosts := []txcost.Record{txCost(21, 0), txCost(700, 0), txCost(20, 5)}
	var fl, cl string
	for _, r := range backFailed {
		fl += jsonLine(t, r)
	}
	for _, r := range backCosts {
		cl += jsonLine(t, r)
	}
	appendLines(t, failedPath, fl)
	appendLines(t, costsPath, cl)
	if r, err := ingest.FailedTxs(st, failedPath, now); err != nil || r.Inserted != 3 || r.Read != 3 || r.Skipped != 0 {
		t.Fatalf("the backfilled failures: %+v %v", r, err)
	}
	if r, err := ingest.TxCosts(st, costsPath, now); err != nil || r.Inserted != 3 || r.Read != 3 || r.Skipped != 0 {
		t.Fatalf("the backfilled costs: %+v %v", r, err)
	}

	heights := func(q string, args ...any) string {
		t.Helper()
		rows, err := st.DB().Query(q, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var h, i int64
			if err := rows.Scan(&h, &i); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprintf("%d/%d", h, i))
		}
		return strings.Join(out, " ")
	}
	if got := heights(`SELECT height, tx_index FROM failed_txs ORDER BY height, tx_index`); got != "19/1 20/0 700/3 1500/0 1501/2" {
		t.Errorf("failed_txs: %s", got)
	}
	if got := heights(`SELECT height, tx_index FROM failed_tx_msgs WHERE account = ? ORDER BY height DESC, tx_index DESC`, "celestia1pub"); got != "1501/2 1500/0 700/3 20/0 19/1" {
		t.Errorf("failed_tx_msgs: %s", got)
	}
	if got := heights(`SELECT height, tx_index FROM tx_costs ORDER BY height, tx_index`); got != "20/5 21/0 700/0 1500/1 1502/0" {
		t.Errorf("tx_costs: %s", got)
	}
	for _, r := range backFailed {
		var raw string
		if err := st.DB().QueryRow(`SELECT raw_json FROM failed_txs WHERE dedupe_key = ?`, r.DedupeKey).Scan(&raw); err != nil || raw+"\n" != jsonLine(t, r) {
			t.Errorf("%s: %v, stored %s", r.DedupeKey, err, raw)
		}
	}
	for _, r := range backCosts {
		var raw string
		if err := st.DB().QueryRow(`SELECT raw_json FROM tx_costs WHERE height = ? AND tx_index = ?`, r.Height, r.TxIndex).Scan(&raw); err != nil || raw+"\n" != jsonLine(t, r) {
			t.Errorf("%s: %v, stored %s", r.DedupeKey, err, raw)
		}
	}

	for _, p := range []string{failedPath, costsPath} {
		if err := st.SetCursor(p, 0, 0, now); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := ingest.FailedTxs(st, failedPath, now); err != nil || r.Inserted != 0 || r.Read != 5 {
		t.Fatalf("the failures again from 0: %+v %v", r, err)
	}
	if r, err := ingest.TxCosts(st, costsPath, now); err != nil || r.Inserted != 0 || r.Read != 5 {
		t.Fatalf("the costs again from 0: %+v %v", r, err)
	}
	var msgs int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM failed_tx_msgs`).Scan(&msgs); err != nil || msgs != 5 {
		t.Fatalf("%d failed_tx_msgs rows: %v", msgs, err)
	}
}
