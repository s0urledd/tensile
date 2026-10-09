package export

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

const failedTxsName = "failed_txs.jsonl"

// failedTxLine is a failed_txs.jsonl line as sentinel-scan writes it,
// dated ts (internal/failedtx.Record; only its time dates it here).
func failedTxLine(ts string, height int64) string {
	return fmt.Sprintf(`{"schema_version":1,"dedupe_key":"h%d:0","height":%d,"time":%q,"app_version":10,"tx_hash":"%064x","tx_index":0,`+
		`"code":5,"codespace":"sdk","log":"failed to execute message; message index: 1: insufficient funds","gas_wanted":200000,"gas_used":91234,`+
		`"ante_passed":true,"fee":"2000utia","messages":[{"index":0,"type_url":"/cosmos.bank.v1beta1.MsgSend"},{"index":1,"type_url":"/celestia.fibre.v1.MsgDepositToEscrow"}],`+
		`"recorded_at":%q}`+"\n", height, height, ts, height, ts)
}

func appendTo(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
}

// failedTxsData is a data dir with a day of measurements, a publication,
// the scanner's state and another vantage's heartbeats; with failed (the
// lines of failed_txs.jsonl) when it is not empty.
func failedTxsData(t *testing.T, failed string) string {
	t.Helper()
	data := t.TempDir()
	d1, d2 := "2026-09-10", "2026-09-11"
	files := map[string]string{
		"measurements.jsonl":               line("started_at", d1+"T10:00:00Z", "m1") + line("started_at", d2+"T10:00:00Z", "m2"),
		"publications.jsonl":               line("settlement_time", d1+"T09:00:00Z", "p1"),
		"vantages/de-1/reachability.jsonl": line("started_at", d1+"T10:00:00Z", "v1") + line("started_at", d2+"T11:00:00Z", "v2"),
		StateFile:                          `{"chain_id":"mocha-4"}`,
	}
	if failed != "" {
		files[failedTxsName] = failed
	}
	for name, body := range files {
		p := filepath.Join(data, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return data
}

// readBuilt reads a built export back as a downloader does and holds it to
// its manifest, its sidecar and, when signer is set, its signature.
func readBuilt(t *testing.T, dir, name string, signer *Signer) *Archive {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	a, err := ReadArchive(raw)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if p := a.CheckMembers(); len(p) != 0 {
		t.Fatalf("%s: %v", name, p)
	}
	side, err := os.ReadFile(filepath.Join(dir, name+".sha256"))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.CheckSidecar(side, name); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if signer != nil {
		sig, err := os.ReadFile(filepath.Join(dir, name+".sig"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.CheckSignature(sig, signer.PublicKey()); err != nil {
			t.Fatalf("%s: signature: %v", name, err)
		}
	}
	return a
}

func memberOf(t *testing.T, a *Archive, name string) (int, Member) {
	t.Helper()
	for i, m := range a.Manifest.Files {
		if m.Name == name {
			return i, m
		}
	}
	t.Fatalf("no %s in the manifest: %+v", name, a.Manifest.Files)
	return -1, Member{}
}

// failed_txs.jsonl is in every export as the observer's last file but
// tx_costs.jsonl, before the other vantages' heartbeats, dated by its
// records' time like any other
// record file: a line goes with its block's day, a line that reached the
// file after its day's export goes, late, with the next, and the members of
// consecutive exports tile the file. Each export reads back whole against
// its manifest, its sidecar and its signature.
func TestBuilder_ExportsFailedTxsLastAndByTime(t *testing.T) {
	d1, d2 := "2026-09-10", "2026-09-11"
	data := failedTxsData(t, failedTxLine(d1+"T08:00:00.123456789Z", 100)+failedTxLine(d1+"T23:59:59Z", 200)+failedTxLine(d2+"T00:00:01Z", 300))
	path := filepath.Join(data, failedTxsName)
	dir := filepath.Join(data, "exports")
	signer := newTestSigner(t)
	b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3, Signer: signer}

	built, err := b.Run(time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("d1: built %v err %v", built, err)
	}
	a := readBuilt(t, dir, built[0], signer)
	i, m := memberOf(t, a, failedTxsName)
	if i != len(Files)-2 || a.Manifest.Files[i+1].Name != txCostsName || a.Manifest.Files[i+2].Name != "vantages/de-1/reachability.jsonl" ||
		len(a.Manifest.Files) != len(Files)+1 {
		t.Fatalf("members %+v: want %s last of the observer's files but %s, the vantage after them", a.Manifest.Files, failedTxsName, txCostsName)
	}
	want := failedTxLine(d1+"T08:00:00.123456789Z", 100) + failedTxLine(d1+"T23:59:59Z", 200)
	if m.TimeField != "time" || m.Lines != 2 || m.LateLines != 0 || m.From != 0 || string(a.Members[failedTxsName]) != want {
		t.Fatalf("d1: member %+v holds %q", m, a.Members[failedTxsName])
	}
	tiles := append([]byte(nil), a.Members[failedTxsName]...)

	// one for d1 the scanner wrote after d1's export (a re-scan), and one for d2
	appendTo(t, path, failedTxLine(d1+"T12:00:00Z", 150)+failedTxLine(d2+"T06:00:00Z", 400))
	built, err = b.Run(time.Date(2026, 9, 12, 4, 0, 0, 0, time.UTC))
	if err != nil || len(built) != 1 {
		t.Fatalf("d2: built %v err %v", built, err)
	}
	a = readBuilt(t, dir, built[0], signer)
	_, m2 := memberOf(t, a, failedTxsName)
	want = failedTxLine(d2+"T00:00:01Z", 300) + failedTxLine(d1+"T12:00:00Z", 150) + failedTxLine(d2+"T06:00:00Z", 400)
	if m2.From != m.To || m2.Lines != 3 || m2.LateLines != 1 || string(a.Members[failedTxsName]) != want {
		t.Fatalf("d2: member %+v holds %q, want from %d", m2, a.Members[failedTxsName], m.To)
	}
	tiles = append(tiles, a.Members[failedTxsName]...)
	whole, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(tiles, whole) || m2.To != int64(len(whole)) {
		t.Fatalf("the members do not tile the file: %q (to %d) vs %q", tiles, m2.To, whole)
	}
}

// The member changes nothing else: every other member of an export built
// with failed transactions has the bytes and the manifest entry of the same
// export built from the same data dir without them. Without the file the
// member is there, empty.
func TestBuilder_FailedTxsChangeNoOtherMember(t *testing.T) {
	d1 := "2026-09-10"
	with := failedTxsData(t, failedTxLine(d1+"T08:00:00Z", 100)+failedTxLine(d1+"T09:00:00Z", 101))
	without := failedTxsData(t, "")
	now := time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC)
	signer := newTestSigner(t)
	build := func(data string) *Archive {
		t.Helper()
		dir := filepath.Join(data, "exports")
		b := &Builder{DataDir: data, Dir: dir, Vantage: "v", Build: "x", Hour: 3, Signer: signer}
		built, err := b.Run(now)
		if err != nil || len(built) != 1 {
			t.Fatalf("built %v err %v", built, err)
		}
		return readBuilt(t, dir, built[0], signer)
	}
	a, b := build(with), build(without)
	if len(a.Manifest.Files) != len(b.Manifest.Files) {
		t.Fatalf("%d members with failed transactions, %d without", len(a.Manifest.Files), len(b.Manifest.Files))
	}
	for i, m := range b.Manifest.Files {
		if m.Name == failedTxsName {
			if m.Lines != 0 || m.Bytes != 0 || m.SHA256 != emptySHA || m.From != 0 || m.To != 0 || len(b.Members[m.Name]) != 0 {
				t.Errorf("no file: the member is %+v, want an empty one", m)
			}
			if got := a.Manifest.Files[i]; got.Name != failedTxsName || got.Lines != 2 {
				t.Errorf("with the file: the member at its place is %+v", got)
			}
			continue
		}
		if a.Manifest.Files[i] != m || !bytes.Equal(a.Members[m.Name], b.Members[m.Name]) {
			t.Errorf("%s: %+v with failed transactions, %+v without", m.Name, a.Manifest.Files[i], m)
		}
	}
	if *a.Manifest.State != *b.Manifest.State || !bytes.Equal(a.Members[StateFile], b.Members[StateFile]) {
		t.Errorf("state: %+v, %+v", a.Manifest.State, b.Manifest.State)
	}
}

// The backup cuts exactly the record files the export carries:
// deploy/backup-manifest.py's RECORD_FILES holds the names of Files, each
// once, and nothing else. A file in one and not the other is a record the
// backup proves and the export leaves out, or the other way round.
func TestBackupManifestCutsTheExportedFiles(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "backup-manifest.py"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, "\nRECORD_FILES = [\n")
	if start < 0 {
		t.Fatal("no RECORD_FILES list in deploy/backup-manifest.py")
	}
	body := src[start+len("\nRECORD_FILES = [\n"):]
	end := strings.Index(body, "\n]\n")
	if end < 0 {
		t.Fatal("RECORD_FILES is not closed")
	}
	quoted := regexp.MustCompile(`"([^"]+)"`)
	var listed []string
	seen := map[string]bool{}
	for _, l := range strings.Split(body[:end], "\n") {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		for _, q := range quoted.FindAllStringSubmatch(l, -1) {
			if seen[q[1]] {
				t.Errorf("RECORD_FILES lists %s twice", q[1])
			}
			seen[q[1]] = true
			listed = append(listed, q[1])
		}
	}
	var exported []string
	for _, f := range Files {
		exported = append(exported, f.Name)
	}
	sort.Strings(listed)
	sort.Strings(exported)
	if strings.Join(listed, ",") != strings.Join(exported, ",") {
		t.Fatalf("RECORD_FILES:  %v\nexport.Files: %v", listed, exported)
	}
	if !seen[failedTxsName] {
		t.Fatalf("RECORD_FILES does not list %s", failedTxsName)
	}
	if !seen["tx_costs.jsonl"] {
		t.Fatal("RECORD_FILES does not list tx_costs.jsonl")
	}
}
