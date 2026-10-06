package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/correct"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// TestGoldenAnswers writes what the API answers over real exported records, every route with the publications,
// validators, publishers and namespaces the records hold, at a fixed clock, so two builds can be compared byte for
// byte and request for request (the slim record work: a store that keeps the slim record must answer exactly as
// today's, as fast). The records go into a new store the way the collector takes them in.
//
// It runs only when asked: TENSILE_GOLDEN_EXPORTS names export day directories (comma-separated, in order) and
// TENSILE_GOLDEN_OUT the directory to write to; TENSILE_GOLDEN_NOW (RFC 3339) is the clock, the end of the last day
// by default. With TENSILE_GOLDEN_RECOMPUTE (a sentinel-recompute binary) it also runs that against the API, over the
// same record, for three windows, and writes what it says; with TENSILE_GOLDEN_KEEP_DB the store is copied out too.
func TestGoldenAnswers(t *testing.T) {
	dirs, out := os.Getenv("TENSILE_GOLDEN_EXPORTS"), os.Getenv("TENSILE_GOLDEN_OUT")
	if dirs == "" || out == "" {
		t.Skip("TENSILE_GOLDEN_EXPORTS and TENSILE_GOLDEN_OUT not set")
	}
	days := strings.Split(dirs, ",")
	now := endOfDay(filepath.Base(days[len(days)-1]))
	if v := os.Getenv("TENSILE_GOLDEN_NOW"); v != "" {
		var err error
		if now, err = time.Parse(time.RFC3339Nano, v); err != nil {
			t.Fatal(err)
		}
	}
	data := t.TempDir()
	gatherDays(t, days, data)

	st, err := store.Open(filepath.Join(data, "observer.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	amend, err := os.OpenFile(filepath.Join(data, "amendments.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer amend.Close()
	corrFile, err := os.OpenFile(filepath.Join(data, "corrections.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer corrFile.Close()
	c := collect.New(collect.Collector{
		St: st, Paths: collect.DefaultPaths(data), Vantage: "ut-1", AmendFile: amend, PruneTolerance: 2 * time.Minute,
		Corrector: correct.New(st, corrFile, 2*time.Minute), Retention: rollup.Config{RollupAfter: 14 * 24 * time.Hour, Vantage: "ut-1"},
		RetentionEvery: time.Nanosecond,
	})
	start := time.Now()
	for i := 0; i < 4; i++ {
		if errs := c.Pass(ctx, now); len(errs) > 0 {
			t.Logf("pass %d: %v", i, errs)
		}
	}
	t.Logf("ingested in %s", time.Since(start).Round(time.Millisecond))

	// the server as observer-api builds it (its routes, its keepers), its clock fixed before anything starts
	srv := NewWithVantage(st, VantageInfo{Name: "ut-1"}, nil, withClock(func() time.Time { return now }))
	defer srv.Close()

	reqs := goldenRequests(t, st, now)
	if err := os.MkdirAll(filepath.Join(out, "answers"), 0o755); err != nil {
		t.Fatal(err)
	}
	tim, err := os.Create(filepath.Join(out, "timings.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer tim.Close()
	fmt.Fprintf(tim, "request\tstatus\tbytes\tfirst_ms\twarm_median_ms\n")
	for _, r := range reqs {
		status, body, ctype, first := goldenAsk(srv, r)
		// once it answers, five more times: the warm figure is their median
		// (a rationed route answers 429 to the repeats: its figure is the first asking alone)
		var warm []time.Duration
		for i := 0; i < 5; i++ {
			t0 := time.Now()
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, r, nil))
			if rec.Code == http.StatusTooManyRequests {
				break
			}
			warm = append(warm, time.Since(t0))
		}
		if len(warm) == 0 {
			warm = []time.Duration{first}
		}
		sort.Slice(warm, func(i, j int) bool { return warm[i] < warm[j] })
		name := goldenName(r)
		f := filepath.Join(out, "answers", name)
		if err := os.WriteFile(f, append([]byte(fmt.Sprintf("GET %s\nstatus %d\ncontent-type %s\n\n", r, status, ctype)), body...), 0o644); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(tim, "%s\t%d\t%d\t%.2f\t%.2f\n", r, status, len(body), ms(first), ms(warm[len(warm)/2]))
	}
	t.Logf("%d answers written to %s", len(reqs), out)

	if bin := os.Getenv("TENSILE_GOLDEN_RECOMPUTE"); bin != "" {
		ts := httptest.NewServer(srv)
		defer ts.Close()
		for _, w := range []string{"24h", "7d", "all"} {
			time.Sleep(2100 * time.Millisecond) // the API answers one pinned window every two seconds
			cmd := exec.Command(bin, "-data-dir", data, "-window", w, "-as-of", now.Format(time.RFC3339), "-api", ts.URL)
			b, err := cmd.CombinedOutput()
			code := 0
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				code = ee.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(out, "recompute-"+w+".txt"), append([]byte(fmt.Sprintf("exit %d\n", code)), b...), 0o644); err != nil {
				t.Fatal(err)
			}
			t.Logf("recompute %s: exit %d", w, code)
		}
	}
	if os.Getenv("TENSILE_GOLDEN_KEEP_DB") != "" {
		if _, err := st.DB().Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			t.Fatal(err)
		}
		if _, err := st.DB().Exec("VACUUM INTO ?", filepath.Join(out, "observer.db")); err != nil {
			t.Fatal(err)
		}
	}
}

// withClock fixes the server's clock from the start, keepers included.
func withClock(f func() time.Time) Option { return func(s *Server) { s.clock = f } }

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// goldenAsk asks until the answer is not "computing" (a snapshot the server builds on first demand), for at most two
// minutes, and says how long the first asking took.
func goldenAsk(srv *Server, r string) (status int, body []byte, ctype string, first time.Duration) {
	deadline := time.Now().Add(2 * time.Minute)
	for i := 0; ; i++ {
		t0 := time.Now()
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, r, nil))
		if i == 0 {
			first = time.Since(t0)
		}
		b, _ := io.ReadAll(rec.Result().Body)
		// 503: a snapshot the server builds on first demand; 429: the ration of pinned and excluding windows
		busy := rec.Code == http.StatusServiceUnavailable || rec.Code == http.StatusTooManyRequests
		if !busy || time.Now().After(deadline) {
			return rec.Code, b, rec.Header().Get("Content-Type"), first
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func goldenName(r string) string {
	h := sha256.Sum256([]byte(r))
	clean := strings.NewReplacer("/", "_", "?", "__", "&", "_", "=", "-", ":", "", "{", "", "}", "").Replace(strings.TrimPrefix(r, "/"))
	if len(clean) > 80 {
		clean = clean[:80]
	}
	return clean + "." + hex.EncodeToString(h[:4]) + ".txt"
}

func endOfDay(day string) time.Time {
	d, err := time.Parse("2006-01-02", day)
	if err != nil {
		return time.Now().UTC()
	}
	return d.Add(24*time.Hour - time.Second)
}

// gatherDays writes each record file of the given export days into data, the days one after another, as the
// observer's data directory holds them; state.json is the last day's.
func gatherDays(t *testing.T, days []string, data string) {
	files := map[string]*os.File{}
	for _, d := range days {
		ents, err := os.ReadDir(d)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			n := e.Name()
			src := filepath.Join(d, n)
			if n == "state.json" {
				b, _ := os.ReadFile(src)
				if err := os.WriteFile(filepath.Join(data, n), b, 0o644); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if !strings.HasSuffix(n, ".jsonl") {
				continue
			}
			f, ok := files[n]
			if !ok {
				if f, err = os.Create(filepath.Join(data, n)); err != nil {
					t.Fatal(err)
				}
				files[n] = f
			}
			b, err := os.ReadFile(src)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(b); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, f := range files {
		f.Close()
	}
}

// goldenRequests is every route with what the store holds: each publication's page, each publisher's, a sample of
// validators (every one with a failure among them), each namespace, the lists and summaries in each window, pinned
// and not, and the readings' filters.
func goldenRequests(t *testing.T, st *store.Store, now time.Time) []string {
	q := func(query string) []string {
		rows, err := st.DB().Query(query)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			out = append(out, s)
		}
		return out
	}
	pubs := q(`SELECT promise_hash FROM publications ORDER BY settlement_height, settlement_tx_index`)
	commits := q(`SELECT DISTINCT commitment FROM publications ORDER BY commitment LIMIT 5`)
	txs := q(`SELECT settlement_tx_hash FROM publications ORDER BY settlement_height LIMIT 5`)
	signers := q(`SELECT DISTINCT signer FROM publications ORDER BY signer`)
	nss := q(`SELECT DISTINCT namespace FROM publications ORDER BY namespace`)
	failing := q(`SELECT DISTINCT validator_address FROM probes WHERE classification NOT IN ('HEALTHY','UNATTESTED') ORDER BY validator_address`)
	some := q(`SELECT validator_address FROM (SELECT validator_address, COUNT(*) n FROM probes GROUP BY validator_address ORDER BY n DESC, validator_address LIMIT 8)`)
	vals := map[string]bool{}
	for _, v := range append(failing, some...) {
		vals[v] = true
	}
	var valList []string
	for v := range vals {
		valList = append(valList, v)
	}
	sort.Strings(valList)
	windows := []string{"24h", "7d", "30d", "all"}
	pin := now.Add(-26 * time.Hour).Truncate(time.Hour).Format(time.RFC3339)

	var r []string
	add := func(s string) { r = append(r, s) }
	for _, p := range []string{"/v1/meta", "/v1/params", "/v1/namespaces", "/v1/namespaces?limit=5", "/v1/sampling", "/v1/signing", "/v1/hosting", "/v1/feed.atom", "/v1/exports"} {
		add(p)
	}
	for _, w := range windows {
		add("/v1/network?window=" + w)
		add("/v1/validators?window=" + w)
		add("/v1/market?window=" + w)
		add("/v1/publishers?window=" + w)
	}
	add("/v1/network?window=24h&as_of=" + url.QueryEscape(pin))
	add("/v1/validators?window=24h&as_of=" + url.QueryEscape(pin))
	if len(valList) > 0 {
		add("/v1/network?window=7d&exclude=" + valList[0])
	}
	for _, v := range valList {
		for _, w := range windows {
			add("/v1/validators/" + v + "?window=" + w)
		}
		add("/v1/validators/" + v + "?window=24h&rows=1")
		add("/v1/validators/" + v + "/status")
		add("/v1/validators/" + v + "/feed.atom")
		add("/v1/probes?validator=" + v + "&limit=50")
		add("/v1/probes?validator=" + v + "&served=no&limit=50")
	}
	for _, h := range pubs {
		add("/v1/blobs/" + h)
		add("/v1/probes?blob=" + h + "&limit=200")
	}
	for _, s := range signers {
		add("/v1/publishers/" + s + "?window=all")
		add("/v1/blobs?publisher=" + s + "&limit=25")
	}
	for _, n := range nss {
		add("/v1/blobs?namespace=" + n + "&limit=25")
	}
	for _, c := range commits {
		add("/v1/blobs?commitment=" + c)
	}
	for _, x := range txs {
		add("/v1/blobs?tx=" + x)
	}
	for _, p := range []string{"/v1/blobs?limit=25", "/v1/blobs?limit=25&offset=25", "/v1/blobs?limit=100", "/v1/probes?served=no&limit=200", "/v1/probes?class=FAULT&limit=200", "/v1/probes?class=UNREACHABLE&limit=200"} {
		add(p)
	}
	return r
}
