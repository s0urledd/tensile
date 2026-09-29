// Command sentinel-recompute re-derives what this observer publishes from
// the record alone, so a third party holding the JSONL files (or a daily
// export, untarred) can check every verdict and every figure without
// trusting the site:
//
//   - every probe row's phase and classification, from the row's own
//     fields and the prune tolerance the prober ran with (runs.jsonl);
//   - each blob's reading (Available, or Unavailable with the Fibre
//     client's error, or not read) and the obligation buckets per validator
//     and network-wide for a window, in a second implementation
//     (observer/verdict) of the rules the API evaluates in SQL, the
//     obligations compared against the API's own answer when -api or
//     -api-json is given;
//   - the height ranges this observer could not say which x/fibre params
//     were in force over (param_uncertainty.jsonl), the verdicts they
//     withhold, and the deadline corrections a verified range produced
//     (corrections.jsonl) — each redrawn rather than trusted, so a
//     fabricated correction is a divergence;
//   - with -sampling, the admission draws of every day whose secret is
//     revealed (sampling-secrets.jsonl): which publications this observer
//     should have probed against which ones it did, and which it recorded
//     as sampled out (sampling_decisions.jsonl, one line per publication,
//     expanded into the NOT_PROBED rows it stands for before anything
//     else is computed).
//
// Exit status 1 when anything differs, 2 on a usage or read error.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/policy"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

func main() {
	var (
		dataDir  = flag.String("data-dir", "./sentinel-data", "directory holding the JSONL record (or an untarred daily export)")
		pruneTol = flag.Duration("prune-tolerance", 0, "prune tolerance to grade phases with; 0 = the prober's own, from runs.jsonl (falls back to 150s when the file has no prober run)")
		window   = flag.String("window", "all", "window to compute obligations over: 24h, 7d, 30d or all")
		asOfStr  = flag.String("as-of", "", "end of the window, RFC 3339 (default now); pass the same value to the API's ?as_of= to compare")
		vantage  = flag.String("vantage", "", "only rows from this vantage (default every row)")
		apiBase  = flag.String("api", "", "API base URL, e.g. https://example.org; /v1/validators?window=&as_of= is fetched and compared")
		apiJSON  = flag.String("api-json", "", "a saved /v1/validators response to compare instead of fetching")
		sampling = flag.Bool("sampling", false, "also recompute the admission draws of every day whose secret is revealed")
		maxDiff  = flag.Int("max-diff", 50, "how many differing rows to print")
	)
	flag.Parse()

	asOf := time.Now().UTC()
	if *asOfStr != "" {
		t, err := time.Parse(time.RFC3339, *asOfStr)
		if err != nil {
			fatal(2, "as-of: %v", err)
		}
		asOf = t.UTC()
	}
	spans := map[string]time.Duration{"24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour, "30d": 30 * 24 * time.Hour, "all": 0}
	span, ok := spans[*window]
	if !ok {
		fatal(2, "window must be 24h, 7d, 30d or all")
	}
	win := verdict.Window{End: asOf, All: span == 0}
	if span > 0 {
		win.Start = asOf.Add(-span)
	}

	pubs, err := scan.LoadPublications(filepath.Join(*dataDir, "publications.jsonl"))
	if err != nil {
		fatal(2, "publications: %v", err)
	}
	ms, err := probe.LoadMeasurements(filepath.Join(*dataDir, "measurements.jsonl"))
	if err != nil {
		fatal(2, "measurements: %v", err)
	}
	// A publication the load policy sampled out is one line of
	// sampling_decisions.jsonl, standing for a NOT_PROBED row per assigned
	// validator per point: expanded here into exactly those rows, so every
	// check and figure below reads them as it read the rows the prober used
	// to write.
	decided, soDecisions, soDiffs := expandSampledOut(filepath.Join(*dataDir, probe.SampledOutFile), pubs, *maxDiff)
	ms = append(ms, decided...)
	if *vantage != "" {
		kept := ms[:0]
		for _, m := range ms {
			if m.Vantage == *vantage {
				kept = append(kept, m)
			}
		}
		ms = kept
	}
	runs := loadRuns(filepath.Join(*dataDir, status.RunsFile))
	fmt.Printf("recompute| %d publications, %d rows, %d prober runs on record, window=%s as_of=%s\n",
		len(pubs), len(ms), len(runs), *window, asOf.Format(time.RFC3339))

	differs := soDiffs > 0
	fmt.Printf("sampled-out| %d publication(s) recorded as sampled out, standing for %d NOT_PROBED row(s); %d decision(s) differ from the publication record\n",
		soDecisions, len(decided), soDiffs)

	// ---- rows: phase and classification from the row's own fields ----
	// A verdict the prober deferred (genuine rows no scanned promise
	// assigned) is drawn here the way the collector draws it late, against
	// every promise in the record and the scanner's frontier, and compared
	// with amendments.jsonl when the record carries it.
	byCommit := map[string][]scan.Publication{}
	for _, p := range pubs {
		byCommit[p.Promise.Commitment] = append(byCommit[p.Promise.Commitment], p)
	}
	// The assignment the chain's own record gives, per (promise, validator).
	// Measurement.Recompute re-derives the phase and re-runs Classify, but it
	// feeds Classify the row's own Assigned and Attested — the two flags that
	// turn an in-window NOT_FOUND into a FAULT rather than an UNATTESTED —
	// so those two are the prober's conclusion replayed, not a check of it.
	// publications.jsonl carries the independent answer, so it is checked
	// here, under its own counter: a drift in the assignment or the signature
	// verification is a different failure from a drift in the taxonomy, and a
	// verifier has to be able to tell them apart.
	type assigned struct {
		rows     int
		attested bool
		known    bool
	}
	byPubVal := map[string]assigned{}
	for _, p := range pubs {
		for _, v := range p.Assignment.Validators {
			byPubVal[p.PromiseHash+"|"+v.Address] = assigned{rows: v.RowCount, attested: v.Attested, known: p.HasAttestation()}
		}
	}
	var attestDiffs int
	frontier := loadFrontier(*dataDir, pubs)
	amendments := loadAmendments(filepath.Join(*dataDir, "amendments.jsonl"))
	var deferred, judged, amendDiffs int
	var rowDiffs, tolFromRuns, tolFallback int
	printed := 0
	for i := range ms {
		m := ms[i]
		tol := *pruneTol
		if tol == 0 {
			var from string
			tol, from = toleranceFor(runs, m)
			if from == "runs.jsonl" {
				tolFromRuns++
			} else {
				tolFallback++
			}
		}
		rc := m.Recompute(tol)
		stored := m.Classification
		if rc.Classification == probe.ClassProbeError && m.Download.ShadowGap != "" && !strings.HasPrefix(m.Download.ShadowGap, probe.ShadowGapScanPrefix) &&
			(m.Outcome == probe.OutcomeWrongRows || m.Outcome == probe.OutcomePartial) && m.Download.CommitmentVerified {
			deferred++
			timeout := time.Duration(pubTimeout(pubs, m.PromiseHash)) * time.Second
			var cands []verdict.Candidate
			for _, p := range byCommit[m.Commitment] {
				if p.PromiseHash == m.PromiseHash {
					continue
				}
				for _, v := range p.Assignment.Validators {
					if v.Address == m.ValidatorAddress {
						cands = append(cands, verdict.Candidate{PromiseHash: p.PromiseHash, Commitment: p.Promise.Commitment,
							SettlementTime: p.SettlementTime, MustServeUntil: p.MustServeUntil, Rows: v.Rows})
					}
				}
			}
			// The bound past must_serve_until a candidate's shard is taken
			// to be on disk is the collector's -prune-tolerance, recorded
			// on the amendment it drew; its default stands in for lines
			// from before the field was recorded.
			lateTol := defaultLateTolerance
			if a, ok := amendments[m.DedupeKey()]; ok && a.PruneToleranceS > 0 {
				lateTol = time.Duration(a.PruneToleranceS) * time.Second
			}
			if cls, by, ok := verdict.LateShadow(m.Download.RowIndices, m.StartedAt, frontier, timeout, lateTol, cands); ok {
				judged++
				rc.Classification = cls
				// Every rate reads the amended classification, so the
				// obligation pass below must see the drawn verdict, not
				// the deferral the prober wrote.
				ms[i].Classification = cls
				if a, ok := amendments[m.DedupeKey()]; ok {
					stored = probe.Classification(a.To)
					if a.To != string(cls) || a.ShadowedBy != by {
						amendDiffs++
						if printed < *maxDiff {
							printed++
							fmt.Printf("late| %s %s %s: amendment says %s (%s), recomputed %s (%s)\n", short(m.PromiseHash), m.ValidatorAddress,
								m.ScheduledAt.UTC().Format(time.RFC3339), a.To, a.ShadowedBy, cls, by)
						}
					}
				} else {
					stored = cls // no amendment on record yet: the late verdict stands as computed
				}
			}
		}
		// The two flags Classify is handed rather than deriving.
		if a, ok := byPubVal[m.PromiseHash+"|"+m.ValidatorAddress]; ok {
			wantAssigned := a.rows > 0
			if m.Assigned != wantAssigned || (a.known && !m.AttestationUnknown && m.Attested != a.attested) {
				attestDiffs++
				if printed < *maxDiff {
					printed++
					fmt.Printf("assign| %s %s %s: row says assigned=%v attested=%v, the publication record says assigned=%v attested=%v\n",
						short(m.PromiseHash), m.ValidatorAddress, m.ScheduledAt.UTC().Format(time.RFC3339),
						m.Assigned, m.Attested, wantAssigned, a.attested)
				}
			}
		} else if m.Assigned {
			// A row claiming an assignment for a promise the record does not
			// carry cannot be checked at all, and its verdict rests on that
			// claim. Counted, never passed over.
			attestDiffs++
			if printed < *maxDiff {
				printed++
				fmt.Printf("assign| %s %s: the row claims an assignment but the promise is not in publications.jsonl\n",
					short(m.PromiseHash), m.ValidatorAddress)
			}
		}
		if rc.Phase != m.Phase || rc.Classification != stored {
			rowDiffs++
			if printed < *maxDiff {
				printed++
				fmt.Printf("row| %s %s %s %s: stored %s/%s, recomputed %s/%s (tolerance %s; outcome %s%s)\n",
					m.Vantage, short(m.PromiseHash), m.ValidatorAddress, m.ScheduledAt.UTC().Format(time.RFC3339),
					m.Phase, m.Classification, rc.Phase, rc.Classification, tol, m.Outcome, phaseNote(m))
			}
		}
	}
	if rowDiffs > 0 || amendDiffs > 0 || attestDiffs > 0 {
		differs = true
	}
	fmt.Printf("rows| %d rows, %d differ from their stored phase or classification (tolerance from runs.jsonl for %d, fallback for %d)\n",
		len(ms), rowDiffs, tolFromRuns, tolFallback)
	fmt.Printf("assign| %d rows whose assigned/attested flags do not match publications.jsonl\n", attestDiffs)
	fmt.Printf("late| %d verdicts deferred at the probe, %d drawable at scanner frontier %s, %d differ from amendments.jsonl (%d amendments on record)\n",
		deferred, judged, frontier.Format(time.RFC3339), amendDiffs, len(amendments))

	// ---- params ranges: the holds and the corrections ----
	//
	// Without these the tool reproduces what the prober stamped, while the
	// API publishes what this observer stands behind — and the difference
	// would read as the API being wrong. The record carries both files for
	// exactly this reason.
	ranges := loadParamRanges(filepath.Join(*dataDir, "param_uncertainty.jsonl"))
	corrections, correctedRanges, correctedDeadlines := loadCorrections(filepath.Join(*dataDir, "corrections.jsonl"))
	var corrDiffs, corrected int
	for i := range ms {
		c, ok := corrections[ms[i].DedupeKey()]
		if !ok && probe.IsSampledOutRow(ms[i]) {
			// A sampled-out decision's point is corrected once for every
			// validator's row at it (store.SampledOutPoint.Key).
			c, ok = corrections[sampledOutPointKey(ms[i])]
		}
		if !ok {
			continue
		}
		corrected++
		tol := *pruneTol
		if tol == 0 {
			tol, _ = toleranceFor(runs, ms[i])
		}
		if c.PruneToleranceS > 0 {
			tol = time.Duration(c.PruneToleranceS) * time.Second
		}
		// Redrawn, not trusted: a correction the record cannot reproduce
		// is a divergence, and a fabricated one is caught here.
		got := ms[i].RecomputeWith(tol, c.ToMustServeUntil)
		if string(got.Classification) != c.ToClassification || string(got.Phase) != c.ToPhase {
			corrDiffs++
			if printed < *maxDiff {
				printed++
				fmt.Printf("corr| %s %s %s: correction says %s/%s, recomputed %s/%s\n", short(ms[i].PromiseHash), ms[i].ValidatorAddress,
					ms[i].ScheduledAt.UTC().Format(time.RFC3339), c.ToPhase, c.ToClassification, got.Phase, got.Classification)
			}
		}
		ms[i].Phase, ms[i].Classification = got.Phase, got.Classification
		ms[i].MustServeUntil = c.ToMustServeUntil
	}
	if corrDiffs > 0 {
		differs = true
	}
	holding := 0
	for _, u := range ranges {
		if u.Holds() && !correctedRanges[u.ID] {
			holding++
		}
	}
	fmt.Printf("params| %d x/fibre params range(s) on record, %d closed by a correction pass, %d still withholding verdicts; %d row(s) corrected, %d differ from corrections.jsonl\n",
		len(ranges), len(correctedRanges), holding, corrected, corrDiffs)

	// ---- obligations ----
	rows := make([]verdict.Row, 0, len(ms))
	for _, m := range ms {
		rows = append(rows, verdict.FromMeasurement(m))
	}
	heights := map[string]verdict.PromiseHeights{}
	for _, p := range pubs {
		heights[p.PromiseHash] = verdict.PromiseHeights{PromiseHeight: p.Promise.Height, SettlementHeight: p.SettlementHeight}
	}
	verdict.MarkRetentionUnverified(rows, heights, ranges, correctedRanges)
	// A row whose deadline disagrees with the one its publication now
	// carries was graded against a deadline this observer has withdrawn,
	// and is held until a correction re-grades it — whether it arrived
	// before the range closed or hours after, which the prober keeps doing
	// because it schedules from the append-only record. Same rule as the
	// store's StaleDeadline, so an export taken between a row landing and
	// the sweep reaching it reproduces the same held rows.
	stale := 0
	for i := range rows {
		want, ok := correctedDeadlines[rows[i].PromiseHash]
		if !ok || want.Equal(ms[i].MustServeUntil) {
			continue
		}
		rows[i].RetentionUnverified = true
		stale++
	}
	if stale > 0 {
		fmt.Printf("params| %d row(s) still carry a deadline their publication has moved away from, and are held\n", stale)
	}
	settled := map[string]time.Time{}
	for _, p := range pubs {
		settled[p.PromiseHash] = p.SettlementTime
	}
	blobs := verdict.BlobsOf(pubs)

	// ---- blobs: each one's reading, as the client's download ends ----
	byBlob := map[string][]verdict.Row{}
	for _, r := range rows {
		byBlob[r.PromiseHash] = append(byBlob[r.PromiseHash], r)
	}
	tally := map[string]int{}
	for _, p := range pubs {
		if !win.All && p.SettlementTime.Before(win.Start) || p.SettlementTime.After(win.End) {
			continue
		}
		if p.Assignment.Error != "" || p.Assignment.ProtocolParams.OriginalRows <= 0 {
			tally["unknown"]++
			continue
		}
		res := verdict.BlobOf(byBlob[p.PromiseHash], blobs[p.PromiseHash], p.MustServeUntil, asOf)
		k := res.Status
		if res.Error != "" {
			k += " (" + res.Error + ")"
		}
		tally[k]++
	}
	fmt.Printf("blobs| available %d; unavailable (%s) %d; unavailable (%s) %d; not read %d; in retention window %d; unknown %d\n",
		tally[verdict.BlobAvailable], probe.ClientErrNoShards, tally[verdict.BlobUnavailable+" ("+probe.ClientErrNoShards+")"],
		probe.ClientErrNotEnoughShards, tally[verdict.BlobUnavailable+" ("+probe.ClientErrNotEnoughShards+")"],
		tally[verdict.BlobNotRead], tally[verdict.BlobPending], tally["unknown"])

	net, byVal := verdict.ComputeObligations(rows, settled, win, blobs)
	fmt.Printf("obligations| network: %s\n", fmtObl(net))
	addrs := make([]string, 0, len(byVal))
	for a := range byVal {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	for _, a := range addrs {
		fmt.Printf("validator| %s: %s\n", a, fmtObl(byVal[a]))
	}

	// ---- against the API ----
	if *apiBase != "" || *apiJSON != "" {
		resp, err := loadAPI(*apiBase, *apiJSON, *window, asOf)
		if err != nil {
			fatal(2, "api: %v", err)
		}
		if resp.Window.AsOf != (*apiBase != "") && *apiJSON == "" {
			fmt.Printf("api| note: the API answer is not pinned to as_of\n")
		}
		apiDiffs := 0
		seen := map[string]bool{}
		for _, v := range resp.Validators {
			seen[v.Address] = true
			g := byVal[v.Address]
			if v.Obligations != g {
				apiDiffs++
				fmt.Printf("api| %s differs:\n     api %s\n     here %s\n", v.Address, fmtObl(v.Obligations), fmtObl(g))
			}
		}
		for _, a := range addrs {
			if !seen[a] && byVal[a].Total > 0 {
				apiDiffs++
				fmt.Printf("api| %s has obligations here (%s) and is absent from the API answer\n", a, fmtObl(byVal[a]))
			}
		}
		if apiDiffs > 0 {
			differs = true
		}
		fmt.Printf("api| %d validators compared, %d differ (API window %s, end %s)\n", len(resp.Validators), apiDiffs, resp.Window.Name, resp.Window.End.Format(time.RFC3339))
	}

	// ---- hosts: host_at_settlement from the event history in the record ----
	if hh, gaps, ok := loadHostHistory(*dataDir); ok {
		checked, hostDiffs := 0, 0
		for _, p := range pubs {
			for _, v := range p.Assignment.Validators {
				if v.HostSource == "" {
					continue // a record from before the field
				}
				checked++
				host, src := hh.HostAt(v.Address, p.SettlementHeight, p.SettlementTxIndex, gaps)
				if host != v.Host || src != v.HostSource {
					hostDiffs++
					if hostDiffs <= *maxDiff {
						fmt.Printf("host| %s %s: record %q (%s), derived %q (%s)\n", short(p.PromiseHash), v.Address, v.Host, v.HostSource, host, src)
					}
				}
			}
		}
		if hostDiffs > 0 {
			differs = true
		}
		fmt.Printf("hosts| %d assignments checked against host_history.jsonl, %d differ\n", checked, hostDiffs)
	}

	// ---- sampling ----
	if *sampling {
		n, d, err := checkSampling(filepath.Join(*dataDir, policy.SecretsFile), pubs, ms, *maxDiff)
		if err != nil {
			fatal(2, "sampling: %v", err)
		}
		if d > 0 {
			differs = true
		}
		fmt.Printf("sampling| %d publications on revealed days checked, %d differ from the recorded decision\n", n, d)
	}

	if differs {
		fmt.Println("recompute| DIFFERS")
		os.Exit(1)
	}
	fmt.Println("recompute| everything matches")
}

func fatal(code int, format string, a ...any) {
	fmt.Fprintf(os.Stderr, "recompute FATAL: "+format+"\n", a...)
	os.Exit(code)
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

func phaseNote(m probe.Measurement) string {
	if m.PhaseNote == "" {
		return ""
	}
	return ", note " + m.PhaseNote
}

func fmtObl(o verdict.Obligations) string {
	s := fmt.Sprintf("total %d served %d broken %d held_param_unverified %d not_counted %d pending %d",
		o.Total, o.Served, o.Broken, o.HeldParamUnverified, o.NotCounted, o.Pending)
	if r, ok := o.Rate(); ok {
		s += fmt.Sprintf(" rate %.4f", r)
	}
	return s
}

// proberRun is one prober start with the prune tolerance it ran with.
type proberRun struct {
	vantage string
	at      time.Time
	tol     time.Duration
}

// loadRuns reads the prober starts from runs.jsonl, oldest first. A
// missing or unreadable file is no runs.
func loadRuns(path string) []proberRun {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []proberRun
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		var e status.RunEvent
		if json.Unmarshal(line, &e) != nil || e.Kind != status.RunStarted || e.Component != "prober" {
			continue
		}
		v, _ := e.Config["prune-tolerance"].(string)
		tol, err := time.ParseDuration(v)
		if err != nil || tol <= 0 {
			continue
		}
		out = append(out, proberRun{vantage: e.Vantage, at: e.At, tol: tol})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].at.Before(out[j].at) })
	return out
}

// defaultPruneTolerance is sentinel-probe's -prune-tolerance default.
const defaultPruneTolerance = 150 * time.Second

// defaultLateTolerance is observer-collector's -prune-tolerance default,
// the bound the deferred shadow verdict is drawn with; an amendment
// records the value actually used (Amendment.PruneToleranceS).
const defaultLateTolerance = 5 * time.Minute

// toleranceFor picks the prune tolerance in force when the row was made:
// the newest prober start at or before it from the same vantage.
func toleranceFor(runs []proberRun, m probe.Measurement) (time.Duration, string) {
	tol, from := defaultPruneTolerance, "fallback"
	for _, r := range runs {
		if r.vantage != m.Vantage || r.at.After(m.StartedAt) {
			continue
		}
		tol, from = r.tol, "runs.jsonl"
	}
	return tol, from
}

type apiValidators struct {
	Window struct {
		Name string    `json:"name"`
		End  time.Time `json:"end"`
		AsOf bool      `json:"as_of"`
	} `json:"window"`
	Validators []struct {
		Address     string              `json:"address"`
		Obligations verdict.Obligations `json:"obligations"`
	} `json:"validators"`
}

func loadAPI(base, file, window string, asOf time.Time) (*apiValidators, error) {
	var raw []byte
	var err error
	if file != "" {
		raw, err = os.ReadFile(file)
	} else {
		url := strings.TrimRight(base, "/") + "/v1/validators?window=" + window + "&as_of=" + asOf.Format(time.RFC3339)
		resp, herr := http.Get(url)
		if herr != nil {
			return nil, herr
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
		}
		raw, err = io.ReadAll(resp.Body)
	}
	if err != nil {
		return nil, err
	}
	var out apiValidators
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// checkSampling recomputes the admission draw of every publication settled
// on a day whose secret is revealed and compares it with what the rows
// say happened: probed (any row past NOT_PROBED) or sampled out (every
// row NOT_PROBED with a budget reason; a sampled_out decision arrives
// here as the rows it stands for, see expandSampledOut). The probability
// is the one the rows carry; a publication with no rows is not checked,
// and one with rows stamped at p = 1 was never drawn.
func checkSampling(path string, pubs []scan.Publication, ms []probe.Measurement, maxDiff int) (checked, diffs int, err error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("sampling| no sampling-secrets.jsonl: no day is revealed yet")
			return 0, 0, nil
		}
		return 0, 0, err
	}
	defer f.Close()
	secrets := map[string][]byte{} // day -> secret
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		var rv policy.Reveal
		if json.Unmarshal(line, &rv) != nil || rv.Day == "" {
			continue
		}
		sec, err := hex.DecodeString(rv.Secret)
		if err != nil {
			continue
		}
		sum := sha256.Sum256(sec)
		if hex.EncodeToString(sum[:]) != rv.Commitment {
			fmt.Printf("sampling| %s: the revealed secret does not hash to its commitment\n", rv.Day)
			diffs++
			continue
		}
		secrets[rv.Day] = sec
	}
	type seen struct {
		p       float64
		stamped bool
		probed  bool
		out     bool
	}
	byPromise := map[string]*seen{}
	for _, m := range ms {
		s, ok := byPromise[m.PromiseHash]
		if !ok {
			s = &seen{p: 1}
			byPromise[m.PromiseHash] = s
		}
		if m.Sampling != nil {
			s.p, s.stamped = m.Sampling.P, true
		}
		switch {
		case m.Classification != probe.ClassNotProbed:
			s.probed = true
		case strings.HasPrefix(m.ClassificationReason, "budget:"):
			s.out = true
		}
	}
	printed := 0
	for _, p := range pubs {
		sec, ok := secrets[p.SettlementTime.UTC().Format("2006-01-02")]
		if !ok {
			continue
		}
		s, ok := byPromise[p.PromiseHash]
		if !ok || !s.stamped || s.p >= 1 {
			continue
		}
		checked++
		h := sha256.New()
		hb, err := hex.DecodeString(p.PromiseHash)
		if err != nil {
			hb = []byte(p.PromiseHash)
		}
		h.Write(hb)
		h.Write(sec)
		v := binary.BigEndian.Uint64(h.Sum(nil)[:8])
		drawnIn := float64(v) < s.p*math.Exp2(64)
		if drawnIn == s.probed && !(drawnIn && s.out) {
			continue
		}
		diffs++
		if printed < maxDiff {
			printed++
			fmt.Printf("sampling| %s settled %s: draw at p=%.3f says %s, rows say probed=%v sampled_out=%v\n",
				short(p.PromiseHash), p.SettlementTime.UTC().Format(time.RFC3339), s.p, map[bool]string{true: "in", false: "out"}[drawnIn], s.probed, s.out)
		}
	}
	return checked, diffs, nil
}

// expandSampledOut reads sampling_decisions.jsonl and expands every decision
// into the NOT_PROBED rows it stands for (probe.SampledOut.Expand) against
// its publication's record. A decision whose publication is not in the
// record, or whose validator count disagrees with the record's, is a
// difference: its rows could not be the ones the observer counted. A
// missing file is no decisions (a record from before the file existed
// carries the rows themselves).
func expandSampledOut(path string, pubs []scan.Publication, maxDiff int) (rows []probe.Measurement, decisions, diffs int) {
	ds, err := probe.LoadSampledOut(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, 0
		}
		fatal(2, "sampling decisions: %v", err)
	}
	byHash := make(map[string]scan.Publication, len(pubs))
	for _, p := range pubs {
		byHash[p.PromiseHash] = p
	}
	for _, d := range ds {
		decisions++
		p, ok := byHash[d.PromiseHash]
		if !ok {
			diffs++
			if diffs <= maxDiff {
				fmt.Printf("sampled-out| %s: the decision names a promise that is not in publications.jsonl\n", short(d.PromiseHash))
			}
			continue
		}
		exp := d.Expand(p)
		if len(d.Points) > 0 && len(exp)/len(d.Points) != d.Validators {
			diffs++
			if diffs <= maxDiff {
				fmt.Printf("sampled-out| %s: the decision covers %d validators, the publication record assigns %d\n",
					short(d.PromiseHash), d.Validators, len(exp)/len(d.Points))
			}
		}
		rows = append(rows, exp...)
	}
	return rows, decisions, diffs
}

// sampledOutPointKey is the corrections.jsonl key of the sampled-out point a
// row stands at (store.SampledOutPoint.Key): its dedupe key without the
// validator.
func sampledOutPointKey(m probe.Measurement) string {
	return m.Vantage + "|" + m.PromiseHash + "|*|" + m.ScheduledAt.UTC().Format(time.RFC3339Nano)
}

// pubTimeout is the payment promise timeout the publication was settled
// under, in seconds; 0 when unknown.
func pubTimeout(pubs []scan.Publication, hash string) int64 {
	for _, p := range pubs {
		if p.PromiseHash == hash {
			return p.ParamsAtPublication.PaymentPromiseTimeoutSeconds
		}
	}
	return 0
}

// loadFrontier is the scanner's frontier on the chain's clock: state.json's
// last_scanned_time when the record carries it, else the newest settlement
// time seen, which is never past the true frontier.
func loadFrontier(dir string, pubs []scan.Publication) time.Time {
	var out time.Time
	for _, p := range pubs {
		if p.SettlementTime.After(out) {
			out = p.SettlementTime
		}
	}
	if b, err := os.ReadFile(filepath.Join(dir, "state.json")); err == nil {
		var st struct {
			LastScannedTime time.Time `json:"last_scanned_time"`
		}
		if json.Unmarshal(b, &st) == nil && st.LastScannedTime.After(out) {
			out = st.LastScannedTime
		}
	}
	return out.UTC()
}

// loadAmendments reads amendments.jsonl by probe key; a missing file is
// no amendments.
func loadAmendments(path string) map[string]store.Amendment {
	out := map[string]store.Amendment{}
	f, err := os.Open(path)
	if err != nil {
		return out
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		var a store.Amendment
		if json.Unmarshal(line, &a) == nil && a.DedupeKey != "" {
			out[a.DedupeKey] = a
		}
	}
	return out
}

// loadHostHistory rebuilds the scanner's host history from
// host_history.jsonl and the scan gaps from state.json; ok is false when
// the record carries no history.
func loadHostHistory(dir string) (*scan.HostHistory, []scan.ScanGap, bool) {
	f, err := os.Open(filepath.Join(dir, "host_history.jsonl"))
	if err != nil {
		return nil, nil, false
	}
	defer f.Close()
	var entries []scan.HostEntry
	seeded, seedAt := false, int64(0)
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		var e scan.HostEvent
		if json.Unmarshal(line, &e) != nil || e.ConsAddress == "" {
			continue
		}
		if e.Source == scan.HostFromSeed {
			seeded, seedAt = true, e.FromHeight
		}
		entries = append(entries, e.HostEntry)
	}
	var st struct {
		Gaps       []scan.ScanGap `json:"gaps"`
		HostSeeded bool           `json:"host_seeded"`
		HostSeedAt int64          `json:"host_seed_height"`
	}
	if b, err := os.ReadFile(filepath.Join(dir, "state.json")); err == nil && json.Unmarshal(b, &st) == nil && st.HostSeeded {
		seeded, seedAt = true, st.HostSeedAt
	}
	return scan.LoadHostHistory(entries, seeded, seedAt), st.Gaps, true
}

// loadParamRanges reads param_uncertainty.jsonl: the height ranges the
// scanner could not say which x/fibre params were in force over. A later
// line for the same id supersedes an earlier one, which is how a range
// that was open when it was written and verified later reads.
func loadParamRanges(path string) []scan.ParamUncertainty {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	byID := map[string]scan.ParamUncertainty{}
	var order []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var u scan.ParamUncertainty
		if json.Unmarshal(line, &u) != nil || u.ID == "" {
			continue
		}
		if _, seen := byID[u.ID]; !seen {
			order = append(order, u.ID)
		}
		byID[u.ID] = u
	}
	out := make([]scan.ParamUncertainty, 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out
}

// loadCorrections reads corrections.jsonl three ways: the probe-row
// corrections keyed by the row each one moved, the set of ranges a
// correction pass finished, and the deadline each corrected publication now
// carries. The last is what decides whether a row that has no correction of
// its own is nonetheless stale — which is every row the prober produced
// after the range closed, against the deadline still on the append-only
// record.
func loadCorrections(path string) (map[string]store.Correction, map[string]bool, map[string]time.Time) {
	done := map[string]bool{}
	deadlines := map[string]time.Time{}
	f, err := os.Open(path)
	if err != nil {
		return nil, done, deadlines
	}
	defer f.Close()
	out := map[string]store.Correction{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 8<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var c store.Correction
		if json.Unmarshal(line, &c) != nil {
			continue
		}
		if c.Kind == store.CorrectionRangeComplete && c.UncertaintyID != "" {
			done[c.UncertaintyID] = true
			continue
		}
		if c.Kind == store.CorrectionPublicationDeadline && c.PromiseHash != "" {
			// The deadline the publication carries now, which is what
			// every row of it must be graded against.
			if prev, ok := deadlines[c.PromiseHash]; !ok || c.ToMustServeUntil.Before(prev) {
				deadlines[c.PromiseHash] = c.ToMustServeUntil
			}
			continue
		}
		if (c.Kind != store.CorrectionProbeVerdict && c.Kind != store.CorrectionSampledOutPoint) || c.DedupeKey == "" {
			continue
		}
		if prev, ok := out[c.DedupeKey]; ok && prev.JudgedAt.After(c.JudgedAt) {
			continue
		}
		out[c.DedupeKey] = c
	}
	return out, done, deadlines
}
