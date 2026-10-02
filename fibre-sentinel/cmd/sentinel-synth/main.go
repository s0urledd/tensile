// sentinel-synth writes a synthetic observer record at network scale: the
// JSONL files sentinel-scan and sentinel-probe would have written over a span
// of days on a chain with a real validator set, real assignments and a
// prescribed mix of validator behaviour.
//
// It exists because every test until now ran on a twelve-validator devnet with
// a ten-minute retention window. Mocha has ~80 validators and a four-hour
// window, which changes the row counts per publication by an order of
// magnitude, the shape of every aggregate query, and the arithmetic of the
// probe schedule. This tool makes that scale reproducible before the chain
// gets there: the record it writes goes through the same collector, the same
// store, the same API and the same sentinel-recompute as a real run.
//
// The record is synthetic and says so: every publication's signer is
// "synthetic", and the state file carries the generator's parameters. It is a
// load and arithmetic fixture, never evidence about any validator.
//
// Each blob is read as the prober reads it: a full reading near the end of
// its window (schedule_label full), asking every validator that endorsed the
// promise for its own rows, largest stake first, and asking one whose answer
// did not serve again, up to twice, 90 s after its last answer (each attempt
// a row of its own, Measurement.Attempt). A validator that did not endorse
// is not asked and never counted. Each endorser is judged on its own
// answers: served when one served, not served when none did. A share of the
// blobs (-lost) is served by nobody, so they are Unavailable and every
// endorser of theirs is not served.
//
// Ground truth is printed at the end: how many obligations were served, not
// served and not counted, so that the API's answers can be checked against
// what was injected rather than against themselves.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// behaviour is what a synthetic validator does when probed.
type behaviour string

const (
	behHealthy     behaviour = "healthy"      // serves every time
	behPrunesEarly behaviour = "prunes_early" // NOT_FOUND at the reading: the shard was deleted before its deadline
	behUnreachable behaviour = "unreachable"  // never completes a handshake
	behBadCert     behaviour = "bad_cert"     // handshake completes, the certificate is not endorsed by its consensus key
	behThrottles   behaviour = "throttles"    // ResourceExhausted from a server-side limit
	behFlaky       behaviour = "flaky"        // an application error at some points, serves at others
	behNoHost      behaviour = "no_host"      // never registered a Fibre host
)

type synthVal struct {
	addrHex string
	moniker string
	power   int64
	host    string
	beh     behaviour
	// attestRate is the share of publications whose settled promise carries a
	// verified signature from this validator. The reference client stops
	// collecting at the safety threshold, so even an honest validator is
	// unattested on some blobs.
	attestRate float64
}

func main() {
	var (
		out       = flag.String("out", "./synth-data", "directory to write the record into")
		powers    = flag.String("powers", "", "TSV of 'power\\tmoniker', largest first (default: a built-in mocha-5 snapshot)")
		days      = flag.Float64("days", 3, "how many days of publications to write")
		perHour   = flag.Float64("per-hour", 20, "publications per hour")
		retention = flag.Duration("retention", 4*time.Hour, "shard_retention, the on-chain param")
		timeout   = flag.Duration("promise-timeout", time.Hour, "payment_promise_timeout, the on-chain param")
		vantage   = flag.String("vantage", "synth-1", "vantage name on every row")
		chainID   = flag.String("chain-id", "mocha-5", "chain id on every promise")
		endAgo    = flag.Duration("end-ago", 2*time.Hour, "how long before now the last publication settles")
		seed      = flag.Uint64("seed", 20260918, "deterministic seed")
		heartbeat = flag.Duration("heartbeat", 5*time.Minute, "reachability heartbeat interval")
		lostShare = flag.Float64("lost", 0.01, "share of blobs no validator serves (Unavailable)")
	)
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatal("mkdir: %v", err)
	}
	vals := loadValidators(*powers, *seed)
	fmt.Printf("validator set: %d validators, total power %d\n", len(vals), totalPower(vals))

	rng := rand.New(rand.NewPCG(*seed, 0x5eed))
	now := time.Now().UTC().Truncate(time.Second)
	end := now.Add(-*endAgo)
	span := time.Duration(*days * float64(24*time.Hour))
	start := end.Add(-span)
	count := int(*days * 24 * *perHour)
	if count < 1 {
		count = 1
	}

	pubFile := create(filepath.Join(*out, "publications.jsonl"))
	defer pubFile.Close()
	measFile := create(filepath.Join(*out, "measurements.jsonl"))
	defer measFile.Close()
	reachFile := create(filepath.Join(*out, "reachability.jsonl"))
	defer reachFile.Close()

	params := scan.ParamsSnapshot{
		WithdrawalDelay:              "24h0m0s",
		PaymentPromiseTimeout:        timeout.String(),
		PaymentPromiseHeightWindow:   1000,
		ShardRetention:               retention.String(),
		FullStakeStorageBudget:       2 << 40,
		WithdrawalDelaySeconds:       int64((24 * time.Hour).Seconds()),
		PaymentPromiseTimeoutSeconds: int64(timeout.Seconds()),
		ShardRetentionSeconds:        int64(retention.Seconds()),
		EffectiveFromHeight:          1,
		EffectiveFromTxIndex:         -1,
		Source:                       "seed",
	}
	window := *retention
	if *timeout > window {
		window = *timeout
	}

	aVals := make([]assign.Validator, 0, len(vals))
	for _, v := range vals {
		aVals = append(aVals, assign.Validator{Address: mustAddr(v.addrHex), VotingPower: v.power})
	}
	ap := assign.ParamsV10BlobV0

	cfg := probe.DefaultScheduleConfig()
	const retrySpacing = 90 * time.Second
	var (
		nPubs, nRows, nObl int
		nUnavailable       int
		gt                 = map[string]int{} // ground truth: classification -> rows
		oblBroken          = map[string]int{} // validator -> obligations not served: none of its answers served
		oblServed          = map[string]int{}
		oblTotal           = map[string]int{}
	)

	baseHeight := int64(1_000_000)
	for i := 0; i < count; i++ {
		frac := float64(i) / float64(count)
		settled := start.Add(time.Duration(frac * float64(span)))
		creation := settled.Add(-time.Duration(rng.IntN(20)+5) * time.Second)
		height := baseHeight + int64(i*7)
		commitment := digest("commitment", *seed, i)
		promiseHash := digest("promise", *seed, i)
		msu := creation.Add(window)

		shards, err := assign.Assign(mustCommit(commitment), aVals, ap)
		if err != nil {
			fatal("assign: %v", err)
		}

		blobSize := []uint32{256 << 10, 1 << 20, 8 << 20, 128 << 20}[rng.IntN(4)]
		table := scan.AssignmentTable{
			ProtocolParams: scan.ProtocolParamsSnapshot{
				OriginalRows: ap.OriginalRows, TotalRows: ap.TotalRows, MinRowsPerValidator: ap.MinRowsPerValidator,
			},
			ValidatorSetHeight: height,
			TotalVotingPower:   totalPower(vals),
		}
		distinct := map[int]bool{}
		attested := map[string]bool{}
		for _, v := range vals {
			rows, _ := shards.Rows(mustAddr(v.addrHex))
			for _, r := range rows {
				distinct[r] = true
			}
			att := rng.Float64() < v.attestRate
			attested[v.addrHex] = att
			host := v.host
			if v.beh == behNoHost {
				host = ""
			}
			table.Validators = append(table.Validators, scan.ValidatorAssignment{
				Address: v.addrHex, VotingPower: v.power, RowCount: len(rows), Rows: rows,
				Attested: att, Host: host, HostSource: "event",
			})
			table.Sigma += len(rows)
			if len(rows) > 0 {
				table.ValidatorsWithRows++
			}
		}
		table.Distinct = len(distinct)

		pub := scan.Publication{
			SchemaVersion: 1, PromiseHash: promiseHash,
			SettlementHeight: height + 3, SettlementTime: settled,
			SettlementTxHash: strings.ToUpper(digest("tx", *seed, i)), SettlementTxIndex: rng.IntN(4), SettlementTxCode: 0,
			Signer: "synthetic", ValidatorSignatureCount: len(vals),
			Promise: scan.PromiseFields{
				ChainID: *chainID, Height: height, Namespace: "0a0b1200", NamespaceVersion: 0,
				NamespaceID: digest("ns", *seed, 0)[:28], BlobSize: blobSize, BlobVersion: 0,
				Commitment: commitment, CreationTimestamp: creation,
				SignerPublicKey: (digest("pk", *seed, 0) + digest("pk2", *seed, 0))[:66], Signature: digest("sig", *seed, i) + digest("sig2", *seed, i),
			},
			ParamsAtPublication: params,
			MustServeUntil:      msu,
			MustServeUntilBasis: fmt.Sprintf("creation_timestamp + max(payment_promise_timeout=%s, shard_retention=%s)", *timeout, *retention),
			Assignment:          table,
			RecordedAt:          settled.Add(2 * time.Second),
		}
		writeJSON(pubFile, pub)
		nPubs++

		pt := probe.ReadPoint(pub, cfg)
		pt.Label = probe.FullReadLabel
		// a reading the observer never made: a gap, not a verdict
		if rng.Float64() < 0.004 {
			for _, v := range vals {
				rows, _ := shards.Rows(mustAddr(v.addrHex))
				if len(rows) == 0 || !attested[v.addrHex] {
					continue
				}
				nObl++
				oblTotal[v.moniker]++
				m := baseMeasurement(*vantage, pub, v, rows, true, pt, cfg)
				m.Outcome, m.Classification = probe.OutcomeMissed, probe.ClassNotProbed
				m.ClassificationReason = "not read in time"
				m.Download, m.DNS, m.TCP, m.TLS, m.Identity = probe.DownloadResult{}, probe.StepResult{}, probe.StepResult{}, probe.TLSResult{}, probe.IdentityResult{}
				writeJSON(measFile, m)
				nRows++
				gt[string(m.Classification)]++
			}
			continue
		}
		lost := rng.Float64() < *lostShare
		have := map[int]bool{}
		var ms []probe.Measurement
		var asked []synthVal
		for _, v := range vals { // every endorser, largest stake first
			rows, _ := shards.Rows(mustAddr(v.addrHex))
			if len(rows) == 0 || !attested[v.addrHex] {
				continue // no obligation: not asked
			}
			nObl++
			oblTotal[v.moniker]++
			m := shape(*vantage, pub, v, rows, pt, cfg, lost, true, rng)
			if m.Download.CommitmentVerified {
				for _, r := range rows {
					have[r] = true
				}
			}
			m.Read = &probe.ReadInfo{Order: len(ms), BlobHaveAfter: len(have)}
			ms = append(ms, m)
			asked = append(asked, v)
		}
		result, clientErr := probe.ReadAvailable, ""
		if len(have) < ap.OriginalRows {
			result, clientErr = probe.ReadUnavailable, probe.ClientError(len(have))
			nUnavailable++
		}
		for j, m := range ms {
			m.Read.BlobResult, m.Read.BlobError = result, clientErr
			// An answer that did not serve is asked again, up to twice,
			// each attempt its own row; the first that serves ends it.
			served := probe.FullServed(m.Download.CommitmentVerified, m.Outcome, m.Classification)
			attempt := m
			for k := 1; !served && k <= probe.FullReadRetries; k++ {
				rows, _ := shards.Rows(mustAddr(asked[j].addrHex))
				next := shape(*vantage, pub, asked[j], rows, pt, cfg, lost, true, rng)
				shift := attempt.FinishedAt.Add(retrySpacing).Sub(next.StartedAt)
				next.StartedAt, next.FinishedAt = next.StartedAt.Add(shift), next.FinishedAt.Add(shift)
				next.LatenessMS = next.StartedAt.Sub(pt.At).Milliseconds()
				next.Attempt = k
				next.Read = &probe.ReadInfo{Order: m.Read.Order, BlobResult: result, BlobError: clientErr}
				// The row before an attempt says when it is due, as the
				// prober's does (Measurement.NextAttemptDue).
				due := next.StartedAt
				attempt.NextAttemptDue = &due
				writeJSON(measFile, attempt)
				nRows++
				gt[string(attempt.Classification)]++
				attempt = next
				served = probe.FullServed(next.Download.CommitmentVerified, next.Outcome, next.Classification)
			}
			if served {
				oblServed[asked[j].moniker]++
			} else {
				oblBroken[asked[j].moniker]++
			}
			writeJSON(measFile, attempt)
			nRows++
			gt[string(attempt.Classification)]++
		}
	}

	// reachability heartbeats over the same span, every interval, for every
	// validator with a registered host
	beats := 0
	for t := start; t.Before(end); t = t.Add(*heartbeat) {
		for _, v := range vals {
			if v.beh == behNoHost {
				continue
			}
			m := probe.Measurement{
				SchemaVersion: probe.MeasurementSchemaVersion, Vantage: *vantage,
				ValidatorAddress: v.addrHex, ValidatorHost: v.host,
				ScheduledAt: t, StartedAt: t.Add(time.Duration(rng.IntN(900)) * time.Millisecond),
			}
			m.FinishedAt = m.StartedAt.Add(30 * time.Millisecond)
			m.DNS = probe.StepResult{Attempted: true, OK: true, DurationMS: 2}
			switch v.beh {
			case behUnreachable:
				m.TCP = probe.StepResult{Attempted: true, OK: false, DurationMS: 5000, Error: "dial tcp: i/o timeout"}
				m.Outcome = probe.OutcomeTCPTimeout
			case behBadCert:
				m.TCP = probe.StepResult{Attempted: true, OK: true, DurationMS: 14}
				m.TLS = probe.TLSResult{Attempted: true, OK: true, DurationMS: 22, Version: "1.3"}
				m.Identity = probe.IdentityResult{Attempted: true, OK: false, Reason: "certificate is not endorsed by this validator's consensus key"}
				m.Outcome = probe.OutcomeIdentityFail
			default:
				m.TCP = probe.StepResult{Attempted: true, OK: true, DurationMS: 12}
				m.TLS = probe.TLSResult{Attempted: true, OK: true, DurationMS: 20, Version: "1.3"}
				m.Identity = probe.IdentityResult{Attempted: true, OK: true}
				m.Outcome = probe.OutcomeReachable
			}
			m.TotalDurationMS = m.FinishedAt.Sub(m.StartedAt).Milliseconds()
			writeJSON(reachFile, m)
			beats++
		}
	}

	st := map[string]any{
		"schema_version":              1,
		"chain_id":                    *chainID,
		"start_height":                baseHeight,
		"last_scanned_height":         baseHeight + int64(count*7) + 100,
		"last_scanned_time":           end.Add(30 * time.Minute).Format(time.RFC3339Nano),
		"protocol_params_fingerprint": ap.Fingerprint(),
		"param_history": []map[string]any{{
			"from_height": 1, "from_tx_index": -1, "source": "seed", "params_json": params,
		}},
		"synthetic": map[string]any{
			"generator": "sentinel-synth", "validators": len(vals), "publications": nPubs,
			"days": *days, "per_hour": *perHour, "retention": retention.String(), "seed": *seed,
		},
	}
	writeFile(filepath.Join(*out, "state.json"), st)

	fmt.Printf("\nwrote %s\n", *out)
	fmt.Printf("  publications      %d over %.1f days (%.0f/h)\n", nPubs, *days, *perHour)
	fmt.Printf("  probe rows        %d\n", nRows)
	fmt.Printf("  heartbeats        %d\n", beats)
	fmt.Printf("  obligations       %d (endorsing validators with rows)\n", nObl)
	fmt.Printf("  unavailable blobs %d\n", nUnavailable)
	fmt.Println("\nground truth by classification:")
	keys := make([]string, 0, len(gt))
	for k := range gt {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return gt[keys[i]] > gt[keys[j]] })
	for _, k := range keys {
		fmt.Printf("  %-24s %8d\n", k, gt[k])
	}
	fmt.Println("\nobligations not served, by validator (what the API must report as not served: none of its answers served):")
	bk := make([]string, 0, len(oblBroken))
	for k := range oblBroken {
		bk = append(bk, k)
	}
	sort.Slice(bk, func(i, j int) bool { return oblBroken[bk[i]] > oblBroken[bk[j]] })
	for _, k := range bk {
		fmt.Printf("  %-24s %6d not served, %d served, of %d\n", k, oblBroken[k], oblServed[k], oblTotal[k])
	}
	fmt.Println("\nbehaviour assignment:")
	for _, v := range vals {
		if v.beh != behHealthy {
			fmt.Printf("  %-24s %s\n", v.moniker, v.beh)
		}
	}
}

// shape produces the measurement one (validator, point) pair yields under the
// validator's behaviour, with the classification drawn by the real Classify so
// the record is exactly what the prober would have written.
func shape(vantage string, pub scan.Publication, v synthVal, rows []int, pt probe.SchedulePoint, cfg probe.ScheduleConfig, lost, endorses bool, rng *rand.Rand) probe.Measurement {
	m := baseMeasurement(vantage, pub, v, rows, endorses, pt, cfg)
	ev := probe.Evidence{Assigned: true, Attested: endorses, Phase: m.Phase}

	served := func() {
		m.Download = probe.DownloadResult{
			Attempted: true, OK: true, DurationMS: int64(40 + rng.IntN(400)),
			RowsReturned: len(rows), RowsExpected: len(rows), CommitmentVerified: true, AssignmentVerified: true,
			RowIndices:    indices(rows),
			BytesReturned: int64(len(rows)) * int64(pub.Promise.BlobSize/4096+1), RPC: "DownloadShard",
		}
		m.Outcome = probe.OutcomeServedOK
	}
	notFound := func() {
		m.Download = probe.DownloadResult{Attempted: true, OK: false, DurationMS: int64(5 + rng.IntN(20)), RPC: "DownloadShard", RPCCode: "NotFound"}
		m.Outcome = probe.OutcomeNotFound
	}

	switch {
	case v.beh == behNoHost:
		m.ValidatorHost = ""
		m.Outcome = probe.OutcomeNoHost
		m.DNS = probe.StepResult{}
	case v.beh == behUnreachable:
		m.DNS = probe.StepResult{Attempted: true, OK: true, DurationMS: 3}
		m.TCP = probe.StepResult{Attempted: true, OK: false, DurationMS: 5000, Error: "dial tcp: i/o timeout"}
		m.TLS = probe.TLSResult{}
		m.Outcome = probe.OutcomeTCPTimeout
	case v.beh == behBadCert:
		m.Identity = probe.IdentityResult{Attempted: true, OK: false, Reason: "certificate is not endorsed by this validator's consensus key"}
		m.Outcome = probe.OutcomeIdentityFail
	case lost:
		notFound()
	case v.beh == behThrottles:
		if rng.Float64() < 0.6 {
			m.Download = probe.DownloadResult{Attempted: true, OK: false, DurationMS: 8, RPC: "DownloadShard", RPCCode: "ResourceExhausted"}
			m.Outcome = probe.OutcomeThrottled
		} else {
			served()
		}
	case v.beh == behFlaky:
		if rng.Float64() < 0.25 {
			m.Download = probe.DownloadResult{Attempted: true, OK: false, DurationMS: 30, RPC: "DownloadShard", RPCCode: "Internal"}
			m.Outcome = probe.OutcomeServerError
		} else {
			served()
		}
	case v.beh == behPrunesEarly:
		notFound()
	default: // healthy
		served()
	}

	ev.Outcome = m.Outcome
	ev.CommitmentVerified = m.Download.CommitmentVerified
	cls, reason := probe.Classify(ev)
	m.Classification, m.ClassificationReason = cls, reason
	m.TotalDurationMS = m.DNS.DurationMS + m.TCP.DurationMS + m.TLS.DurationMS + m.Download.DurationMS
	m.FinishedAt = m.StartedAt.Add(time.Duration(m.TotalDurationMS) * time.Millisecond)
	return m
}

func baseMeasurement(vantage string, pub scan.Publication, v synthVal, rows []int, att bool, pt probe.SchedulePoint, cfg probe.ScheduleConfig) probe.Measurement {
	started := pt.At.Add(time.Duration(len(v.addrHex)%800) * time.Millisecond)
	m := probe.Measurement{
		SchemaVersion: probe.MeasurementSchemaVersion, Vantage: vantage,
		PromiseHash: pub.PromiseHash, Commitment: pub.Promise.Commitment, BlobVersion: pub.Promise.BlobVersion,
		MustServeUntil: pub.MustServeUntil, ValidatorSetHeight: pub.Assignment.ValidatorSetHeight,
		ValidatorAddress: v.addrHex, ValidatorHost: v.host, HostSource: "bonded",
		HostAtSettlement: v.host, Assigned: true, Attested: att, AssignedRowCount: len(rows),
		ScheduleLabel: pt.Label, ScheduledAt: pt.At, StartedAt: started,
		LatenessMS: started.Sub(pt.At).Milliseconds(),
		Phase:      probe.PhaseAtWindow(started, pub.MustServeUntil, cfg.PruneTolerance),
	}
	m.DNS = probe.StepResult{Attempted: true, OK: true, DurationMS: 2}
	m.TCP = probe.StepResult{Attempted: true, OK: true, DurationMS: 12}
	m.TLS = probe.TLSResult{Attempted: true, OK: true, DurationMS: 21, Version: "1.3", SharedWithDownload: true}
	m.Identity = probe.IdentityResult{Attempted: true, OK: true}
	m.FinishedAt = started.Add(80 * time.Millisecond)
	return m
}

func indices(rows []int) []uint32 {
	out := make([]uint32, len(rows))
	for i, r := range rows {
		out[i] = uint32(r)
	}
	return out
}

// loadValidators builds the synthetic set: real voting powers, deterministic
// consensus addresses, and a behaviour mix that puts one of each interesting
// case in the set.
func loadValidators(path string, seed uint64) []synthVal {
	lines := builtinPowers
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			fatal("powers: %v", err)
		}
		lines = strings.Split(strings.TrimSpace(string(b)), "\n")
	}
	var out []synthVal
	for i, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" || strings.HasPrefix(ln, "#") {
			continue
		}
		parts := strings.SplitN(ln, "\t", 2)
		p, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil || p <= 0 {
			continue
		}
		name := fmt.Sprintf("validator-%02d", i)
		if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
			name = strings.TrimSpace(parts[1])
		}
		h := sha256.Sum256([]byte(fmt.Sprintf("synth/%d/%s/%d", seed, name, p)))
		out = append(out, synthVal{
			addrHex: hex.EncodeToString(h[:20]), moniker: name, power: p,
			host: fmt.Sprintf("fibre-%02d.synthetic.invalid:7980", i), beh: behHealthy, attestRate: 0.97,
		})
	}
	if len(out) == 0 {
		fatal("no validators")
	}
	// one of each interesting behaviour, spread across the stake range
	assignBeh := func(idx int, b behaviour, attest float64) {
		if idx < len(out) {
			out[idx].beh = b
			if attest > 0 {
				out[idx].attestRate = attest
			}
		}
	}
	assignBeh(len(out)/3, behPrunesEarly, 0.99) // a mid-stake validator that prunes early
	assignBeh(len(out)-4, behPrunesEarly, 0.99) // and a small one
	assignBeh(len(out)/2, behUnreachable, 0.95) // unreachable throughout
	assignBeh(len(out)/2+3, behBadCert, 0.95)   // an unusable certificate
	assignBeh(4, behThrottles, 0.98)            // a large validator that rate limits
	assignBeh(len(out)/4, behFlaky, 0.96)       // intermittent application errors
	assignBeh(len(out)-2, behNoHost, 0.9)       // never registered
	assignBeh(len(out)-7, behHealthy, 0.35)     // healthy but rarely attested
	return out
}

func totalPower(v []synthVal) int64 {
	var t int64
	for _, x := range v {
		t += x.power
	}
	return t
}

func digest(kind string, seed uint64, i int) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seed)
	h := sha256.Sum256([]byte(kind + "/" + string(b[:]) + "/" + strconv.Itoa(i)))
	return hex.EncodeToString(h[:])
}

func mustAddr(h string) assign.Address {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 20 {
		fatal("bad address %q", h)
	}
	var a assign.Address
	copy(a[:], b)
	return a
}

func mustCommit(h string) [32]byte {
	b, err := hex.DecodeString(h)
	if err != nil || len(b) != 32 {
		fatal("bad commitment %q", h)
	}
	var c [32]byte
	copy(c[:], b)
	return c
}

func create(path string) *os.File {
	f, err := os.Create(path)
	if err != nil {
		fatal("create %s: %v", path, err)
	}
	return f
}

func writeJSON(f *os.File, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		fatal("marshal: %v", err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		fatal("write: %v", err)
	}
}

func writeFile(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fatal("marshal: %v", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		fatal("write %s: %v", path, err)
	}
}

func fatal(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "sentinel-synth: "+f+"\n", a...)
	os.Exit(1)
}

// builtinPowers is the mocha-5 bonded set's voting-power distribution, largest
// first, taken on 18 September 2026 before Fibre was live. The monikers are
// deliberately not carried: the distribution is what shapes the fixture (it
// decides every validator's row count and therefore the shape of every
// aggregate), while a real name beside an injected misbehaviour would be a
// statement about an operator, which this fixture must never make.
// It is here so the fixture has a real stake distribution — the row counts per
// validator, and therefore the shape of every aggregate, follow from it — and
// so the tool runs with no network access. Override with -powers.
var builtinPowers = []string{
	"23110002",
	"22100000",
	"21105026",
	"10110010",
	"10110001",
	"8100000",
	"8100000",
	"8100000",
	"8100000",
	"7100000",
	"7100000",
	"5635000",
	"5600000",
	"5600000",
	"5600000",
	"4130000",
	"4111111",
	"4110001",
	"4101000",
	"4100005",
	"4100005",
	"4100001",
	"4100001",
	"4100001",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"4100000",
	"2100002",
	"2100001",
	"2100001",
	"2100001",
	"2100000",
	"2100000",
	"2100000",
	"2100000",
	"2100000",
	"1122000",
	"1121001",
	"1111516",
	"1101001",
	"1100105",
	"1100100",
	"1100009",
	"1100000",
	"1100000",
	"1100000",
	"1100000",
	"1100000",
	"1100000",
	"1100000",
	"1100000",
	"1100000",
	"1100000",
	"1100000",
	"1100000",
	"100000",
	"100000",
	"99999",
	"1000",
	"5",
	"1",
	"0",
	"0",
}
