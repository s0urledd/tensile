package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/collect"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/correct"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// The simulated observer the day partials are proved on: a record written
// the way the scanner and the prober write theirs (the production types,
// appended to the files the collector tails), fed through the real
// collector pass by pass under a clock of its own, with the API reading
// the store the way it does in production (store.OpenReadOnly). What it
// writes is chosen to reach every write path the partials must notice:
// late and restarted readings, backlogs, sampled-out decisions before
// their publication, holds raised, corrected and never closed,
// corrections that fail part way, amendments, a collapse of rows written
// on two days, the rollup and the prune, a second vantage with a tie, odd
// times, rows that start long before their settlement.

// simConfig sizes a simulation.
type simConfig struct {
	seed      uint64
	vals      int
	perDay    int
	days      int
	retention time.Duration
	// retainRaw and rollupAfter are the collector's retention policy, short
	// enough that the rollup and the prune happen inside the run.
	retainRaw, rollupAfter time.Duration
}

func defaultSimConfig(seed uint64) simConfig {
	return simConfig{seed: seed, vals: 12, perDay: 200, days: 12, retention: 4 * time.Hour,
		retainRaw: 6 * 24 * time.Hour, rollupAfter: 2 * 24 * time.Hour}
}

type simVal struct {
	addr, host string
	power      int64
	beh        string
	attest     float64
}

type simEvent struct {
	at   time.Time
	seq  int
	file string
	line []byte
}

// simPub is one publication and what became of it.
type simPub struct {
	pub     scan.Publication
	rows    map[string][]int
	emitAt  time.Time
	lost    bool
	sampled bool
}

// sim is one simulated observer.
type sim struct {
	t      testing.TB
	cfg    simConfig
	rng    *rand.Rand
	dir    string
	snaps  string
	dbPath string
	t0     time.Time
	now    time.Time
	vals   []simVal
	events []simEvent
	dark   []simEvent // the prober's lines held back (scen.dark)
	// again is a correction applied once, to be applied again (reapply).
	again  *store.Correction
	seq    int
	next   int
	height int64
	pubs   []*simPub
	byHash map[string]*simPub
	// scen places the scenarios; heldRange is the verified range's heights
	// (from, to, the height the shorter retention began at).
	scen      simScen
	heldRange [3]int64
	// logs are the collector's log lines; dropped counts the sealed days
	// the API's catch-ups dropped, over its restarts.
	logs    []string
	dropped int
	// seen are the scenarios' witnesses reached so far (observe); forced
	// the rare cases already written once.
	seen   map[string]bool
	forced map[string]bool
	// saved is set once the API has written its partials; sealedDark once
	// a day was sealed empty while the collector was down.
	saved      bool
	sealedDark bool
	// the collector's side
	st         *store.Store
	coll       *collect.Collector
	corrFile   *os.File
	amendFile  *os.File
	afterPrune func(step, day string)
	// the API's side
	ro  *store.Store
	srv *Server
}

const simVantage = "v1"

// newSim writes nothing yet: the record is planned in full (plan) and
// emitted as the clock passes each line's time (advance).
func newSim(t testing.TB, cfg simConfig) *sim {
	t.Helper()
	dir := t.TempDir()
	s := &sim{t: t, cfg: cfg, rng: rand.New(rand.NewPCG(cfg.seed, 0x7a1)), dir: dir, snaps: filepath.Join(dir, "snapshots"),
		dbPath: filepath.Join(dir, "observer.db"), t0: time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC), byHash: map[string]*simPub{}, forced: map[string]bool{}}
	s.now = s.t0
	s.height = 1_000_000
	for i := 0; i < cfg.vals; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("sim/%d/%d", cfg.seed, i)))
		v := simVal{addr: hex.EncodeToString(h[:20]), host: fmt.Sprintf("v%02d.sim.invalid:7980", i),
			power: int64(1000 - 60*i), beh: "healthy", attest: 0.93}
		s.vals = append(s.vals, v)
	}
	behs := []string{"prunes", "unreachable", "flaky", "throttles", "nohost", "prunes"}
	for i, b := range behs {
		if j := 2 + 2*i; j < len(s.vals) {
			s.vals[j].beh = b
		}
	}
	var err error
	if s.st, err = store.Open(s.dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.st.Close() })
	if s.corrFile, err = os.OpenFile(filepath.Join(dir, "corrections.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.corrFile.Close() })
	if s.amendFile, err = os.OpenFile(filepath.Join(dir, "amendments.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.amendFile.Close() })
	s.coll = collect.New(collect.Collector{
		St: s.st, Paths: collect.DefaultPaths(dir), Vantage: simVantage, AmendFile: s.amendFile,
		Logf:           func(format string, args ...any) { s.logs = append(s.logs, fmt.Sprintf(format, args...)) },
		PruneTolerance: 5 * time.Minute, Corrector: correct.New(s.st, s.corrFile, 5*time.Minute),
		Retention: rollup.Config{RetainRaw: cfg.retainRaw, RetainRawJSON: 4 * 24 * time.Hour, RollupAfter: cfg.rollupAfter, Vantage: simVantage,
			AfterPrune: func(step, day string) {
				if s.afterPrune != nil {
					s.afterPrune(step, day)
				}
			}},
		RetentionEvery: time.Hour,
	})
	return s
}

// openAPI opens the API over the store as observer-api does: read-only,
// with the day partials kept under the snapshot directory.
func (s *sim) openAPI(opts ...Option) *Server {
	s.t.Helper()
	if s.ro == nil {
		// The first pass creates the schema; the API opens what it made.
		s.pass()
		ro, err := store.OpenReadOnly(s.dbPath)
		if err != nil {
			s.t.Fatal(err)
		}
		s.ro = ro
		s.t.Cleanup(func() { ro.Close() })
	}
	srv := newServer(s.ro, VantageInfo{Name: simVantage}, nil, append([]Option{WithSnapshotDir(s.snaps)}, opts...)...)
	srv.clock = func() time.Time { return s.now }
	return srv
}

func (s *sim) hash(kind string, i int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%d", kind, s.cfg.seed, i)))
	return hex.EncodeToString(h[:])
}

// emit schedules a line of a file at a moment.
func (s *sim) emit(at time.Time, file string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		s.t.Fatal(err)
	}
	s.seq++
	s.events = append(s.events, simEvent{at: at, seq: s.seq, file: file, line: b})
}

// ---- the record ----

// plan writes the whole record's schedule: publications, their readings,
// heartbeats, decisions, payments, and the scenarios on top.
func (s *sim) plan() {
	end := s.t0.Add(time.Duration(s.cfg.days) * 24 * time.Hour)
	s.planScenarios()
	n := 0
	for d := 0; d < s.cfg.days; d++ {
		day := s.t0.Add(time.Duration(d) * 24 * time.Hour)
		times := make([]time.Time, 0, s.cfg.perDay+1)
		for i := 0; i < s.cfg.perDay; i++ {
			// a busy afternoon on top of a steady day
			frac := s.rng.Float64()
			if s.rng.IntN(3) == 0 {
				frac = 0.55 + 0.2*s.rng.Float64()
			}
			times = append(times, day.Add(time.Duration(frac*float64(24*time.Hour))).Truncate(time.Millisecond))
		}
		// one settling at 23:30, whose reading is after midnight
		times = append(times, day.Add(23*time.Hour+30*time.Minute))
		sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
		for _, at := range times {
			s.addPublication(n, at)
			n++
		}
	}
	for at := s.t0; at.Before(end.Add(6 * time.Hour)); at = at.Add(10 * time.Minute) {
		for i, v := range s.vals {
			if v.beh == "nohost" {
				continue
			}
			s.emit(at, "reachability.jsonl", s.heartbeat(v, simVantage, at.Add(time.Duration(i*37)*time.Millisecond)))
		}
	}
	s.afterPlan()
	// A second vantage's heartbeats, copied in: counted in no figure.
	for at := s.t0; at.Before(end); at = at.Add(30 * time.Minute) {
		s.emit(at, filepath.Join("vantages", "v2", "reachability.jsonl"), s.heartbeat(s.vals[3], "v2", at))
	}
	sort.SliceStable(s.events, func(i, j int) bool {
		if !s.events[i].at.Equal(s.events[j].at) {
			return s.events[i].at.Before(s.events[j].at)
		}
		return s.events[i].seq < s.events[j].seq
	})
}

// params is the x/fibre params every publication is recorded under.
func (s *sim) params() scan.ParamsSnapshot {
	return scan.ParamsSnapshot{
		WithdrawalDelaySeconds: int64((24 * time.Hour).Seconds()), PaymentPromiseTimeoutSeconds: int64(time.Hour.Seconds()),
		PaymentPromiseHeightWindow: 1000, ShardRetentionSeconds: int64(s.cfg.retention.Seconds()),
		FullStakeStorageBudget: 2 << 40, EffectiveFromHeight: 1, EffectiveFromTxIndex: -1, Source: "seed",
	}
}

// addPublication plans publication i settling at at, and its reading.
func (s *sim) addPublication(i int, at time.Time) {
	s.height += int64(1 + s.rng.IntN(20))
	creation := at.Add(-time.Duration(5+s.rng.IntN(25)) * time.Second)
	orig := 4096
	switch s.rng.IntN(40) {
	case 0:
		orig = 2048
	case 1:
		orig = 8192
	}
	if sameDay(at, s.scen.odd4000) && (s.rng.IntN(20) == 0 || !s.forced["4000"]) {
		s.forced["4000"] = true
		orig = 4000 // not a power of two: the load bytes are read with the shipped statement
	}
	sizes := []uint32{256 << 10, 1 << 20, 8 << 20, 128 << 20, 1<<31 + 12345}
	size := sizes[s.rng.IntN(len(sizes))]
	schema := scan.AttestationSchemaVersion
	if s.rng.IntN(25) == 0 {
		schema = 1 // recorded before signatures were verified
	}
	p := scan.Publication{
		SchemaVersion: schema, PromiseHash: s.hash("promise", i),
		SettlementHeight: s.height, SettlementTime: at, SettlementTxHash: s.hash("tx", i)[:40], SettlementTxIndex: s.rng.IntN(3),
		Signer: "celestia1sim", ValidatorSignatureCount: len(s.vals),
		Promise: scan.PromiseFields{ChainID: "sim-1", Height: s.height - int64(1+s.rng.IntN(4)), Namespace: "0a0b1200",
			BlobSize: size, Commitment: s.hash("commitment", i), CreationTimestamp: creation, SignerPublicKey: "02" + s.hash("pk", 0)[:64]},
		ParamsAtPublication: s.params(),
		MustServeUntil:      creation.Add(s.cfg.retention),
		MustServeUntilBasis: "creation_timestamp + shard_retention",
		RecordedAt:          at,
	}
	if s.rng.IntN(60) == 0 {
		p.SettlementTxCode = 11 // a failed settlement: counted, never signed or loaded
	}
	sp := &simPub{pub: p, rows: map[string][]int{}}
	table := scan.AssignmentTable{
		ProtocolParams:     scan.ProtocolParamsSnapshot{OriginalRows: orig, TotalRows: 4 * orig},
		ValidatorSetHeight: p.Promise.Height,
	}
	if s.rng.IntN(80) == 0 {
		table.Error = "validator set not readable at the promise height"
	}
	var total int64
	for _, v := range s.vals {
		total += v.power
	}
	table.TotalVotingPower = total
	off := 0
	for vi, v := range s.vals {
		rc := int(int64(4*orig) * v.power / total)
		if vi == len(s.vals)-1 {
			rc = 4*orig - off
		}
		rows := make([]int, rc)
		for k := range rows {
			rows[k] = off + k
		}
		off += rc
		att := s.rng.Float64() < v.attest
		host, src := v.host, scan.HostFromEvent
		if v.beh == "nohost" {
			host = ""
			src = scan.HostNone
		}
		if s.rng.IntN(50) == 0 {
			src = "" // the registry could not be read at settlement
		}
		// The record carries no row lists, as a scanner run with -rows=false
		// writes it: they are most of a record's bytes, and the rows each
		// reading returns are the sim's own.
		table.Validators = append(table.Validators, scan.ValidatorAssignment{Address: v.addr, VotingPower: v.power, RowCount: rc,
			Attested: att, Host: host, HostSource: src})
		table.Sigma += rc
		if rc > 0 {
			table.ValidatorsWithRows++
		}
		if att && rc > 0 {
			table.AttestedWithRows++
			table.AttestedVotingPower += v.power
		}
		sp.rows[v.addr] = rows
	}
	table.Distinct = table.Sigma
	table.SignatureEntries, table.SignaturesVerified = table.AttestedWithRows, table.AttestedWithRows
	p.Assignment = table
	sp.pub = p
	s.pubs = append(s.pubs, sp)
	s.byHash[p.PromiseHash] = sp
	sp.emitAt = at.Add(time.Duration(3+s.rng.IntN(20)) * time.Second)
	if s.rng.IntN(50) == 0 {
		// the scanner behind: the record arrives after the reading and
		// after the deadline
		sp.emitAt = at.Add(s.cfg.retention + time.Hour)
	}
	sp.pub.RecordedAt = sp.emitAt
	cfg := probe.DefaultScheduleConfig()
	pt := probe.ReadPoint(p, cfg)
	if s.scen.collapse < 0 && sameDay(at, s.scen.deferDay) && at.Hour() >= 22 && table.Error == "" {
		// written the old way, its record two days late
		s.scen.collapse = i
		sp.sampled = true
		sp.emitAt = at.Add(48 * time.Hour)
		s.emit(sp.emitAt, "publications.jsonl", sp.pub)
		s.oldStyleSampledOut(sp, pt)
		return
	}
	s.emit(sp.emitAt, "publications.jsonl", sp.pub)
	s.emit(at, "payments.jsonl", scan.Payment{SchemaVersion: 1, DedupeKey: "settle/" + p.PromiseHash, Kind: "settlement", Height: p.SettlementHeight,
		Time: at, TxHash: p.SettlementTxHash, Publisher: "celestia1publisher" + strconv.Itoa(i%3), PromiseHash: p.PromiseHash,
		Namespace: p.Promise.Namespace, BlobSize: size, Denom: "utia"})
	if table.Error != "" {
		return
	}
	r := s.rng.IntN(100)
	switch {
	case r < 4:
		// drawn out of the sample: one decision, standing for a NOT_PROBED
		// row per assigned validator at the point
		sp.sampled = true
		d := probe.SampledOut{SchemaVersion: probe.SampledOutSchemaVersion, Kind: probe.SampledOutKind, Vantage: simVantage,
			PromiseHash: p.PromiseHash, Commitment: p.Promise.Commitment, SettlementTime: at, MustServeUntil: p.MustServeUntil,
			ValidatorSetHeight: table.ValidatorSetHeight, DecidedAt: at.Add(20 * time.Second),
			Sampling: probe.SamplingDecision{P: 0.25, Binding: "budget", DayCommitment: s.hash("day", 0)[:16]},
			Reason:   "budget:p=0.25:budget:day_commitment=" + s.hash("day", 0)[:16],
			Points:   []probe.SampledOutPoint{{Label: pt.Label, At: pt.At, Phase: pt.Phase}}, Validators: table.ValidatorsWithRows}
		// Sometimes the decision is written before the scanner's record of
		// the publication reaches the collector.
		s.emit(d.DecidedAt, probe.SampledOutFile, d)
		if s.rng.IntN(3) == 0 {
			s.moveEvent(sp.emitAt, p.PromiseHash, at.Add(2*time.Hour))
			sp.emitAt = at.Add(2 * time.Hour)
		}
	case r < 6:
		// the reading was not made in time: this observer's gap
		for _, v := range s.vals {
			if s.endorses(sp, v) {
				s.emit(pt.At.Add(time.Second), "measurements.jsonl", s.missed(sp, v, pt, pt.At.Add(time.Second)))
			}
		}
	case s.inOutage(pt.At):
		// The prober was down at the reading: when it restarts it writes
		// the reading as not made, stamped at its restart, with the
		// point's phase.
		for k, v := range s.vals {
			if s.endorses(sp, v) {
				stamped := s.scen.outage[1].Add(time.Duration(k) * time.Millisecond)
				s.emit(stamped.Add(time.Second), "measurements.jsonl", s.missed(sp, v, pt, stamped))
			}
		}
	default:
		sp.lost = s.rng.IntN(30) == 0
		started := pt.At.Add(time.Duration(s.rng.IntN(4000)) * time.Millisecond)
		tie := s.rng.IntN(25) == 0
		if tie {
			started = pt.At.Add(time.Second)
		}
		defer1 := sameDay(at, s.scen.deferDay) && (s.rng.IntN(6) == 0 || !s.forced["defer"])
		for k, m := range s.read(sp, pt, simVantage, started) {
			if defer1 && k == 0 && m.Download.CommitmentVerified {
				m = s.deferred(m)
				s.forced["defer"] = true
			}
			s.emit(m.StartedAt.Add(2*time.Second), "measurements.jsonl", m)
		}
		if tie {
			// a second vantage reads the same blob at the very same instant,
			// which ties the newest row of every pair both read; before
			// scen.tieFrom a millisecond later, which does not, so that the
			// days a tie keeps raw are the few the scenario places them on
			second := started
			if sp.pub.SettlementTime.Before(s.scen.tieFrom) {
				second = second.Add(time.Millisecond)
			}
			for _, m := range s.read(sp, pt, "v2", second) {
				s.emit(m.StartedAt.Add(3*time.Second), "measurements.jsonl", m)
			}
		}
		if s.rng.IntN(100) == 0 || (!s.forced["early"] && at.After(s.t0.Add(30*time.Hour))) {
			s.forced["early"] = true
			// a reading stamped by a clock hours behind: it starts long
			// before the publication settled
			early := probe.SchedulePoint{At: at.Add(-3 * time.Hour), Phase: probe.PhaseInWindow, Label: "w1"}
			for _, m := range s.read(sp, early, simVantage, early.At) {
				s.emit(sp.emitAt.Add(time.Minute), "measurements.jsonl", m)
			}
		}
		if s.rng.IntN(150) == 0 {
			// and one by a clock two days ahead
			ahead := probe.SchedulePoint{At: at.Add(48 * time.Hour), Phase: probe.PhaseInWindow, Label: "w4"}
			for _, m := range s.read(sp, ahead, simVantage, ahead.At) {
				s.emit(pt.At.Add(time.Minute), "measurements.jsonl", m)
			}
		}
		if s.rng.IntN(12) == 0 {
			// the earlier schedule: several readings of one pair in the
			// window, and one past the deadline
			for k, f := range []float64{0.12, 0.45, 0.72, 0.92, 1.01} {
				at := p.SettlementTime.Add(time.Duration(f * float64(p.MustServeUntil.Sub(p.SettlementTime))))
				label := "w" + strconv.Itoa(k+1)
				if f > 1 {
					label = "grace"
				}
				ept := probe.SchedulePoint{At: at, Phase: probe.PhaseAtWindow(at, p.MustServeUntil, cfg.PruneTolerance), Label: label}
				for _, m := range s.read(sp, ept, simVantage, at) {
					s.emit(m.StartedAt.Add(time.Second), "measurements.jsonl", m)
				}
			}
		}
		if s.rng.IntN(25) == 0 {
			// a second vantage reads the same blob a little later
			for _, m := range s.read(sp, pt, "v2", pt.At.Add(time.Duration(5000+s.rng.IntN(4000))*time.Millisecond)) {
				s.emit(m.StartedAt.Add(3*time.Second), "measurements.jsonl", m)
			}
		}
	}
}

// endorses is whether the prober reads v for the publication: assigned and
// attested, or assigned on a record from before attestation.
func (s *sim) endorses(sp *simPub, v simVal) bool {
	for _, a := range sp.pub.Assignment.Validators {
		if a.Address == v.addr {
			return a.RowCount > 0 && (a.Attested || !sp.pub.HasAttestation())
		}
	}
	return false
}

func (s *sim) assignment(sp *simPub, addr string) scan.ValidatorAssignment {
	for _, a := range sp.pub.Assignment.Validators {
		if a.Address == addr {
			return a
		}
	}
	return scan.ValidatorAssignment{}
}

// base is a measurement of v for the publication at the point.
func (s *sim) base(sp *simPub, v simVal, pt probe.SchedulePoint, vantage string, started time.Time) probe.Measurement {
	a := s.assignment(sp, v.addr)
	p := sp.pub
	return probe.Measurement{
		SchemaVersion: probe.MeasurementSchemaVersion, Vantage: vantage,
		PromiseHash: p.PromiseHash, Commitment: p.Promise.Commitment, MustServeUntil: p.MustServeUntil,
		ValidatorSetHeight: p.Assignment.ValidatorSetHeight, ValidatorAddress: v.addr, ValidatorHost: v.host, HostSource: "bonded",
		Assigned: a.RowCount > 0, Attested: a.Attested, AttestationUnknown: !p.HasAttestation(), AssignedRowCount: a.RowCount,
		ScheduleLabel: pt.Label, ScheduledAt: pt.At, StartedAt: started, FinishedAt: started.Add(90 * time.Millisecond),
		LatenessMS: started.Sub(pt.At).Milliseconds(), Phase: probe.PhaseAtWindow(started, p.MustServeUntil, probe.DefaultScheduleConfig().PruneTolerance),
	}
}

// missed is the NOT_PROBED row the prober writes for a reading it did not
// make: stamped when it is written, with the point's phase.
func (s *sim) missed(sp *simPub, v simVal, pt probe.SchedulePoint, stamped time.Time) probe.Measurement {
	m := s.base(sp, v, pt, simVantage, stamped)
	m.Phase, m.Outcome, m.Classification = pt.Phase, probe.OutcomeMissed, probe.ClassNotProbed
	m.ClassificationReason = "scheduled point elapsed before the prober ran it"
	m.FinishedAt = stamped
	return m
}

// read is one reading of a blob as the client makes it: every validator
// with rows in stake order, endorsing or not, until the rows are enough.
func (s *sim) read(sp *simPub, pt probe.SchedulePoint, vantage string, started time.Time) []probe.Measurement {
	var out []probe.Measurement
	have := 0
	need := sp.pub.Assignment.ProtocolParams.OriginalRows
	for i, v := range s.vals {
		a := s.assignment(sp, v.addr)
		if a.RowCount == 0 {
			continue
		}
		if have >= need {
			break
		}
		m := s.base(sp, v, pt, vantage, started.Add(time.Duration(i)*time.Millisecond))
		m.DNS = probe.StepResult{Attempted: true, OK: true, DurationMS: 2}
		m.TCP = probe.StepResult{Attempted: true, OK: true, DurationMS: int64(5 + s.rng.IntN(30))}
		m.TLS = probe.TLSResult{Attempted: true, OK: true, DurationMS: int64(10 + s.rng.IntN(30)), Version: "1.3"}
		m.Identity = probe.IdentityResult{Attempted: true, OK: true}
		served := false
		switch {
		case v.beh == "nohost":
			m.ValidatorHost, m.Outcome = "", probe.OutcomeNoHost
			m.DNS, m.TCP, m.TLS, m.Identity = probe.StepResult{}, probe.StepResult{}, probe.TLSResult{}, probe.IdentityResult{}
		case v.beh == "unreachable":
			m.TCP = probe.StepResult{Attempted: true, OK: false, DurationMS: 5000, Error: "dial tcp: i/o timeout"}
			m.TLS, m.Identity = probe.TLSResult{}, probe.IdentityResult{}
			m.Outcome = probe.OutcomeTCPTimeout
		case sp.lost || v.beh == "prunes":
			m.Download = probe.DownloadResult{Attempted: true, DurationMS: int64(5 + s.rng.IntN(20)), RPCCode: "NotFound"}
			m.Outcome = probe.OutcomeNotFound
		case v.beh == "throttles" && s.rng.IntN(2) == 0:
			m.Download = probe.DownloadResult{Attempted: true, DurationMS: 8, RPCCode: "ResourceExhausted"}
			m.Outcome = probe.OutcomeThrottled
		case v.beh == "flaky" && s.rng.IntN(4) == 0:
			m.Download = probe.DownloadResult{Attempted: true, DurationMS: 30, RPCCode: "Internal"}
			m.Outcome = probe.OutcomeServerError
		default:
			served = true
		}
		if served {
			idx := make([]uint32, a.RowCount)
			for k, r := range a.Rows {
				idx[k] = uint32(r)
			}
			m.Download = probe.DownloadResult{Attempted: true, OK: true, DurationMS: int64(20 + s.rng.IntN(900)),
				RowsReturned: a.RowCount, RowsExpected: a.RowCount, CommitmentVerified: true, AssignmentVerified: true, RowIndices: idx,
				BytesReturned: int64(a.RowCount) * int64(sp.pub.Promise.BlobSize/uint32(need)+1)}
			m.Outcome = probe.OutcomeServedOK
			have += a.RowCount
		}
		cls, reason := probe.Classify(probe.Evidence{Assigned: true, Attested: a.Attested, AttestationUnknown: !sp.pub.HasAttestation(),
			Phase: m.Phase, Outcome: m.Outcome, CommitmentVerified: served})
		m.Classification, m.ClassificationReason = cls, reason
		m.TotalDurationMS = m.DNS.DurationMS + m.TCP.DurationMS + m.TLS.DurationMS + m.Download.DurationMS + 1
		m.FinishedAt = m.StartedAt.Add(time.Duration(m.TotalDurationMS) * time.Millisecond)
		out = append(out, m)
	}
	return out
}

func (s *sim) heartbeat(v simVal, vantage string, at time.Time) probe.Measurement {
	m := probe.Measurement{SchemaVersion: probe.MeasurementSchemaVersion, Vantage: vantage, ValidatorAddress: v.addr, ValidatorHost: v.host,
		ScheduledAt: at, StartedAt: at, FinishedAt: at.Add(40 * time.Millisecond)}
	m.DNS = probe.StepResult{Attempted: true, OK: true, DurationMS: 2}
	switch {
	case v.beh == "unreachable" || (v.beh == "flaky" && s.rng.IntN(6) == 0):
		m.TCP = probe.StepResult{Attempted: true, OK: false, DurationMS: 5000}
		m.Outcome = probe.OutcomeTCPTimeout
	default:
		m.TCP = probe.StepResult{Attempted: true, OK: true, DurationMS: 12}
		m.TLS = probe.TLSResult{Attempted: true, OK: true, DurationMS: 20, Version: "1.3"}
		m.Identity = probe.IdentityResult{Attempted: true, OK: s.rng.IntN(20) != 0}
		m.Outcome = probe.OutcomeReachable
	}
	m.TotalDurationMS = 40
	return m
}

// ---- running it ----

// advance moves the clock to at, appending every line due by then, and
// writes the scanner's state.
func (s *sim) advance(at time.Time) {
	s.t.Helper()
	s.now = at
	files := map[string]*os.File{}
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	var due []simEvent
	for s.next < len(s.events) && !s.events[s.next].at.After(at) {
		ev := s.events[s.next]
		s.next++
		if (ev.file == "measurements.jsonl" || ev.file == probe.SampledOutFile) && (s.inDark(ev.at) || s.inDark(startOf(ev.line))) {
			s.dark = append(s.dark, ev) // the prober's file lands late
			continue
		}
		due = append(due, ev)
	}
	if !s.inDark(at) && len(s.dark) > 0 {
		due = append(s.dark, due...)
		s.dark = nil
	}
	for _, ev := range due {
		f, ok := files[ev.file]
		if !ok {
			path := filepath.Join(s.dir, ev.file)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				s.t.Fatal(err)
			}
			var err error
			if f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
				s.t.Fatal(err)
			}
			files[ev.file] = f
		}
		if _, err := f.Write(append(ev.line, '\n')); err != nil {
			s.t.Fatal(err)
		}
	}
	frontier := at.Add(-30 * time.Second)
	if !at.Before(s.scen.frontier[0]) && at.Before(s.scen.frontier[1]) {
		frontier = s.scen.frontier[0] // held back
	}
	st := map[string]any{
		"schema_version": 1, "chain_id": "sim-1", "start_height": 1_000_000,
		"last_scanned_height": s.height, "last_scanned_time": frontier.Format(time.RFC3339Nano),
		"protocol_params_fingerprint": "sim",
		"param_history":               []scan.ParamEntry{{FromHeight: 1, FromTxIndex: -1, Source: "seed", ParamsJSON: s.params()}},
	}
	b, _ := json.Marshal(st)
	if err := os.WriteFile(filepath.Join(s.dir, "state.json"), b, 0o644); err != nil {
		s.t.Fatal(err)
	}
}

// pass runs the collector's pass at the clock.
func (s *sim) pass() {
	s.t.Helper()
	if errs := s.coll.Pass(context.Background(), s.now); len(errs) > 0 {
		s.t.Logf("pass at %s: %v", s.now.Format(time.RFC3339), errs)
	}
}

// makeSampled turns a planned publication into one drawn out of the
// sample: its readings go, and a decision stands for them.
func (s *sim) makeSampled(sp *simPub) {
	p := sp.pub
	var kept []simEvent
	for _, ev := range s.events {
		if ev.file == "measurements.jsonl" && bytes.Contains(ev.line, []byte(p.PromiseHash)) {
			continue
		}
		kept = append(kept, ev)
	}
	s.events = kept
	sp.sampled = true
	pt := probe.ReadPoint(p, probe.DefaultScheduleConfig())
	s.emit(p.SettlementTime.Add(20*time.Second), probe.SampledOutFile, probe.SampledOut{SchemaVersion: probe.SampledOutSchemaVersion,
		Kind: probe.SampledOutKind, Vantage: simVantage, PromiseHash: p.PromiseHash, Commitment: p.Promise.Commitment,
		SettlementTime: p.SettlementTime, MustServeUntil: p.MustServeUntil, ValidatorSetHeight: p.Assignment.ValidatorSetHeight,
		DecidedAt: p.SettlementTime.Add(20 * time.Second), Sampling: probe.SamplingDecision{P: 0.25, Binding: "budget"},
		Reason: "budget:p=0.25:budget", Points: []probe.SampledOutPoint{{Label: pt.Label, At: pt.At, Phase: pt.Phase}},
		Validators: p.Assignment.ValidatorsWithRows})
}

// moveEvent moves the publication line of promise planned at from to at.
func (s *sim) moveEvent(from time.Time, promise string, at time.Time) {
	for i := range s.events {
		ev := &s.events[i]
		if ev.file == "publications.jsonl" && ev.at.Equal(from) && bytes.Contains(ev.line, []byte(promise)) {
			ev.at = at
			return
		}
	}
}

func sameDay(a, b time.Time) bool { return a.UTC().Format(dayLayout) == b.UTC().Format(dayLayout) }

// emitNow appends a line at once, outside the plan.
func (s *sim) emitNow(file string, v any) {
	s.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		s.t.Fatal(err)
	}
	path := filepath.Join(s.dir, file)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		s.t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		s.t.Fatal(err)
	}
}

// ---- the scenarios ----

// simScen is where, in the run, each scenario happens. Every seed places
// them at other moments and over other publications.
type simScen struct {
	// outage is a prober outage: the readings it should have made are
	// written when it restarts, NOT_PROBED, stamped at its restart.
	outage [2]time.Time
	// backlog is a collector outage: no pass runs in it.
	backlog [2]time.Time
	// dark holds the prober's lines back over a whole day (its file lands
	// late, as a vantage's pulled file does when the pull stalls) while
	// the heartbeats go on: the day is sealed with heartbeats and no
	// reading, and sealed again once its readings arrive.
	dark [2]time.Time
	// frontier holds the scanner's frontier back, so deferred shadow
	// verdicts wait, and releases it.
	frontier [2]time.Time
	// deferDay is the day whose readings carry deferred shadow verdicts.
	deferDay time.Time
	// collapse is the publication written the old way, a NOT_PROBED row
	// per validator at two points on two days, with its record late.
	collapse int
	// held is a params range raised open and later verified, corrected
	// and lifted; failAfter is how many row corrections its pass applies
	// before the corrector fails.
	heldOpen, heldVerified time.Time
	failAfter              int
	// unresolvable is a range that never closes.
	unresolvable time.Time
	// amendAt replays an amendment of a row already sealed; weirdAt writes
	// a row start that is not a store timestamp; lateAt a reading of a
	// pruned day; reapplyAt applies corrections again under their range.
	amendAt, weirdAt, lateAt, reapplyAt time.Time
	// odd4000 is the day whose publications record original_rows 4000.
	odd4000 time.Time
	// tieFrom is where the second vantage's readings begin to tie with this
	// one's: a settlement day with a tie is read raw, never sealed.
	tieFrom time.Time
}

func (s *sim) planScenarios() {
	day := func(d int, h float64) time.Time {
		return s.t0.Add(time.Duration(d)*24*time.Hour + time.Duration(h*float64(time.Hour)))
	}
	r := func(lo, hi float64) float64 { return lo + (hi-lo)*s.rng.Float64() }
	sc := &s.scen
	sc.outage = [2]time.Time{day(5, r(15, 20)), day(6, r(6, 12))}
	sc.backlog = [2]time.Time{day(8, r(3, 8)), day(8, r(14, 22))}
	sc.frontier = [2]time.Time{day(5, 0), day(7, r(0, 12))}
	sc.deferDay = day(5, 0)
	sc.heldOpen, sc.heldVerified = day(4, r(15, 18)), day(5, r(10, 14))
	sc.failAfter = 1 + s.rng.IntN(4)
	sc.unresolvable = day(3, 8)
	sc.amendAt, sc.weirdAt, sc.lateAt = day(7, r(2, 20)), day(6, r(12, 20)), day(10, r(1, 20))
	sc.odd4000 = day(6, 0)
	sc.tieFrom = day(9, 0)
	sc.collapse = -1
	// The scenarios added later draw from an order of their own, so the
	// record every seed writes is the one the ones above were placed in.
	later := rand.New(rand.NewPCG(s.cfg.seed, 0x1a7e))
	r = func(lo, hi float64) float64 { return lo + (hi-lo)*later.Float64() }
	sc.reapplyAt = day(7, r(0, 12))
	sc.dark = [2]time.Time{day(0, r(18, 22)), day(2, r(6, 9))}
}

// inOutage reports whether a reading at t falls in the prober's outage.
func (s *sim) inOutage(t time.Time) bool {
	return !t.Before(s.scen.outage[0]) && t.Before(s.scen.outage[1])
}

// inDark reports whether the prober's lines are held back at t.
func (s *sim) inDark(t time.Time) bool {
	return !t.Before(s.scen.dark[0]) && t.Before(s.scen.dark[1])
}

// startOf is when a prober's line says its row starts: a reading's
// started_at, a decision's decided_at. A line stamped into the held-back
// span (a clock ahead) is held back with it.
func startOf(line []byte) time.Time {
	var v struct {
		StartedAt time.Time `json:"started_at"`
		DecidedAt time.Time `json:"decided_at"`
	}
	_ = json.Unmarshal(line, &v)
	if !v.StartedAt.IsZero() {
		return v.StartedAt
	}
	return v.DecidedAt
}

// afterPlan places what needs the publications first: the params ranges
// over their heights, the collapse, the amendment replay, the late row.
func (s *sim) afterPlan() {
	sc := &s.scen
	// cover is the n publications settled last before a moment, and the
	// heights a range over them spans.
	cover := func(before time.Time, n int) (lo, hi int64, pubs []*simPub) {
		for _, sp := range s.pubs {
			if sp.pub.SettlementTime.Before(before) {
				pubs = append(pubs, sp)
			}
		}
		pubs = pubs[max(0, len(pubs)-n):]
		for _, sp := range pubs {
			if lo == 0 || sp.pub.SettlementHeight < lo {
				lo = sp.pub.SettlementHeight
			}
			hi = max(hi, sp.pub.SettlementHeight)
		}
		return lo, hi, pubs
	}
	short := s.params()
	short.ShardRetentionSeconds = int64((s.cfg.retention - time.Hour).Seconds())
	// A range raised open over part of day 4, then verified: the values
	// read at every height say the retention was an hour shorter from the
	// middle of it on, so those deadlines move earlier and the rows drawn
	// against them are graded again.
	lo, hi, pubs := cover(s.scen.heldOpen.Add(-3*time.Hour), 8)
	if len(pubs) > 1 {
		mid := pubs[len(pubs)/2].pub.SettlementHeight
		s.heldRange = [3]int64{lo, hi, mid}
		u := scan.ParamUncertainty{SchemaVersion: scan.ParamUncertaintySchemaVersion, ID: fmt.Sprintf("sim-1:silent_change:%d-%d", lo, hi),
			ChainID: "sim-1", Kind: scan.UncertaintySilentChange, FromHeight: lo, ToHeight: hi, EffectiveFromHeight: hi + 1,
			IntervalStartKnown: true, Direction: "shorter", PublicationsAffected: int64(len(pubs)), DetectedAt: sc.heldOpen}
		s.emit(sc.heldOpen, "param_uncertainty.jsonl", u)
		at := sc.heldVerified
		u.Resolution, u.ResolveMethod, u.HeightsRead, u.ResolvedAt = scan.ResolutionVerified, "exhaustive_read", hi-lo+1, &at
		long := s.params()
		long.EffectiveFromHeight = lo - 1
		sh := short
		sh.EffectiveFromHeight = mid
		u.Values = []scan.ResolvedValue{{FromHeight: lo - 1, Params: long}, {FromHeight: mid, Params: sh}}
		s.emit(sc.heldVerified, "param_uncertainty.jsonl", u)
		// The last publication it covers is drawn out of the sample, so the
		// verified range moves a decision's points too.
		if last := pubs[len(pubs)-1]; !last.sampled && last.pub.Assignment.Error == "" {
			s.makeSampled(last)
		}
	}
	// A range that never closes, raised over the small hours of day 3 and
	// still holding when the run ends, past the raw retention.
	lo3, hi3, pubs3 := cover(sc.unresolvable.Add(-4*time.Hour), 3)
	if len(pubs3) > 0 {
		at := sc.unresolvable
		u := scan.ParamUncertainty{SchemaVersion: scan.ParamUncertaintySchemaVersion, ID: fmt.Sprintf("sim-1:silent_change:%d-%d", lo3, hi3),
			ChainID: "sim-1", Kind: scan.UncertaintySilentChange, FromHeight: lo3, ToHeight: hi3, EffectiveFromHeight: hi3 + 1,
			PublicationsAffected: int64(len(pubs3)), DetectedAt: at, Resolution: scan.ResolutionUnresolvable, ResolvedAt: &at,
			ResolveError: "the node has pruned that state"}
		s.emit(at, "param_uncertainty.jsonl", u)
	}
	// A reading of a day long pruned, arriving late.
	for _, sp := range s.pubs {
		if sp.pub.SettlementTime.After(s.t0.Add(26*time.Hour)) && !sp.sampled && sp.pub.Assignment.Error == "" {
			v := s.vals[0]
			m := s.missed(sp, v, probe.ReadPoint(sp.pub, probe.DefaultScheduleConfig()), sp.pub.SettlementTime.Add(90*time.Minute))
			m.ScheduledAt = m.ScheduledAt.Add(-time.Hour) // another point, so it is a row of its own
			m.ScheduleLabel = "w2"
			s.emit(sc.lateAt, "measurements.jsonl", m)
			break
		}
	}
}

// oldStyleSampledOut writes publication sp the way the prober wrote a
// sampled-out publication before decisions had a file of their own: a
// NOT_PROBED row for every assigned validator at every point, here two
// points on two days, the first late on its settlement day.
func (s *sim) oldStyleSampledOut(sp *simPub, pt probe.SchedulePoint) {
	p := sp.pub
	early := probe.SchedulePoint{At: p.SettlementTime.Add(10 * time.Minute), Phase: probe.PhaseInWindow, Label: "w1"}
	draw := probe.SamplingDecision{P: 0.25, Binding: "budget", DayCommitment: s.hash("day", 1)[:16]}
	for _, point := range []probe.SchedulePoint{early, pt} {
		for _, v := range s.vals {
			a := s.assignment(sp, v.addr)
			if a.RowCount == 0 {
				continue
			}
			m := s.missed(sp, v, point, point.At)
			m.ClassificationReason = "budget:p=0.25:budget:day_commitment=" + draw.DayCommitment
			d := draw
			m.Sampling = &d
			s.emit(point.At.Add(time.Second), "measurements.jsonl", m)
		}
	}
}

// deferred makes a reading's row carry a deferred shadow verdict: genuine
// rows of this blob that no scanned promise assigns this validator, judged
// by the collector once the scanner's frontier passes the promise timeout.
func (s *sim) deferred(m probe.Measurement) probe.Measurement {
	m.Outcome = probe.OutcomeWrongRows
	m.Classification = probe.ClassProbeError
	m.ClassificationReason = "genuine rows no known promise assigns; the verdict waits for the scanner"
	m.Download.AssignmentVerified = false
	m.Download.ShadowGap = probe.ShadowGapPendingPrefix + ": a promise uploaded before this reading may settle later and own these rows"
	return m
}

// failCorrector makes the corrector fail part way through its next run:
// the first n row corrections apply, and every one after them is refused
// by a trigger on the collector's own connection, as a write failing
// under it would be. The lines of the refused ones are already in
// corrections.jsonl (they are written first), so the next pass replays
// them, and the revision is never bumped for them.
func (s *sim) failCorrector(n int) {
	s.t.Helper()
	for _, q := range []string{
		`CREATE TEMP TABLE IF NOT EXISTS sim_hits (n INTEGER)`,
		`DELETE FROM temp.sim_hits`,
		`INSERT INTO temp.sim_hits VALUES (0)`,
		`CREATE TEMP TRIGGER IF NOT EXISTS sim_fail_probe BEFORE UPDATE OF phase ON probes BEGIN
			SELECT RAISE(ABORT, 'injected: the corrector fails part way') WHERE (SELECT n FROM temp.sim_hits) >= ` + strconv.Itoa(n) + `;
			UPDATE temp.sim_hits SET n = n + 1;
		END`,
		`CREATE TEMP TRIGGER IF NOT EXISTS sim_fail_point BEFORE UPDATE OF phase ON sampling_decision_points BEGIN
			SELECT RAISE(ABORT, 'injected: the corrector fails part way') WHERE (SELECT n FROM temp.sim_hits) >= ` + strconv.Itoa(n) + `;
			UPDATE temp.sim_hits SET n = n + 1;
		END`,
	} {
		if _, err := s.st.DB().Exec(q); err != nil {
			s.t.Fatal(err)
		}
	}
}

func (s *sim) healCorrector() {
	s.t.Helper()
	for _, q := range []string{`DROP TRIGGER IF EXISTS temp.sim_fail_probe`, `DROP TRIGGER IF EXISTS temp.sim_fail_point`} {
		if _, err := s.st.DB().Exec(q); err != nil {
			s.t.Fatal(err)
		}
	}
}

// weirdRow writes, straight into the store as a foreign writer would, a
// copy of a reading whose start is not a store timestamp and sorts between
// two days: no day's bounds hold it. leap makes it one of the right shape
// that is no time at all, the last minute's sixtieth second, which only
// strftime tells from a store timestamp.
func (s *sim) weirdRow(between time.Time, leap bool) {
	s.t.Helper()
	v, tag := store.TS(between.Add(-time.Nanosecond))+"9", "weird|"
	if leap {
		v, tag = between.Add(-time.Nanosecond).Format("2006-01-02T15:04:")+"60.000000000Z", "leap|"
	}
	s.copyRow(between, v, tag)
}

// copyRow writes, straight into the store as a foreign writer would, a copy
// of the newest reading started before a moment, with its start set to v
// and tag before its key.
func (s *sim) copyRow(before time.Time, v, tag string) {
	s.t.Helper()
	db := s.st.DB()
	for _, q := range []string{
		`DROP TABLE IF EXISTS temp.sim_weird`,
		`CREATE TEMP TABLE sim_weird AS SELECT * FROM probes WHERE started_at < '` + store.TS(before) + `' ORDER BY rowid DESC LIMIT 1`,
		`UPDATE temp.sim_weird SET dedupe_key = '` + tag + `' || dedupe_key, started_at = '` + v + `'`,
		`INSERT INTO probes SELECT * FROM temp.sim_weird`,
		`DROP TABLE temp.sim_weird`,
	} {
		if _, err := db.Exec(q); err != nil {
			s.t.Fatal(err)
		}
	}
}

// amendReplay appends to amendments.jsonl, as a rebuild replays it, a late
// verdict on a row of a day sealed long since.
func (s *sim) amendReplay() {
	s.t.Helper()
	var key, cls, started string
	err := s.st.DB().QueryRow(`SELECT dedupe_key, classification, started_at FROM probes WHERE amended_at IS NULL AND outcome = 'NOT_FOUND'
		AND started_at >= ? AND started_at < ? ORDER BY rowid LIMIT 1`, store.TS(s.t0.Add(48*time.Hour)), store.TS(s.t0.Add(72*time.Hour))).Scan(&key, &cls, &started)
	if err != nil {
		return
	}
	s.emitNow("amendments.jsonl", store.Amendment{DedupeKey: key, From: cls, To: string(probe.ClassUnmatchedGenuine),
		Reason: "replayed: genuine rows no promise assigns", JudgedAt: s.now, ScannerFrontier: s.now})
}

// reapply applies a publication's correction and a row's correction again,
// each under the range it was first applied under, as the corrector does
// when the deadline its range pass recomputes moves and its sweep then
// grades the rows again: the store rewrites the publication and the row,
// and neither log gains a line (ON CONFLICT DO NOTHING). The publication's
// deadline moves past the clock, so a publication of a day long sealed is
// unread again; the row, of another publication where there is one, moves
// into or out of its window on a deadline six hours earlier, and the sweep
// grades it back.
func (s *sim) reapply() {
	s.t.Helper()
	db := s.st.DB()
	var key, ru, rh, val, sched, phase, cls, rmsu string
	if err := db.QueryRow(`SELECT k.dedupe_key, k.uncertainty_id, r.promise_hash, r.validator_address, r.scheduled_at, r.phase, r.classification, r.must_serve_until
		FROM probe_corrections k JOIN probes r ON r.dedupe_key = k.dedupe_key ORDER BY r.started_at LIMIT 1`).
		Scan(&key, &ru, &rh, &val, &sched, &phase, &cls, &rmsu); err != nil {
		s.t.Fatalf("no row correction to apply again: %v", err)
	}
	var h, u, msu string
	if err := db.QueryRow(`SELECT k.promise_hash, k.uncertainty_id, p.must_serve_until FROM publication_corrections k
		JOIN publications p ON p.promise_hash = k.promise_hash ORDER BY k.promise_hash = ?, p.settlement_time LIMIT 1`, rh).Scan(&h, &u, &msu); err != nil {
		s.t.Fatalf("no publication correction to apply again: %v", err)
	}
	from, _ := time.Parse(store.TimeLayout, msu)
	pc := store.Correction{SchemaVersion: store.CorrectionSchemaVersion, Kind: store.CorrectionPublicationDeadline,
		UncertaintyID: u, PromiseHash: h, FromMustServeUntil: from, ToMustServeUntil: s.now.Add(3 * time.Hour),
		FromBasis: "sim", ToBasis: "sim; CORRECTED: recomputed again", Reason: "sim: applied again under its range", JudgedAt: s.now}
	at, _ := time.Parse(store.TimeLayout, sched)
	rfrom, _ := time.Parse(store.TimeLayout, rmsu)
	to := probe.PhasePost
	if phase == string(probe.PhasePost) {
		to = probe.PhaseInWindow
	}
	rc := store.Correction{SchemaVersion: store.CorrectionSchemaVersion, Kind: store.CorrectionProbeVerdict,
		UncertaintyID: ru, PromiseHash: rh, DedupeKey: key, ValidatorAddress: val, ScheduledAt: at,
		FromPhase: phase, ToPhase: string(to), FromClassification: cls, ToClassification: cls,
		FromMustServeUntil: rfrom, ToMustServeUntil: rfrom.Add(-6 * time.Hour), PruneToleranceS: 300,
		Reason: "sim: applied again under its range", JudgedAt: s.now}
	for _, c := range []store.Correction{pc, rc} {
		b, err := json.Marshal(c)
		if err != nil {
			s.t.Fatal(err)
		}
		if _, err := s.corrFile.Write(append(b, '\n')); err != nil {
			s.t.Fatal(err)
		}
	}
	if _, err := s.st.ApplyPublicationCorrection(pc); err != nil {
		s.t.Fatal(err)
	}
	if _, err := s.st.ApplyProbeCorrection(rc); err != nil {
		s.t.Fatal(err)
	}
	// And a row of a publication no correction moved, corrected a first
	// time (a line of the log) now and again later (none): the store takes
	// both from anyone, whatever the corrector itself would do.
	var sh string
	if err := db.QueryRow(`SELECT r.dedupe_key, r.promise_hash, r.validator_address, r.scheduled_at, r.phase, r.classification, r.must_serve_until
		FROM probes r JOIN publications p ON p.promise_hash = r.promise_hash
		WHERE p.corrected_at IS NULL AND r.corrected_at IS NULL AND r.phase = 'in_window' AND r.started_at < ?
		ORDER BY r.started_at DESC LIMIT 1`, store.TS(s.now.Add(-36*time.Hour))).
		Scan(&key, &sh, &val, &sched, &phase, &cls, &rmsu); err != nil {
		s.t.Fatalf("no row of an uncorrected publication: %v", err)
	}
	at, _ = time.Parse(store.TimeLayout, sched)
	rfrom, _ = time.Parse(store.TimeLayout, rmsu)
	first := store.Correction{SchemaVersion: store.CorrectionSchemaVersion, Kind: store.CorrectionProbeVerdict,
		UncertaintyID: "sim-again", PromiseHash: sh, DedupeKey: key, ValidatorAddress: val, ScheduledAt: at,
		FromPhase: phase, ToPhase: phase, FromClassification: cls, ToClassification: cls,
		FromMustServeUntil: rfrom, ToMustServeUntil: rfrom, PruneToleranceS: 300,
		Reason: "sim: a first correction", JudgedAt: s.now}
	if _, err := s.st.ApplyProbeCorrection(first); err != nil {
		s.t.Fatal(err)
	}
	again := first
	again.ToPhase, again.ToMustServeUntil, again.Reason = string(probe.PhasePost), rfrom.Add(-6*time.Hour), "sim: the same correction again, other values"
	s.again = &again
}

// reapplyAgain applies again, under the same range and with other values,
// the correction reapply applied a first time to a row of a publication no
// correction moved: the row moves, the log gains no line.
func (s *sim) reapplyAgain() {
	s.t.Helper()
	s.again.JudgedAt = s.now
	if _, err := s.st.ApplyProbeCorrection(*s.again); err != nil {
		s.t.Fatal(err)
	}
	s.again = nil
}
