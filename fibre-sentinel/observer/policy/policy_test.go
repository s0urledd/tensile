package policy

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

func newTest(t *testing.T, cfg Config) *Policy {
	t.Helper()
	// A process-local master secret is refused outside tests, because the
	// day commitments it stamps cannot be verified after a restart. These
	// tests are the case it exists for.
	cfg.Sampling.AllowEphemeralSecret = true
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.master = []byte("fixed-test-master-secret-32-bytes!!")
	return p
}

func pubOf(hash string, settled time.Time, blobSize uint32, rows ...int) scan.Publication {
	var pub scan.Publication
	pub.PromiseHash = hash
	pub.SettlementTime = settled
	pub.Promise.BlobSize = blobSize
	pub.Assignment.ProtocolParams.OriginalRows = 4096
	pub.Assignment.ProtocolParams.TotalRows = 16384
	for i, r := range rows {
		pub.Assignment.Validators = append(pub.Assignment.Validators, scan.ValidatorAssignment{
			Address: string(rune('a'+i)) + "0000000000000000000000000000000000000000"[1:], RowCount: r,
		})
	}
	return pub
}

func TestShardBytesMatchPolicyTable(t *testing.T) {
	// 128 MiB blob, 148 rows -> 4,986,836 B; 4096 rows -> 136,265,732 B.
	if got := ShardBytes(128<<20, 4096, 148); got != 4_986_836 {
		t.Fatalf("floor shard bytes = %d", got)
	}
	if got := ShardBytes(128<<20, 4096, 4096); got != 136_265_732 {
		t.Fatalf("max shard bytes = %d", got)
	}
	if got := ShardBytes(256<<10, 4096, 148); got != 146_644 {
		t.Fatalf("256 KiB floor shard bytes = %d", got)
	}
}

func TestSamplingIsDeterministicAndUnbiased(t *testing.T) {
	p := newTest(t, Default())
	day := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
	in := 0
	const n = 4000
	for i := 0; i < n; i++ {
		h := hexHash(i)
		a := p.Sampled(h, day, 0.4)
		b := p.Sampled(h, day, 0.4)
		if a != b {
			t.Fatalf("non-deterministic decision for %s", h)
		}
		if a {
			in++
		}
	}
	frac := float64(in) / n
	if frac < 0.36 || frac > 0.44 {
		t.Fatalf("sampled fraction %.3f, want about 0.40", frac)
	}
	if !p.Sampled(hexHash(1), day, 1) || p.Sampled(hexHash(1), day, 0) {
		t.Fatal("p=1 must admit, p=0 must reject")
	}
	// a different day secret gives a different sample.
	other := day.Add(24 * time.Hour)
	diff := 0
	for i := 0; i < 200; i++ {
		if p.Sampled(hexHash(i), day, 0.5) != p.Sampled(hexHash(i), other, 0.5) {
			diff++
		}
	}
	if diff == 0 {
		t.Fatal("day secret has no effect on the sample")
	}
	if p.DayCommitment(day) == p.DayCommitment(other) {
		t.Fatal("day commitments must differ")
	}
}

func hexHash(i int) string {
	const hexdigits = "0123456789abcdef"
	b := make([]byte, 64)
	for j := range b {
		b[j] = hexdigits[(i>>(uint(j%8)*4)+j)&15]
	}
	return string(b)
}

func TestAdmitEverythingUnderBudget(t *testing.T) {
	p := newTest(t, Default())
	now := time.Now()
	for i := 0; i < 50; i++ {
		pub := pubOf(hexHash(i), now.Add(-time.Duration(i)*time.Minute), 1<<20, 148, 3035, 3186)
		if ok, reason := p.Admit(pub, false); !ok {
			t.Fatalf("1 MiB blob %d rejected: %s", i, reason)
		}
	}
	if prob, _ := p.State(); prob != 1 {
		t.Fatalf("p = %.3f, want 1", prob)
	}
}

func TestAdmitSamplesWhenGlobalCapBinds(t *testing.T) {
	cfg := Default()
	cfg.Caps.Global.BytesPerHour = 50 << 30
	p := newTest(t, cfg)
	now := time.Now()
	// Stress scenario: 60 × 128 MiB blobs in the last hour over 100
	// validators (Σ rows ≈ 12,288) needs ~125 GB/h; the 50 GB/h cap binds
	// at p ≈ 0.4.
	rows := make([]int, 0, 100)
	for i := 0; i < 100; i++ {
		rows = append(rows, 123)
	}
	admitted := 0
	for i := 0; i < 60; i++ {
		pub := pubOf(hexHash(1000+i), now.Add(-time.Duration(i)*time.Minute), 128<<20, rows...)
		if ok, _ := p.Admit(pub, false); ok {
			admitted++
		}
	}
	prob, binding := p.State()
	// The global DAILY cap binds first, not the hourly one. 600 GiB/day over
	// 50 GiB/h is twelve hours of headroom, so a load sustained for a full
	// day runs out of daily budget at half the hourly rate. The sampler has
	// to see that when it picks p, or the day's later probes get denied one
	// by one after their publications were already admitted — and because
	// the schedule is packed toward the deadline, the points lost are the
	// late in-window and grace ones, which is where a breach shows.
	if binding != "global_bytes_per_day" {
		t.Fatalf("binding cap = %s, want the daily cap: it is tighter than the hourly one at sustained load", binding)
	}
	if prob > 0.4 || prob <= 0 {
		t.Fatalf("p = %.3f, want tighter than the hourly cap's ~0.4", prob)
	}
	if admitted == 0 || admitted == 60 {
		t.Fatalf("admitted %d of 60", admitted)
	}
	// sticky: asking again returns the same answers.
	for i := 0; i < 60; i++ {
		pub := pubOf(hexHash(1000+i), now.Add(-time.Duration(i)*time.Minute), 128<<20, rows...)
		ok1, _ := p.Admit(pub, false)
		ok2, _ := p.Admit(pub, false)
		if ok1 != ok2 {
			t.Fatal("decision changed between calls")
		}
	}
	// a started publication is always admitted.
	pub := pubOf(hexHash(9999), now, 128<<20, rows...)
	if ok, _ := p.Admit(pub, true); !ok {
		t.Fatal("already-started publication must be admitted")
	}
}

// Transport failures never change what a probe does: there is no backoff,
// so the fourth request is refused by the per-minute cap alone, and the
// next minute is admitted in full after three failures in a row.
func TestBudgetWithoutBackoff(t *testing.T) {
	cfg := Default()
	cfg.Caps.PerValidator.MinRequestSpacing = 0
	cfg.Caps.PerValidator.RequestsPerMinute = 3
	p := newTest(t, cfg)
	now := time.Now()
	pub := pubOf(hexHash(7), now, 1<<20, 148)
	tgt := probe.Target{AddressHex: pub.Assignment.Validators[0].Address, RowCount: 148, Assigned: true}

	m := probe.Measurement{ValidatorAddress: tgt.AddressHex, AssignedRowCount: 148, StartedAt: now, Outcome: probe.OutcomeTCPRefused}
	for i := 0; i < 3; i++ {
		if allow, reason := p.BeforeProbe(pub, tgt, now); !allow {
			t.Fatalf("probe %d denied: %s", i, reason)
		}
		p.AfterProbe(pub, m)
	}
	if allow, reason := p.BeforeProbe(pub, tgt, now); allow || reason != "budget:validator_requests_per_minute=3" {
		t.Fatalf("4th probe within a minute: allow=%v reason=%s", allow, reason)
	}
	if allow, reason := p.BeforeProbe(pub, tgt, now.Add(61*time.Second)); !allow {
		t.Fatalf("the next minute, after three transport failures: denied: %s", reason)
	}
	// byte cap: a 128 MiB blob's floor shard is ~5 MB; cap is 2.9 GB/h, so
	// ~585 downloads fit; force the cap low instead.
	cfg2 := Default()
	cfg2.Caps.PerValidator.MinRequestSpacing = 0
	cfg2.Caps.PerValidator.RequestsPerMinute = 0
	cfg2.Capacity.FloorValidatorBps = 8 * 1024 // 1 KiB/s -> 36 KB/h at 1%
	p2 := newTest(t, cfg2)
	big := pubOf(hexHash(8), now, 128<<20, 148)
	if allow, reason := p2.BeforeProbe(big, tgt, now); allow {
		t.Fatalf("5 MB shard should exceed a 36 KB/h cap; reason=%s", reason)
	}
}

// The shipped YAML files must load, and the caps they compute must be the
// numbers their own comments promise validators.
func TestShippedPoliciesLoad(t *testing.T) {
	cases := []struct {
		file    string
		perHour int64
		perDay  int64
	}{
		{"policy.example.yaml", 2_925_000_000, 52_650_000_000},
		{"policy.mocha.yaml", 0, 0}, // no per-validator byte budget
	}
	for _, c := range cases {
		cfg, err := Load(c.file)
		if err != nil {
			t.Fatalf("%s: %v", c.file, err)
		}
		if err := cfg.validate(); err != nil {
			t.Fatalf("%s: %v", c.file, err)
		}
		if got := cfg.bytesPerHourCap(cfg.Capacity.FloorRows); got != c.perHour {
			t.Errorf("%s: per-hour cap at floor rows = %d, want %d", c.file, got, c.perHour)
		}
		if got := cfg.bytesPerDayCap(cfg.Capacity.FloorRows); got != c.perDay {
			t.Errorf("%s: per-day cap at floor rows = %d, want %d", c.file, got, c.perDay)
		}
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	bad := []struct {
		name  string
		mutfn func(*Config)
	}{
		{"negative day fraction", func(c *Config) { c.Caps.PerValidator.BytesPerDayFraction = -0.1 }},
		{"zero global day", func(c *Config) { c.Caps.Global.BytesPerDay = 0 }},
		{"negative requests", func(c *Config) { c.Caps.PerValidator.RequestsPerMinute = -1 }},
		{"negative spacing", func(c *Config) { c.Caps.PerValidator.MinRequestSpacing = -time.Second }},
		{"zero lookback", func(c *Config) { c.Sampling.ProjectionLookback = 0 }},
		{"zero floor rows", func(c *Config) { c.Capacity.FloorRows = 0 }},
	}
	for _, b := range bad {
		cfg := Default()
		b.mutfn(&cfg)
		if err := cfg.validate(); err == nil {
			t.Errorf("%s: validate accepted it", b.name)
		}
	}
	if err := Default().validate(); err != nil {
		t.Fatalf("the defaults must validate: %v", err)
	}
}

// The sticky admit/deny map must stay bounded, and eviction must drop the
// publications whose windows closed longest ago. It used to empty the whole
// map, which re-decided every publication still in flight at whatever
// admission probability the load happened to give — admitting some points of
// a schedule whose earlier points were denied.
func TestDecisionsAreBoundedAndEvictOldestFirst(t *testing.T) {
	p := newTest(t, Default())
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	hash := func(i int) string { return fmt.Sprintf("%08x", i) }
	for i := 0; i < maxDecisions+1000; i++ {
		p.remember(scan.Publication{PromiseHash: hash(i), SettlementTime: base.Add(time.Duration(i) * time.Second)}, i%2 == 0, 1, "test")
	}
	if len(p.decisions) >= maxDecisions {
		t.Fatalf("decisions grew to %d, bound is %d", len(p.decisions), maxDecisions)
	}
	// the newest publication is always still known
	newest := hash(maxDecisions + 999)
	if _, ok := p.decisions[newest]; !ok {
		t.Fatalf("the newest publication was evicted")
	}
	// and the ones that survived are newer than the ones that did not
	oldestKept := base.Add(time.Duration(maxDecisions+1000) * time.Second)
	for h, d := range p.decisions {
		if d.at.Before(oldestKept) {
			oldestKept = d.at
		}
		if d.at.IsZero() {
			t.Fatalf("%s kept with no settlement time", h)
		}
	}
	if _, ok := p.decisions[hash(0)]; ok {
		t.Fatalf("the oldest publication survived eviction while newer ones were dropped")
	}
	// a decision, once made, never changes while it is remembered
	pub := scan.Publication{PromiseHash: newest, SettlementTime: base}
	first, _ := p.Admit(pub, false)
	for i := 0; i < 5; i++ {
		if again, _ := p.Admit(pub, false); again != first {
			t.Fatalf("Admit flipped from %v to %v for a publication still in the map", first, again)
		}
	}
	// Forget releases it
	p.Forget(newest)
	if _, ok := p.decisions[newest]; ok {
		t.Fatalf("Forget did not drop the decision")
	}
}

// Every row carries the probability its publication was drawn at, and the
// commit-and-reveal audit the methodology page publishes recomputes exactly
// that: seed, hash, compare to p. The probability moves with load, and the
// rows are stamped hours after the decision, so reading the process-wide last
// value gave a publication admitted at p=1 rows that said p=0.24. A verifier
// following the recipe would then derive a sample that does not match the
// record — which reads as the observer having probed something other than
// what it drew.
func TestSamplingForIsThePublicationsOwnDraw(t *testing.T) {
	cfg := Default()
	cfg.Caps.Global.BytesPerHour = 50 << 30
	p := newTest(t, cfg)
	now := time.Now()
	rows := make([]int, 100)
	for i := range rows {
		rows[i] = 123
	}

	// The first publication is drawn while nothing is projected, so it is
	// admitted at p = 1.
	first := pubOf(hexHash(1), now, 128<<20, rows...)
	if ok, _ := p.Admit(first, false); !ok {
		t.Fatal("the first publication under an empty projection was denied")
	}
	firstP, firstBinding, _ := p.SamplingFor(first)
	if firstP != 1 {
		t.Fatalf("the first publication's p = %.3f, want 1", firstP)
	}

	// Load builds until a cap binds and p falls.
	for i := 0; i < 60; i++ {
		p.Admit(pubOf(hexHash(1000+i), now.Add(-time.Duration(i)*time.Minute), 128<<20, rows...), false)
	}
	nowP, _ := p.State()
	if nowP >= 1 {
		t.Fatalf("no cap bound after 60 large publications (p = %.3f); the fixture cannot show the bug", nowP)
	}

	// The first publication's stamp must not have moved with the load.
	againP, againBinding, _ := p.SamplingFor(first)
	if againP != firstP || againBinding != firstBinding {
		t.Fatalf("the first publication's draw changed from p=%.3f/%s to p=%.3f/%s as other publications were drawn",
			firstP, firstBinding, againP, againBinding)
	}
	if againP == nowP {
		t.Fatalf("the first publication reports the process-wide current p (%.3f) rather than its own", nowP)
	}

	// Every publication reports a p that is its own, and a denial's reason
	// quotes the same number.
	for i := 0; i < 60; i++ {
		pub := pubOf(hexHash(1000+i), now.Add(-time.Duration(i)*time.Minute), 128<<20, rows...)
		gotP, gotBinding, commitment := p.SamplingFor(pub)
		if gotP <= 0 || gotP > 1 {
			t.Fatalf("publication %d: p = %.3f, outside (0,1]", i, gotP)
		}
		if gotBinding == "" {
			t.Fatalf("publication %d: no binding cap recorded", i)
		}
		if commitment == "" {
			t.Fatalf("publication %d: no day commitment", i)
		}
		if ok, reason := p.Admit(pub, false); !ok {
			if want := fmt.Sprintf("p=%.3f", gotP); !strings.Contains(reason, want) {
				t.Fatalf("publication %d denied with reason %q, which does not quote its own %s", i, reason, want)
			}
		}
	}

	// An already-started publication is not a draw, and says so rather than
	// borrowing a probability it was never subject to.
	started := pubOf(hexHash(7777), now, 128<<20, rows...)
	if ok, _ := p.Admit(started, true); !ok {
		t.Fatal("an already-started publication was denied")
	}
	if gotP, gotBinding, _ := p.SamplingFor(started); gotP != 1 || gotBinding != "already_started" {
		t.Fatalf("already-started publication: p=%.3f binding=%q, want 1/already_started", gotP, gotBinding)
	}
}

// The sampling audit is: reveal the day's secret afterwards, recompute each
// publication's draw, check it against the rows. A master secret that is new
// on every start makes every commitment already published unverifiable — not
// visibly wrong, just impossible to check, which for an observer whose claim
// is "recompute this yourself" is the worse failure. It has to be refused
// rather than warned about, because nothing downstream can detect it.
func TestEphemeralSamplingSecretIsRefusedOutsideTests(t *testing.T) {
	cfg := Default()
	if cfg.Sampling.MasterSecretFile != "" {
		t.Fatalf("the default config now sets a secret file (%q); this test no longer covers the case", cfg.Sampling.MasterSecretFile)
	}
	if _, err := New(cfg); err == nil {
		t.Fatal("a policy with no master_secret_file was accepted")
	} else if !strings.Contains(err.Error(), "master_secret_file") {
		t.Fatalf("the refusal does not name what to set: %v", err)
	}

	// The escape exists, and says what it is.
	cfg.Sampling.AllowEphemeralSecret = true
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("allow_ephemeral_secret did not permit it: %v", err)
	}
	if !p.EphemeralSecret() {
		t.Fatal("a process-local secret does not report itself as one")
	}

	// A configured path is the ordinary case and reports itself as durable.
	cfg2 := Default()
	cfg2.Sampling.MasterSecretFile = filepath.Join(t.TempDir(), "sampling-master.key")
	p2, err := New(cfg2)
	if err != nil {
		t.Fatalf("a configured secret file was refused: %v", err)
	}
	if p2.EphemeralSecret() {
		t.Fatal("a secret read from a file reports itself as process-local")
	}
	// And it survives: a second policy over the same file draws the same way.
	p3, err := New(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	if p2.DayCommitment(day) != p3.DayCommitment(day) {
		t.Fatal("two policies over the same secret file produced different day commitments")
	}
}

// admitConcurrently asks BeforeProbe for the same validator from n
// goroutines at once, as the prober's workers do on the first burst after a
// start, and returns the answers.
func admitConcurrently(p *Policy, pub scan.Publication, tgt probe.Target, n int) (allowed int, reasons []string) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			allow, reason := p.BeforeProbe(pub, tgt, time.Now())
			mu.Lock()
			defer mu.Unlock()
			if allow {
				allowed++
			} else {
				reasons = append(reasons, reason)
			}
		}()
	}
	close(start)
	wg.Wait()
	return allowed, reasons
}

// Admission reserves: N concurrent asks for one validator, none of them yet
// accounted, admit no more than the request cap allows. BeforeProbe used to
// only read, and AfterProbe was the only writer, so all N passed on the same
// empty state.
func TestBeforeProbe_ConcurrentAdmissionsHoldTheRequestCap(t *testing.T) {
	cfg := Default()
	cfg.Caps.PerValidator.MinRequestSpacing = 0
	cfg.Caps.PerValidator.RequestsPerMinute = 3
	p := newTest(t, cfg)
	pub := pubOf(hexHash(21), time.Now(), 1<<20, 148)
	tgt := probe.Target{AddressHex: pub.Assignment.Validators[0].Address, RowCount: 148, Assigned: true}

	allowed, reasons := admitConcurrently(p, pub, tgt, 8)
	if allowed != 3 {
		t.Fatalf("8 concurrent asks admitted %d, want exactly the cap of 3 (denials: %v)", allowed, reasons)
	}
	for _, r := range reasons {
		if r != "budget:validator_requests_per_minute=3" {
			t.Fatalf("denied for %q", r)
		}
	}
	// A released reservation frees its slot; an accounted one keeps it.
	p.Release(pub, tgt)
	if allow, reason := p.BeforeProbe(pub, tgt, time.Now()); !allow {
		t.Fatalf("a released slot was not given back: %s", reason)
	}
	p.AfterProbe(pub, probe.Measurement{ValidatorAddress: tgt.AddressHex, AssignedRowCount: 148, StartedAt: time.Now(), Outcome: probe.OutcomeNotFound})
	if allow, _ := p.BeforeProbe(pub, tgt, time.Now()); allow {
		t.Fatal("an accounted probe gave its slot back")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if n := len(p.pending); n != 2 {
		t.Fatalf("%d reservations pending, want 2 (3 admitted, 1 released, 1 re-admitted, 1 accounted)", n)
	}
}

// The same for the byte cap: the shard every admission may download is
// charged when it is admitted, so a burst cannot overshoot the hourly cap
// by the shards still in flight.
func TestBeforeProbe_ConcurrentAdmissionsHoldTheByteCap(t *testing.T) {
	cfg := Default()
	cfg.Caps.PerValidator.MinRequestSpacing = 0
	cfg.Caps.PerValidator.RequestsPerMinute = 0
	pub := pubOf(hexHash(22), time.Now(), 1<<20, 148)
	tgt := probe.Target{AddressHex: pub.Assignment.Validators[0].Address, RowCount: 148, Assigned: true}
	shard := ShardBytes(pub.Promise.BlobSize, pub.Assignment.ProtocolParams.OriginalRows, 148)
	// an hourly cap of two and a half shards
	cfg.Capacity.FloorValidatorBps = int64(float64(shard)*2.5*8/3600/cfg.Caps.PerValidator.BytesPerHourFraction) + 1
	cfg.Caps.PerValidator.BytesPerDayFraction = 1
	p := newTest(t, cfg)
	if c := p.cfg.bytesPerHourCap(148); c < 2*shard || c >= 3*shard {
		t.Fatalf("test setup: cap %d for a %d-byte shard", c, shard)
	}
	allowed, reasons := admitConcurrently(p, pub, tgt, 8)
	if allowed != 2 {
		t.Fatalf("8 concurrent asks admitted %d, want the 2 shards the hourly cap holds (denials: %v)", allowed, reasons)
	}
	for _, r := range reasons {
		if r != "budget:validator_bytes_per_hour" {
			t.Fatalf("denied for %q", r)
		}
	}
}

// Admission books the spacing too: concurrent asks for one validator are
// admitted MinRequestSpacing apart, not all in the same instant.
func TestBeforeProbe_ConcurrentAdmissionsAreSpaced(t *testing.T) {
	cfg := Default()
	cfg.Caps.PerValidator.MinRequestSpacing = 60 * time.Millisecond
	cfg.Caps.PerValidator.RequestsPerMinute = 0
	p := newTest(t, cfg)
	pub := pubOf(hexHash(23), time.Now(), 1<<20, 148)
	tgt := probe.Target{AddressHex: pub.Assignment.Validators[0].Address, RowCount: 148, Assigned: true}

	const n = 5
	allowed, reasons := admitConcurrently(p, pub, tgt, n)
	if allowed != n {
		t.Fatalf("admitted %d of %d: %v", allowed, n, reasons)
	}
	p.mu.Lock()
	ats := make([]time.Time, 0, len(p.pending))
	for _, r := range p.pending {
		ats = append(ats, r.at)
	}
	p.mu.Unlock()
	sort.Slice(ats, func(i, j int) bool { return ats[i].Before(ats[j]) })
	for i := 1; i < len(ats); i++ {
		if gap := ats[i].Sub(ats[i-1]); gap < cfg.Caps.PerValidator.MinRequestSpacing {
			t.Fatalf("admissions %d and %d only %s apart, spacing is %s", i-1, i, gap, cfg.Caps.PerValidator.MinRequestSpacing)
		}
	}
	// And an ask inside the spacing of the last admission waits it out
	// rather than going straight through.
	began := time.Now()
	if allow, _ := p.BeforeProbe(pub, tgt, time.Now()); !allow {
		t.Fatal("denied")
	}
	if waited := time.Since(began); waited < cfg.Caps.PerValidator.MinRequestSpacing/2 {
		t.Fatalf("an ask right after an admission went through after %s", waited)
	}
}
