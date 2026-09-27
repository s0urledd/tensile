// Package policy implements the probe load policy (docs/SYSTEM.md, "Load
// policy and sampling"): deterministic, unpredictable sampling
// of publications when the byte or request budget would be exceeded,
// and per-validator and global caps.
//
// There is no backoff. It used to skip the download for twenty minutes after
// three transport failures in a row, which turned the scheduled points of a
// validator that had recovered into handshake-only rows: the observer's own
// state, not the validator, decided what was measured. Every scheduled point
// that the caps admit now downloads the shard.
//
// The policy plugs into the prober through probe.Policy. Its counters live
// in memory and, when a state file is configured, are saved to the data
// dir so a restart does not start on a fresh budget (budgetstate.go); the
// durable record of every decision is what the prober writes with the
// policy's reason: for a publication drawn out of the sample, one
// probe.SampledOut line in sampling_decisions.jsonl; for a probe a cap
// denied, that probe's NOT_PROBED measurement.
//
// Sampling and the per-validator byte caps rest on a capacity model: an
// assumption about what a validator can serve. Both can be turned off
// (sampling.enabled: false, a per-validator byte fraction of 0), which leaves
// only limits that assume nothing about the validator: the spacing between
// two requests to one validator, the request rate, and this observer's own
// global byte caps. The master secret and the reveal loop stay, so the draws
// already published remain verifiable.
package policy

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// Config is the on-disk policy (YAML). Zero values take the defaults in
// Default.
type Config struct {
	// PointsPerPublication is how many probes one publication costs a
	// validator: set by the prober from its schedule, not from the file.
	PointsPerPublication float64 `yaml:"-"`
	// DownloadsPerPublication is how many of those points can transfer
	// bytes: all of them. The post point expects NOT_FOUND, but a validator
	// that keeps the blob serves it there in full, and the projection is a
	// ceiling. Set from the same flag as PointsPerPublication so the byte
	// side and the request side of the projection cannot drift.
	DownloadsPerPublication float64 `yaml:"-"`
	Capacity                struct {
		FloorRows         int   `yaml:"floor_rows"`          // rows of the smallest validator the capacity model is stated for (148)
		FloorValidatorBps int64 `yaml:"floor_validator_bps"` // assumed serving capacity of a floor validator, bits per second (0.65 Gbps, UNVERIFIED)
		ScaleWithRows     bool  `yaml:"scale_with_rows"`     // cap(rows) = cap(floor) * rows / floor_rows
	} `yaml:"capacity_model"`
	Caps struct {
		PerValidator struct {
			RequestsPerMinute    int           `yaml:"requests_per_minute"`
			MinRequestSpacing    time.Duration `yaml:"min_request_spacing"`
			BytesPerHourFraction float64       `yaml:"bytes_per_hour_fraction"` // of the capacity model
			BytesPerDayFraction  float64       `yaml:"bytes_per_day_fraction"`
		} `yaml:"per_validator"`
		Global struct {
			BytesPerHour int64 `yaml:"bytes_per_hour"`
			BytesPerDay  int64 `yaml:"bytes_per_day"`
		} `yaml:"global"`
	} `yaml:"caps"`
	Sampling struct {
		// Enabled draws publications out of the sample when the budget
		// would be exceeded. Off, every publication is admitted and no draw
		// is made or stamped; the caps below still bound each probe.
		Enabled          bool   `yaml:"enabled"`
		MasterSecretFile string `yaml:"master_secret_file"`
		// AllowEphemeralSecret permits a process-local master secret, which
		// is only ever right in a test: the day commitments it produces
		// cannot be verified after a restart. Not settable from YAML, so a
		// deployment cannot reach it by accident.
		AllowEphemeralSecret bool          `yaml:"-"`
		ProjectionLookback   time.Duration `yaml:"projection_lookback"`
		AlwaysProbe          []string      `yaml:"always_probe"` // promise hashes exempt from sampling
	} `yaml:"sampling"`
	// State is where the budget and backoff state survives a restart
	// (budgetstate.go). File is set by sentinel-probe to
	// <data-dir>/probe-budget.json, like the master secret, and is not read
	// from YAML; empty (tests) keeps everything in memory, as before.
	State struct {
		File string `yaml:"-"`
		// UnknownCooldown is how long nothing is admitted after a start that
		// found no trustworthy state file (missing, corrupt, another
		// version): the budget already spent is unknown, so it is assumed
		// spent for this long rather than fresh. 0 turns the cool-down off.
		UnknownCooldown time.Duration `yaml:"unknown_cooldown"`
	} `yaml:"budget_state"`
}

// Default returns the built-in policy defaults.
func Default() Config {
	var c Config
	c.Capacity.FloorRows = 148
	c.Capacity.FloorValidatorBps = 650_000_000
	c.Capacity.ScaleWithRows = true
	c.Caps.PerValidator.RequestsPerMinute = 30
	c.Caps.PerValidator.MinRequestSpacing = 2 * time.Second
	c.Caps.PerValidator.BytesPerHourFraction = 0.01
	c.Caps.PerValidator.BytesPerDayFraction = 0.0075
	c.Caps.Global.BytesPerHour = 50 << 30 // 50 GiB
	c.Caps.Global.BytesPerDay = 600 << 30 // 600 GiB
	c.Sampling.Enabled = true
	c.Sampling.ProjectionLookback = time.Hour
	c.State.UnknownCooldown = 10 * time.Minute
	return c
}

// Load reads a YAML file over the defaults. An empty path returns Default().
func Load(path string) (Config, error) {
	c := Default()
	if path == "" {
		return c, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := yaml.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("parse %s: %w", path, err)
	}
	return c, c.validate()
}

func (c Config) validate() error {
	pv := c.Caps.PerValidator
	// A per-validator byte fraction of 0 means no per-validator byte cap.
	// Negative is a typo, and would deny every probe.
	if pv.BytesPerHourFraction < 0 || pv.BytesPerDayFraction < 0 {
		return errors.New("caps: bytes_per_hour_fraction and bytes_per_day_fraction must not be negative (0 = no per-validator byte cap)")
	}
	// The capacity model only sizes the per-validator byte caps; with both
	// off it is not read.
	if (pv.BytesPerHourFraction > 0 || pv.BytesPerDayFraction > 0) && (c.Capacity.FloorRows <= 0 || c.Capacity.FloorValidatorBps <= 0) {
		return errors.New("capacity_model: floor_rows and floor_validator_bps must be positive")
	}
	// A zero or negative global cap does not fail loudly at startup, it makes
	// every probe fail a budget check (or divide by zero in the sampler), so
	// the observer would quietly record nothing at all.
	if c.Caps.Global.BytesPerHour <= 0 || c.Caps.Global.BytesPerDay <= 0 {
		return errors.New("caps: global.bytes_per_hour and global.bytes_per_day must be positive")
	}
	if c.Caps.PerValidator.RequestsPerMinute < 0 {
		return errors.New("caps: requests_per_minute must not be negative (0 = no limit)")
	}
	if c.Caps.PerValidator.MinRequestSpacing < 0 {
		return errors.New("caps: min_request_spacing must not be negative")
	}
	if c.Sampling.ProjectionLookback <= 0 {
		return errors.New("sampling: projection_lookback must be positive")
	}
	if c.State.UnknownCooldown < 0 {
		return errors.New("budget_state: unknown_cooldown must not be negative")
	}
	return nil
}

// bytesPerHourCap is the per-validator hourly byte cap for a validator with
// rows assigned rows; 0 when the cap is off.
func (c Config) bytesPerHourCap(rows int) int64 {
	if c.Caps.PerValidator.BytesPerHourFraction <= 0 {
		return 0
	}
	capBytes := float64(c.Capacity.FloorValidatorBps) / 8 * 3600 * c.Caps.PerValidator.BytesPerHourFraction
	if c.Capacity.ScaleWithRows && rows > c.Capacity.FloorRows {
		capBytes *= float64(rows) / float64(c.Capacity.FloorRows)
	}
	return int64(capBytes)
}

func (c Config) bytesPerDayCap(rows int) int64 {
	if c.Caps.PerValidator.BytesPerDayFraction <= 0 {
		return 0
	}
	capBytes := float64(c.Capacity.FloorValidatorBps) / 8 * 86400 * c.Caps.PerValidator.BytesPerDayFraction
	if c.Capacity.ScaleWithRows && rows > c.Capacity.FloorRows {
		capBytes *= float64(rows) / float64(c.Capacity.FloorRows)
	}
	return int64(capBytes)
}

// ShardBytes is probe.ShardBytes; kept here because the policy is where the
// byte budget is reasoned about.
func ShardBytes(blobSize uint32, originalRows, rows int) int64 {
	return probe.ShardBytes(blobSize, originalRows, rows)
}

// defaultDownloadsPerBlob is how many schedule points can transfer bytes
// under the default schedule: four in-window points, the grace point and the
// post point, which expects NOT_FOUND but gets the whole shard from a
// validator that kept it. It is only the fallback: the real count comes
// from the schedule through Config.DownloadsPerPublication, because the
// request side of the projection already follows the schedule and a constant
// on the byte side would drift from it the first time -in-window-probes is
// changed — projecting too few bytes, holding p at 1, and letting the hard
// caps bite part-way through schedules instead.
const defaultDownloadsPerBlob = 6.0

// event is one accounted probe.
type event struct {
	at    time.Time
	bytes int64
}

type validatorState struct {
	events       []event // trailing 24h
	lastRequest  time.Time
	rowsLastSeen int
}

// Policy is a probe.Policy backed by Config.
type Policy struct {
	cfg    Config
	master []byte

	mu         sync.Mutex
	validators map[string]*validatorState
	global     []event
	// pending is every probe BeforeProbe admitted that AfterProbe or Release
	// has not settled yet (see BeforeProbe).
	pending []reservation
	// publications seen in the projection lookback, keyed by promise hash,
	// with the bytes a full schedule over all assigned validators would cost.
	recentPubs map[string]pubLoad
	// decisions is the sticky admit/deny per promise hash. It must survive
	// for as long as the publication can still be asked about, because the
	// admission probability moves with load: re-deciding a publication mid
	// schedule would probe some of its points and not others, and the whole
	// point of the sticky map is "the whole schedule or none of it".
	decisions map[string]decision
	lastP     float64
	lastCap   string
	lastProj  Projection
	logf      func(string, ...any)
	// ephemeral: the master secret is process-local (tests only). Published
	// beside the commitment so a reader is never told to verify something
	// that cannot be verified.
	ephemeral bool

	// Persisted budget state (budgetstate.go). stateFile is empty when the
	// state lives in memory only. cooldownUntil closes admission after a
	// start that found no trustworthy state; restoredAt is the loaded
	// file's save time, from which every validator's spacing counts;
	// saveFailing closes admission while the state cannot be written;
	// dirty is whether there is anything to write.
	stateFile     string
	cooldownUntil time.Time
	restoredAt    time.Time
	saveFailing   bool
	dirty         bool
	saveTimer     *time.Timer
	saveMu        sync.Mutex // one save at a time, taken before mu
	startupLog    []string   // what restore said before SetLogger
}

// EphemeralSecret reports whether the master secret is process-local, and so
// whether the day commitments this policy stamps will survive a restart.
func (p *Policy) EphemeralSecret() bool { return p.ephemeral }

type pubLoad struct {
	settled  time.Time
	global   int64
	perVal   map[string]int64 // validator hex -> bytes for the whole schedule
	perValRs map[string]int
}

// New builds a Policy. The master secret is read from cfg.Sampling.
// MasterSecretFile, or generated (and written, 0600) if the file is absent
// and the path is set.
//
// With no path the secret is process-local, which silently breaks the only
// thing the sampling machinery exists for. Every row carries a commitment to
// that day's secret, /v1/sampling publishes it, and the audit the methodology
// page describes is: reveal the secret after the day closes, recompute each
// publication's draw, check it against the rows. A secret that does not
// survive a restart makes the commitments already published unverifiable —
// not wrong in a way anyone can see, just impossible to check, which for an
// observer whose claim is "recompute this yourself" is the worse failure.
// So it is refused unless the caller says in as many words that this is a
// test.
func New(cfg Config) (*Policy, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Sampling.MasterSecretFile == "" && !cfg.Sampling.AllowEphemeralSecret {
		return nil, errors.New("sampling: master_secret_file is not set, so the master secret would be new on every restart " +
			"and every day commitment already published would become unverifiable; set it (deploy/README.md) " +
			"or set allow_ephemeral_secret for a test")
	}
	master, err := loadOrCreateSecret(cfg.Sampling.MasterSecretFile)
	if err != nil {
		return nil, err
	}
	p := &Policy{
		cfg:        cfg,
		ephemeral:  cfg.Sampling.MasterSecretFile == "",
		master:     master,
		validators: map[string]*validatorState{},
		recentPubs: map[string]pubLoad{},
		decisions:  map[string]decision{},
		lastP:      1,
		stateFile:  cfg.State.File,
	}
	if p.stateFile != "" {
		p.restore(time.Now())
	}
	return p, nil
}

func loadOrCreateSecret(path string) ([]byte, error) {
	if path == "" {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return nil, err
		}
		return b, nil
	}
	b, err := os.ReadFile(path)
	if err == nil {
		if len(b) < 16 {
			return nil, fmt.Errorf("%s: master secret shorter than 16 bytes", path)
		}
		return b, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	b = make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	// The default path sits under /var/lib/fibre-observer, which a first run on
	// a fresh host does not have yet; failing here took the prober down with
	// "no such file or directory" on the secret it was about to create.
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create master secret directory: %w", err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return nil, fmt.Errorf("write master secret: %w", err)
	}
	return b, nil
}

// DaySecret derives the per-day secret for the UTC day containing t.
func (p *Policy) DaySecret(t time.Time) []byte {
	mac := hmac.New(sha256.New, p.master)
	mac.Write([]byte(t.UTC().Format("2006-01-02")))
	return mac.Sum(nil)
}

// DayCommitment is SHA256(day secret), safe to publish before the day so the
// sample can be audited after the secret is revealed.
func (p *Policy) DayCommitment(t time.Time) string {
	s := sha256.Sum256(p.DaySecret(t))
	return hex.EncodeToString(s[:])
}

// Sampled reports whether promiseHash is in the sample at probability prob,
// using the day secret of the publication's settlement day. Deterministic
// and, without the secret, unpredictable.
func (p *Policy) Sampled(promiseHash string, settled time.Time, prob float64) bool {
	if prob >= 1 {
		return true
	}
	if prob <= 0 {
		return false
	}
	h := sha256.New()
	hb, err := hex.DecodeString(promiseHash)
	if err != nil {
		hb = []byte(promiseHash)
	}
	h.Write(hb)
	h.Write(p.DaySecret(settled))
	v := binary.BigEndian.Uint64(h.Sum(nil)[:8])
	return float64(v) < prob*math.Exp2(64)
}

// Observe registers a publication's projected load. The prober calls it for
// every publication it plans, every cycle; only publications settled within
// the lookback count.
func (p *Policy) observe(pub scan.Publication, now time.Time) {
	if _, ok := p.recentPubs[pub.PromiseHash]; ok {
		return
	}
	// In the lookback if its settlement is, or if any of its schedule is
	// still to come. Testing the settlement time alone dropped every
	// publication the prober had backlogged — after a restart, or while the
	// scanner catches up — although their probes were still going to be run
	// and their bytes still going to be spent. The projection then said the
	// vantage was idle, p stayed at 1, everything was admitted, and the hard
	// per-validator caps denied probes part-way through the schedules. The
	// schedule is packed toward the deadline, so the points lost that way
	// are the late in-window and grace ones: the readings that catch an
	// early prune.
	if now.Sub(pub.SettlementTime) > p.cfg.Sampling.ProjectionLookback && !pub.MustServeUntil.After(now) {
		return
	}
	pl := pubLoad{settled: pub.SettlementTime, perVal: map[string]int64{}, perValRs: map[string]int{}}
	orig := pub.Assignment.ProtocolParams.OriginalRows
	for _, v := range pub.Assignment.Validators {
		b := int64(float64(ShardBytes(pub.Promise.BlobSize, orig, v.RowCount)) * p.cfg.downloadsPerPublication())
		pl.perVal[v.Address] = b
		pl.perValRs[v.Address] = v.RowCount
		pl.global += b
	}
	p.recentPubs[pub.PromiseHash] = pl
}

// defaultPointsPerPublication is how many probes one admitted publication
// costs a single validator when the caller does not say: the four default
// in-window points, the grace point and the post point. The prober passes
// its real schedule length through Config.PointsPerPublication so the two
// cannot drift apart.
const defaultPointsPerPublication = 6.0

// pointsPerPublication is the schedule length the sampler budgets for.
func (c Config) pointsPerPublication() float64 {
	if c.PointsPerPublication > 0 {
		return c.PointsPerPublication
	}
	return defaultPointsPerPublication
}

// downloadsPerPublication is how many of those points transfer bytes.
func (c Config) downloadsPerPublication() float64 {
	if c.DownloadsPerPublication > 0 {
		return c.DownloadsPerPublication
	}
	return defaultDownloadsPerBlob
}

// projectedP computes the admission probability from the trailing lookback:
// p = min(1, cap/projected) over EVERY cap BeforeProbe enforces, not just the
// hourly ones.
//
// The sampler's promise is "the whole schedule or none of it": a publication
// is admitted once and every one of its points is probed. That only holds if
// the probability it is admitted at is one the rest of the day's budget can
// sustain. When p was computed from the hourly caps alone, the daily caps
// then denied probes mid schedule — and because the schedule is packed toward
// the deadline, the points that were dropped were the late in-window and
// grace ones, which are exactly where a retention breach shows.
// Projection is the load estimate an admission probability was drawn
// from: every input the cap comparison used, so a reader of the log or the
// status file can check the arithmetic behind p rather than take it. Bytes
// are per hour or per day as the cap they are held against.
type Projection struct {
	At           time.Time     `json:"at"`
	P            float64       `json:"p"`
	Binding      string        `json:"binding"`
	Lookback     time.Duration `json:"-"`
	LookbackS    int64         `json:"lookback_s"`
	Publications int           `json:"publications_in_lookback"`

	GlobalBytesPerHour int64 `json:"global_bytes_per_hour"`
	GlobalCapPerHour   int64 `json:"global_cap_bytes_per_hour"`
	GlobalBytesPerDay  int64 `json:"global_bytes_per_day"`
	GlobalCapPerDay    int64 `json:"global_cap_bytes_per_day"`

	// The validator whose projected hourly load is the largest share of
	// its own cap: the one that binds first as load grows.
	HeaviestValidator    string  `json:"heaviest_validator,omitempty"`
	HeaviestRows         int     `json:"heaviest_rows,omitempty"`
	HeaviestBytesHour    int64   `json:"heaviest_bytes_per_hour,omitempty"`
	HeaviestCapHour      int64   `json:"heaviest_cap_bytes_per_hour,omitempty"`
	HeaviestBytesDay     int64   `json:"heaviest_bytes_per_day,omitempty"`
	HeaviestCapDay       int64   `json:"heaviest_cap_bytes_per_day,omitempty"`
	RequestsPerMinute    float64 `json:"requests_per_minute"`
	RequestsCapMinute    int     `json:"requests_cap_per_minute"`
	PointsPerPublication float64 `json:"points_per_publication"`
}

func (p *Policy) projectedP(now time.Time) (float64, string) {
	pj := p.project(now)
	p.lastProj = pj
	return pj.P, pj.Binding
}

// project computes the Projection for now. It drops publications that
// left the lookback.
func (p *Policy) project(now time.Time) Projection {
	lookback := p.cfg.Sampling.ProjectionLookback
	var global int64
	perVal := map[string]int64{}
	perValRows := map[string]int{}
	for h, pl := range p.recentPubs {
		if now.Sub(pl.settled) > lookback {
			delete(p.recentPubs, h)
			continue
		}
		global += pl.global
		for a, b := range pl.perVal {
			perVal[a] += b
			perValRows[a] = pl.perValRs[a]
		}
	}
	// scale the lookback window to one hour and to one day.
	hourly := float64(time.Hour) / float64(lookback)
	daily := float64(24*time.Hour) / float64(lookback)

	pj := Projection{At: now.UTC(), P: 1, Binding: "none", Lookback: lookback, LookbackS: int64(lookback / time.Second),
		Publications: len(p.recentPubs), PointsPerPublication: p.cfg.pointsPerPublication(),
		GlobalBytesPerHour: int64(float64(global) * hourly), GlobalCapPerHour: p.cfg.Caps.Global.BytesPerHour,
		GlobalBytesPerDay: int64(float64(global) * daily), GlobalCapPerDay: p.cfg.Caps.Global.BytesPerDay}
	tighten := func(capBytes int64, projected float64, name string) {
		if capBytes <= 0 || projected <= float64(capBytes) {
			return
		}
		if q := float64(capBytes) / projected; q < pj.P {
			pj.P, pj.Binding = q, name
		}
	}

	tighten(p.cfg.Caps.Global.BytesPerHour, float64(global)*hourly, "global_bytes_per_hour")
	tighten(p.cfg.Caps.Global.BytesPerDay, float64(global)*daily, "global_bytes_per_day")

	// deterministic iteration for stable logs.
	addrs := make([]string, 0, len(perVal))
	for a := range perVal {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	var heaviest float64
	for _, a := range addrs {
		rows := perValRows[a]
		capH, capD := p.cfg.bytesPerHourCap(rows), p.cfg.bytesPerDayCap(rows)
		tighten(capH, float64(perVal[a])*hourly, "validator_bytes_per_hour")
		tighten(capD, float64(perVal[a])*daily, "validator_bytes_per_day")
		if capH > 0 {
			if share := float64(perVal[a]) * hourly / float64(capH); share > heaviest {
				heaviest = share
				pj.HeaviestValidator, pj.HeaviestRows = a, rows
				pj.HeaviestBytesHour, pj.HeaviestCapHour = int64(float64(perVal[a])*hourly), capH
				pj.HeaviestBytesDay, pj.HeaviestCapDay = int64(float64(perVal[a])*daily), capD
			}
		}
	}

	// The request cap is counted in probes rather than bytes. Each admitted
	// publication costs one validator pointsPerPublication requests over the
	// whole retention window, so the rate that matters is how many
	// publications land per minute, not how many bytes they carry.
	if rpm := p.cfg.Caps.PerValidator.RequestsPerMinute; rpm > 0 && len(addrs) > 0 {
		perMinute := float64(len(p.recentPubs)) * p.cfg.pointsPerPublication() * (float64(time.Minute) / float64(lookback))
		pj.RequestsPerMinute, pj.RequestsCapMinute = perMinute, rpm
		tighten(int64(rpm), perMinute, "validator_requests_per_minute")
	}
	return pj
}

// Projection returns the load estimate behind the last admission decision.
func (p *Policy) Projection() Projection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastProj
}

// ProjectionDetail is Projection as a status-file entry (see probe.Prober,
// which asks for it through an interface so as not to import this package).
func (p *Policy) ProjectionDetail() map[string]any {
	pj := p.Projection()
	if pj.At.IsZero() {
		return nil
	}
	b, err := json.Marshal(pj)
	if err != nil {
		return nil
	}
	var m map[string]any
	if json.Unmarshal(b, &m) != nil {
		return nil
	}
	return m
}

// SetLogger installs where admission decisions are explained. Every change
// of the admission probability or of the cap that binds it is logged with
// the projection's inputs, so the log alone says why a publication was
// sampled out.
func (p *Policy) SetLogger(logf func(string, ...any)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.logf = logf
	for _, msg := range p.startupLog {
		logf("%s", msg)
	}
	p.startupLog = nil
}

func (p *Policy) logProjection(prev Projection, cur Projection) {
	if p.logf == nil || (prev.P == cur.P && prev.Binding == cur.Binding) {
		return
	}
	msg := fmt.Sprintf("sampling: p=%.3f binding=%s over %d publications in the last %s: global %s/h of cap %s, %s/d of cap %s",
		cur.P, cur.Binding, cur.Publications, cur.Lookback,
		humanBytes(cur.GlobalBytesPerHour), humanBytes(cur.GlobalCapPerHour), humanBytes(cur.GlobalBytesPerDay), humanBytes(cur.GlobalCapPerDay))
	if cur.HeaviestValidator != "" {
		msg += fmt.Sprintf("; heaviest validator %s (%d rows) %s/h of cap %s", cur.HeaviestValidator, cur.HeaviestRows,
			humanBytes(cur.HeaviestBytesHour), humanBytes(cur.HeaviestCapHour))
	}
	if cur.RequestsCapMinute > 0 {
		msg += fmt.Sprintf("; %.2f requests/min per validator of cap %d (%.0f points per publication)", cur.RequestsPerMinute, cur.RequestsCapMinute, cur.PointsPerPublication)
	}
	p.logf("%s", msg)
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.0f KiB", float64(b)/(1<<10))
	}
	return fmt.Sprintf("%d B", b)
}

// Admit implements probe.Policy.
func (p *Policy) Admit(pub scan.Publication, alreadyStarted bool) (bool, string) {
	if !p.cfg.Sampling.Enabled {
		return true, ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if alreadyStarted {
		// Not a draw: the schedule was already running when the policy was
		// asked, so it is carried to its end whatever the load is now. Its
		// remaining points are still going to be spent, though, so they stay
		// in the projection — otherwise a restart mid-schedule makes the
		// vantage look idle and admits a second wave on top of the first.
		p.observe(pub, time.Now())
		p.remember(pub, true, 1, "already_started")
		return true, ""
	}
	if d, ok := p.decisions[pub.PromiseHash]; ok {
		if d.in {
			return true, ""
		}
		return false, p.reasonFor(pub, d)
	}
	for _, h := range p.cfg.Sampling.AlwaysProbe {
		if h == pub.PromiseHash {
			p.remember(pub, true, 1, "always_probe")
			return true, ""
		}
	}
	now := time.Now()
	// The draw is made against the load already in the lookback, not against
	// a projection this publication has just been added to. Observing first
	// put the publication's own bytes in the denominator of its own
	// probability, so two publications arriving at the same instant against
	// the same prior load did not get the same p — the larger one got a
	// strictly smaller one. That is a bias running against exactly the
	// publications whose retention is most worth checking, and it is
	// invisible in the sampling audit, which checks the draw against the p
	// on the row and never asks where the p came from. The projection now
	// lags by one publication, which is the unbiased form.
	prev := p.lastProj
	prob, binding := p.projectedP(now)
	p.lastP, p.lastCap = prob, binding
	p.logProjection(prev, p.lastProj)
	in := p.Sampled(pub.PromiseHash, pub.SettlementTime, prob)
	p.observe(pub, now)
	p.remember(pub, in, prob, binding)
	if in {
		return true, ""
	}
	return false, p.reasonFor(pub, decision{in: in, p: prob, binding: binding})
}

// decision is one sticky admit/deny plus the settlement time it belongs to,
// which is what orders eviction.
type decision struct {
	in bool
	at time.Time
	// p and binding are the draw's own inputs: the probability this
	// publication was admitted at and the cap that bound it. They are kept
	// per publication because the probability moves with load, and the rows
	// are stamped hours after the decision. Reading the process-wide last
	// values instead gave every row whatever some other publication's draw
	// had most recently computed — a publication admitted at p=1 carrying
	// rows that say p=0.24 — and that is the number the commit-and-reveal
	// audit recomputes. With it wrong, a verifier following the published
	// recipe derives a sample that does not match the record, which reads
	// as the observer having probed something other than what it drew.
	p       float64
	binding string
}

// maxDecisions bounds the sticky admit/deny map. A decision only matters
// while a publication still has schedule points; keeping every hash for the
// life of the process is a slow leak on a long-running vantage.
const maxDecisions = 20000

// keepDecisions is how many survive an eviction. Evicting down to a margin
// rather than to the bound keeps eviction from running on every insert.
const keepDecisions = maxDecisions * 3 / 4

// remember stores a decision, evicting the oldest publications first when the
// map is full.
//
// The map is not a cache that may be dropped. A publication's admission
// probability is computed from the load at the moment it is first seen, and
// that probability moves with load, so re-deciding a publication that is
// still in flight can admit points 3 and 4 of a schedule whose points 1 and 2
// were denied. The earlier code emptied the whole map when it filled, which
// did exactly that to every publication still running. Evicting by settlement
// time instead drops the ones whose windows closed longest ago, which are the
// ones that can no longer be asked about.
func (p *Policy) remember(pub scan.Publication, in bool, prob float64, binding string) {
	if len(p.decisions) >= maxDecisions {
		p.evictOldestDecisions()
	}
	p.decisions[pub.PromiseHash] = decision{in: in, at: pub.SettlementTime, p: prob, binding: binding}
}

func (p *Policy) evictOldestDecisions() {
	type ent struct {
		hash string
		at   time.Time
	}
	all := make([]ent, 0, len(p.decisions))
	for h, d := range p.decisions {
		all = append(all, ent{h, d.at})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].at.Equal(all[j].at) {
			return all[i].hash < all[j].hash
		}
		return all[i].at.Before(all[j].at)
	})
	for i := 0; i < len(all)-keepDecisions; i++ {
		delete(p.decisions, all[i].hash)
	}
}

// Forget drops the decision for a publication whose schedule is finished, so
// the bound above is reached only when a vantage really is tracking that many
// live publications.
func (p *Policy) Forget(promiseHash string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.decisions, promiseHash)
}

// reasonFor explains one publication's denial with that publication's own
// draw, not with the last one the process happened to make.
func (p *Policy) reasonFor(pub scan.Publication, d decision) string {
	return fmt.Sprintf("budget:p=%.3f:%s:day_commitment=%s", d.p, d.binding, p.DayCommitment(pub.SettlementTime))
}

// SamplingFor returns what this publication's admission decision was made
// with: the probability it was sampled at, the cap that bound that
// probability, and the commitment to the day secret the draw used.
//
// The prober stamps these on every row it writes for an admitted
// publication, and on the one decision it records for a denied one, which
// is what makes the sample auditable at all. Recording them only on denials
// left the admitted side with no record: the commit-and-reveal audit the
// methodology page describes could not be carried out against half the
// decisions it was supposed to cover.
func (p *Policy) SamplingFor(pub scan.Publication) (prob float64, binding, commitment string) {
	if !p.cfg.Sampling.Enabled {
		// No draw was made, so there is nothing to stamp or to audit.
		return 1, "sampling_off", ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if d, ok := p.decisions[pub.PromiseHash]; ok {
		return d.p, d.binding, p.DayCommitment(pub.SettlementTime)
	}
	// No decision on record: this publication was never put to the policy
	// (a probe that predates it, or a decision evicted after its window
	// closed). The process-wide last values are all there is, and they are
	// marked as such so a reader does not take them for this publication's
	// own draw.
	return p.lastP, p.lastCap + ":unrecorded", p.DayCommitment(pub.SettlementTime)
}

// State returns the last computed admission probability and binding cap,
// for logs and the API.
func (p *Policy) State() (prob float64, binding string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastP, p.lastCap
}

func (p *Policy) state(addr string) *validatorState {
	vs, ok := p.validators[addr]
	if !ok {
		vs = &validatorState{}
		p.validators[addr] = vs
	}
	return vs
}

func sumSince(events []event, since time.Time) (n int, bytes int64) {
	for _, e := range events {
		if !e.at.Before(since) {
			n++
			bytes += e.bytes
		}
	}
	return n, bytes
}

func trim(events []event, since time.Time) []event {
	i := 0
	for i < len(events) && events[i].at.Before(since) {
		i++
	}
	return events[i:]
}

// BeforeProbe implements probe.Policy.
//
// An allow is a reservation, not a reading. The checks and the booking of
// the slot happen in one critical section: the request is entered as
// pending (at now, for the shard it may download), every cap check after it
// counts it, and lastRequest moves to now, so the next admission of this
// validator waits out the spacing from this one. AfterProbe turns the
// pending entry into the accounted event with the bytes actually asked for;
// Release drops it when the admitted probe is not run after all.
//
// It used to only read. lastRequest and the byte events were written by
// AfterProbe alone, once the probe had finished, so every admission taken
// while a probe of the same validator was in flight decided on state that
// did not include it yet. The first burst after a start puts one validator
// in several work items at once; with eight workers they all passed here in
// the same instant (no spacing to wait out, caps nowhere near) and then ran
// back to back: up to seven shards over the per-validator byte and request
// caps and no spacing at all, on exactly the endpoints the policy promises
// to be gentlest with.
func (p *Policy) BeforeProbe(pub scan.Publication, t probe.Target, now time.Time) (allow bool, reason string) {
	// The spacing wait happens outside the lock. It used to run inside the
	// critical section, which meant one validator's two-second wait blocked
	// admission for every other validator in the pool: with eight workers
	// and a flat lateness bound that turned into probes recorded as gaps for
	// whoever happened to be scheduled behind it. The wait is read again
	// under the lock after every sleep, because a concurrent admission of
	// the same validator can have booked the slot this one woke up for; only
	// when that read finds no wait does the lock stay held, through the
	// checks and the reservation, so no second admission can slip in between.
	//
	// A start with no trustworthy budget state, or a state that cannot be
	// saved, denies before anything else, and before any spacing is waited
	// out for a probe that will not run.
	p.mu.Lock()
	if denied := p.stateGuard(now); denied != "" {
		p.mu.Unlock()
		return false, denied
	}
	p.mu.Unlock()
	for {
		p.mu.Lock()
		wait := p.spacingWait(t.AddressHex, now)
		if wait <= 0 {
			break
		}
		p.mu.Unlock()
		time.Sleep(wait)
		now = time.Now()
	}
	defer p.mu.Unlock()
	vs := p.state(t.AddressHex)
	vs.events = trim(vs.events, now.Add(-24*time.Hour))
	p.global = trim(p.global, now.Add(-24*time.Hour))
	p.pending = trimPending(p.pending, now.Add(-24*time.Hour))
	pv := p.cfg.Caps.PerValidator

	// Every sum below is the accounted events plus the reservations still in
	// flight: an admitted probe costs the budget from the moment it is
	// admitted, not from the moment it happens to finish.
	if reqs, _ := p.sumWithPending(vs.events, t.AddressHex, now.Add(-time.Minute)); pv.RequestsPerMinute > 0 && reqs >= pv.RequestsPerMinute {
		return false, fmt.Sprintf("budget:validator_requests_per_minute=%d", pv.RequestsPerMinute)
	}
	rows := t.RowCount
	if rows == 0 {
		rows = vs.rowsLastSeen
	}
	est := ShardBytes(pub.Promise.BlobSize, pub.Assignment.ProtocolParams.OriginalRows, rows)
	if capH := p.cfg.bytesPerHourCap(rows); capH > 0 {
		if _, hb := p.sumWithPending(vs.events, t.AddressHex, now.Add(-time.Hour)); hb+est > capH {
			return false, "budget:validator_bytes_per_hour"
		}
	}
	if capD := p.cfg.bytesPerDayCap(rows); capD > 0 {
		if _, db := p.sumWithPending(vs.events, t.AddressHex, now.Add(-24*time.Hour)); db+est > capD {
			return false, "budget:validator_bytes_per_day"
		}
	}
	if _, gh := p.sumWithPending(p.global, "", now.Add(-time.Hour)); gh+est > p.cfg.Caps.Global.BytesPerHour {
		return false, "budget:global_bytes_per_hour"
	}
	if _, gd := p.sumWithPending(p.global, "", now.Add(-24*time.Hour)); p.cfg.Caps.Global.BytesPerDay > 0 && gd+est > p.cfg.Caps.Global.BytesPerDay {
		return false, "budget:global_bytes_per_day"
	}
	p.pending = append(p.pending, reservation{addr: t.AddressHex, at: now, bytes: est})
	vs.lastRequest = now
	p.markDirty()
	return true, ""
}

// reservation is one admitted probe not yet accounted: booked by
// BeforeProbe, turned into an event by AfterProbe or dropped by Release.
type reservation struct {
	addr  string
	at    time.Time
	bytes int64
}

// sumWithPending is sumSince over events plus the reservations of addr (of
// every validator when addr is empty) made since since. The caller holds the
// lock.
func (p *Policy) sumWithPending(events []event, addr string, since time.Time) (n int, bytes int64) {
	n, bytes = sumSince(events, since)
	for _, r := range p.pending {
		if (addr == "" || r.addr == addr) && !r.at.Before(since) {
			n++
			bytes += r.bytes
		}
	}
	return n, bytes
}

// trimPending drops reservations older than since. A reservation is settled
// by AfterProbe or Release within one probe's time; one still here a day
// later was leaked by a caller, and until it goes it has only ever made the
// policy stricter (a request charged that never went out), never looser.
func trimPending(rs []reservation, since time.Time) []reservation {
	out := rs[:0]
	for _, r := range rs {
		if !r.at.Before(since) {
			out = append(out, r)
		}
	}
	return out
}

// takePending removes addr's oldest reservation. The prober runs one probe
// of a validator at a time, so the oldest is the one being settled; for a
// caller that does not, any of them stands for the same request against the
// same caps, and the bytes are reconciled to the actual figure either way.
// The caller holds the lock.
func (p *Policy) takePending(addr string) {
	for i, r := range p.pending {
		if r.addr == addr {
			p.pending = append(p.pending[:i], p.pending[i+1:]...)
			return
		}
	}
}

// Release implements probe.Policy: the probe BeforeProbe admitted for t is
// not going to run (it went stale waiting for the validator, a queued retry
// was abandoned, the sweep is stopping), so its reservation is given back.
// lastRequest stays where the admission put it: at worst the next probe of
// the validator waits out a spacing for a request that never went out,
// which errs on the side of less load.
func (p *Policy) Release(_ scan.Publication, t probe.Target) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.takePending(t.AddressHex)
	p.markDirty()
}

// spacingWait reports how long to hold off before touching this validator
// again. The caller holds the lock, and sleeps (if it must) without it.
func (p *Policy) spacingWait(addr string, now time.Time) time.Duration {
	vs := p.state(addr)
	spacing := p.cfg.Caps.PerValidator.MinRequestSpacing
	// After a restore, spacing counts from the save at the latest: a request
	// made after the last save and before the restart is not on record.
	last := vs.lastRequest
	if p.restoredAt.After(last) {
		last = p.restoredAt
	}
	if last.IsZero() || spacing <= 0 {
		return 0
	}
	if d := spacing - now.Sub(last); d > 0 {
		return d
	}
	return 0
}

// AfterProbe implements probe.Policy. It settles the reservation BeforeProbe
// made for this probe, replacing the estimate with what was actually asked
// for. A probe the policy was never asked about (the prober has none; a test
// may) is accounted all the same.
func (p *Policy) AfterProbe(pub scan.Publication, m probe.Measurement) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.takePending(m.ValidatorAddress)
	vs := p.state(m.ValidatorAddress)
	// The reservation already moved lastRequest to the admission time; the
	// request itself started at or after it, and spacing counts from there.
	if m.StartedAt.After(vs.lastRequest) {
		vs.lastRequest = m.StartedAt
	}
	if m.AssignedRowCount > 0 {
		vs.rowsLastSeen = m.AssignedRowCount
	}
	// Charge the budget for what the probe asked for, not for what came
	// back. Charging only successful downloads made the stopping rule depend
	// on the thing being measured: a validator that served ran out of budget
	// part way through the day while one that answered NOT_FOUND was probed
	// for the full day, so the two validators' "24h" rates covered different
	// spans and were published side by side as if they did not.
	var bytes int64
	if m.Download.Attempted {
		rows := m.Download.RowsExpected
		if rows <= 0 {
			rows = m.AssignedRowCount
		}
		if rows > 0 {
			bytes = ShardBytes(pub.Promise.BlobSize, pub.Assignment.ProtocolParams.OriginalRows, rows)
		}
	}
	ev := event{at: m.StartedAt, bytes: bytes}
	vs.events = append(vs.events, ev)
	p.global = append(p.global, ev)
	p.markDirty()

}

var _ probe.Policy = (*Policy)(nil)
