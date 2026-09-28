// Package api is the observer's read-only JSON API, versioned under /v1/.
//
// Every aggregate carries the probe count it was computed from, the time
// window it covers, and the vantage. Rates follow docs/verdicts.md: serve
// rate is HEALTHY / (HEALTHY + FAULT) over assigned, attested probes in the
// in-window phase only; grace is recorded and shown but never rated;
// NOT_PROBED and PROBE_ERROR are reported as gaps, never folded into a rate.
package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	assign "github.com/plsgiveup/fibre/fibre-assign"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/hosting"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/keybase"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// Version is reported in /v1/meta.
const Version = "0.1.0"

// VantageInfo describes where this observer watches from.
//
// Every reachability observation on the site is a statement about a network
// path, and half that path is ours, so a reader cannot judge an UNREACHABLE
// without knowing roughly where it was measured from: the place and the
// hosting company. Both are the operator's word, and the response says so.
//
// The observer's own addresses and autonomous system are not published. They
// used to be, as an anchor an operator could match against their logs, but a
// public API is the wrong place for them: the site's diagnosis box names the
// address where an operator is told to allow it.
type VantageInfo struct {
	// Name is the short label every response already carries.
	Name string `json:"name"`
	// Location is where the machine physically sits, e.g. "Helsinki,
	// Finland". Operator's word; an address cannot prove it.
	Location string `json:"location,omitempty"`
	// Provider is the hosting company, e.g. "Hetzner".
	Provider string `json:"provider,omitempty"`
	// Verifiability says, per field, what a reader can check and how, so the
	// page rendering these cannot present a guess as a fact.
	Verifiability map[string]string `json:"verifiability"`
	// Complete is false while the operator has not filled this in, which is
	// what the dashboard checks before claiming the vantage is described.
	Complete bool `json:"complete"`
}

// Server serves the API over a store.
type Server struct {
	st      *store.Store
	vantage string
	info    VantageInfo
	mux     *http.ServeMux
	log     *scan.Logger // may be nil (tests)
	// The two window aggregates the dashboard waits for, held as snapshots per
	// window. See snapshot.go: both are aggregates over the whole window and
	// are computed on a schedule rather than per request, so a reader never
	// waits for one and never sees one without its age.
	net  *snapshotCache[*networkResponse]
	vals *snapshotCache[validatorSnapshot]
	// The publisher-side summary, same treatment: see market.go.
	market *snapshotCache[*marketResponse]
	// labels is the operator-maintained publisher name registry.
	labels map[string]PublisherLabel
	// dataDir holds the status files the processes write (internal/status);
	// empty means liveness is not reported.
	dataDir string

	// Per-publication verdicts, keyed by what the publication's probes look
	// like right now. See blobcache.go.
	blobs *blobCache
	// asOf rations pinned-window requests (see asOfLimiter).
	asOf asOfLimiter
	// details caches the validator page (see validator_detail.go).
	details detailCache
	// signing caches /v1/signing (see signing.go).
	signing signingCache
	// origRows remembers each publication's original_rows for the load
	// figures (see origrows.go).
	origRows originalRowsMemo
	// recent keeps each validator's newest endorsements (see signing.go).
	recent endorsementLedger
	// bg counts the server's own background work (the blob-page warm-up,
	// the snapshot keeper), for Close.
	bg sync.WaitGroup
	// tip holds the block ticker's answer for a second.
	tip tipCache
	// stop ends the snapshot keeper; Close closes it once.
	stop     chan struct{}
	stopOnce sync.Once
}

// Option configures a Server before it warms its caches.
type Option func(*Server)

// WithPublisherLabels installs the publisher name registry (see market.go).
func WithPublisherLabels(m map[string]PublisherLabel) Option {
	return func(s *Server) {
		if m != nil {
			s.labels = m
		}
	}
}

// WithDataDir tells the server where the processes' status files live.
func WithDataDir(dir string) Option { return func(s *Server) { s.dataDir = dir } }

// New builds a Server. vantage is the label rendered on every response.
func New(st *store.Store, vantage string) *Server { return NewWithLogger(st, vantage, nil) }

// NewWithLogger is New with somewhere to put the detail of an internal error
// that the response deliberately withholds.
func NewWithLogger(st *store.Store, vantage string, log *scan.Logger) *Server {
	return NewWithVantage(st, VantageInfo{Name: vantage}, log)
}

// NewWithVantage is NewWithLogger with the vantage described rather than only
// named.
func NewWithVantage(st *store.Store, info VantageInfo, log *scan.Logger, opts ...Option) *Server {
	info.Verifiability = map[string]string{
		"provider": "the operator's word: the hosting company",
		"location": "the operator's word: geolocating an address is a guess, so nothing here proves it",
	}
	info.Complete = info.Location != "" && info.Provider != ""
	s := &Server{st: st, vantage: info.Name, info: info, mux: http.NewServeMux(), log: log, blobs: newBlobCache(), labels: map[string]PublisherLabel{}}
	for _, o := range opts {
		o(s)
	}
	// The cached summary is the unfiltered one. A `?exclude=` answer is
	// computed per request and never stored here: writing it into the shared
	// snapshot would publish one reader's filter as everyone's headline.
	s.net = newSnapshotCache("network", func(ctx context.Context, win Window) (*networkResponse, error) {
		resp, err := s.computeNetwork(ctx, win, excludeSet{}, nil)
		if err == nil {
			resp.RecordThrough = s.recordThrough(ctx)
		}
		return resp, err
	})
	// The publisher-side summary and the publisher list are one snapshot, so
	// the publisher page's board and its table describe the same moment.
	s.market = newSnapshotCache("market", s.computePublishing)
	s.market.accept = func(r *marketResponse) bool { return r != nil && r.Publishers != nil }
	s.vals = newSnapshotCache("validators", func(ctx context.Context, win Window) (validatorSnapshot, error) {
		rows, err := s.validatorRows(ctx, win, "")
		if err != nil {
			return validatorSnapshot{}, err
		}
		return validatorSnapshot{Window: win, Rows: rows, RecordThrough: s.recordThrough(ctx)}, nil
	})
	// Both of these publish faults beside named validators, and both are
	// cached for up to fifteen minutes. A hold landing in the database moves
	// nothing they hold, so without this the figure a hold withdrew stays
	// on the front page until the TTL runs out. The market snapshot carries
	// no verdicts and needs no hold, but all three change meaning the moment
	// Fibre goes live: a pre-activation zero served for minutes after the
	// first publication says "nothing happened" when something did.
	s.net.revision = s.snapshotRevision
	s.vals.revision = s.snapshotRevision
	s.market.revision = s.activationRevision
	// The windows that do not take ttlFor's pace: the live keeper's
	// (keepLiveFresh) and the network's "all", which holds the overview's
	// Available figure. Set before anything reads the caches: ttl reads
	// these without the lock.
	s.vals.ttls = map[string]time.Duration{}
	for _, name := range liveVals {
		s.vals.ttls[name] = liveTTL
	}
	s.market.ttls = map[string]time.Duration{}
	for _, name := range warmWindows {
		s.market.ttls[name] = liveTTL
	}
	s.net.ttls = map[string]time.Duration{"all": networkAllTTL}
	// Serve the previous process's snapshots at once, then warm every window
	// so the first visitor is not the one who waits.
	if s.dataDir != "" {
		dir := filepath.Join(s.dataDir, "snapshots")
		s.net.persistTo(dir, s.logf())
		s.vals.persistTo(dir, s.logf())
		s.market.persistTo(dir, s.logf())
	}
	s.net.warm(s.logf(), time.Now())
	s.vals.warm(s.logf(), time.Now())
	s.market.warm(s.logf(), time.Now())
	// Then keep every window inside its TTL whether or not anyone reads it,
	// so a quiet night does not leave the first morning visitor a figure
	// from the evening before.
	s.stop = make(chan struct{})
	s.bg.Add(2)
	go func() {
		defer s.bg.Done()
		s.keepSnapshotsFresh(keeperInterval)
	}()
	go func() {
		defer s.bg.Done()
		s.keepLiveFresh(liveInterval)
	}()
	// And the first page of blobs, for the same reason: with the verdict cache
	// empty that page costs six queries per row, which is the one cold path
	// left on the site. It is a single read of what /v1/blobs answers by
	// default, discarded — the point is the cache it leaves behind.
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
		defer cancel()
		if _, err := s.blobRows(ctx, "", blobPageDefault); err != nil && log != nil {
			log.Printf("warming the blob page: %v", err)
		}
	}()
	s.mux.HandleFunc("GET /v1/meta", s.handleMeta)
	s.mux.HandleFunc("GET /v1/network", s.handleNetwork)
	s.mux.HandleFunc("GET /v1/validators", s.handleValidators)
	s.mux.HandleFunc("GET /v1/validators/{addr}", s.handleValidator)
	s.mux.HandleFunc("GET /v1/blobs", s.handleBlobs)
	s.mux.HandleFunc("GET /v1/namespaces", s.handleNamespaces)
	s.mux.HandleFunc("GET /v1/blobs/{hash}", s.handleBlob)
	s.mux.HandleFunc("GET /v1/probes", s.handleProbes)
	s.mux.HandleFunc("GET /v1/runs", s.handleRuns)
	s.mux.HandleFunc("GET /v1/sampling", s.handleSampling)
	s.mux.HandleFunc("GET /v1/exports", s.handleExports)
	s.mux.HandleFunc("GET /v1/exports/pubkey", s.handleExportPubkey) // exports_signing.go; more specific than {name}
	s.mux.HandleFunc("GET /v1/exports/{name}", s.handleExportFile)
	s.mux.HandleFunc("GET /v1/avatars/{identity}", s.handleAvatar)
	s.mux.HandleFunc("GET /v1/health", s.handleHealth)
	s.mux.HandleFunc("GET /v1/tip", s.handleTip)
	s.mux.HandleFunc("GET /v1/market", s.handleMarket)
	s.mux.HandleFunc("GET /v1/publishers", s.handlePublishers)
	s.mux.HandleFunc("GET /v1/publishers/{addr}", s.handlePublisher)
	s.mux.HandleFunc("GET /v1/params", s.handleParams)
	s.mux.HandleFunc("GET /v1/signing", s.handleSigning)
	s.registerExtraRoutes()
	return s
}

// Close waits for the server's background work (snapshot warm-ups and
// refreshes, the blob-page warm-up) to finish, so that nothing is still
// writing under the data directory once the caller tears it down. It does
// not stop the HTTP side; the caller's listener does that.
func (s *Server) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.bg.Wait()
	s.net.wait()
	s.vals.wait()
	s.market.wait()
}

// ServeHTTP implements http.Handler with the headers every response shares.
// Only successful responses are cacheable: a 400 or a 404 held for 15 seconds
// by a proxy outlives the mistake that caused it.
// writeDeadlineFor is how long a route may take to write its answer. It is
// per route, because one number cannot fit all of them: an export is a
// tarball of a whole day's records and a pinned window is a full aggregate
// computed on demand, measured at 23 seconds on an 80-validator fixture,
// while every other route answers from a snapshot in under a millisecond and
// should not be allowed to hang.
func writeDeadlineFor(path string) time.Duration {
	switch {
	case strings.HasPrefix(path, "/v1/exports/"):
		return 30 * time.Minute
	case strings.HasPrefix(path, "/v1/avatars/"):
		return 30 * time.Second
	default:
		return 5 * time.Minute
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// The server's own WriteTimeout is off (cmd/observer-api), because a
	// single one truncated the two answers that are legitimately slow: a
	// day's export tarball and a pinned-window aggregate. The bound is set
	// here instead, by route. A deadline that cannot be set (an older
	// ResponseWriter, a test recorder) is not an error: the handler's own
	// context still bounds the work.
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Now().Add(writeDeadlineFor(r.URL.Path)))
	}
	rec := &statusWriter{ResponseWriter: w}
	s.mux.ServeHTTP(rec, r)
}

// statusWriter sets Cache-Control from the status code as the handler writes
// its header, and answers an unmatched route in the JSON shape the rest of
// the API uses.
type statusWriter struct {
	http.ResponseWriter
	wrote bool
	// swallow is set when this writer supplied the body itself (the JSON 404
	// in place of ServeMux's text/plain one), so the handler's own bytes are
	// dropped instead of being appended to it.
	swallow bool
}

func (w *statusWriter) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.wrote = true
	switch {
	case status == http.StatusNotModified || status == http.StatusPartialContent:
		// Neither is an error, and a 304's headers update the cache entry it
		// revalidates (RFC 9111). Forcing no-store here told every cache to
		// throw away the avatar or export it had just been told to keep for
		// a day, so a successful revalidation undid the caching it was meant
		// to confirm. Whatever the 200 set stands.
	case status < 200 || status >= 300:
		w.Header().Set("Cache-Control", "no-store")
	case w.Header().Get("Cache-Control") == "":
		// A handler that set its own policy (an immutable export, an
		// uncached pinned window) keeps it.
		w.Header().Set("Cache-Control", "public, max-age=15")
	}
	if status == http.StatusNotFound && !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		w.swallow = true
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.ResponseWriter.WriteHeader(status)
		_, _ = w.ResponseWriter.Write([]byte(`{"error":"no such endpoint; see /v1/meta"}` + "\n"))
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	if w.swallow {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

// writeInternal logs the real error and tells the client only that something
// went wrong: SQLite errors carry the database path and schema details that a
// public endpoint has no business publishing.
func (s *Server) writeInternal(w http.ResponseWriter, where string, err error) {
	if s.log != nil {
		s.log.Printf("api: %s: %v", where, err)
	}
	writeJSON(w, 500, map[string]any{"error": "internal error"})
}

// Window is a fixed lookback the dashboard offers.
type Window struct {
	Name  string        `json:"name"`
	Span  time.Duration `json:"-"`
	Start time.Time     `json:"start"`
	End   time.Time     `json:"end"`
	// AsOf: End was pinned by the caller (?as_of=), so every figure is what
	// the observer would have published at End from the rows it had by
	// then: rows started after End are left out. What the chain says now
	// (jailed, bonded, the current registry) is not rewound; see
	// AsOfNote.
	AsOf bool `json:"as_of,omitempty"`
}

// rolledUp is the label beside figures that rest on the daily rollup: past
// the raw retention the "all" window is the rollup for every day before
// RawFrom plus the raw rows from RawFrom on. See observer/rollup.
type rolledUp struct {
	RawFrom string `json:"raw_from"`
	Days    int64  `json:"days"`
	Note    string `json:"note"`
}

// rolledFor returns the rollups the window folds in, and their label: only
// the "all" window, only once a day has been pruned. A pinned end before
// RawFrom takes whole rolled days up to and including its own.
func (s *Server) rolledFor(ctx context.Context, win Window, only string) (*rollup.Rolled, *rolledUp, error) {
	if win.Span != 0 {
		return nil, nil, nil
	}
	from, ok := rollup.RawFrom(s.st)
	if !ok {
		return nil, nil, nil
	}
	before := from
	if end := win.End.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour); end.Before(before) {
		before = end
	}
	r, err := rollup.Load(ctx, s.st.DB(), before, only)
	if err != nil {
		return nil, nil, err
	}
	label := &rolledUp{RawFrom: from.Format("2006-01-02"), Days: r.Days,
		Note: "rolled up after " + rolledNote + ": obligations, classes, faults, probe counts, gaps, heartbeats and endorsement for days before raw_from come from the daily rollup; latency, by-point, attestation and throughput figures cover the raw record from raw_from on"}
	return r, label, nil
}

// rolledNote names the raw retention in the label; the API does not read
// the collector's flag, so it states the decision (deploy/README.md).
const rolledNote = "90 days"

func addRolledObligations(o *obligationStats, r rollup.Obligations) {
	o.Total += r.Total
	o.Served += r.Served
	o.Broken += r.Broken
	o.EndUnobserved += r.EndUnobserved
	o.HeldParamUnverified += r.HeldParamUnverified
	o.UnobservedReachable += r.UnobservedReachable
	o.UnobservedUnreachable += r.UnobservedUnreachable
	o.UnobservedNotProbed += r.UnobservedNotProbed
	o.Pending += r.Pending
	o.finish()
}

// AsOfNote goes beside a pinned window's figures. It names what is not
// rewound, so it has to be exact: an over-broad disclaimer is as misleading
// as a missing one. Probe rows, heartbeats, payments, reconstructability and
// the endpoint census are all bounded by the pin. What is not is the chain
// state the store keeps only currently: a validator's jailed flag, its bond
// status and the host it advertises today.
const AsOfNote = "rows started after as_of are left out, and so are the verdicts drawn from them; a validator's jailed flag, bond_status and current host are as of now, not as_of"

var windows = map[string]time.Duration{"24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour, "30d": 30 * 24 * time.Hour, "all": 0}

func parseWindow(r *http.Request, now time.Time) (Window, error) {
	name := r.URL.Query().Get("window")
	if name == "" {
		name = "24h"
	}
	span, ok := windows[name]
	if !ok {
		return Window{}, fmt.Errorf("window must be one of 24h, 7d, 30d, all")
	}
	if v := r.URL.Query().Get("as_of"); v != "" {
		at, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return Window{}, fmt.Errorf("as_of must be RFC 3339, e.g. 2026-09-18T12:00:00Z")
		}
		if at.After(now.Add(time.Minute)) {
			return Window{}, fmt.Errorf("as_of is in the future")
		}
		now = at
	}
	w := Window{Name: name, Span: span, End: now, AsOf: r.URL.Query().Get("as_of") != ""}
	if span > 0 {
		w.Start = now.Add(-span)
	}
	return w, nil
}

func (w Window) startArg() string {
	if w.Span == 0 {
		return "0000"
	}
	return store.TS(w.Start)
}

// endArg bounds a query at the window's end. With an unpinned window End
// is the moment the query was planned, so the bound admits every row the
// store holds; with ?as_of= it is what makes the answer reproducible.
func (w Window) endArg() string { return store.TS(w.End) }

// asOfArg is the end bound for queries that only pinned windows need.
func (w Window) asOfArg() string {
	if !w.AsOf {
		return ""
	}
	return w.endArg()
}

// asOfLimiter rations pinned-window requests, which bypass the snapshot
// cache and cost a full aggregate each: a small burst, then one every two
// seconds.
type asOfLimiter struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
	// inFlight is the number of pinned windows being computed right now.
	inFlight int
}

// enter reserves one of the asOfConcurrent computation slots. The caller must
// call leave when it is done. ok is false when every slot is busy, which is a
// 429 like the rate limit: the work, not the request, is what is scarce.
func (l *asOfLimiter) enter() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inFlight >= asOfConcurrent {
		return false
	}
	l.inFlight++
	return true
}

func (l *asOfLimiter) leave() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inFlight > 0 {
		l.inFlight--
	}
}

const (
	asOfBurst    = 4.0
	asOfInterval = 2 * time.Second
	// asOfConcurrent bounds how many pinned windows are computed at once. The
	// rate limit above bounds arrivals; this bounds work, and the two are not
	// the same thing: a pinned "all" window is every aggregate recomputed from
	// the raw rows, which on a network-scale store is tens of seconds of CPU
	// and hundreds of megabytes of heap. Measured on a fixture of 80
	// validators and three days of publications (691k probe rows): 23s and
	// ~900 MB for one /v1/network?window=all&as_of=..., so "one every two
	// seconds" admits an order of magnitude more work than the process can
	// carry. Two at a time keeps a pinned request answerable without letting
	// the public endpoint decide how much of the box it gets.
	asOfConcurrent = 2
)

func (l *asOfLimiter) allow(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.last.IsZero() {
		l.tokens = asOfBurst
	} else {
		l.tokens = min(asOfBurst, l.tokens+now.Sub(l.last).Seconds()/asOfInterval.Seconds())
	}
	l.last = now
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}

// Rate is a numerator, denominator and their ratio, never a bare percentage.
type Rate struct {
	Num   int64    `json:"num"`
	Den   int64    `json:"den"`
	Value *float64 `json:"value"` // null when den == 0
}

func rate(num, den int64) Rate {
	r := Rate{Num: num, Den: den}
	if den > 0 {
		v := float64(num) / float64(den)
		r.Value = &v
	}
	return r
}

// ---- meta ----

type metaResponse struct {
	APIVersion string `json:"api_version"`
	// MethodologyVersion is verdict.MethodologyVersion: the rules the figures
	// on every page were computed under.
	MethodologyVersion     string      `json:"methodology_version"`
	Vantage                string      `json:"vantage"`
	VantageInfo            VantageInfo `json:"vantage_info"`
	VantageCount           int         `json:"vantage_count"`
	ObservedFromOneVantage bool        `json:"observed_from_one_location"`
	ChainID                string      `json:"chain_id"`

	// Vantages are the heartbeats whose rows reached this store in the last
	// hour, this observer's own and any other vantage's copied in beside it,
	// each with its newest row, in name order. Only this observer's rows are
	// counted in the figures; another's confirm or contradict a failed check.
	Vantages []vantageSeen `json:"vantages"`

	// AppVersion is the chain's current application version, and FibreActive
	// is whether that is high enough for x/fibre and x/valaddr to exist. Below
	// FibreAppVersion the modules are not there, so an empty registry and an
	// empty publication list say nothing about any validator — and a site that
	// cannot tell "the module is absent" from "the module is empty" will imply
	// the second while the first is true. Empty when the collector has not
	// reached a node yet, which is itself worth showing.
	AppVersion      string `json:"app_version,omitempty"`
	FibreAppVersion string `json:"fibre_app_version,omitempty"`
	FibreActive     bool   `json:"fibre_active"`
	// ChainHeight is the chain's tip as the collector last saw it, which is not
	// LastScannedHeight: that is how far the SCANNER has read, and before Fibre
	// activates there is nothing for it to read, so it stays empty while the
	// chain is plainly making blocks. Reporting the chain's progress as our own,
	// or ours as the chain's, would be wrong in opposite directions.
	ChainHeight          string       `json:"chain_height,omitempty"`
	LastScannedHeight    string       `json:"last_scanned_height"`
	EndpointsHeight      string       `json:"endpoints_height"`
	ProtocolParamsFinger string       `json:"protocol_params_fingerprint"`
	PinnedCelestiaApp    string       `json:"pinned_celestia_app_commit"`
	Counts               store.Counts `json:"counts"`
	Collector            *runStatus   `json:"collector"`
	Prober               *runStatus   `json:"prober"`
	// LastProbeAt is the newest measurement's start time. The prober writes
	// JSONL only (it never touches this database), so this is the only live
	// signal of it; a quiet chain makes it old without anything being wrong.
	LastProbeAt *string           `json:"last_probe_at"`
	Meta        map[string]string `json:"meta"`
	ServerTime  time.Time         `json:"server_time"`
	// Components is every observer process with its liveness, from the
	// status files in the data directory (see /v1/health). Health is the
	// same verdict /v1/health returns: ok, degraded or down.
	Components []componentStatus `json:"components"`
	Health     string            `json:"health"`
	// Checks is every row behind Health, the same list /v1/health serves.
	// Health alone told the site that something was wrong; the components
	// told it which process, and nothing told it about a check that is not
	// a process — a chain that stopped producing blocks, a scan gap, a stale
	// pin — so the site announced "degraded" with nothing after the colon,
	// on the one day (an upgrade halt) when everyone was looking.
	Checks []healthCheck `json:"checks"`
	// ScanGaps are height ranges the scanner could not read from its node,
	// or that the operator told it to skip (-skip-heights; Reason says which).
	// A publication in one of them is unknown to this observer.
	ScanGaps []scan.ScanGap `json:"scan_gaps,omitempty"`
	// ParamUncertainty is every range of heights this observer could not
	// say which x/fibre params were in force over, with what came of
	// trying to close it. Published whether or not it still holds
	// anything: a range that was closed is part of the record of what this
	// observer did and did not know, and a reader checking a corrected
	// deadline needs the range that moved it.
	ParamUncertainty []paramUncertainty `json:"param_uncertainty,omitempty"`
	// PinStatus says whether the chain's app version matches the celestia-app
	// major this build's assignment constants are pinned to: matches,
	// chain_ahead, chain_behind or unknown.
	PinStatus string `json:"pin_status"`
	// UnassignablePublications is how many publications have no row
	// assignment (a blob version this build does not know), and so are never
	// probed.
	UnassignablePublications int64 `json:"unassignable_publications"`
	// Evidence says, per headline figure, which of the three kinds of
	// evidence it rests on; EvidenceKinds defines the three.
	Evidence      map[string]string `json:"evidence"`
	EvidenceKinds map[string]string `json:"evidence_kinds"`
	// UpgradeSignal is x/signal's tally for the app version that brings
	// Fibre, published only while the chain is below it: how much voting
	// power has signalled, the threshold, who has not, and the scheduled
	// height once there is one. A chain record, nothing measured here.
	UpgradeSignal *upgradeSignal `json:"upgrade_signal,omitempty"`
}

type upgradeSignal struct {
	Version          int64   `json:"version"`
	VotingPower      int64   `json:"voting_power"`
	ThresholdPower   int64   `json:"threshold_power"`
	TotalVotingPower int64   `json:"total_voting_power"`
	Share            float64 `json:"share"`           // voting_power / total_voting_power
	ThresholdShare   float64 `json:"threshold_share"` // threshold_power / total_voting_power
	UpgradeHeight    int64   `json:"upgrade_height,omitempty"`
	// BlocksRemaining is upgrade_height minus the chain tip the collector
	// last saw, while the upgrade is scheduled and still ahead.
	BlocksRemaining int64 `json:"blocks_remaining,omitempty"`
	// BlockTimeS is the chain's average seconds per block, measured from the
	// older pace anchor the collector keeps (store.NotePace) to the tip, over
	// PaceWindowS seconds; ETASeconds is BlocksRemaining at that pace. Absent
	// until the measurement spans at least half an hour. An estimate at the
	// chain's recent pace, not a promise about its next block.
	BlockTimeS  float64 `json:"block_time_s,omitempty"`
	PaceWindowS int64   `json:"pace_window_s,omitempty"`
	ETASeconds  int64   `json:"eta_seconds,omitempty"`
	// MissingValidators is the monikers x/signal reports as not having
	// signalled; the module answers by moniker, not by address.
	MissingValidators []string `json:"missing_validators"`
	PolledAt          string   `json:"polled_at"`
}

// paceMinWindow is the least span the block-time measurement must cover
// before the API states it.
const paceMinWindow = 30 * time.Minute

// upgradeSignalOf builds the block from the collector's meta keys, or nil
// once Fibre is live or nothing was polled yet. now is only for the ETA,
// which is left out while the tip is older than chainStaleAfter: a pace
// measured up to a chain that stopped says nothing about when it resumes.
func upgradeSignalOf(meta map[string]string, now time.Time) *upgradeSignal {
	if meta["fibre_active"] == "yes" || meta["signal_version"] == "" {
		return nil
	}
	n := func(k string) int64 { v, _ := strconv.ParseInt(meta[k], 10, 64); return v }
	u := &upgradeSignal{
		Version: n("signal_version"), VotingPower: n("signal_voting_power"), ThresholdPower: n("signal_threshold_power"),
		TotalVotingPower: n("signal_total_voting_power"), UpgradeHeight: n("signal_upgrade_height"), PolledAt: meta["signal_polled_at"],
		MissingValidators: []string{},
	}
	if u.TotalVotingPower > 0 {
		u.Share = float64(u.VotingPower) / float64(u.TotalVotingPower)
		u.ThresholdShare = float64(u.ThresholdPower) / float64(u.TotalVotingPower)
	}
	_ = json.Unmarshal([]byte(meta["signal_missing"]), &u.MissingValidators)
	if u.MissingValidators == nil {
		// "null" on record, from a collector that stored a nil list, is
		// nobody missing, and the field is a list either way.
		u.MissingValidators = []string{}
	}
	tip := n("chain_height")
	if u.UpgradeHeight > 0 && tip > 0 && u.UpgradeHeight > tip {
		u.BlocksRemaining = u.UpgradeHeight - tip
		fromH := n("chain_pace_from_height")
		fromT, errFrom := time.Parse(store.TimeLayout, meta["chain_pace_from_time"])
		tipT, errTip := time.Parse(store.TimeLayout, meta["chain_tip_time"])
		if errFrom == nil && errTip == nil && fromH > 0 && tip > fromH {
			if window := tipT.Sub(fromT); window >= paceMinWindow && now.Sub(tipT) <= chainStaleAfter {
				u.BlockTimeS = window.Seconds() / float64(tip-fromH)
				u.PaceWindowS = int64(window.Seconds())
				u.ETASeconds = int64(float64(u.BlocksRemaining)*u.BlockTimeS + 0.5)
			}
		}
	}
	return u
}

// upgradeSignalSets is what attributing the signal to a validator needs,
// while the signal is being published: the monikers x/signal reports as
// not having signalled, and the monikers that more than one bonded
// validator carries — which cannot be attributed to either. The second set
// is counted over every bonded identity the observer knows, never over the
// rows a request happens to be building: a request for one validator sees
// one row, and would count its moniker as unique with its twin out of
// sight. ok is false when nothing is published.
func (s *Server) upgradeSignalSets(ctx context.Context) (missing, shared map[string]bool, ok bool) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT key, value FROM meta WHERE key IN ('fibre_active', 'signal_version', 'signal_missing')`)
	if err != nil {
		return nil, nil, false
	}
	meta := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err == nil {
			meta[k] = v
		}
	}
	rows.Close()
	u := upgradeSignalOf(meta, time.Now())
	if u == nil {
		return nil, nil, false
	}
	missing = map[string]bool{}
	for _, m := range u.MissingValidators {
		missing[m] = true
	}
	shared = map[string]bool{}
	srows, err := s.st.DB().QueryContext(ctx, `SELECT moniker FROM validator_identities
		WHERE status = 'BOND_STATUS_BONDED' AND moniker <> '' GROUP BY moniker HAVING COUNT(*) > 1`)
	if err != nil {
		return nil, nil, false
	}
	defer srows.Close()
	for srows.Next() {
		var m string
		if err := srows.Scan(&m); err == nil {
			shared[m] = true
		}
	}
	return missing, shared, true
}

type runStatus struct {
	RunID         int64   `json:"run_id"`
	StartedAt     string  `json:"started_at"`
	LastHeartbeat string  `json:"last_heartbeat_at"`
	StoppedAt     *string `json:"stopped_at"`
	Alive         bool    `json:"alive"` // heartbeat within the last 2 minutes
}

// vantageCount counts the distinct vantages that ever wrote a probe or a
// heartbeat (a second location that only runs the heartbeat still counts).
// vantageCount is how many distinct places the stored observations were made
// from. It decides one sentence on every page — whether this is a single
// vantage or several — and it was the most expensive query /v1/meta ran: no
// index covered `vantage`, so it scanned both probe tables in full and unioned
// them through a temp B-tree (49ms of the endpoint's 58ms of SQL on an
// 85,000-probe store). A cache keyed on each table's highest rowid kept it off
// most requests, but under traffic the tables grow every collector pass, so the
// full scan came back every ten seconds and grew with the store.
//
// It is now asked of probes_vantage and reachability_vantage (migration 22),
// one seek per distinct vantage, which is cheap enough to run on every request
// and so needs no cache at all. Exact, as before: the claim this drives is the
// one-vantage caveat printed above every page, and a cached count is a claim
// about how much the site's own evidence is worth.
func (s *Server) vantageCount(ctx context.Context) int {
	var n int
	_ = s.st.DB().QueryRowContext(ctx, vantageCountSQL).Scan(&n)
	if n == 0 {
		n = 1
	}
	return n
}

// vantageCountSQL counts the distinct vantages across both tables by
// walking probes_vantage and reachability_vantage (migration 22) from one
// distinct value to the next — a loose index scan, one seek per vantage —
// instead of reading every row's vantage and de-duplicating them.
const vantageCountSQL = `WITH RECURSIVE
	vp(v) AS (SELECT (SELECT MIN(vantage) FROM probes)
	         UNION ALL SELECT (SELECT MIN(vantage) FROM probes WHERE vantage > vp.v) FROM vp WHERE vp.v IS NOT NULL),
	vr(v) AS (SELECT (SELECT MIN(vantage) FROM reachability)
	         UNION ALL SELECT (SELECT MIN(vantage) FROM reachability WHERE vantage > vr.v) FROM vr WHERE vr.v IS NOT NULL)
	SELECT COUNT(*) FROM (SELECT v FROM vp WHERE v IS NOT NULL UNION SELECT v FROM vr WHERE v IS NOT NULL)`

// vantageSeen is one heartbeat vantage with recent rows in the store.
type vantageSeen struct {
	Name     string `json:"name"`
	NewestAt string `json:"newest_at"` // started_at of its newest row
	Primary  bool   `json:"primary"`   // this observer's own: the one the figures count
}

// vantageRecent is how new a vantage's newest row must be for /v1/meta to
// list it: twelve heartbeats at five minutes, so one late copy does not
// drop it.
const vantageRecent = time.Hour

// recentVantagesSQL lists every vantage in the reachability table with the
// start of its newest row: the distinct names by the same loose index scan
// as vantageCountSQL, and each one's newest row as the last entry under its
// name in reachability_vantage, which keeps them in rowid (ingest) order.
const recentVantagesSQL = `WITH RECURSIVE
	vr(v) AS (SELECT (SELECT MIN(vantage) FROM reachability)
	         UNION ALL SELECT (SELECT MIN(vantage) FROM reachability WHERE vantage > vr.v) FROM vr WHERE vr.v IS NOT NULL)
	SELECT vr.v, (SELECT q.started_at FROM reachability q WHERE q.vantage = vr.v ORDER BY q.rowid DESC LIMIT 1)
	FROM vr WHERE vr.v IS NOT NULL`

// recentVantages is the heartbeat vantages whose newest row started within
// vantageRecent of now, in name order. Never nil, so the field is always a
// list.
func (s *Server) recentVantages(ctx context.Context, now time.Time) []vantageSeen {
	out := []vantageSeen{}
	rows, err := s.st.DB().QueryContext(ctx, recentVantagesSQL)
	if err != nil {
		return out
	}
	defer rows.Close()
	since := store.TS(now.Add(-vantageRecent))
	for rows.Next() {
		var name string
		var at sql.NullString
		if rows.Scan(&name, &at) != nil || !at.Valid || at.String < since {
			continue
		}
		out = append(out, vantageSeen{Name: name, NewestAt: at.String, Primary: name == s.vantage})
	}
	return out
}

// latestRun is the newest run row for a component, with whether its heartbeat
// is recent enough to call it alive.
func (s *Server) latestRun(ctx context.Context, component string, now time.Time) (*runStatus, error) {
	var rs runStatus
	err := s.st.DB().QueryRowContext(ctx, `SELECT id, started_at, last_heartbeat_at, stopped_at FROM observer_runs
		WHERE component = ? ORDER BY started_at DESC LIMIT 1`, component).Scan(&rs.RunID, &rs.StartedAt, &rs.LastHeartbeat, &rs.StoppedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if t, err := time.Parse(time.RFC3339Nano, rs.LastHeartbeat); err == nil && rs.StoppedAt == nil {
		rs.Alive = now.Sub(t) < 2*time.Minute
	}
	return &rs, nil
}

func (s *Server) handleMeta(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := time.Now()
	counts, err := s.st.Count(ctx)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	meta := map[string]string{}
	rows, err := s.st.DB().QueryContext(ctx, `SELECT key, value FROM meta`)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err == nil {
			meta[k] = v
		}
	}
	rows.Close()
	vantages := s.vantageCount(ctx)
	col, _ := s.latestRun(ctx, "collector", now)
	pr, _ := s.latestRun(ctx, "prober", now)
	var lastProbe *string
	var lp sql.NullString
	if err := s.st.DB().QueryRowContext(ctx, `SELECT MAX(t) FROM (SELECT MAX(started_at) AS t FROM probes UNION ALL SELECT MAX(decided_at) FROM sampling_decisions)`).Scan(&lp); err == nil && lp.Valid {
		lastProbe = &lp.String
	}
	var pinned string
	_ = s.st.DB().QueryRowContext(ctx, `SELECT pinned_celestia_app FROM publications ORDER BY settlement_height DESC LIMIT 1`).Scan(&pinned)
	// Before the first publication there is no row to read it from, and the
	// footer printed no pin at all. The commit this binary assigns rows with
	// is the same answer until a publication says otherwise.
	if pinned == "" {
		pinned = assign.PinnedCelestiaAppCommit
	}
	h := s.health(ctx, now)
	// A run row's heartbeat is only as fresh as what reached the database:
	// the prober writes JSONL and never this table, so its row kept the
	// start time and read "alive": false beside a components entry, from
	// the process's own status file, that said it was running. The status
	// file is the live signal /v1/health already trusts; it decides here too.
	for _, c := range h.Components {
		if !c.Present {
			continue
		}
		switch {
		case c.Component == "collector" && col != nil:
			col.Alive = c.Alive
		case c.Component == "prober" && pr != nil:
			pr.Alive = c.Alive
		}
	}
	var ranges []paramUncertainty
	if us, err := s.st.ParamRanges(ctx); err == nil {
		for _, u := range us {
			ranges = append(ranges, paramUncertaintyOf(u))
		}
	}
	writeJSON(w, 200, metaResponse{
		Components: h.Components, Health: h.Status, Checks: h.Checks, ScanGaps: h.ScanGaps, PinStatus: h.PinStatus,
		Evidence: evidenceOf, EvidenceKinds: evidenceKinds, UpgradeSignal: upgradeSignalOf(meta, now),
		ParamUncertainty:         ranges,
		UnassignablePublications: s.unassignablePublications(ctx),
		APIVersion:               Version, Vantage: s.vantage, VantageInfo: s.info,
		MethodologyVersion: verdict.MethodologyVersion,
		VantageCount:       vantages, ObservedFromOneVantage: vantages == 1, Vantages: s.recentVantages(ctx, now),
		ChainID: meta["chain_id"], LastScannedHeight: meta["last_scanned_height"], EndpointsHeight: meta["endpoints_height"],
		AppVersion: meta["app_version"], FibreAppVersion: meta["fibre_app_version"], FibreActive: meta["fibre_active"] == "yes",
		ChainHeight:          meta["chain_height"],
		ProtocolParamsFinger: meta["protocol_params_fingerprint"], PinnedCelestiaApp: pinned,
		Counts: counts, Collector: col, Prober: pr, LastProbeAt: lastProbe, Meta: meta, ServerTime: now.UTC(),
	})
}

// ---- runs (gaps) ----

type runRow struct {
	ID            int64   `json:"id"`
	Component     string  `json:"component"`
	Vantage       string  `json:"vantage"`
	Version       string  `json:"version"`
	StartedAt     string  `json:"started_at"`
	LastHeartbeat string  `json:"last_heartbeat_at"`
	StoppedAt     *string `json:"stopped_at"`
	StopReason    *string `json:"stop_reason"`
	PID           *int64  `json:"pid"`
	Hostname      *string `json:"hostname"`
	// Config is what the run was started with: every flag by name, as the
	// component recorded it in runs.jsonl (status.RunEvent). It is what a
	// verifier needs to re-derive this run's rows: the prune tolerance
	// behind a phase, the schedule points, the timeouts, the policy file.
	// Null for a run recorded before the file existed, and for the
	// collector's own row.
	Config json.RawMessage `json:"config"`
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	win, err := parseWindow(r, time.Now())
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	rows, err := s.st.DB().QueryContext(r.Context(), `SELECT id, component, vantage, version, started_at, last_heartbeat_at, stopped_at, stop_reason, pid, hostname, config_json
		FROM observer_runs WHERE last_heartbeat_at >= ? AND started_at <= ? ORDER BY started_at`, win.startArg(), win.endArg())
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	defer rows.Close()
	out := []runRow{}
	for rows.Next() {
		var rr runRow
		var cfg *string
		if err := rows.Scan(&rr.ID, &rr.Component, &rr.Vantage, &rr.Version, &rr.StartedAt, &rr.LastHeartbeat, &rr.StoppedAt, &rr.StopReason, &rr.PID, &rr.Hostname, &cfg); err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		if cfg != nil && json.Valid([]byte(*cfg)) {
			rr.Config = json.RawMessage(*cfg)
		} else {
			rr.Config = json.RawMessage("null")
		}
		out = append(out, rr)
	}
	writeJSON(w, 200, map[string]any{"window": win, "runs": out,
		"note": "a run without stopped_at whose component's status file is stale is a crash; config is the component's flags at start, from runs.jsonl"})
}

// ---- network ----

type classCounts map[string]int64

// recordThrough says which point of the chain a set of figures rests on: the
// scanner's checkpoint, and its block time, as they stood when the snapshot
// was computed, beside the tip the collector had last seen. computed_at says
// when the figures were taken; this says through which block. A reader who
// wants to check a figure needs the height the record ran to, not the wall
// clock, because the record is indexed by height and the clock is not.
type recordThrough struct {
	// Height is the last height the scanner had read into the record.
	Height int64 `json:"height"`
	// BlockTime is that block's time on the chain's own clock.
	BlockTime string `json:"block_time,omitempty"`
	// ChainHeight and ChainTipTime are the chain's tip as the collector last
	// polled it; the difference to Height is how far the record lags.
	ChainHeight  int64  `json:"chain_height,omitempty"`
	ChainTipTime string `json:"chain_tip_time,omitempty"`
}

// recordThrough reads the four meta keys the collector keeps for this. Nil
// when the scanner has not written a checkpoint yet.
func (s *Server) recordThrough(ctx context.Context) *recordThrough {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT key, value FROM meta
		WHERE key IN ('last_scanned_height', 'last_scanned_time', 'chain_height', 'chain_tip_time')`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	rt := &recordThrough{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil
		}
		switch k {
		case "last_scanned_height":
			rt.Height, _ = strconv.ParseInt(v, 10, 64)
		case "last_scanned_time":
			rt.BlockTime = v
		case "chain_height":
			rt.ChainHeight, _ = strconv.ParseInt(v, 10, 64)
		case "chain_tip_time":
			rt.ChainTipTime = v
		}
	}
	if rt.Height == 0 {
		return nil
	}
	return rt
}

// evidenceKinds names the three kinds of evidence a figure on the site can
// rest on, and evidenceOf says which each headline figure rests on. They
// are published on /v1/meta so an API reader has the same labels the site
// prints, and so the three are never blurred: a count of what the chain
// recorded, bytes this observer fetched and verified, and what this
// observer's own network saw from one place are different claims.
var evidenceKinds = map[string]string{
	"chain_record":        "a count of something the chain recorded; nothing here was measured by this observer",
	"verified_response":   "bytes this observer fetched and verified against the on-chain commitment, or a certificate checked against the validator's consensus key",
	"vantage_observation": "what this observer's own network saw from one location; it says nothing about any shard",
}
var evidenceOf = map[string]string{
	"serve_rate": "verified_response", "faults": "verified_response", "obligations": "verified_response", "endorsed": "verified_response",
	"reachability": "vantage_observation", "throughput": "vantage_observation",
	"publications": "chain_record", "signed_shards": "chain_record", "registered_endpoints": "chain_record", "fees_settled": "chain_record",
	"publishers": "chain_record", "paid_per_mib": "chain_record", "timed_out": "chain_record", "settlement_rate": "chain_record", "escrow_held": "chain_record",
}

type networkResponse struct {
	// AsOfNote is set on a pinned window (see Window.AsOf).
	AsOfNote string `json:"as_of_note,omitempty"`
	// RolledUp is set when figures rest partly on the daily rollup.
	RolledUp *rolledUp `json:"rolled_up,omitempty"`
	// RetentionUncertainty is set only while something is withheld because
	// an x/fibre params range has not been read at every height. Absent on
	// a healthy deployment, so its presence is the signal.
	RetentionUncertainty *retentionUncertainty `json:"retention_uncertainty,omitempty"`
	Window               Window                `json:"window"`
	Vantage              string                `json:"vantage"`
	// Previous is the same span ending where this window starts, for the
	// change beside a headline figure. Absent on "all" and on a pinned
	// window.
	Previous *previousWindow `json:"previous,omitempty"`
	// ComputedAt and ComputeMs say when this summary was taken and how long it
	// took. It is a snapshot refreshed on a schedule, not a live query, so its
	// age is published rather than left for a reader to assume.
	ComputedAt string `json:"computed_at,omitempty"`
	ComputeMs  int64  `json:"compute_ms,omitempty"`
	// RecordThrough is the point of the chain these figures rest on, taken
	// when they were computed.
	RecordThrough          *recordThrough `json:"record_through,omitempty"`
	ObservedFromOneVantage bool           `json:"observed_from_one_location"`
	// Excluded and ExcludeNote are set when a reader asked for figures
	// without named validators (?exclude=); see excludeSet.
	Excluded            []string `json:"excluded,omitempty"`
	ExcludeNote         string   `json:"exclude_note,omitempty"`
	RegisteredEndpoints int64    `json:"registered_endpoints"`
	ValidatorsProbed    int64    `json:"validators_probed"`
	Reachability        Rate     `json:"reachability"` // endpoints whose latest heartbeat or probe reached TLS
	// ReachabilityWindow is every reachability heartbeat in the window that
	// completed TLS, over every heartbeat sent. Reachability above is a census
	// of the endpoints right now; this is how the whole window went, which is
	// the difference between "two are down" and "two have been down all week".
	// Heartbeats are pooled, so a validator that registered mid-window
	// contributes fewer samples than one that was there throughout.
	ReachabilityWindow Rate `json:"reachability_window"`
	// ServeRate is HEALTHY / (HEALTHY + FAULT) over assigned probes in the
	// in-window and grace phases. Probes of validators whose storage the
	// settled promise does not prove are classified UNATTESTED and fall out
	// of both sides of this fraction: see Attestation for how many.
	// ServeRate is HEALTHY / (HEALTHY + FAULT) over probes of an assigned
	// shard while the validator was under obligation. FAULT means the
	// observer reached the validator and it failed to hand over a shard the
	// chain proves it stored. Everything the rate does not speak for is in
	// HeldOut, and ExcludedClasses says why each class is out.
	ServeRate Rate `json:"serve_rate"`
	// Coverage is how much of the rate's own population produced a verdict:
	// (HEALTHY + FAULT) over every probe in that population. A high rate over
	// low coverage is a statement about a handful of probes.
	Coverage Rate `json:"serve_rate_coverage"`
	// Obligations counts one observation per (validator, blob) instead of
	// one per probe, judged by the newest probe: see obligationStats. This is
	// the headline figure and the number a confidence interval may honestly
	// be drawn around; ByObligation repeats its rate.
	Obligations     obligationStats  `json:"obligations"`
	ByObligation    Rate             `json:"serve_rate_by_obligation"`
	HeldOut         map[string]int64 `json:"serve_rate_held_out"`
	ExcludedClasses []excludedClass  `json:"serve_rate_excluded_classes"`
	Attestation     attestationStats `json:"attestation"`
	ProbeCount      int64            `json:"probe_count"` // all probe rows in window
	Classes         classCounts      `json:"classes"`
	// Faults is every FAULT of an assigned shard in the window, in any
	// phase. The serve rate's population is in-window only; corrupt bytes
	// returned in grace are still corrupt bytes, and docs/verdicts.md counts
	// INVALID_ROWS in any phase, so the count is wider than the rate.
	Faults           int64              `json:"faults"`
	Publications     int64              `json:"publications"`
	PublicationBytes int64              `json:"publication_bytes"`
	Reconstructable  reconstructSummary `json:"reconstructable"`
	Gaps             int64              `json:"probe_gaps"` // NOT_PROBED + PROBE_ERROR rows in window
	// GapsByOutcome breaks the gaps down by what actually happened, because
	// they are not all the same thing. RPC_DEADLINE in particular is "the
	// download did not finish in time", and the observer's deadline scales
	// with shard size: a validator that is alive but slow lands there rather
	// than in the rate, and that population is concentrated among exactly the
	// validators most likely to be struggling. Publishing the breakdown is
	// what lets a reader see how big it is.
	GapsByOutcome map[string]int64 `json:"probe_gaps_by_outcome"`
	// VantageHealth is the worst single schedule point in the window: how
	// many distinct validators were unreachable there out of how many were
	// probed. Validators fail independently; this observer's own network does
	// not. A point where nearly every validator was unreachable at once is
	// far more likely to be a route, resolver or peering problem here than
	// twenty operators going down together, and a reader has to be able to
	// see that rather than infer it.
	VantageHealth vantageHealth `json:"vantage_health"`
	// ByPoint is the serve rate per schedule point. The points sit at
	// different fractions of the retention window, so a rate that is fine
	// early and poor late is a different finding from one that is uniformly
	// poor, and the pooled number cannot tell them apart.
	ByPoint []stratum `json:"serve_rate_by_point"`
	// LatencyP50 and LatencyP95 are the network's own service times: the
	// median and 95th percentile of a whole probe, dial to verified rows, over
	// every probe that came back HEALTHY in this window. See the per-validator
	// fields for why this is published without a threshold.
	LatencyP50    *int64 `json:"serve_latency_p50_ms"`
	LatencyP95    *int64 `json:"serve_latency_p95_ms"`
	LatencySample int64  `json:"serve_latency_sample"`

	// ProvisionalFaults is the part of Obligations.Broken still settling
	// (provisional.go); absent when there is none.
	ProvisionalFaults *provisionalFaults `json:"provisional_faults,omitempty"`
}

// latencyWhere returns the median and 95th percentile of a whole probe over the
// rows matching where, and how many rows that is.
func (s *Server) latencyWhere(ctx context.Context, where string, args ...any) (p50, p95 *int64, n int64, err error) {
	var a, b sql.NullInt64
	err = s.st.DB().QueryRowContext(ctx, `SELECT
			MAX(CASE WHEN rn = (c + 1) / 2         THEN ms END),
			MAX(CASE WHEN rn = (c * 95 + 99) / 100 THEN ms END),
			COALESCE(MAX(c), 0)
		FROM (
			SELECT total_duration_ms AS ms,
			       ROW_NUMBER() OVER (ORDER BY total_duration_ms) AS rn,
			       COUNT(*)     OVER ()                           AS c
			FROM probes WHERE `+where+`
			  AND classification = 'HEALTHY' AND total_duration_ms > 0
		)`, args...).Scan(&a, &b, &n)
	if err != nil {
		return nil, nil, 0, err
	}
	if a.Valid {
		x := a.Int64
		p50 = &x
	}
	if b.Valid {
		x := b.Int64
		p95 = &x
	}
	return p50, p95, n, nil
}

func (s *Server) classCountsWhere(ctx context.Context, where string, args ...any) (classCounts, int64, error) {
	// The effective class, not the stored one: a row whose deadline this
	// observer cannot vouch for publishes no serve verdict. It is an
	// override on the same row, so the tally still covers exactly the
	// population `where` selects and coverage.den is unchanged.
	rows, err := s.st.DB().QueryContext(ctx, `SELECT `+rollup.EffectiveClass("")+`, COUNT(*) FROM probe_rows WHERE `+where+` GROUP BY `+rollup.EffectiveClass(""), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := classCounts{}
	var total int64
	for rows.Next() {
		var c string
		var n int64
		if err := rows.Scan(&c, &n); err != nil {
			return nil, 0, err
		}
		out[c] = n
		total += n
	}
	return out, total, rows.Err()
}

// attestationStats discloses what the serve rate left out. A validator is
// only obliged to serve a blob it stored, and the only on-chain proof it
// stored one is a signature on the settled promise that this observer
// verified against the validator's consensus key. Where that proof is
// missing the probe is classified UNATTESTED and excluded from the serve
// rate — in both directions, so neither a success nor a failure can move a
// rate the validator was never proven to owe.
type attestationStats struct {
	// Attested and Unattested are probes of assigned validators in the
	// in-window and grace phases: proven obliged, and not proven obliged.
	Attested   int64 `json:"attested_probes"`
	Unattested int64 `json:"unattested_probes"`
	// Coverage is Attested / (Attested + Unattested). Well below 1 means the
	// publisher stopped collecting signatures once it had a safe quorum, so
	// most of the set is unproven and the serve rate speaks for a minority.
	Coverage Rate `json:"coverage"`
	// Unknown counts probes from records written before the observer verified
	// signatures. Their attestation is absent, not negative, so they stay in
	// the serve rate under the older taxonomy and out of Coverage.
	Unknown int64 `json:"unknown_probes"`

	// The same three counts per (validator, blob) obligation rather than per
	// probe, which is the unit an operator reads them in.
	//
	// Each obligation is probed at four schedule points, so the probe counts
	// above run about four times these. A page that told an operator "812
	// unattested" when the true statement is "203 blobs carried no signature
	// from you" would have multiplied its own evidence by the size of a
	// schedule the reader cannot see, and the figure it multiplied is the one
	// most likely to be misread as an accusation. An obligation is counted
	// attested if any probe of it carries verified proof, so a mix of NULL and
	// 1 is proven rather than unknown.
	AttestedBlobs   int64 `json:"attested_blobs"`
	UnattestedBlobs int64 `json:"unattested_blobs"`
	UnknownBlobs    int64 `json:"unknown_blobs"`
	// BlobCoverage is AttestedBlobs / (AttestedBlobs + UnattestedBlobs). It is
	// not the same number as Coverage: obligations differ in how many times
	// they were probed, so the probe-counted ratio silently weights an
	// obligation by its probe count.
	BlobCoverage Rate `json:"blob_coverage"`
}

// excludedFromRate names the classes published beside the serve rate rather
// than inside it, with the reason each one is out. It lives in one place so a
// page cannot describe the exclusions differently from the API.
//
// The grace phase is outside the rate's population for the same kind of
// reason: a grace probe can only ever add HEALTHY, since NOT_FOUND and
// unreachability there are TOLERATED by design. Including it gave a validator
// that prunes promptly a lower rate than one that over-retains, with
// identical in-window behaviour, and the "worst first" table sorts on exactly
// that axis. Grace probes are still recorded and still shown; they just do
// not move a retention rate.
var excludedFromRate = []excludedClass{
	{"UNATTESTED", "no verified endorsement from this validator: nothing proves it stored the shard"},
	{"UNREACHABLE", "no answer" + endNotServed},
	{"NOT_REGISTERED", "no Fibre host in x/valaddr" + endNotServed},
	{"SHADOWED_SHARD", "genuine rows of the blob, another settled promise's set" + endServed},
	{"UNMATCHED_GENUINE", "genuine rows of the blob, no settled promise's set" + endServed},
	{"IDENTITY_EXPIRED", "certificate outside its signed validity window" + endNotServed},
	{"IDENTITY_MISMATCH", "certificate not endorsed by this validator's consensus key" + endNotServed},
	{"SERVER_ERROR", "an application error instead of the shard" + endNotServed},
	{"THROTTLED", "refused with a rate limit" + endNotServed},
	{"NOT_PROBED", "not read by this observer: a gap, never a zero"},
	{"PROBE_ERROR", "this observer's own reading failed"},
	{"RETENTION_UNVERIFIED", "must_serve_until is unknown until an x/fibre params range is read (param_uncertainty in /v1/meta); served and not served are both withheld"},
}

// What the end-of-window reading makes of a class in obligations
// (probe.EndReadClass); serve_rate itself counts classes as they happened.
const (
	endNotServed = "; in obligations, at the end reading, not served"
	endServed    = "; in obligations, at the end reading, served"
)

type excludedClass struct {
	Class  string `json:"class"`
	Reason string `json:"reason"`
}

// serveRate is HEALTHY over HEALTHY + FAULT, where FAULT means the observer
// reached the validator and it failed to hand over a shard it was proven to
// hold. Every other class is published under its own name beside the rate.
func serveRate(c classCounts) Rate {
	return rate(c["HEALTHY"], c["HEALTHY"]+c["FAULT"])
}

// obligationStats counts one observation per (validator, blob) rather than
// one per probe, and says what became of each.
//
// The schedule visits the same validator and blob four times in window, and
// every bonded validator is assigned every blob because of the minimum-rows
// floor, so the probes inside one obligation are near-perfectly correlated: a
// certificate that lapsed, or a disk that lost a shard, produces four FAULT
// rows for one event. Counting those as four independent trials makes any
// confidence interval far narrower than the evidence supports, which is the
// wrong error to make under a public accusation.
//
// The verdict on an obligation is the newest probe of it, the same rule the
// per-blob reconstructability verdict uses. "Kept when no probe faulted" was
// the old rule, and it let a validator that served at the first point and
// answered 500 at the next three count as fully kept: that is the profile of
// a server that pruned early, and the one this observer exists to notice.
//
//	served         newest probe HEALTHY, no fault anywhere, and one of the
//	               HEALTHY readings taken in the tail of the retention window
//	               (verdict.EndSegmentDivisor)
//	broken         any probe FAULT
//	end_unobserved a HEALTHY probe, but not one that speaks for the end of
//	               the window: the newest probe produced no verdict, or every
//	               reading was taken too early to say the shard survived
//	unobserved     never seen serving, and never faulted, split by what the
//	               probes did see: the endpoint completed TLS and still
//	               handed nothing over; it never completed TLS; or this
//	               observer never attempted the download (backoff, budget,
//	               a slot that elapsed)
//	pending        the retention window has not ended, so the newest probe
//	               is not the last one; no verdict yet
//
// Only served and broken enter the rate. The rest is published beside it so a
// reader can see how many obligations the rate does not speak for.
//
// The two are not symmetric, and the asymmetry is the point. A FAULT is
// conclusive from a single reading: the shard was gone at that minute. A
// serve is a claim about a window, so it needs a reading near the end of one.
// An obligation this observer watched early and then lost sight of is
// therefore end_unobserved rather than served — it leaves the rate instead of
// padding it, which is why an outage here lowers the number of obligations
// the rate speaks for instead of raising the rate. The direction of the
// remaining bias is worth stating plainly: while this observer is blind, the
// obligations it can still judge are enriched for faults, because faults need
// less evidence than serves do.
//
// The population is obligations the settled promise proves (attested = 1):
// an unattested one is nothing to keep or break, and a record from before
// signatures were verified (attested NULL) is not evidence either way, so it
// is outside this count and reported under attestation.unknown. An
// obligation belongs to a window by its publication's settlement time, not
// by each probe's time, so an obligation is judged whole or not at all: a
// window cut through the middle of one would decide it on half its probes.
// The window's end is the moment the verdict is drawn (as_of); an obligation
// whose must_serve_until is later than that is pending.
type obligationStats struct {
	Total         int64 `json:"total"`
	Served        int64 `json:"served"`
	Broken        int64 `json:"broken"`
	EndUnobserved int64 `json:"end_unobserved"`
	// HeldParamUnverified is obligations whose only serve evidence sits
	// inside an x/fibre params range this observer has not read every
	// height of, so it publishes neither the fault nor the credit. It is
	// deliberately not folded into Unobserved: the observer looked, and
	// cannot speak for what it saw.
	HeldParamUnverified   int64 `json:"held_param_unverified"`
	Unobserved            int64 `json:"unobserved"`
	UnobservedReachable   int64 `json:"unobserved_reachable"`
	UnobservedUnreachable int64 `json:"unobserved_unreachable"`
	UnobservedNotProbed   int64 `json:"unobserved_not_probed"`
	Pending               int64 `json:"pending"`
	// Rate is served / (served + broken).
	Rate Rate `json:"rate"`
}

// obligationBuckets is the per-obligation reduction the two obligation
// queries share: one row per (validator, blob), with the newest probe's class
// and what the other probes saw. A gap row (NOT_PROBED, PROBE_ERROR) never
// becomes the newest probe while a real one exists, so a slot this observer
// missed does not turn a served obligation into an unobserved one.
//
// Arguments, in order: as_of (pending cut), window start (settlement_time),
// then whatever the caller appends (suspect points, a validator filter).
// The obligation SQL lives in observer/rollup, which computes the daily
// rollups with the same statements; see rollup.ObligationBuckets.
var (
	obligationBuckets = rollup.ObligationBuckets
	obligationSums    = rollup.ObligationSums
)

func (o *obligationStats) finish() {
	o.Unobserved = o.UnobservedReachable + o.UnobservedUnreachable + o.UnobservedNotProbed
	o.Rate = rate(o.Served, o.Served+o.Broken)
}

// obligationArgs is the argument list obligationBuckets expects: as_of (the
// pending cut), the window's settlement bounds, the row upper and lower
// bounds, the suspect points, then the caller's own.
//
// Past the first prune the "all" window's raw part starts at raw_from:
// the rollup holds every obligation of a promise settled before it (the
// rollup attributes obligations by settlement day, rows by start day), and
// a promise settled late on a rolled day can still have rows that started
// on a retained day. Without the bound those rows would count the
// obligation a second time, beside the rollup's.
func (s *Server) obligationArgs(win Window, ss suspectSet, extra ...any) []any {
	start := win.startArg()
	if win.Span == 0 {
		if from, ok := rollup.RawFrom(s.st); ok {
			start = store.TS(from)
		}
	}
	args := []any{store.TS(win.End), start, win.endArg(), win.endArg(), rollup.RowLowerBound(start)}
	args = append(args, ss.args...)
	return append(args, extra...)
}

// obligationsWhere reduces the window's proven obligations to buckets. extra
// is appended to the WHERE clause (a validator filter), its arguments last.
func (s *Server) obligationsWhere(ctx context.Context, win Window, ss suspectSet, extra string, extraArgs ...any) (obligationStats, error) {
	var o obligationStats
	err := s.st.DB().QueryRowContext(ctx, `SELECT `+obligationSums+` FROM (`+obligationBuckets+ss.clause("pr.scheduled_at")+extra+`)
			GROUP BY validator_address, promise_hash)`, s.obligationArgs(win, ss, extraArgs...)...).
		Scan(&o.Total, &o.Broken, &o.Served, &o.EndUnobserved, &o.HeldParamUnverified, &o.UnobservedReachable, &o.UnobservedUnreachable, &o.UnobservedNotProbed, &o.Pending)
	if err != nil {
		return obligationStats{}, err
	}
	o.finish()
	return o, nil
}

// obligationsByValidator is obligationsWhere grouped by validator.
func (s *Server) obligationsByValidator(ctx context.Context, win Window, ss suspectSet, extra string, extraArgs ...any) (map[string]obligationStats, error) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT validator_address, `+obligationSums+` FROM (`+obligationBuckets+ss.clause("pr.scheduled_at")+extra+`)
			GROUP BY validator_address, promise_hash) GROUP BY validator_address`, s.obligationArgs(win, ss, extraArgs...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]obligationStats{}
	for rows.Next() {
		var addr string
		var o obligationStats
		if err := rows.Scan(&addr, &o.Total, &o.Broken, &o.Served, &o.EndUnobserved, &o.HeldParamUnverified, &o.UnobservedReachable, &o.UnobservedUnreachable, &o.UnobservedNotProbed, &o.Pending); err != nil {
			return nil, err
		}
		o.finish()
		out[addr] = o
	}
	return out, rows.Err()
}

// vantageHealth reports the correlated failures the window contains, and
// which schedule points every rate leaves out because of them.
type vantageHealth struct {
	// WorstPoint is the fraction unreachable at the worst schedule point.
	WorstPoint Rate `json:"worst_point"`
	// At and Label identify that point, so it can be looked up in /v1/probes.
	At    string `json:"at,omitempty"`
	Label string `json:"label,omitempty"`
	// Correlated is true when that fraction is at or above the threshold
	// below, which is the observer saying it does not trust its own reading
	// at that point.
	Correlated bool `json:"correlated"`
	// Threshold and FaultThreshold are published so the judgement is not a
	// hidden constant.
	Threshold      float64 `json:"threshold"`
	FaultThreshold float64 `json:"fault_threshold"`
	// MinValidators is the floor on how many validators must share the
	// failure before a share means anything: one of two is half.
	MinValidators int64 `json:"min_validators"`
	// Suspect lists every schedule point in the window at which the share
	// of validators unreachable, or the share faulting, reached its
	// threshold. Every probe row at those points is left out of the serve
	// rate, the obligation buckets, the per-point breakdown and the fault
	// count: validators fail independently and one observer's network, or
	// one observer's stale assignment, does not. SuspectRows is how many
	// rows that removed.
	Suspect     []suspectPoint `json:"suspect"`
	SuspectRows int64          `json:"suspect_rows"`
}

// suspectPoint is one schedule point the observer does not trust itself at.
type suspectPoint struct {
	At          string `json:"at"`
	Label       string `json:"label"`
	Validators  int64  `json:"validators"`
	Unreachable Rate   `json:"unreachable"`
	Fault       Rate   `json:"fault"`
	// Reason is "unreachable", "fault" or "unreachable,fault".
	Reason string `json:"reason"`
}

// suspectSet is the SQL side of vantageHealth.Suspect: the clause that drops
// those points from a population query, and its arguments.
type suspectSet struct {
	points []suspectPoint
	args   []any
}

// clause is " AND <col> NOT IN (?, ...)" or "" when nothing is suspect.
func (ss suspectSet) clause(col string) string {
	if len(ss.args) == 0 {
		return ""
	}
	return " AND " + col + " NOT IN (?" + strings.Repeat(", ?", len(ss.args)-1) + ")"
}

// excludeSet is the validator exclusion a reader asks for with `?exclude=`.
//
// This observer's own operator runs a validator on the network it measures,
// which is a conflict a reader should be able to check rather than take on
// trust. The check is a query filter: leave the
// validator in the tables, and let anyone recompute the headline figures
// without it. So nothing is excluded by default, the exclusion is asked for
// per request rather than configured on the deployment, and what a request
// excluded is echoed in its answer.
//
// It applies to every per-validator measurement population: the class counts,
// the faults, the obligations, attestation coverage, latency, both
// reachability figures, the probe and gap counts, the per-point rate, and the
// previous window the deltas compare against.
//
// Two figures it deliberately leaves whole, because excluding a validator
// from either would answer a different question than the one asked:
//
//   - the correlated-failure guard (vantage_health.suspect), which is a
//     statement about this observer's own minute rather than about any
//     validator: dropping one from the share would change which points this
//     observer distrusts itself at;
//   - reconstructability, which asks whether a blob could still be rebuilt
//     from the rows that came back. Removing a validator's rows lowers that
//     for real, so "the network without you" is not the network's actual
//     recoverability, and printing it as such would understate the thing the
//     figure exists to measure.
//
// Both are said in the response (exclude_note) rather than left to be
// discovered.
type excludeSet struct{ addrs []any }

func (e excludeSet) on() bool { return len(e.addrs) > 0 }

// has reports whether one validator is excluded, for the few figures reduced
// in Go rather than in SQL.
func (e excludeSet) has(addr string) bool {
	for _, a := range e.addrs {
		if a == addr {
			return true
		}
	}
	return false
}

// clause is " AND <col> NOT IN (?, ...)" or "" when nothing is excluded.
func (e excludeSet) clause(col string) string {
	if len(e.addrs) == 0 {
		return ""
	}
	return " AND " + col + " NOT IN (?" + strings.Repeat(", ?", len(e.addrs)-1) + ")"
}

// args appends the exclusion's arguments after base, matching clause being
// appended last in every query that carries it.
func (e excludeSet) args(base ...any) []any {
	if len(e.addrs) == 0 {
		return base
	}
	return append(append([]any{}, base...), e.addrs...)
}

// ExcludeNote is what the response says about a filtered answer, so a figure
// cannot be quoted as this observer's headline without the qualifier.
const ExcludeNote = "Figures exclude the validators named in `excluded`. The correlated-failure guard and reconstructability are computed over the whole set, because excluding a validator from either would answer a different question."

// parseExclude reads `?exclude=`, which may be repeated or comma-separated,
// and takes either form of address the rest of the API takes.
func parseExclude(r *http.Request) (excludeSet, []string, error) {
	var ex excludeSet
	var names []string
	seen := map[string]bool{}
	for _, raw := range r.URL.Query()["exclude"] {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			addr, err := parseAddr(part)
			if err != nil {
				return excludeSet{}, nil, fmt.Errorf("exclude %q: address must be 40 hex chars or celestiavalcons1...", part)
			}
			if seen[addr] {
				continue
			}
			seen[addr] = true
			ex.addrs = append(ex.addrs, addr)
			names = append(names, addr)
		}
	}
	if len(ex.addrs) > maxExclude {
		return excludeSet{}, nil, fmt.Errorf("exclude takes at most %d validators", maxExclude)
	}
	return ex, names, nil
}

// maxExclude bounds the filter. It exists to answer "recompute without the
// people who run this site", not to let a caller carve an arbitrary subset
// out of the network and quote the result as this observer's figure.
const maxExclude = 8

// correlatedUnreachableThreshold: at or above this share of the validators
// probed at one schedule point being unreachable, the likeliest explanation
// is this observer's own network rather than that many independent
// operators. correlatedFaultThreshold is the same judgement for faults: half
// the set losing data at the same minute is not a finding about the set, it
// is a finding about the observer (a stale assignment pin, a broken coder).
//
// correlatedMinValidators is the floor under which a share is not a signal:
// one validator of two failing is half the set and an ordinary Tuesday.
const (
	correlatedUnreachableThreshold = verdict.UnreachableThreshold
	correlatedFaultThreshold       = verdict.FaultThreshold
	correlatedMinValidators        = verdict.MinValidators
)

// suspectPoints finds every schedule point in the window at which the share
// of distinct validators unreachable, or faulting, reached its threshold,
// among points where more than one validator was probed. The points are
// judged over the whole set at that minute, so the same point is suspect on
// every page and for every validator.
func (s *Server) suspectPoints(ctx context.Context, win Window) (vantageHealth, suspectSet, error) {
	out := vantageHealth{Threshold: correlatedUnreachableThreshold, FaultThreshold: correlatedFaultThreshold,
		MinValidators: correlatedMinValidators, Suspect: []suspectPoint{}}
	var ss suspectSet
	pts, err := rollup.SuspectPoints(ctx, s.st.DB(), `started_at >= ? AND started_at <= ?`, win.startArg(), win.endArg())
	if err != nil {
		return out, ss, err
	}
	var best float64
	for _, p := range pts {
		if p.Validators == 0 {
			continue
		}
		fu := float64(p.Unreachable) / float64(p.Validators)
		if fu > best || out.At == "" {
			best = fu
			out.WorstPoint = rate(p.Unreachable, p.Validators)
			out.At, out.Label = p.At, p.Label
		}
		reason := p.Reason()
		if reason == "" {
			continue
		}
		out.Suspect = append(out.Suspect, suspectPoint{At: p.At, Label: p.Label, Validators: p.Validators,
			Unreachable: rate(p.Unreachable, p.Validators), Fault: rate(p.Faulted, p.Validators), Reason: reason})
		out.SuspectRows += p.Rows
		ss.args = append(ss.args, p.At)
	}
	out.Correlated = out.WorstPoint.Den > 0 && best >= correlatedUnreachableThreshold && out.WorstPoint.Num >= correlatedMinValidators
	ss.points = out.Suspect
	return out, ss, nil
}

// byPoint breaks a rate down by schedule point. The four in-window points are
// deliberately packed toward the deadline (0.12, 0.45, 0.72, 0.92 of the
// window), so they are not interchangeable: a validator that prunes early
// fails late points and passes early ones, and a validator with a broken disk
// fails all four. Pooling them hides which of those two a low rate is, and
// two validators probed over different mixes of blob sizes and points can
// have their pooled rates reverse relative to their per-stratum ones. The
// breakdown is published so a reader can look rather than assume.
func (s *Server) rateByPoint(ctx context.Context, where string, args ...any) ([]stratum, error) {
	// ObligationClass: an end-of-window reading is rated at its point the way
	// the obligations count it.
	rows, err := s.st.DB().QueryContext(ctx, `SELECT schedule_label,
			COALESCE(SUM(CASE WHEN `+rollup.ObligationClass("")+` = 'HEALTHY' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN `+rollup.ObligationClass("")+` = 'FAULT' THEN 1 ELSE 0 END), 0)
		FROM probe_rows WHERE `+where+` GROUP BY schedule_label ORDER BY schedule_label`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stratum{}
	for rows.Next() {
		var st stratum
		var ok, bad int64
		if err := rows.Scan(&st.Key, &ok, &bad); err != nil {
			return nil, err
		}
		st.Rate = rate(ok, ok+bad)
		out = append(out, st)
	}
	return out, rows.Err()
}

// stratum is one slice of a rate's population, with the rate over that slice.
type stratum struct {
	Key  string `json:"key"`
	Rate Rate   `json:"serve_rate"`
}

// coverage is how much of the rate's own population produced a verdict.
// Without it a reader cannot tell a rate resting on twelve probes from one
// resting on four hundred scheduled slots, and the probe and gap counts
// published next to it are over a different population entirely.
func coverage(c classCounts) Rate {
	var rated, all int64
	for cls, n := range c {
		all += n
		if cls == "HEALTHY" || cls == "FAULT" {
			rated += n
		}
	}
	return rate(rated, all)
}

// heldOut counts, per excluded class, how many probes of this population the
// rate does not speak for.
func heldOut(c classCounts) map[string]int64 {
	out := map[string]int64{}
	for _, e := range excludedFromRate {
		if n := c[e.Class]; n > 0 {
			out[e.Class] = n
		}
	}
	return out
}

// attestationWhere counts proven, unproven and unknown obligations over the
// probe rows matching where.
func (s *Server) attestationWhere(ctx context.Context, where string, args ...any) (attestationStats, error) {
	var st attestationStats
	err := s.st.DB().QueryRowContext(ctx, `SELECT
			COALESCE(SUM(CASE WHEN attested = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN attested = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN attested IS NULL THEN 1 ELSE 0 END), 0)
		FROM probe_rows WHERE `+where, args...).Scan(&st.Attested, &st.Unattested, &st.Unknown)
	if err != nil {
		return attestationStats{}, err
	}
	st.Coverage = rate(st.Attested, st.Attested+st.Unattested)

	// The same over obligations. MAX ignores NULLs in SQLite and in Postgres,
	// so an obligation with any verified evidence resolves to that evidence
	// and only one with no evidence at all stays unknown.
	err = s.st.DB().QueryRowContext(ctx, `SELECT
			COALESCE(SUM(CASE WHEN a = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a IS NULL THEN 1 ELSE 0 END), 0)
		FROM (
			SELECT MAX(attested) AS a FROM probe_rows WHERE `+where+`
			GROUP BY validator_address, promise_hash
		)`, args...).Scan(&st.AttestedBlobs, &st.UnattestedBlobs, &st.UnknownBlobs)
	if err != nil {
		return attestationStats{}, err
	}
	st.BlobCoverage = rate(st.AttestedBlobs, st.AttestedBlobs+st.UnattestedBlobs)
	return st, nil
}

func (s *Server) handleNetwork(w http.ResponseWriter, r *http.Request) {
	win, err := parseWindow(r, time.Now())
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	ex, excluded, err := parseExclude(r)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if win.AsOf || ex.on() {
		// A pinned window, or one a reader asked to recompute without named
		// validators, is computed on demand, uncached and rationed: by
		// arrivals (allow) and by work in flight (enter). Neither may reach
		// the shared snapshot — one reader's filter is not everyone's
		// headline, and the cache is keyed by window alone.
		if !s.asOf.allow(time.Now()) {
			w.Header().Set("Retry-After", "2")
			writeErr(w, 429, "as_of and exclude requests are limited to one every two seconds")
			return
		}
		if !s.asOf.enter() {
			w.Header().Set("Retry-After", "5")
			writeErr(w, 429, "uncached computations already in flight; try again shortly")
			return
		}
		defer s.asOf.leave()
		t0 := time.Now()
		resp, err := s.computeNetwork(r.Context(), win, ex, excluded)
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		resp.RecordThrough = s.recordThrough(r.Context())
		resp.ComputedAt, resp.ComputeMs = t0.UTC().Format(time.RFC3339Nano), time.Since(t0).Milliseconds()
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, 200, resp)
		return
	}
	resp, at, ms, err := s.net.get(r.Context(), s.logf(), win)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	// A copy, so a reader cannot mutate the cached snapshot and two concurrent
	// readers cannot race on it.
	out := *resp
	out.ComputedAt, out.ComputeMs = at.UTC().Format(time.RFC3339Nano), ms
	writeJSON(w, 200, &out)
}

// logf adapts the server's logger, which may be absent in tests, to what the
// snapshot cache needs.
func (s *Server) logf() logf {
	if s.log == nil {
		return nil
	}
	return func(format string, args ...any) { s.log.Printf(format, args...) }
}

// computeNetwork does the work handleNetwork used to do inline. It is called
// from the snapshot cache rather than from the request, so its context outlives
// the reader who triggered it.
// previousWindow holds the few figures a delta compares against: the same
// span ending where the window starts, computed with the same statements,
// its own suspect points left out.
type previousWindow struct {
	Window       Window          `json:"window"`
	Obligations  obligationStats `json:"obligations"`
	Reachability Rate            `json:"reachability_window"`
	Faults       int64           `json:"faults"`
	LatencyP50   *int64          `json:"serve_latency_p50_ms"`
}

func (s *Server) previousWindow(ctx context.Context, win Window, ex excludeSet) (*previousWindow, error) {
	if win.Span <= 0 || win.AsOf {
		return nil, nil
	}
	prev := Window{Name: win.Name, Span: win.Span, Start: win.Start.Add(-win.Span), End: win.Start}
	_, ss, err := s.suspectPoints(ctx, prev)
	if err != nil {
		return nil, err
	}
	p := &previousWindow{Window: prev}
	if p.Obligations, err = s.obligationsWhere(ctx, prev, ss, ex.clause("pr.validator_address"), ex.addrs...); err != nil {
		return nil, err
	}
	db := s.st.DB()
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM probes WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND `+rollup.EffectiveClass("")+` = 'FAULT'`+ss.clause("scheduled_at")+ex.clause("validator_address"),
		ex.args(append([]any{prev.startArg(), prev.endArg()}, ss.args...)...)...).Scan(&p.Faults); err != nil {
		return nil, err
	}
	var beats, beatsUp int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),
			COALESCE(SUM(CASE WHEN tcp_ok = 1 AND tls_ok = 1 THEN 1 ELSE 0 END), 0)
		FROM reachability WHERE started_at >= ? AND started_at <= ? AND outcome <> 'PROBE_ERROR' AND +vantage = ?`, prev.startArg(), prev.endArg(), s.vantage).Scan(&beats, &beatsUp); err != nil {
		return nil, err
	}
	p.Reachability = rate(beatsUp, beats)
	if p.LatencyP50, _, _, err = s.latencyWhere(ctx,
		`started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`, prev.startArg(), prev.endArg()); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Server) computeNetwork(ctx context.Context, win Window, ex excludeSet, excluded []string) (*networkResponse, error) {
	db := s.st.DB()
	var resp networkResponse
	resp.Window, resp.Vantage = win, s.vantage
	resp.ObservedFromOneVantage = s.vantageCount(ctx) == 1
	if ex.on() {
		resp.Excluded, resp.ExcludeNote = excluded, ExcludeNote
	}
	// Appended last in every population below, so its arguments go last too.
	exv := ex.clause("validator_address")

	if win.AsOf {
		resp.AsOfNote = AsOfNote
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM endpoints WHERE first_seen_at <= ? AND (closed_at IS NULL OR closed_at > ?)`, win.endArg(), win.endArg()).Scan(&resp.RegisteredEndpoints)
	} else {
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM endpoints WHERE closed_at IS NULL`).Scan(&resp.RegisteredEndpoints)
	}
	_ = db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT validator_address) FROM probe_rows WHERE started_at >= ? AND started_at <= ?`+exv,
		ex.args(win.startArg(), win.endArg())...).Scan(&resp.ValidatorsProbed)

	// The points this observer does not trust itself at come first: every
	// population below leaves them out.
	vh, ss, err := s.suspectPoints(ctx, win)
	if err != nil {
		return nil, err
	}
	resp.VantageHealth = vh
	pop := `started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'` + ss.clause("scheduled_at") + exv
	popArgs := ex.args(append([]any{win.startArg(), win.endArg()}, ss.args...)...)

	classes, total, err := s.classCountsWhere(ctx, pop, popArgs...)
	if err != nil {
		return nil, err
	}
	resp.Classes = classes
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM probes WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND `+rollup.EffectiveClass("")+` = 'FAULT'`+ss.clause("scheduled_at")+exv, popArgs...).Scan(&resp.Faults)
	resp.ServeRate = serveRate(classes)
	resp.Coverage = coverage(classes)
	resp.HeldOut = heldOut(classes)
	resp.ExcludedClasses = excludedFromRate
	resp.RetentionUncertainty = s.retentionUncertaintyNow(ctx)
	if resp.Obligations, err = s.obligationsWhere(ctx, win, ss, ex.clause("pr.validator_address"), ex.addrs...); err != nil {
		return nil, err
	}
	resp.ByObligation = resp.Obligations.Rate
	prov, err := s.provisionalByValidator(ctx, win, ss, time.Now(), ex.clause("pr.validator_address"), ex.addrs...)
	if err != nil {
		return nil, err
	}
	resp.ProvisionalFaults = provisionalTotal(prov)
	_ = total
	// The same population the class tally above was drawn from, suspect
	// points and all. Without the exclusion here the two were counts of
	// different row sets, so serve_rate_coverage.den stopped equalling
	// attested + unattested + unknown and serve_rate_held_out.UNATTESTED
	// stopped equalling attestation.unattested_probes — an unexplained
	// gap for anyone reconciling the response against itself, which is
	// exactly what a reader checking this observer's arithmetic does.
	if resp.Attestation, err = s.attestationWhere(ctx,
		`started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`+ss.clause("scheduled_at")+exv, popArgs...); err != nil {
		return nil, err
	}
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM probe_rows WHERE started_at >= ? AND started_at <= ?`+exv, ex.args(win.startArg(), win.endArg())...).Scan(&resp.ProbeCount)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM probe_rows WHERE started_at >= ? AND started_at <= ? AND classification IN ('NOT_PROBED','PROBE_ERROR')`+exv, ex.args(win.startArg(), win.endArg())...).Scan(&resp.Gaps)
	resp.GapsByOutcome = map[string]int64{}
	if grows, gerr := db.QueryContext(ctx,
		`SELECT outcome, COUNT(*) FROM probe_rows WHERE started_at >= ? AND started_at <= ? AND classification IN ('NOT_PROBED','PROBE_ERROR')`+exv+` GROUP BY outcome`,
		ex.args(win.startArg(), win.endArg())...); gerr == nil {
		for grows.Next() {
			var o string
			var n int64
			if err := grows.Scan(&o, &n); err == nil {
				resp.GapsByOutcome[o] = n
			}
		}
		grows.Close()
	}

	if resp.ByPoint, err = s.rateByPoint(ctx, pop, popArgs...); err != nil {
		return nil, err
	}
	if resp.LatencyP50, resp.LatencyP95, resp.LatencySample, err = s.latencyWhere(ctx,
		`started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`+exv, ex.args(win.startArg(), win.endArg())...); err != nil {
		return nil, err
	}

	reach, err := s.reachabilityNow(ctx, "", win.asOfArg())
	if err != nil {
		return nil, err
	}
	// The census is over the validators that advertise a Fibre endpoint
	// right now, not over every validator this observer has ever probed.
	// Without the restriction the denominator only ever grew — an operator
	// that left the bonded provider list months ago still counted, with its
	// last handshake frozen — and the numerator was printed on the overview
	// beside registered_endpoints, a count from a different population, so
	// it could read "79 registered endpoints; 80 answering". Both figures
	// are published here as one rate, num over den, so the page cannot pair
	// them with anything else.
	registered, err := s.registeredValidators(ctx, win)
	if err != nil {
		return nil, err
	}
	var reachable, census int64
	for a, v := range reach {
		if !registered[a] || ex.has(a) {
			continue
		}
		census++
		if v.up() {
			reachable++
		}
	}
	resp.Reachability = rate(reachable, census)

	// This observer's own heartbeats only, here and in every other figure
	// over the table: another vantage's copied rows confirm or contradict a
	// failed check (reachabilityNow) and are counted in no rate. The unary
	// plus keeps the vantage term off reachability_vantage, which would
	// otherwise be chosen for the equality and walk every row this observer
	// ever wrote instead of the window's span of reachability_started.
	var beats, beatsUp int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),
			COALESCE(SUM(CASE WHEN tcp_ok = 1 AND tls_ok = 1 THEN 1 ELSE 0 END), 0)
		FROM reachability WHERE started_at >= ? AND started_at <= ? AND outcome <> 'PROBE_ERROR' AND +vantage = ?`+exv,
		ex.args(win.startArg(), win.endArg(), s.vantage)...).Scan(&beats, &beatsUp); err != nil {
		return nil, err
	}
	resp.ReachabilityWindow = rate(beatsUp, beats)

	_ = db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(blob_size),0) FROM publications WHERE settlement_time >= ? AND settlement_time <= ?`, win.startArg(), win.endArg()).Scan(&resp.Publications, &resp.PublicationBytes)

	recon, err := s.reconstructableCount(ctx, win)
	if err != nil {
		return nil, err
	}
	resp.Reconstructable = recon

	// Past the raw retention the "all" window rests on the daily rollup
	// for the pruned days: fold it in and say so.
	rolled, label, err := s.rolledFor(ctx, win, "")
	if err != nil {
		return nil, err
	}
	rolled = rolled.Without(excluded)
	if rolled != nil {
		resp.RolledUp = label
		for c, n := range rolled.Classes {
			resp.Classes[c] += n
		}
		resp.ServeRate = serveRate(resp.Classes)
		resp.Coverage = coverage(resp.Classes)
		resp.HeldOut = heldOut(resp.Classes)
		// The attestation counts move with the classes, over the same rows.
		// Folding the classes in and leaving these behind broke the identity
		// docs/verdicts.md tells a reader to check the answer against —
		// coverage.den equals attested + unattested + unknown — on exactly
		// the window they would check it on.
		resp.Attestation.Attested += rolled.Attested
		resp.Attestation.Unattested += rolled.Unattested
		resp.Attestation.Unknown += rolled.UnknownAtt
		resp.Attestation.Coverage = rate(resp.Attestation.Attested, resp.Attestation.Attested+resp.Attestation.Unattested)
		resp.Faults += rolled.Faults
		resp.ProbeCount += rolled.Probes
		resp.Gaps += rolled.Gaps
		addRolledObligations(&resp.Obligations, rolled.Obligations)
		resp.ByObligation = resp.Obligations.Rate
		resp.ReachabilityWindow = rate(beatsUp+rolled.BeatsUp, beats+rolled.Beats)
		// validators probed: the raw set and the rolled set together
		seen := map[string]bool{}
		for a := range rolled.ProbesByVal {
			seen[a] = true
		}
		if vrows, err := db.QueryContext(ctx, `SELECT DISTINCT validator_address FROM probe_rows WHERE started_at >= ? AND started_at <= ?`+exv, ex.args(win.startArg(), win.endArg())...); err == nil {
			for vrows.Next() {
				var a string
				if vrows.Scan(&a) == nil {
					seen[a] = true
				}
			}
			vrows.Close()
		}
		resp.ValidatorsProbed = int64(len(seen))
	}
	if resp.Previous, err = s.previousWindow(ctx, win, ex); err != nil {
		return nil, err
	}
	return &resp, nil
}

// reachState is the latest reachability evidence for one validator: the most
// recent heartbeat or probe row, whichever is newer.
type reachState struct {
	at             string
	host           string
	reachable      bool // TCP and TLS ok
	tcpOK          bool
	tlsOK          bool
	identityOK     bool
	identityReason string
	source         string // heartbeat | probe
	// flaky: the newest check failed but the one before it, on the same
	// host, succeeded. One timeout is not an outage (a 10s TLS deadline
	// meets a transient path loss often enough), so the endpoint still
	// counts as up, is published as reachable, and keeps the identity
	// verdict of that last good check. It is down after two failures in a
	// row.
	flaky bool
	// confirmedFrom names the other vantage whose recent check of the same
	// host completed TCP and TLS while this observer's own checks were
	// failing: the endpoint is up and the fault is on this observer's path.
	// reachable is then set, with that check's identity verdict.
	// alsoFailedFrom names the other vantage whose recent check failed too.
	// Both empty when no other vantage checked the host recently.
	confirmedFrom  string
	alsoFailedFrom string
}

// confirmWithin is how recent another vantage's check must be to confirm or
// contradict this observer's failing one: three heartbeats at five minutes.
const confirmWithin = 15 * time.Minute

// up is the debounced answer: reachable now, or failed only once since.
func (r reachState) up() bool { return r.reachable || r.flaky }

// reachabilityNow returns the latest evidence per validator, or for just one
// when only is set: a request about a single validator has no reason to walk
// the whole set, and the detail page is the caller that asks for one.
// registeredValidators is the set of validators with an open Fibre endpoint,
// as of the window's end when it is pinned and as of now otherwise — the
// same population registered_endpoints is counted over.
// registeredValidators is the set of validators with an open Fibre endpoint
// (at the window's end when it is pinned), keyed by the 20-byte consensus
// address in hex: the form every probe and heartbeat row carries. The
// endpoints table stores the bech32 form, and a set keyed by that matched
// nothing in the census above, which printed the network's reachability as
// 0 of 0 while every row in the table said otherwise.
func (s *Server) registeredValidators(ctx context.Context, win Window) (map[string]bool, error) {
	q, args := `SELECT DISTINCT validator_cons_address FROM endpoints WHERE closed_at IS NULL`, []any{}
	if win.AsOf {
		q = `SELECT DISTINCT validator_cons_address FROM endpoints WHERE first_seen_at <= ? AND (closed_at IS NULL OR closed_at > ?)`
		args = []any{win.endArg(), win.endArg()}
	}
	rows, err := s.st.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		if hexAddr, err := consHex(a); err == nil {
			out[hexAddr] = true
		} else {
			// Not a bech32 consensus address: keep the value as stored, so
			// a registry written in another form still matches its rows.
			out[a] = true
		}
	}
	return out, rows.Err()
}

func (s *Server) reachabilityNow(ctx context.Context, only, asOf string) (map[string]reachState, error) {
	out := map[string]reachState{}
	// The newest row per validator is the highest rowid: both files are
	// ingested in write order. A pinned window (asOf) asks for the newest
	// row started by then.
	//
	// This used to be MAX(rowid) ... GROUP BY validator_address, which is
	// the right answer asked the expensive way: the outcome filter is not in
	// any index on validator_address, so SQLite walked every probe and every
	// heartbeat in the store, through the table, to keep ~80 rows (1.7s on
	// a 990,000-probe fixture, on every validators snapshot and every
	// validator page, growing with every row stored). The same rows are now
	// found by seeking:
	//
	//   - the validators, by a loose index scan (the recursive CTE): each
	//     step is one seek to the next distinct validator_address, so
	//     listing them costs one seek per validator, not one step per row;
	//   - each one's newest qualifying row, from probes_latest_answer and
	//     reachability_latest_answer (migration 22):
	//     an index on validator_address alone, partial on exactly this
	//     outcome test, keeps a validator's entries in rowid order, so
	//     ORDER BY rowid DESC LIMIT 1 reads its last entry and stops.
	//
	// Pinned to a moment, the walk goes back from the newest entry to the
	// first one started by then. started_at is written with a unary plus
	// there so SQLite keeps walking that index in rowid order rather than
	// switching to probes_validator_time, which would
	// hand back every earlier row to sort. TestHotQueriesUseIndexes pins
	// both plans.
	//
	// Heartbeats are this observer's own (s.vantage): another vantage's rows
	// never set the state, they only confirm or contradict a failure below.
	for _, t := range []struct{ table, ok, source, vantage string }{
		{"reachability", `outcome <> 'PROBE_ERROR'`, "heartbeat", s.vantage},
		{"probes", `outcome NOT IN ('MISSED','PROBE_ERROR')`, "probe", ""},
	} {
		q, args := latestAnswerSQL(t.table, t.ok, t.source, t.vantage, only, asOf)
		rows, err := s.st.DB().QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var addr string
			var st reachState
			var tcp, tls, id int
			if err := rows.Scan(&addr, &st.host, &st.at, &tcp, &tls, &id, &st.identityReason, &st.source); err != nil {
				rows.Close()
				return nil, err
			}
			st.tcpOK, st.tlsOK = tcp == 1, tls == 1
			st.reachable = st.tcpOK && st.tlsOK
			st.identityOK = id == 1
			if cur, ok := out[addr]; !ok || st.at > cur.at {
				out[addr] = st
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	// The few endpoints whose newest answer failed: was the answer before it,
	// on the same host, a success? One seek each, on the index the newest-row
	// query already uses; nearly every endpoint answers and skips this.
	for addr, st := range out {
		if st.reachable {
			continue
		}
		var tcp, tls, id int
		var reason string
		err := s.st.DB().QueryRowContext(ctx, `SELECT q.tcp_ok, q.tls_ok, q.identity_ok, q.identity_reason FROM reachability q
			WHERE q.validator_address = ? AND q.outcome <> 'PROBE_ERROR' AND +q.started_at < ? AND q.validator_host = ? AND +q.vantage = ?
			ORDER BY q.rowid DESC LIMIT 1`, addr, st.at, st.host, s.vantage).Scan(&tcp, &tls, &id, &reason)
		if err != nil {
			continue // no earlier answer, or unreadable: not flaky
		}
		if tcp == 1 && tls == 1 {
			st.flaky = true
			st.identityOK, st.identityReason = id == 1, reason
			out[addr] = st
		}
	}
	if err := s.confirmFromOtherVantages(ctx, out, asOf); err != nil {
		return nil, err
	}
	return out, nil
}

// otherVantageSQL is the newest check of one host by any vantage but this
// observer's, started inside a bounded span. It seeks
// reachability_validator_time by address and time and reads the span
// newest first; outcome is written with a unary plus so the partial
// reachability_latest_answer, which would hand back every row of the
// validator in rowid order, is not a candidate. TestHotQueriesUseIndexes
// pins the plan.
const otherVantageSQL = `SELECT q.vantage, q.tcp_ok, q.tls_ok, q.identity_ok, q.identity_reason FROM reachability q
	WHERE q.validator_address = ? AND q.started_at > ? AND q.started_at <= ?
	  AND +q.outcome <> 'PROBE_ERROR' AND q.vantage <> ? AND q.validator_host = ?
	ORDER BY q.started_at DESC LIMIT 1`

// confirmFromOtherVantages asks, for every endpoint this observer calls
// unreachable (not up, so not flaky either), whether another vantage checked
// the same host within confirmWithin of asOf (of now when unpinned). Its
// newest such check decides: TCP and TLS completed there, so the endpoint is
// up and the failure is on this observer's path — reachable, with that
// check's identity verdict, and confirmedFrom set; or it failed there too,
// and alsoFailedFrom says so. With no recent check from elsewhere nothing
// changes. This observer's own failing check stays what lastEndpointCheck
// reports.
func (s *Server) confirmFromOtherVantages(ctx context.Context, out map[string]reachState, asOf string) error {
	ref := time.Now().UTC()
	hi := store.TS(ref.Add(time.Minute)) // a second clock a little ahead is still recent
	if asOf != "" {
		t, err := time.Parse(store.TimeLayout, asOf)
		if err != nil {
			return fmt.Errorf("as_of %q: %w", asOf, err)
		}
		ref, hi = t, asOf
	}
	lo := store.TS(ref.Add(-confirmWithin))
	for addr, st := range out {
		if st.up() {
			continue
		}
		var vantage, reason string
		var tcp, tls, id int
		err := s.st.DB().QueryRowContext(ctx, otherVantageSQL, addr, lo, hi, s.vantage, st.host).Scan(&vantage, &tcp, &tls, &id, &reason)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if tcp == 1 && tls == 1 {
			st.reachable, st.tcpOK, st.tlsOK = true, true, true
			st.identityOK, st.identityReason = id == 1, reason
			st.confirmedFrom = vantage
		} else {
			st.alsoFailedFrom = vantage
		}
		out[addr] = st
	}
	return nil
}

// latestAnswerSQL is the query reachabilityNow runs against one table: the
// newest row per validator (or for only) among those whose outcome passes
// ok, started no later than asOf when it is set. ok must be spelled exactly
// as the partial index's WHERE is (probes_latest_answer,
// reachability_latest_answer), or SQLite cannot use it.
func latestAnswerSQL(table, ok, source, vantage, only, asOf string) (string, []any) {
	var args []any
	// The validators: one, or every one with a qualifying row, listed by
	// seeking from each to the next.
	v := `WITH RECURSIVE v(a) AS (
			SELECT (SELECT MIN(validator_address) FROM ` + table + ` WHERE ` + ok + `)
			UNION ALL
			SELECT (SELECT MIN(validator_address) FROM ` + table + ` WHERE ` + ok + ` AND validator_address > v.a)
			FROM v WHERE v.a IS NOT NULL)`
	if only != "" {
		v = `WITH v(a) AS (SELECT ?)`
		args = append(args, only)
	}
	// One vantage's rows only, when set. The unary plus keeps the term off
	// every index, so the walk stays on the partial index in rowid order and
	// steps over the other vantages' entries, which interleave with these.
	vq := ""
	if vantage != "" {
		vq = ` AND +q.vantage = ?`
		args = append(args, vantage)
	}
	pin := ""
	if asOf != "" {
		pin = ` AND +q.started_at <= ?`
		args = append(args, asOf)
	}
	return v + `
		SELECT r.validator_address, r.validator_host, r.started_at, r.tcp_ok, r.tls_ok, r.identity_ok, r.identity_reason, '` + source + `'
		FROM v JOIN ` + table + ` r ON r.rowid = (
			SELECT q.rowid FROM ` + table + ` q
			WHERE q.validator_address = v.a AND q.` + ok + vq + pin + `
			ORDER BY q.rowid DESC LIMIT 1)`, args
}

// ---- validators ----

// Throughput counts only shards large enough for bandwidth, not round trips,
// to decide the download time, and is not published from fewer of them.
const (
	throughputMinBytes  = 2 << 20
	throughputMinSample = 3
)

type validatorRow struct {
	Address     string `json:"address"`      // 20-byte consensus address, hex
	ConsAddress string `json:"cons_address"` // celestiavalcons1...: from the registry when registered, else derived from the hex address
	// Moniker is the name the operator set in the staking module, read from
	// the chain itself. Empty when the chain has no validator at this
	// consensus address, or before identities have been polled once. A reader
	// recognises a validator by this, not by twenty hex characters.
	Moniker string `json:"moniker,omitempty"`
	// Operator is the celestiavaloper... address, for linking out.
	Operator string `json:"operator_address,omitempty"`
	// KeybaseIdentity is the operator's Keybase key suffix when it set one,
	// which is how an avatar could be resolved later. Deliberately NOT called
	// "identity": on this row that word already means the TLS consensus-key
	// binding this observer checks, and the two are unrelated.
	KeybaseIdentity string `json:"keybase_identity,omitempty"`
	// AvatarURL is where this API serves the Keybase picture behind
	// KeybaseIdentity, once the collector has fetched it; absent until then
	// and for validators without one. Served from the store, so a reader
	// never contacts Keybase.
	AvatarURL string `json:"avatar_url,omitempty"`
	Website   string `json:"website,omitempty"`
	// Jailed and BondStatus are the chain's own words about the validator,
	// unlike everything else on this row, which this observer measured. A
	// jailed validator still owes the shards it signed for, so these are
	// shown rather than used to drop anyone from the table.
	Jailed     bool   `json:"jailed"`
	BondStatus string `json:"bond_status,omitempty"`
	// SignaledUpgrade says whether this validator has signalled for the app
	// version that brings Fibre, from x/signal, only while the chain is
	// below it. x/signal answers by moniker, so a moniker shared by two
	// bonded validators cannot be attributed and is left unset.
	SignaledUpgrade *bool   `json:"signaled_upgrade,omitempty"`
	Host            string  `json:"host"`
	EndpointSince   *string `json:"endpoint_since"`
	// ProviderSince is when this validator first appeared in the bonded
	// Fibre provider list (x/valaddr AllBondedFibreProviders, polled every
	// minute), whatever host it had then: unlike EndpointSince, a new host or
	// a return to the bonded set does not move it.
	ProviderSince *string `json:"provider_since,omitempty"`
	// LastHost and EndpointClosedAt describe the newest endpoint row that
	// has closed, for a validator with no open one: the host this observer
	// last saw registered and when it stopped appearing in the bonded
	// provider list. Host stays empty for such a validator; it used to be
	// back-filled from the newest probe row, which made the table's word for
	// a jailed validator depend on whether the prober had restarted since.
	LastHost         string  `json:"last_host,omitempty"`
	EndpointClosedAt *string `json:"endpoint_closed_at,omitempty"`
	VotingPower      int64   `json:"voting_power"` // from the latest assignment seen
	LastSeenAt       *string `json:"last_seen_at"`
	Reachable        *bool   `json:"reachable"` // debounced: up at the newest check, or failed only once since the one before; null if never probed
	// EndpointState says which: reachable (one failed check after a success
	// still counts) | unreachable (two failures in a row, or no success on
	// record). Empty when never checked.
	EndpointState  string `json:"endpoint_state,omitempty"`
	IdentityStatus string `json:"identity_status"` // verified | expired | mismatch | unverified | no_tls | unreachable | unknown
	IdentityReason string `json:"identity_reason,omitempty"`
	// ConfirmedFrom names another vantage whose check of the same host, in the
	// last fifteen minutes, completed the handshake while this observer's own
	// checks failed: EndpointState is then reachable, on that check's word,
	// and last_endpoint_check still shows what this observer saw.
	// AlsoFailedFrom names the vantage whose recent check failed too, on a
	// row that stays unreachable. Both absent when no other vantage checked
	// it recently.
	ConfirmedFrom  string `json:"confirmed_from,omitempty"`
	AlsoFailedFrom string `json:"also_failed_from,omitempty"`
	// Reachability is how often this observer completed a TLS conversation with the
	// endpoint over the window, from the reachability heartbeat: every
	// registered validator, every five minutes, whether or not it was assigned
	// anything. It is the closest thing here to "is the Fibre service
	// running", and unlike the serve rate its coverage does not depend on
	// attestation — an operator the publisher never collected a signature
	// from still gets 288 samples a day.
	//
	// It is not an accusation. Half of every path measured here is this
	// observer's own, so a dip is a statement about a route as much as about
	// a server, which is why it is published beside the serve rate rather
	// than folded into it. It is not signing uptime either: a validator
	// can sign every block with its Fibre endpoint down, and the reverse.
	Reachability Rate `json:"reachability_window"`
	// IdentityValid is how often the certificate presented was endorsed by
	// this validator's consensus key, over the heartbeats that got far enough
	// to see a certificate. A validator whose endpoint is up but whose
	// endorsement has lapsed is serving nothing a client will accept, and
	// nothing in the serve rate says so.
	IdentityValid Rate `json:"identity_rate_window"`
	// LastUnreachableAt is the most recent heartbeat in the window that could
	// not complete TLS, and LastReachableAt the most recent that did, so a
	// reader can tell a single outage from a service that is flapping, and
	// the table can say how long the current state has held.
	LastUnreachableAt *string `json:"last_unreachable_at"`
	LastReachableAt   *string `json:"last_reachable_at"`
	// ServeRate is HEALTHY / (HEALTHY + FAULT) over this validator's assigned
	// probes in the in-window and grace phases. Probes where the settled
	// promise does not prove this validator stored the blob are UNATTESTED
	// and sit outside the fraction, so an unproven obligation can neither
	// reward nor punish it. Attestation says how many those were.
	ServeRate Rate `json:"serve_rate"`
	// Coverage and HeldOut carry the same meaning as on /v1/network: how much
	// of this validator's own obligation population produced a verdict, and
	// what the rate does not speak for.
	Coverage Rate `json:"serve_rate_coverage"`
	// Obligations counts one observation per (validator, blob) instead of
	// one per probe, judged by the newest probe: see obligationStats. This
	// is the headline figure; ByObligation repeats its rate.
	Obligations  obligationStats  `json:"obligations"`
	ByObligation Rate             `json:"serve_rate_by_obligation"`
	HeldOut      map[string]int64 `json:"serve_rate_held_out"`
	Attestation  attestationStats `json:"attestation"`
	ProbeCount   int64            `json:"probe_count"`
	Classes      classCounts      `json:"classes"`
	// Faults is every FAULT of an assigned shard in the window, in any
	// phase; see networkResponse.Faults.
	Faults int64 `json:"faults"`
	// FaultsCleared is how many failed probes of the window a second
	// vantage cleared: it fetched the same rows within the confirmation
	// window and they verified, so the row is filed PROBE_ERROR (outside
	// the rate) with cleared_by. Not in Faults. Absent when none.
	FaultsCleared    int64  `json:"faults_cleared,omitempty"`
	AssignedRowsLast int    `json:"assigned_rows_last"`
	ExpectedLoadBand string `json:"expected_load_band"` // floor | low | mid | high, by assigned rows
	// Latency is how long this observer waited for a shard it did get: the
	// median and 95th percentile of the whole probe, dial to verified rows,
	// over the probes that came back HEALTHY in this window.
	//
	// Nothing else on this site measures performance, and Fibre exists to be
	// fast — a validator that serves everything in twenty seconds is not doing
	// its job, and every other figure here would call it perfect. The numbers
	// are published without a threshold and without a word attached: this
	// observer sits in one place, so part of every millisecond is its own
	// path, and naming a validator "slow" from one vantage would be the same
	// mistake as calling one unreachable from one vantage.
	//
	// BytesPerSecond is the size-normalised companion: the median transfer
	// rate over the download step alone, bytes handed over divided by the
	// time DownloadShard took. Assignments run from 148 rows to 4,096, so raw
	// duration is not comparable between validators, and neither was rows
	// per second over the whole probe: the dial, handshake and identity
	// check cost the same for a small shard as for a large one, so the old
	// figure rose with stake by construction. Only shards of at least
	// throughputMinBytes are counted, and the figure is null under
	// throughputMinSample of them. ThroughputSample is how many healthy
	// probes of such a shard carried a byte count.
	LatencyP50       *int64 `json:"serve_latency_p50_ms"`
	LatencyP95       *int64 `json:"serve_latency_p95_ms"`
	LatencySample    int64  `json:"serve_latency_sample"`
	BytesPerSecond   *int64 `json:"serve_bytes_per_second"`
	ThroughputSample int64  `json:"serve_throughput_sample"`
	// ByPoint is this validator's serve rate per schedule point, the same
	// breakdown /v1/network publishes for the whole set. The points sit at
	// different fractions of the retention window, so a validator that serves
	// early and not late has pruned before it was allowed to, and a validator
	// that is uniformly poor has a different problem. The pooled rate cannot
	// tell those apart, and pruning early is the specific failure this
	// observer exists to catch.
	ByPoint []stratum `json:"serve_rate_by_point"`
	// AttestedLast reports whether the newest publication this validator
	// appears in proves it stored that blob: true, false (assigned but
	// unproven) or null (recorded before the observer verified signatures).
	AttestedLast *bool `json:"attested_last"`
	// AssignmentHeight is the settlement height the voting power, row count
	// and attestation above were read at. It is the newest publication this
	// validator appears in, which is not necessarily the newest publication:
	// a validator that has left the set keeps the figures from when it was
	// last assigned, and this says when that was.
	AssignmentHeight int64 `json:"assignment_height"`
	// TimeoutsEnforced is how many MsgPaymentPromiseTimeout this validator's
	// operator account submitted in the window: promises it held that the
	// publisher abandoned, reported to the chain so the escrow was charged.
	// The chain pays nothing for it; a count above zero says the operator
	// runs the enforcement path at all. Matched on address bytes, so an
	// operator that submits from another account is not counted.
	TimeoutsEnforced int64 `json:"timeouts_enforced"`
	// Signing is how often this validator's verified signature is on the
	// settled promises that assigned it rows in the window. Descriptive, never
	// a fault: see signing.go.
	Signing signingStats `json:"signing"`
	// Load is the row data the protocol assigned this validator in the
	// window, what it holds now, and the same share sized at mainnet scale.
	// All from the chain: see load.go.
	Load loadStats `json:"load"`
	// Hosting is the network (origin AS, provider bucket) and country the
	// open endpoint's host resolved into, as resolved from this vantage;
	// absent when the lookup is off or has not reached this host. See
	// hosting.go and observer/hosting.
	Hosting *hosting.Info `json:"hosting,omitempty"`

	// ProvisionalFaults is the part of Obligations.Broken whose faults are
	// all still settling (provisional.go); absent when there is none.
	ProvisionalFaults *provisionalFaults `json:"provisional_faults,omitempty"`
}

func loadBand(rows int) string {
	switch {
	case rows <= 0:
		return ""
	case rows <= 148:
		return "floor"
	case rows < 1024:
		return "low"
	case rows < 2731:
		return "mid"
	default:
		return "high"
	}
}

func identityStatus(st *reachState) string {
	if st == nil {
		return "unknown"
	}
	if !st.up() {
		if st.tcpOK && !st.tlsOK {
			return "no_tls"
		}
		return "unreachable"
	}
	if st.identityOK {
		return "verified"
	}
	switch st.identityReason {
	case "":
		// TCP and TLS both succeeded, but no identity verdict was recorded
		// (an older row, or a probe that stopped before the identity step).
		return "unverified"
	case "cert_expired", "cert_not_yet_valid", "window_empty", "window_too_long":
		// The right key endorsed it and the signed window has lapsed: a
		// renewal running late, which the prober files as IDENTITY_EXPIRED.
		// Calling it a mismatch accused the operator of the wrong thing.
		return "expired"
	}
	return "mismatch"
}

func (s *Server) validatorRows(ctx context.Context, win Window, only string) ([]validatorRow, error) {
	db := s.st.DB()
	// Every aggregate below groups by validator, and a request for one
	// validator used to compute all of them and throw the rest away at the
	// end. On a store with 60 validators that made the detail page 1.4s, and
	// the cost is in the population, not the window: the assignment join alone
	// walks every assignment of every publication. With `only` pushed into the
	// SQL each of these becomes an index seek on validator_address.
	//
	// `vfilter` is appended last in every query it appears in, so its argument
	// is appended last too.
	vfilter := func(col string) string {
		if only == "" {
			return ""
		}
		return " AND " + col + " = ?"
	}
	vargs := func(base ...any) []any {
		if only == "" {
			return base
		}
		return append(base, only)
	}
	// The points the observer does not trust itself at, left out of every
	// per-validator population below exactly as they are network-wide.
	_, ss, err := s.suspectPoints(ctx, win)
	if err != nil {
		return nil, err
	}
	sus := ss.clause("scheduled_at")
	winArgs := append([]any{win.startArg(), win.endArg()}, ss.args...)
	byAddr := map[string]*validatorRow{}
	get := func(addr string) *validatorRow {
		v, ok := byAddr[addr]
		if !ok {
			v = &validatorRow{Address: addr, Classes: classCounts{}, IdentityStatus: "unknown"}
			byAddr[addr] = v
		}
		return v
	}
	// registry: every open endpoint (bech32 -> hex)
	eps, err := s.st.CurrentEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	for _, e := range eps {
		hexAddr, err := consHex(e.ValidatorConsAddress)
		if err != nil {
			continue
		}
		v := get(hexAddr)
		v.ConsAddress, v.Host = e.ValidatorConsAddress, e.Host
		since := e.FirstSeenAt
		v.EndpointSince = &since
	}
	// The first time each validator appeared in the list, over every row.
	prows, err := db.QueryContext(ctx, `SELECT validator_cons_address, MIN(first_seen_at) FROM endpoints GROUP BY validator_cons_address`)
	if err != nil {
		return nil, err
	}
	for prows.Next() {
		var cons, first string
		if err := prows.Scan(&cons, &first); err != nil {
			prows.Close()
			return nil, err
		}
		hexAddr, err := consHex(cons)
		if err != nil {
			continue
		}
		v := get(hexAddr)
		if v.ConsAddress == "" {
			v.ConsAddress = cons
		}
		v.ProviderSince = &first
	}
	prows.Close()
	if err := prows.Err(); err != nil {
		return nil, err
	}
	// The newest closed endpoint row, for validators with no open one: what
	// was registered and when it left the bonded list. Ordered so the newest
	// closure per validator is the one that lands.
	crows, err := db.QueryContext(ctx, `SELECT validator_cons_address, host, closed_at FROM endpoints
		WHERE closed_at IS NOT NULL ORDER BY closed_at`)
	if err != nil {
		return nil, err
	}
	for crows.Next() {
		var cons, host, closed string
		if err := crows.Scan(&cons, &host, &closed); err != nil {
			crows.Close()
			return nil, err
		}
		hexAddr, err := consHex(cons)
		if err != nil {
			continue
		}
		v := get(hexAddr)
		if v.ConsAddress == "" {
			v.ConsAddress = cons
		}
		if v.Host == "" {
			v.LastHost = host
			at := closed
			v.EndpointClosedAt = &at
		}
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return nil, err
	}
	// Voting power and row count come from the newest publication each
	// validator actually appears in, not from the newest publication overall.
	// Reading them from the latest publication alone rendered a validator
	// that had left the set as zero power with zero rows beside its fault
	// count, and the table sorts faults to the top: the row that looked worst
	// was the one we had the least current information about. The height the
	// figures come from is published with them.
	//
	// Found by seeking, not by joining every assignment to its publication:
	// assignments are never pruned and grow by one row per validator per
	// blob, so the GROUP BY this replaced grew without bound (0.37s at
	// 180,000 assignments). The validators are listed by a loose index
	// scan over assignments_validator_height (one seek each), the newest
	// height per validator is the last entry under it (one seek), and the
	// rows at that height are an equality seek on both columns. The height
	// is still checked against the publication's own, so a copy that ever
	// disagreed would drop the row rather than publish a wrong height.
	q, assignArgs := latestAssignmentSQL(only)
	tieHash := map[string]string{}
	rows, err := db.QueryContext(ctx, q, assignArgs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr string
		var vp int64
		var rc int
		var att sql.NullInt64
		var h int64
		var ph string
		if err := rows.Scan(&addr, &vp, &rc, &att, &h, &ph); err != nil {
			rows.Close()
			return nil, err
		}
		v := get(addr)
		// A block can carry several publications; keep the row with the
		// highest height and, within one block, the greatest promise hash,
		// so two snapshots of the same data agree whatever order SQLite
		// hands the rows back in.
		if v.AssignmentHeight > h || (v.AssignmentHeight == h && tieHash[addr] >= ph) {
			continue
		}
		v.AssignmentHeight, tieHash[addr] = h, ph
		v.VotingPower, v.AssignedRowsLast, v.ExpectedLoadBand = vp, rc, loadBand(rc)
		v.AttestedLast = nil
		if att.Valid {
			b := att.Int64 == 1
			v.AttestedLast = &b
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// The effective class throughout, as the network figures use: a held
	// row publishes no serve verdict for one validator either, and the
	// per-validator figures must add up to the network's.
	cls := rollup.EffectiveClass("")
	frows, err := db.QueryContext(ctx, `SELECT validator_address, COUNT(*) FROM probes
		WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND `+cls+` = 'FAULT'`+sus+vfilter("validator_address")+`
		GROUP BY validator_address`, vargs(winArgs...)...)
	if err != nil {
		return nil, err
	}
	for frows.Next() {
		var addr string
		var n int64
		if err := frows.Scan(&addr, &n); err != nil {
			frows.Close()
			return nil, err
		}
		get(addr).Faults = n
	}
	frows.Close()
	if err := frows.Err(); err != nil {
		return nil, err
	}
	// Faults a second vantage cleared (verdict.ConfirmFault). They are no
	// longer in Faults, which counts the class they now carry; this says
	// how many were withdrawn that way. Over the raw rows only: a rolled
	// day keeps its counts, not who cleared what.
	crows, err = db.QueryContext(ctx, `SELECT validator_address, COUNT(*) FROM probes INDEXED BY probes_cleared
		WHERE cleared_by IS NOT NULL AND started_at >= ? AND started_at <= ? AND assigned = 1`+sus+vfilter("validator_address")+`
		GROUP BY validator_address`, vargs(winArgs...)...)
	if err != nil {
		return nil, err
	}
	for crows.Next() {
		var addr string
		var n int64
		if err := crows.Scan(&addr, &n); err != nil {
			crows.Close()
			return nil, err
		}
		get(addr).FaultsCleared = n
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return nil, err
	}
	// classes per validator in window
	rows, err = db.QueryContext(ctx, `SELECT validator_address, `+cls+`, COUNT(*) FROM probe_rows
		WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`+sus+vfilter("validator_address")+`
		GROUP BY validator_address, `+cls, vargs(winArgs...)...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr, c string
		var n int64
		if err := rows.Scan(&addr, &c, &n); err != nil {
			rows.Close()
			return nil, err
		}
		v := get(addr)
		v.Classes[c] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// The retention profile per validator: the same population as the classes
	// above, sliced by schedule point, rated as the obligations count it (an
	// end-of-window reading that returned no rows is not served).
	ocls := rollup.ObligationClass("")
	rows, err = db.QueryContext(ctx, `SELECT validator_address, schedule_label,
			COALESCE(SUM(CASE WHEN `+ocls+` = 'HEALTHY' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN `+ocls+` = 'FAULT' THEN 1 ELSE 0 END), 0)
		FROM probe_rows WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`+sus+vfilter("validator_address")+`
		GROUP BY validator_address, schedule_label
		ORDER BY validator_address, schedule_label`, vargs(winArgs...)...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr string
		var st stratum
		var ok, bad int64
		if err := rows.Scan(&addr, &st.Key, &ok, &bad); err != nil {
			rows.Close()
			return nil, err
		}
		st.Rate = rate(ok, ok+bad)
		v := get(addr)
		v.ByPoint = append(v.ByPoint, st)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// How long a served shard took, per validator. Percentiles rather than a
	// mean: a mean over a few hundred probes is moved by one timeout, and the
	// figure a reader wants is "what does it usually take" beside "what does
	// it take when it is bad".
	//
	// HEALTHY only. A probe that failed has a duration too, and it measures
	// how long a failure took, which is not a service figure. total_duration_ms
	// is the whole probe — dial, TLS, DownloadShard, row verification against
	// the commitment — because that is what a client actually waits for.
	//
	// Throughput is over the download step alone, bytes over download_ms,
	// and ranked on its own (rb): the probe with the median duration can
	// carry the highest transfer rate of the set. Only shards of at least
	// throughputMinBytes count (cb): a small shard's download is almost all
	// round trips, so pooling it with large ones made the median say which
	// blobs a validator happened to be probed on (the same server read
	// 110 KB/s on 130 KB shards and 8.7 MB/s on 27 MB ones). Smaller or
	// uncounted records sort last and are outside the sample.
	rows, err = db.QueryContext(ctx, `SELECT validator_address,
			MAX(CASE WHEN rn = (c + 1) / 2          THEN ms END),
			MAX(CASE WHEN rn = (c * 95 + 99) / 100  THEN ms END),
			MAX(c),
			MAX(CASE WHEN rb = (cb + 1) / 2         THEN bps END),
			MAX(cb)
		FROM (
			SELECT validator_address AS validator_address,
			       total_duration_ms AS ms,
			       CASE WHEN bytes_returned >= ? AND download_ms > 0 THEN bytes_returned * 1000 / download_ms END AS bps,
			       ROW_NUMBER() OVER (PARTITION BY validator_address ORDER BY total_duration_ms) AS rn,
			       COUNT(*)     OVER (PARTITION BY validator_address)                            AS c,
			       ROW_NUMBER() OVER (PARTITION BY validator_address
			                          ORDER BY (bytes_returned IS NULL OR bytes_returned < ? OR download_ms <= 0), bytes_returned * 1000.0 / NULLIF(download_ms, 0)) AS rb,
			       SUM(CASE WHEN bytes_returned >= ? AND download_ms > 0 THEN 1 ELSE 0 END)
			                    OVER (PARTITION BY validator_address)                            AS cb
			FROM probes
			WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'
			  AND classification = 'HEALTHY' AND total_duration_ms > 0`+vfilter("validator_address")+`
		) GROUP BY validator_address`, vargs(throughputMinBytes, throughputMinBytes, throughputMinBytes, win.startArg(), win.endArg())...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr string
		var p50, p95, bps sql.NullInt64
		var n, nb int64
		if err := rows.Scan(&addr, &p50, &p95, &n, &bps, &nb); err != nil {
			rows.Close()
			return nil, err
		}
		v := get(addr)
		v.LatencySample = n
		if p50.Valid {
			x := p50.Int64
			v.LatencyP50 = &x
		}
		if p95.Valid {
			x := p95.Int64
			v.LatencyP95 = &x
		}
		v.ThroughputSample = nb
		if bps.Valid && nb >= throughputMinSample {
			x := bps.Int64
			v.BytesPerSecond = &x
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// proven / unproven / unknown per validator, same scope as the classes
	// above so the serve rate and its exclusions line up. Counted twice: once
	// per probe, which is what the rate's population is, and once per
	// (validator, blob) obligation, which is what an operator reads. The two
	// differ by the size of the probe schedule, so publishing only the first
	// would inflate every disclosure about a named validator fourfold.
	rows, err = db.QueryContext(ctx, `SELECT validator_address,
			COALESCE(SUM(CASE WHEN attested = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN attested = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN attested IS NULL THEN 1 ELSE 0 END), 0)
		FROM probe_rows WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`+sus+vfilter("validator_address")+`
		GROUP BY validator_address`, vargs(winArgs...)...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr string
		var at, un, unk int64
		if err := rows.Scan(&addr, &at, &un, &unk); err != nil {
			rows.Close()
			return nil, err
		}
		v := get(addr)
		v.Attestation = attestationStats{Attested: at, Unattested: un, Unknown: unk, Coverage: rate(at, at+un)}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = db.QueryContext(ctx, `SELECT validator_address,
			COALESCE(SUM(CASE WHEN a = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a IS NULL THEN 1 ELSE 0 END), 0)
		FROM (
			SELECT validator_address AS validator_address, MAX(attested) AS a
			FROM probe_rows WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`+vfilter("validator_address")+`
			GROUP BY validator_address, promise_hash
		) GROUP BY validator_address`, vargs(win.startArg(), win.endArg())...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr string
		var at, un, unk int64
		if err := rows.Scan(&addr, &at, &un, &unk); err != nil {
			rows.Close()
			return nil, err
		}
		v := get(addr)
		v.Attestation.AttestedBlobs, v.Attestation.UnattestedBlobs, v.Attestation.UnknownBlobs = at, un, unk
		v.Attestation.BlobCoverage = rate(at, at+un)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = db.QueryContext(ctx, `SELECT validator_address, COUNT(*), MAX(started_at) FROM probe_rows
		WHERE started_at >= ? AND started_at <= ?`+vfilter("validator_address")+` GROUP BY validator_address`, vargs(win.startArg(), win.endArg())...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr, last string
		var n int64
		if err := rows.Scan(&addr, &n, &last); err != nil {
			rows.Close()
			return nil, err
		}
		v := get(addr)
		v.ProbeCount = n
		l := last
		v.LastSeenAt = &l
	}
	rows.Close()
	// The heartbeat history, which until now was written every five minutes for
	// every registered validator and read only for its newest row. It is the
	// one stability signal here whose coverage does not depend on being
	// assigned or attested anything, which is exactly what an operator asking
	// "is my Fibre server up" needs.
	// A PROBE_ERROR heartbeat is the observer's own failure (a host it could
	// not parse, an identity check it starved of CPU) and is left out of
	// every count, as its probe-side twin is.
	hrows, err := db.QueryContext(ctx, `SELECT validator_address, COUNT(*),
			COALESCE(SUM(CASE WHEN tcp_ok = 1 AND tls_ok = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN tcp_ok = 1 AND tls_ok = 1 AND identity_ok = 1 THEN 1 ELSE 0 END), 0),
			MAX(CASE WHEN tcp_ok = 1 AND tls_ok = 1 THEN NULL ELSE started_at END),
			MAX(CASE WHEN tcp_ok = 1 AND tls_ok = 1 THEN started_at END)
		FROM reachability WHERE started_at >= ? AND started_at <= ? AND outcome <> 'PROBE_ERROR' AND +vantage = ?`+vfilter("validator_address")+`
		GROUP BY validator_address`, vargs(win.startArg(), win.endArg(), s.vantage)...)
	if err != nil {
		return nil, err
	}
	for hrows.Next() {
		var addr string
		var seen, up, ident int64
		var lastDown, lastUp sql.NullString
		if err := hrows.Scan(&addr, &seen, &up, &ident, &lastDown, &lastUp); err != nil {
			hrows.Close()
			return nil, err
		}
		v := get(addr)
		v.Reachability = rate(up, seen)
		// Denominator is the heartbeats that reached TLS, not all of them: an
		// unreachable endpoint presented no certificate, and counting that as
		// an identity failure would report the same outage twice.
		v.IdentityValid = rate(ident, up)
		if lastDown.Valid {
			at := lastDown.String
			v.LastUnreachableAt = &at
		}
		if lastUp.Valid {
			at := lastUp.String
			v.LastReachableAt = &at
		}
	}
	hrows.Close()
	if err := hrows.Err(); err != nil {
		return nil, err
	}
	reach, err := s.reachabilityNow(ctx, only, win.asOfArg())
	if err != nil {
		return nil, err
	}
	for addr, st := range reach {
		v := get(addr)
		r := st.up()
		v.Reachable = &r
		switch {
		case st.reachable || st.flaky:
			v.EndpointState = "reachable"
		default:
			v.EndpointState = "unreachable"
		}
		stc := st
		v.IdentityStatus = identityStatus(&stc)
		v.IdentityReason = st.identityReason
		v.ConfirmedFrom, v.AlsoFailedFrom = st.confirmedFrom, st.alsoFailedFrom
		if v.LastSeenAt == nil || st.at > *v.LastSeenAt {
			at := st.at
			v.LastSeenAt = &at
		}
	}
	// Names from the staking module, and a row for every bonded validator
	// whether or not anything has been measured about it yet.
	//
	// This used to name only validators the observer already had a reason to
	// show, on the grounds that a name is not evidence and listing the whole
	// chain would bury the ones with a Fibre endpoint. That reasoning holds
	// after Fibre activates — and before it, it leaves the page empty. On a
	// chain below app version 10 there is no x/valaddr to register in and no
	// publication to be assigned, so nothing produces a row, and the site that
	// exists to watch these validators cannot say which ones it is watching.
	//
	// Seeding from the bonded set fixes that without becoming a second mode:
	// every bonded validator is assigned every blob, so after activation these
	// rows are a subset of what the assignment table produces anyway. Before
	// it, they are the whole answer to "am I in your list", with every measured
	// column honestly empty.
	irows, err := db.QueryContext(ctx, `SELECT vi.cons_address, vi.operator_address, vi.moniker, vi.identity, vi.website, vi.jailed, vi.status, vi.tokens,
			EXISTS (SELECT 1 FROM validator_avatars a WHERE UPPER(a.identity) = UPPER(vi.identity) AND a.status = 'ok')
		FROM validator_identities vi`)
	if err != nil {
		return nil, err
	}
	for irows.Next() {
		var addr, op, moniker, identity, website, status, tokens string
		var jailed, hasAvatar int
		if err := irows.Scan(&addr, &op, &moniker, &identity, &website, &jailed, &status, &tokens, &hasAvatar); err != nil {
			irows.Close()
			return nil, err
		}
		hexAddr := strings.ToLower(addr)
		v, known := byAddr[hexAddr]
		if !known {
			// Only the active set gets a row of its own: an unbonded validator
			// with nothing measured has no Fibre obligation to report on, and
			// one WITH something measured is already in byAddr from its probes.
			if status != "BOND_STATUS_BONDED" || (only != "" && hexAddr != only) {
				continue
			}
			v = get(hexAddr)
		}
		v.Moniker, v.Operator, v.KeybaseIdentity, v.Website = moniker, op, identity, website
		if hasAvatar == 1 {
			v.AvatarURL = "/v1/avatars/" + strings.ToUpper(identity)
		}
		v.Jailed, v.BondStatus = jailed == 1, status
		// Voting power from the staking module, only where no assignment has
		// given one. After activation the assignment's figure wins: it is the
		// power the shard split was actually computed from, at a height this
		// row publishes, rather than the power right now.
		if v.VotingPower == 0 {
			if n, err := strconv.ParseInt(tokens, 10, 64); err == nil {
				v.VotingPower = n / 1_000_000
			}
		}
	}
	irows.Close()
	if err := irows.Err(); err != nil {
		return nil, err
	}

	// While the chain is below the version that brings Fibre, x/signal says
	// who has signalled for it — by moniker, the only key it offers. A
	// moniker two bonded validators share cannot be attributed and is left
	// unset rather than guessed, on the list and on the single-validator
	// route alike: the shared set is counted over every bonded identity,
	// not over the rows this request is building.
	if missing, shared, ok := s.upgradeSignalSets(ctx); ok {
		for _, v := range byAddr {
			if v.Moniker == "" || v.BondStatus != "BOND_STATUS_BONDED" || shared[v.Moniker] {
				continue
			}
			signaled := !missing[v.Moniker]
			v.SignaledUpgrade = &signaled
		}
	}

	// one observation per (validator, blob): see obligationStats.
	byObligation, err := s.obligationsByValidator(ctx, win, ss, vfilter("pr.validator_address"), vargs()...)
	if err != nil {
		return nil, err
	}
	provisional, err := s.provisionalByValidator(ctx, win, ss, time.Now(), vfilter("pr.validator_address"), vargs()...)
	if err != nil {
		return nil, err
	}

	timeouts, err := s.timeoutsByAccount(ctx, win)
	if err != nil {
		return nil, err
	}
	// Past the raw retention the "all" window folds the daily rollup in
	// for the pruned days (see rolledFor).
	rolled, _, err := s.rolledFor(ctx, win, only)
	if err != nil {
		return nil, err
	}
	if rolled != nil {
		for addr, rp := range rolled.ProbesByVal {
			v := get(addr)
			for c, n := range rp.Classes {
				v.Classes[c] += n
			}
			v.Faults += rp.Faults
			v.ProbeCount += rp.Probes
			v.Reachability = rate(v.Reachability.Num+rp.BeatsUp, v.Reachability.Den+rp.Beats)
			// Endorsement is measured over the handshakes that completed, so
			// its denominator is the rolled BeatsUp, not the rolled Beats.
			v.IdentityValid = rate(v.IdentityValid.Num+rp.IdentityUp, v.IdentityValid.Den+rp.BeatsUp)
			// And the attestation split beside the classes, over the same
			// rows, so this row reconciles against itself the way the
			// network row does.
			v.Attestation.Attested += rp.Attested
			v.Attestation.Unattested += rp.Unattested
			v.Attestation.Unknown += rp.UnknownAtt
			v.Attestation.Coverage = rate(v.Attestation.Attested, v.Attestation.Attested+v.Attestation.Unattested)
		}
		for addr, ro := range rolled.ObligationsByVal {
			get(addr)
			o := byObligation[addr]
			addRolledObligations(&o, ro)
			byObligation[addr] = o
		}
	}
	if err := s.fillSigning(ctx, win, only, byAddr); err != nil {
		return nil, err
	}
	if err := s.fillLoad(ctx, win, only, byAddr); err != nil {
		return nil, err
	}
	out := make([]validatorRow, 0, len(byAddr))
	for addr, v := range byAddr {
		if only != "" && addr != only {
			continue
		}
		v.ServeRate = serveRate(v.Classes)
		v.Coverage = coverage(v.Classes)
		v.HeldOut = heldOut(v.Classes)
		v.Obligations = byObligation[addr]
		v.ByObligation = v.Obligations.Rate
		v.ProvisionalFaults = provisional[addr]
		if v.Operator != "" {
			v.TimeoutsEnforced = timeouts[accountKey(v.Operator)]
		}
		// The registry supplies the bech32 form only for validators that
		// registered an endpoint; before Fibre is live that is nobody, and
		// every row printed its raw hex. The two are the same 20 bytes.
		if v.ConsAddress == "" {
			if bech, err := consBech(addr); err == nil {
				v.ConsAddress = bech
			}
		}
		out = append(out, *v)
	}
	if err := s.attachHosting(ctx, out, win); err != nil {
		return nil, err
	}
	// voting power desc, then address
	sortRows(out)
	return out, nil
}

func sortRows(v []validatorRow) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0; j-- {
			a, b := v[j-1], v[j]
			if a.VotingPower > b.VotingPower || (a.VotingPower == b.VotingPower && a.Address <= b.Address) {
				break
			}
			v[j-1], v[j] = v[j], v[j-1]
		}
	}
}

// consBech is the reverse of consHex: the form the chain prints
// (celestiavalcons1…) for a 20-byte consensus address in hex.
func consBech(hexAddr string) (string, error) {
	raw, err := hex.DecodeString(hexAddr)
	if err != nil {
		return "", err
	}
	if len(raw) != 20 {
		return "", fmt.Errorf("consensus address %d bytes, want 20", len(raw))
	}
	return bech32.ConvertAndEncode("celestiavalcons", raw)
}

func consHex(bech string) (string, error) {
	hrp, raw, err := bech32.DecodeAndConvert(bech)
	if err != nil {
		return "", err
	}
	// An operator address (…valoper1…) and an account address decode to 20
	// bytes just as well, and would silently be looked up as a consensus
	// address that can never match.
	if !strings.HasSuffix(hrp, "valcons") {
		return "", fmt.Errorf("address prefix %q is not a consensus address (…valcons1…)", hrp)
	}
	if len(raw) != 20 {
		return "", fmt.Errorf("consensus address %d bytes, want 20", len(raw))
	}
	return hex.EncodeToString(raw), nil
}

// parseAddr accepts a 40-hex consensus address or a bech32 celestiavalcons
// address. It is pure, for the places that must not touch the store (the
// exclude list); the routes a person types an address into use resolveAddr
// (validator_addr.go), which also takes operator and account addresses.
func parseAddr(s string) (string, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if len(s) == 40 {
		if _, err := hex.DecodeString(s); err == nil {
			return s, nil
		}
	}
	return consHex(s)
}

// validatorSnapshot is the cached validator list with the window its SQL
// actually used. A snapshot is served for as long as its TTL allows, so the
// window the caller asked for at request time can be minutes newer than the
// one the rows were selected with — fifteen minutes or more on 30d and "all".
// Echoing the request's window would have published bounds no figure in the
// response was computed over, and this API's whole claim is that a reader can
// recompute what it prints. computed_at says when; this says over what.
type validatorSnapshot struct {
	Window        Window         `json:"window"`
	Rows          []validatorRow `json:"rows"`
	RecordThrough *recordThrough `json:"record_through,omitempty"`
}

func (s *Server) handleValidators(w http.ResponseWriter, r *http.Request) {
	win, err := parseWindow(r, time.Now())
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if win.AsOf {
		if !s.asOf.allow(time.Now()) {
			w.Header().Set("Retry-After", "2")
			writeErr(w, 429, "as_of requests are limited to one every two seconds")
			return
		}
		if !s.asOf.enter() {
			w.Header().Set("Retry-After", "5")
			writeErr(w, 429, "as_of computations already in flight; try again shortly")
			return
		}
		defer s.asOf.leave()
		t0 := time.Now()
		rows, err := s.validatorRows(r.Context(), win, "")
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, 200, map[string]any{
			"window": win, "vantage": s.vantage, "validators": rows, "as_of_note": AsOfNote,
			"record_through": s.recordThrough(r.Context()),
			"computed_at":    t0.UTC().Format(time.RFC3339Nano), "compute_ms": time.Since(t0).Milliseconds(),
		})
		return
	}
	snap, at, ms, err := s.vals.get(r.Context(), s.logf(), win)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	rows := snap.Rows
	if rows == nil {
		rows = []validatorRow{}
	}
	out := map[string]any{
		"window": snap.Window, "vantage": s.vantage, "validators": rows,
		"record_through": snap.RecordThrough,
		"computed_at":    at.UTC().Format(time.RFC3339Nano), "compute_ms": ms,
	}
	if _, label, err := s.rolledFor(r.Context(), win, ""); err == nil && label != nil {
		out["rolled_up"] = label
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleValidator(w http.ResponseWriter, r *http.Request) {
	// Any spelling an operator would paste: consensus, operator or account
	// address (validator_addr.go). Everything below works on the consensus
	// address alone, so the cache and the answer are one per validator.
	addr, err := s.resolveAddr(r.Context(), r.PathValue("addr"))
	if err != nil {
		s.writeAddrErr(w, r.URL.Path, err)
		return
	}
	now := time.Now()
	ctx := r.Context()
	// The embedded validator object is built over a window like every other
	// response, and the window it was built over is echoed at the top level.
	// It used to be pinned to 24h with nothing saying so, so a caller reading
	// the raw JSON had a rate with no window attached to it.
	win, err := parseWindow(r, now)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if win.AsOf {
		// This route builds the aggregates the network and validator lists
		// do, four spans of them, and a pinned answer is never cached — so it
		// was the one uncapped way to buy a full pinned computation. Rationed
		// like the others.
		if !s.asOf.allow(time.Now()) {
			w.Header().Set("Retry-After", "2")
			writeErr(w, 429, "as_of requests are limited to one every two seconds")
			return
		}
		if !s.asOf.enter() {
			w.Header().Set("Retry-After", "5")
			writeErr(w, 429, "as_of computations already in flight; try again shortly")
			return
		}
		defer s.asOf.leave()
		w.Header().Set("Cache-Control", "no-store")
		status, out, err := s.validatorDetail(ctx, addr, win, now)
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		writeJSON(w, status, out)
		return
	}
	s.serveValidatorDetail(w, r, addr, win, now)
}

const validatorNotSeen = "validator not seen in the registry or in any probe"

// validatorDetail builds the /v1/validators/{addr} answer: the row over win,
// the four standard spans beside it, and the newest probes. status is 200, or
// 404 with an error body when nothing about addr is on record.
func (s *Server) validatorDetail(ctx context.Context, addr string, win Window, now time.Time) (int, any, error) {
	// The spans beside the row are anchored on the same moment the row is:
	// the pin when there is one, the clock otherwise. Anchoring them on the
	// clock while the row was pinned put a rewound validator beside four
	// present-day breakdowns of itself, under one window object naming only
	// the pin.
	spanEnd := now
	if win.AsOf {
		spanEnd = win.End
	}
	type span struct {
		Window Window `json:"window"`
		Rate   Rate   `json:"serve_rate"`
		// Count is every assigned in-window probe in this window, rated or
		// held out, not every row for the validator (validator.probe_count).
		Count        int64            `json:"probe_count"`
		Coverage     Rate             `json:"serve_rate_coverage"`
		Obligations  obligationStats  `json:"obligations"`
		ByObligation Rate             `json:"serve_rate_by_obligation"`
		HeldOut      map[string]int64 `json:"serve_rate_held_out"`
		Classes      classCounts      `json:"classes"`
		RolledUp     *rolledUp        `json:"rolled_up,omitempty"`
		// Provisional is the part of obligations.broken still settling.
		Provisional *provisionalFaults `json:"provisional_faults,omitempty"`
	}
	var spans []span
	// The suspect points over all time, so the recent-probes list can mark
	// rows that no rate counts.
	suspectAll := []suspectPoint{}
	for _, name := range []string{"24h", "7d", "30d", "all"} {
		sw := Window{Name: name, Span: windows[name], End: spanEnd, AsOf: win.AsOf}
		if sw.Span > 0 {
			sw.Start = spanEnd.Add(-sw.Span)
		}
		_, ss, err := s.suspectPoints(ctx, sw)
		if err != nil {
			return 0, nil, err
		}
		if sw.Name == "all" {
			suspectAll = ss.points
		}
		classes, total, err := s.classCountsWhere(ctx, `validator_address = ? AND started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'`+ss.clause("scheduled_at"),
			append([]any{addr, sw.startArg(), sw.endArg()}, ss.args...)...)
		if err != nil {
			return 0, nil, err
		}
		obl, err := s.obligationsWhere(ctx, sw, ss, ` AND pr.validator_address = ?`, addr)
		if err != nil {
			return 0, nil, err
		}
		prov, err := s.provisionalByValidator(ctx, sw, ss, time.Now(), ` AND pr.validator_address = ?`, addr)
		if err != nil {
			return 0, nil, err
		}
		rolled, label, err := s.rolledFor(ctx, sw, addr)
		if err != nil {
			return 0, nil, err
		}
		if rolled != nil {
			if rp, ok := rolled.ProbesByVal[addr]; ok {
				for c, n := range rp.Classes {
					classes[c] += n
					total += n
				}
			}
			addRolledObligations(&obl, rolled.ObligationsByVal[addr])
		}
		spans = append(spans, span{
			Window: sw, Rate: serveRate(classes), Count: total,
			Coverage: coverage(classes), Obligations: obl, ByObligation: obl.Rate, HeldOut: heldOut(classes), Classes: classes,
			RolledUp: label, Provisional: prov[addr],
		})
	}
	rows, err := s.validatorRows(ctx, win, addr)
	if err != nil {
		return 0, nil, err
	}
	if len(rows) == 0 {
		return 404, map[string]any{"error": validatorNotSeen}, nil
	}
	probeWhere, probeArgs := `validator_address = ?`, []any{addr}
	if win.AsOf {
		probeWhere += ` AND started_at <= ?`
		probeArgs = append(probeArgs, win.endArg())
	}
	probes, err := s.probeRows(ctx, probeWhere, 50, probeArgs...)
	if err != nil {
		return 0, nil, err
	}
	probes, moreProbes := trim(probes, 50)
	// The publications assigned to it that the load policy drew out of the
	// sample: one decision each, where recent_probes used to carry a
	// NOT_PROBED row per point for every one of them.
	soWhere, soArgs := `EXISTS (SELECT 1 FROM assignments a WHERE a.promise_hash = d.promise_hash AND a.validator_address = ? AND a.row_count > 0)`, []any{addr}
	if win.AsOf {
		soWhere += ` AND d.decided_at <= ?`
		soArgs = append(soArgs, win.endArg())
	}
	sampled, err := s.st.SampledOutDecisions(ctx, soWhere, 51, soArgs...)
	if err != nil {
		return 0, nil, err
	}
	sampled, moreSampled := trim(sampled, 50)
	out := map[string]any{
		"window":                      win,
		"record_through":              s.recordThrough(ctx),
		"validator":                   rows[0],
		"windows":                     spans,
		"recent_probes":               probes,
		"recent_probes_truncated":     moreProbes,
		"suspect_points":              suspectAll,
		"serve_rate_excluded_classes": excludedFromRate,
		"vantage":                     s.vantage,
	}
	out["recent_sampled_out"], out["recent_sampled_out_truncated"] = sampled, moreSampled
	if win.AsOf {
		out["as_of_note"] = AsOfNote
	}
	hm, err := s.validatorHeatmap(ctx, addr, win)
	if err != nil {
		return 0, nil, err
	}
	out["heatmap"] = hm
	// The network's rate over the same window from the same vantage, for the
	// page's service-rate context (provisional.go).
	if ref := s.networkReference(ctx, win); ref != nil {
		out["network_reference"] = ref
	}
	if c, err := s.lastEndpointCheck(ctx, addr, win); err == nil && c != nil {
		out["last_endpoint_check"] = c
	}
	if _, label, err := s.rolledFor(ctx, win, addr); err == nil && label != nil {
		out["rolled_up"] = label
	}
	return 200, out, nil
}

// ---- blobs ----

type blobRow struct {
	PromiseHash string `json:"promise_hash"`
	Commitment  string `json:"commitment"`
	Namespace   string `json:"namespace"`
	BlobSize    int64  `json:"blob_size"`
	// Signer is MsgPayForFibre.signer, the account that submitted the
	// settlement. Anyone can submit one, typically an endorsing validator,
	// so it is not necessarily who paid.
	Signer string `json:"signer"`
	// Publisher is who paid: the escrow owner, whose key signed the promise
	// (scan.PublisherOf; the payment's publisher when there is one).
	Publisher        string `json:"publisher"`
	SettlementHeight int64  `json:"settlement_height"`
	// SettlementTxIndex is the other half of this route's cursor, published
	// so a caller paging with before_height/before_tx_index does not have to
	// guess it.
	SettlementTxIndex  int    `json:"settlement_tx_index"`
	SettlementTime     string `json:"settlement_time"`
	CreationTimestamp  string `json:"creation_timestamp"`
	MustServeUntil     string `json:"must_serve_until"`
	ValidatorsWithRows int    `json:"validators_with_rows"`
	SigmaRows          int    `json:"sigma_rows"`
	DistinctRows       int    `json:"distinct_rows"`
	AssignmentError    string `json:"assignment_error,omitempty"`
	// AttestedPower is the voting power whose signature over the promise
	// verified, over TotalPower, the set's total at the promise height; absent
	// for a record from before signatures were verified.
	AttestedPower *int64 `json:"attested_voting_power,omitempty"`
	TotalPower    int64  `json:"total_voting_power,omitempty"`
	// AttestedWithRows is how many of ValidatorsWithRows carry a verified
	// signature over the promise: the endorsements MsgPayForFibre settled
	// with. Absent, like AttestedPower, before signatures were verified.
	AttestedWithRows *int         `json:"attested_with_rows,omitempty"`
	ProbeCount       int64        `json:"probe_count"`
	Classes          classCounts  `json:"classes"`
	Reconstructable  *reconstruct `json:"reconstructable"`
	// SampledOut is set when the load policy drew this publication out of
	// the sample: the draw it was decided by, recorded once. probe_count
	// and classes still count the NOT_PROBED rows it stands for (one per
	// assigned validator per point), as every figure does.
	SampledOut *store.SampledOutDecision `json:"sampled_out,omitempty"`
	// Charge is the fee side of this promise from the payments table: what
	// the module charged, and whether the promise settled or timed out. Null
	// for a publication whose payment was not recorded (ingested before the
	// scanner wrote payments).
	Charge *blobCharge `json:"charge"`
}

// reconstruct is the per-blob reconstructability verdict at the latest
// in-window schedule point that has probes: distinct rows held by
// validators that served correctly versus the rows needed (OriginalRows).
// Grace and post points are never used: "not found" is tolerated or
// expected there, so a quiet grace point says nothing about the promise.
// WindowOver says whether the obligation has ended since.
type reconstruct struct {
	Status        string `json:"status"` // yes | degraded | no | pending | unknown
	Point         string `json:"point"`  // schedule label the verdict is taken at
	PointAt       string `json:"point_at"`
	WindowOver    bool   `json:"window_over"`
	ServedRows    int    `json:"served_distinct_rows"`
	NeededRows    int    `json:"needed_rows"`
	ServedBy      int    `json:"served_by_validators"`
	AssignedTotal int    `json:"assigned_validators"`
	// TotalRows is the blob's encoded row count (16384 for blob v0): the
	// denominator a reader should draw the served rows against.
	TotalRows int `json:"total_rows"`
	// ProbedValidators is how many assigned validators have a real result
	// (not a gap) at the point. Status is "pending" while it is short of
	// assigned_validators: the sweep is still running or was skipped by the
	// policy, and an absent row is a gap, never a zero.
	ProbedValidators int `json:"probed_validators"`
	// AttestedValidators is how many of the assigned validators the settled
	// promise proves stored the blob. It is the denominator for "yes": a
	// validator with no proof of storage cannot demote the verdict by not
	// serving. AttestationKnown is false for publications recorded before the
	// observer verified signatures, where the whole assigned set is used
	// instead.
	AttestedValidators int  `json:"attested_validators"`
	AttestationKnown   bool `json:"attestation_known"`
	// ServedByAttested is how many proven-obliged validators served correctly
	// at the point.
	ServedByAttested int `json:"served_by_attested"`
}

// maxBlobOffset bounds ?offset=: deep pages are what the cursor is for.
const maxBlobOffset = 100000

func (s *Server) blobRows(ctx context.Context, where string, limit int, args ...any) ([]blobRow, error) {
	return s.blobRowsAt(ctx, where, limit, 0, args...)
}

// blobRowsAt is blobRows from the offset-th row of the same order.
func (s *Server) blobRowsAt(ctx context.Context, where string, limit, offset int, args ...any) ([]blobRow, error) {
	q := `SELECT promise_hash, commitment, namespace, blob_size, signer, signer_public_key, settlement_height, settlement_tx_index, settlement_time, creation_timestamp,
		must_serve_until, validators_with_rows, sigma_rows, distinct_rows, assignment_error, attested_voting_power, total_voting_power, attested_with_rows FROM publications`
	if where != "" {
		q += " WHERE " + where
	}
	// One more than asked: see probeRows. The caller trims and reports it.
	q += " ORDER BY settlement_height DESC, settlement_tx_index DESC LIMIT " + strconv.Itoa(limit+1)
	if offset > 0 {
		q += " OFFSET " + strconv.Itoa(offset)
	}
	rows, err := s.st.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []blobRow
	for rows.Next() {
		var b blobRow
		var key sql.NullString
		if err := rows.Scan(&b.PromiseHash, &b.Commitment, &b.Namespace, &b.BlobSize, &b.Signer, &key, &b.SettlementHeight, &b.SettlementTxIndex, &b.SettlementTime,
			&b.CreationTimestamp, &b.MustServeUntil, &b.ValidatorsWithRows, &b.SigmaRows, &b.DistinctRows, &b.AssignmentError, &b.AttestedPower, &b.TotalPower, &b.AttestedWithRows); err != nil {
			return nil, err
		}
		// Who paid, from the promise's own key. A row whose key cannot be
		// read keeps the submitter, the only account on record for it.
		b.Publisher = b.Signer
		if p, err := scan.PublisherOf(key.String); err == nil {
			b.Publisher = p
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	hashes := make([]string, len(out))
	for i := range out {
		hashes[i] = out[i].PromiseHash
	}
	charges, err := s.chargesFor(ctx, hashes)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Charge = charges[out[i].PromiseHash]
		// the account the chain charged, where a payment is on record
		if c := out[i].Charge; c != nil && c.Publisher != "" {
			out[i].Publisher = c.Publisher
		}
	}
	sampledOut, err := s.sampledOutFor(ctx, hashes)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].SampledOut = sampledOut[out[i].PromiseHash]
	}
	// Per blob, deliberately. Batching both of these was tried and measured on
	// a store with 2,200 publications: the class tally got about 8% slower,
	// because fifty seeks on probes_promise beat one CTE-joined GROUP BY, and
	// batching the row lists was far worse, taking /v1/blobs?limit=200 from
	// 1.35s to 3.58s by pulling every in-window point's JSON when only the
	// chosen point is wanted. A page is a few hundred rows at most. The batch
	// is kept for the network summary, which examines two thousand and
	// publishes no row count.
	//
	// What made the page fast was not batching but not recomputing: both of
	// these are functions of the publication's probes, which are append-only,
	// so a settled verdict is settled for good. One query fetches a
	// fingerprint of every row's probes and the rest is a map lookup. See
	// blobcache.go.
	// The rows this call read, look-ahead row included: a fingerprint of any
	// other selection would let a cached verdict outlive a change to these.
	fps, err := s.probeFingerprints(ctx, where, limit+1, offset, args...)
	if err != nil {
		return nil, err
	}
	for i := range out {
		hash := out[i].PromiseHash
		fp := fps[hash]
		if v, ok := s.blobs.get(hash, fp); ok {
			out[i].Classes, out[i].ProbeCount, out[i].Reconstructable = v.classes, v.total, v.rc
			continue
		}
		classes, total, err := s.classCountsWhere(ctx, `promise_hash = ?`, hash)
		if err != nil {
			return nil, err
		}
		out[i].Classes, out[i].ProbeCount = classes, total
		rc, err := s.reconstructable(ctx, hash, asOfPin{now: time.Now()})
		if err != nil {
			return nil, err
		}
		out[i].Reconstructable = rc
		// Only once the obligation has ended. window_over is the one part of a
		// verdict that depends on the clock rather than on the store, and
		// caching it before it flips would freeze "still under obligation" onto
		// a blob whose deadline has since passed.
		if rc != nil && rc.WindowOver {
			s.blobs.put(hash, blobVerdict{fp: fp, classes: classes, total: total, rc: rc})
		}
	}
	return out, nil
}

// reconstructable computes the verdict at the latest COMPLETE in-window
// schedule point: the newest point at which every assigned validator has a
// real result (HEALTHY, FAULT, ... but not NOT_PROBED or PROBE_ERROR). The
// distinct row indices of validators that served correctly there are
// compared to OriginalRows. If no point is complete yet, the newest point in
// progress is reported with status "pending": a validator without a row is
// a gap in observation, not a validator that failed to serve.
func (s *Server) reconstructable(ctx context.Context, hash string, pin asOfPin) (*reconstruct, error) {
	db := s.st.DB()
	var assigned int
	var needed, total sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT validators_with_rows,
			json_extract(raw_json, '$.assignment.protocol_params.original_rows'),
			json_extract(raw_json, '$.assignment.protocol_params.total_rows')
		FROM publications WHERE promise_hash = ?`, hash).Scan(&assigned, &needed, &total)
	if errors.Is(err, sql.ErrNoRows) {
		return &reconstruct{Status: "unknown"}, nil
	}
	if err != nil {
		return nil, err
	}

	// How many assigned validators the promise proves stored the blob.
	// COUNT(attested) skips NULLs, so knownAtt = 0 means this publication
	// predates signature verification and attestation says nothing here.
	var knownAtt, attested int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(attested), COALESCE(SUM(attested), 0) FROM assignments WHERE promise_hash = ? AND row_count > 0`,
		hash).Scan(&knownAtt, &attested); err != nil {
		return nil, err
	}
	attestationKnown := knownAtt > 0

	// in-window points with real results, newest first, with how many
	// distinct assigned validators answered at each. A point the
	// correlated-failure guard calls suspect is not one to judge the blob
	// at: most of its validators failing at once is the observer's own
	// trouble as likely as theirs, and every rate already leaves it out.
	// Judging there published "not rebuildable" on the observer's
	// blindness. Tallied over this blob's rows, as reconstructBatch does.
	pb, pargs := pin.bound("probes", []any{hash})
	spts, err := rollup.SuspectPoints(ctx, db, `promise_hash = ?`+pb, pargs...)
	if err != nil {
		return nil, err
	}
	sx, sxArgs := rollup.Exclusion("scheduled_at", spts)
	rows, err := db.QueryContext(ctx, `SELECT scheduled_at, schedule_label, must_serve_until, COUNT(DISTINCT validator_address),
			COUNT(DISTINCT CASE WHEN attested = 1 THEN validator_address END) FROM probes
		WHERE promise_hash = ? AND phase = 'in_window' AND assigned = 1
		  AND classification NOT IN ('NOT_PROBED','PROBE_ERROR')`+pb+sx+`
		GROUP BY scheduled_at ORDER BY scheduled_at DESC`, append(pargs, sxArgs...)...)
	if err != nil {
		return nil, err
	}
	var pointAt, label, msu string
	var probed int
	complete := false
	first := true
	for rows.Next() {
		var at, lb, m string
		var n, nAtt int
		if err := rows.Scan(&at, &lb, &m, &n, &nAtt); err != nil {
			rows.Close()
			return nil, err
		}
		if first {
			pointAt, label, msu, probed = at, lb, m, n
			first = false
		}
		if pointComplete(n, nAtt, assigned, attestationKnown, attested) {
			pointAt, label, msu, probed = at, lb, m, n
			complete = true
			break
		}
	}
	rows.Close()
	if first {
		return &reconstruct{Status: "unknown"}, nil
	}
	windowOver := pin.over(msu)

	// distinct validators that served correctly at the point (any vantage
	// counts once) and the union of their assigned rows.
	srows, err := db.QueryContext(ctx, `SELECT DISTINCT p.validator_address, a.rows_json, a.attested FROM probes p
		JOIN assignments a ON a.promise_hash = p.promise_hash AND a.validator_address = p.validator_address
		WHERE p.promise_hash = ? AND p.scheduled_at = ? AND p.assigned = 1 AND p.phase = 'in_window' AND p.outcome = 'SERVED_OK'`+func() string { b, _ := pin.bound("p", nil); return b }(), append([]any{hash, pointAt}, pinArgs(pin)...)...)
	if err != nil {
		return nil, err
	}
	defer srows.Close()
	served := map[int]struct{}{}
	servedBy, servedAtt := 0, 0
	rowsKnown := true
	for srows.Next() {
		var addr string
		var rj sql.NullString
		var att sql.NullInt64
		if err := srows.Scan(&addr, &rj, &att); err != nil {
			return nil, err
		}
		servedBy++
		if att.Valid && att.Int64 == 1 {
			servedAtt++
		}
		// A shard this validator served counts toward reconstruction whether
		// or not its storage was proven: the rows came back, so the data was
		// there. Attestation decides blame, never availability.
		if !rj.Valid {
			rowsKnown = false
			continue
		}
		var idx []int
		if err := json.Unmarshal([]byte(rj.String), &idx); err != nil {
			rowsKnown = false
			continue
		}
		for _, i := range idx {
			served[i] = struct{}{}
		}
	}
	rc := &reconstruct{Point: label, PointAt: pointAt, WindowOver: windowOver, NeededRows: int(needed.Int64),
		TotalRows: int(total.Int64), ServedBy: servedBy, AssignedTotal: assigned, ServedRows: len(served),
		ProbedValidators: probed, AttestedValidators: attested, AttestationKnown: attestationKnown,
		ServedByAttested: servedAtt}

	// "yes" means nobody who was proven to owe this blob failed to serve it.
	// Without proof of storage, a validator that stayed quiet is not a fault,
	// so it must not demote a blob whose rows all came back.
	whole := servedBy == assigned
	if attestationKnown {
		whole = servedAtt == attested
	}
	switch {
	case !rowsKnown || !needed.Valid || needed.Int64 == 0:
		rc.Status = "unknown"
	case !complete:
		rc.Status = "pending"
	case len(served) >= rc.NeededRows && whole:
		rc.Status = "yes"
	case len(served) >= rc.NeededRows:
		rc.Status = "degraded"
	default:
		rc.Status = "no"
	}
	return rc, nil
}

// pointComplete reports whether a schedule point has heard from everyone it
// can judge the blob on: every assigned validator, or, where the promise's
// signatures were verified, every validator it proves obliged. The second
// is what an end-of-window reading of the endorsed validators alone
// (sentinel-probe -end-read) can complete; a validator the promise does not
// name as a signer owes the blob nothing, so its missing row leaves nothing
// undecided. reconstructBatch applies the same rule.
func pointComplete(probed, probedAtt, assigned int, attestationKnown bool, attested int) bool {
	if assigned > 0 && probed >= assigned {
		return true
	}
	return attestationKnown && attested > 0 && probedAtt >= attested
}

// reconstructSample bounds how many of the newest publications the network
// reconstructability rate is computed over per request. The bound is real and
// is published: reconstructSummary carries how many publications the window
// holds and how many were examined, so a rate over the newest 2000 of 50000
// cannot be read as a rate over the window.
const reconstructSample = 2000

// reconstructSummary is the network reconstructability figure with everything
// a reader needs to know what it covers. "Degraded" is reported on its own
// rather than folded into the numerator: it means the rows were all there but
// a validator proven to owe the blob did not answer, which is not the same
// statement as "the blob could be rebuilt with everyone serving".
type reconstructSummary struct {
	// Rate is fully-served publications over those with a verdict.
	Rate Rate `json:"rate"`
	// Recoverable counts publications where enough distinct rows came back to
	// rebuild the blob, whether or not every obliged validator answered. This
	// is the availability question; Rate is the compliance one.
	Recoverable Rate  `json:"recoverable"`
	Yes         int64 `json:"yes"`
	Degraded    int64 `json:"degraded"`
	No          int64 `json:"no"`
	// Pending and Unknown are publications with no verdict yet: a sweep still
	// running, or row lists that were not recorded.
	Pending int64 `json:"pending"`
	Unknown int64 `json:"unknown"`
	// PublicationsInWindow is every publication the window holds; Examined is
	// how many this request actually looked at. They differ when the window
	// holds more than the sample bound, and the dashboard says so when they do.
	PublicationsInWindow int64 `json:"publications_in_window"`
	Examined             int64 `json:"publications_examined"`
	SampleLimit          int   `json:"sample_limit"`
}

func (s *Server) reconstructableCount(ctx context.Context, win Window) (reconstructSummary, error) {
	out := reconstructSummary{SampleLimit: reconstructSample}
	if err := s.st.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM publications WHERE settlement_time >= ? AND settlement_time <= ?`, win.startArg(), win.endArg()).Scan(&out.PublicationsInWindow); err != nil {
		return out, err
	}
	// Statuses only: the summary publishes no row count, so the bounds settle
	// every verdict and not one row list is parsed.
	verdicts, err := s.reconstructBatch(ctx, `settlement_time >= ? AND settlement_time <= ?`, reconstructSample, pinFor(win), win.startArg(), win.endArg())
	if err != nil {
		return out, err
	}
	out.Examined = int64(len(verdicts))
	for _, rc := range verdicts {
		if rc == nil {
			out.Unknown++
			continue
		}
		switch rc.Status {
		case "unknown":
			out.Unknown++
		case "pending":
			out.Pending++
		case "yes":
			out.Yes++
		case "degraded":
			out.Degraded++
		default:
			out.No++
		}
	}
	den := out.Yes + out.Degraded + out.No
	out.Rate = rate(out.Yes, den)
	out.Recoverable = rate(out.Yes+out.Degraded, den)
	return out, nil
}

// blobPageDefault is the page size /v1/blobs answers with when the caller does
// not ask for one, and the size the startup warm-up fills the verdict cache to.
const blobPageDefault = 50

func (s *Server) handleBlobs(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r, blobPageDefault, 500)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	var where string
	var args []any
	if ns := r.URL.Query().Get("namespace"); ns != "" {
		where, args = `namespace = ?`, []any{strings.ToLower(ns)}
	}
	if before := r.URL.Query().Get("before_height"); before != "" {
		// The cursor is (height, tx_index) because a block can carry several
		// publications: "< height" alone drops the rest of the block the page
		// boundary fell inside. before_tx_index defaults to 0, which with
		// the tuple comparison means "everything before this height".
		h, err := strconv.ParseInt(before, 10, 64)
		if err != nil {
			writeErr(w, 400, "before_height must be an integer")
			return
		}
		idx := int64(0)
		if raw := r.URL.Query().Get("before_tx_index"); raw != "" {
			idx, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || idx < 0 {
				writeErr(w, 400, "before_tx_index must be a non-negative integer")
				return
			}
		}
		if where != "" {
			where += " AND "
		}
		where += `(settlement_height < ? OR (settlement_height = ? AND settlement_tx_index < ?))`
		args = append(args, h, h, idx)
	}
	// offset: numbered pages over the same order. total counts every
	// publication the filters select, cursor included, so a page reads
	// "51–75 of total".
	offset := 0
	if raw := r.URL.Query().Get("offset"); raw != "" {
		o, err := strconv.Atoi(raw)
		if err != nil || o < 0 || o > maxBlobOffset {
			writeErr(w, 400, fmt.Sprintf("offset must be an integer from 0 to %d", maxBlobOffset))
			return
		}
		offset = o
	}
	blobs, err := s.blobRowsAt(r.Context(), where, limit, offset, args...)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	if blobs == nil {
		blobs = []blobRow{}
	}
	blobs, truncated := trim(blobs, limit)
	var total int64
	countQ := `SELECT COUNT(*) FROM publications`
	if where != "" {
		countQ += " WHERE " + where
	}
	if err := s.st.DB().QueryRowContext(r.Context(), countQ, args...).Scan(&total); err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	out := map[string]any{"vantage": s.vantage, "blobs": blobs, "limit": limit, "offset": offset, "total": total, "truncated": truncated,
		"namespace": strings.ToLower(r.URL.Query().Get("namespace"))}
	if truncated && len(blobs) > 0 {
		// The cursor this route already takes, filled in so a caller does not
		// have to read the last row to build it.
		last := blobs[len(blobs)-1]
		out["next_before_height"], out["next_before_tx_index"] = last.SettlementHeight, last.SettlementTxIndex
	}
	writeJSON(w, 200, out)
}

type assignmentRow struct {
	ValidatorAddress string `json:"validator_address"`
	// Moniker is the name from the staking module, so this table reads like
	// a list of validators rather than a list of hashes. Empty when the chain
	// has no validator at this consensus address.
	Moniker     string `json:"moniker,omitempty"`
	VotingPower int64  `json:"voting_power"`
	RowCount    int    `json:"row_count"`
	// Attested: the settled promise carries a signature from this validator
	// that verified against its consensus key, which is proof it stored the
	// shard. false means unproven, null means the record predates
	// verification. Never "did not store".
	Attested *bool `json:"attested"`
	// HostAtSettlement is the host registered when the promise settled:
	// where the upload went. Null when the scanner could not read the
	// registry at that height; empty when none was registered.
	HostAtSettlement *string `json:"host_at_settlement"`
	// Service is this validator's obligation on this blob, by the rule the
	// obligation counts use (rollup.ObligationBuckets): served, not_served,
	// in_retention_window, deadline_unverified, no_verdict (read, but no
	// reading near the end that counts either way) or not_read. Absent when
	// nothing is owed (not endorsed) or no reading counts.
	Service string `json:"service,omitempty"`
	// Provisional marks a not_served still younger than the settling period.
	Provisional bool `json:"provisional,omitempty"`
}

// blobServiceSQL reduces one publication's obligation buckets to one word
// per validator, in the order ObligationSums partitions them.
const blobServiceSQL = `SELECT validator_address,
			CASE WHEN pending THEN 'in_retention_window'
			     WHEN faults > 0 THEN 'not_served'
			     WHEN last_cls = 'HEALTHY' AND late_healthy > 0 THEN 'served'
			     WHEN healthy > 0 THEN 'no_verdict'
			     WHEN held > 0 THEN 'deadline_unverified'
			     WHEN attempted > 0 THEN 'no_verdict'
			     ELSE 'not_read' END,
			COALESCE(NOT pending AND faults > 0 AND first_fault > ?, 0)
		FROM (`

func (s *Server) handleBlob(w http.ResponseWriter, r *http.Request) {
	hash := strings.ToLower(r.PathValue("hash"))
	ctx := r.Context()
	blobs, err := s.blobRows(ctx, `promise_hash = ?`, 1, hash)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	if len(blobs) == 0 {
		writeErr(w, 404, "no publication with this promise hash")
		return
	}
	rows, err := s.st.DB().QueryContext(ctx, `SELECT a.validator_address, a.voting_power, a.row_count, a.attested,
			COALESCE(i.moniker, ''), a.host_at_settlement
		FROM assignments a
		LEFT JOIN validator_identities i ON i.cons_address = a.validator_address
		WHERE a.promise_hash = ? ORDER BY a.voting_power DESC, a.validator_address`, hash)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	assigns := []assignmentRow{} // a list, never null, when nothing was assigned
	for rows.Next() {
		var a assignmentRow
		var att sql.NullInt64
		if err := rows.Scan(&a.ValidatorAddress, &a.VotingPower, &a.RowCount, &att, &a.Moniker, &a.HostAtSettlement); err != nil {
			rows.Close()
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		if att.Valid {
			b := att.Int64 == 1
			a.Attested = &b
		}
		assigns = append(assigns, a)
	}
	rows.Close()
	probes, err := s.probeRows(ctx, `promise_hash = ?`, 1000, hash)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	probes, moreProbes := trim(probes, 1000)
	// The points of this blob the correlated-failure guard calls suspect,
	// tallied as reconstructable tallies them: published with the blob, and
	// left out of each validator's service word as every count leaves them out.
	suspect := []suspectPoint{}
	var ss suspectSet
	spts, err := rollup.SuspectPoints(ctx, s.st.DB(), `promise_hash = ?`, hash)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	for _, p := range spts {
		if reason := p.Reason(); reason != "" {
			suspect = append(suspect, suspectPoint{At: p.At, Label: p.Label, Validators: p.Validators,
				Unreachable: rate(p.Unreachable, p.Validators), Fault: rate(p.Faulted, p.Validators), Reason: reason})
			ss.args = append(ss.args, p.At)
		}
	}
	if err := s.blobService(ctx, hash, ss, assigns); err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	var params struct {
		ShardRetentionS        int64 `json:"shard_retention_s"`
		PaymentPromiseTimeoutS int64 `json:"payment_promise_timeout_s"`
	}
	_ = s.st.DB().QueryRowContext(ctx, `SELECT shard_retention_s, payment_promise_timeout_s FROM publications WHERE promise_hash = ?`, hash).Scan(&params.ShardRetentionS, &params.PaymentPromiseTimeoutS)
	writeJSON(w, 200, map[string]any{"blob": blobs[0], "params": params, "assignments": assigns,
		"probes": probes, "probes_truncated": moreProbes, "suspect_points": suspect, "vantage": s.vantage})
}

// blobService fills each assignment's Service from the obligation buckets
// of this one publication, with the same suspect points left out as the
// page shows, so a validator's word here is the one its counts carry.
func (s *Server) blobService(ctx context.Context, hash string, ss suspectSet, assigns []assignmentRow) error {
	var settled, msu string
	if err := s.st.DB().QueryRowContext(ctx, `SELECT settlement_time, must_serve_until FROM publications WHERE promise_hash = ?`, hash).Scan(&settled, &msu); err != nil {
		return err
	}
	now := time.Now().UTC()
	args := []any{provisionalCutoff(now), store.TS(now), settled, settled, store.TS(now), rollup.RowLowerBound(settled)}
	args = append(args, ss.args...)
	rows, err := s.st.DB().QueryContext(ctx, blobServiceSQL+obligationBuckets+ss.clause("pr.scheduled_at")+` AND pr.promise_hash = ?)
			GROUP BY validator_address, promise_hash)`, append(args, hash)...)
	if err != nil {
		return err
	}
	defer rows.Close()
	by := map[string]int{}
	for i, a := range assigns {
		by[a.ValidatorAddress] = i
	}
	for rows.Next() {
		var addr, service string
		var prov bool
		if err := rows.Scan(&addr, &service, &prov); err != nil {
			return err
		}
		if i, ok := by[addr]; ok {
			assigns[i].Service, assigns[i].Provisional = service, prov
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// Before its first reading an endorsed validator has no bucket yet; while
	// the window runs it owes the blob all the same.
	if msu > store.TS(now) {
		for i, a := range assigns {
			if a.Service == "" && a.Attested != nil && *a.Attested {
				assigns[i].Service = "in_retention_window"
			}
		}
	}
	return nil
}

// ---- sampling ----

// handleSampling publishes the load policy's admission decisions so the
// commit-and-reveal audit the methodology page describes can actually be
// carried out. Each row is one day's commitment to the secret the draws used,
// with the publications decided under it and the probability each was drawn
// at. Once the day's secret is revealed, anyone can recompute
// H(promise_hash || secret) < p * 2^64 for every promise hash of that day and
// check this observer's sample against their own.
func (s *Server) handleSampling(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	win, err := parseWindow(r, time.Now())
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	type day struct {
		DayCommitment string  `json:"day_commitment"`
		Binding       string  `json:"binding"`
		P             float64 `json:"p"`
		Publications  int64   `json:"publications"`
		Probed        int64   `json:"publications_probed"`
		SampledOut    int64   `json:"publications_sampled_out"`
		// Day, Secret and RevealedAt are filled once the prober has
		// published the day's secret (sampling-secrets.jsonl): from then
		// on H(promise_hash || secret) < p * 2^64 can be recomputed by
		// anyone for every promise settled that day.
		Day        *string `json:"day"`
		Secret     *string `json:"secret"`
		RevealedAt *string `json:"revealed_at"`
	}
	secrets := map[string]struct{ day, secret, at string }{}
	if srows, err := s.st.DB().QueryContext(ctx, `SELECT commitment, day, secret, revealed_at FROM sampling_secrets`); err == nil {
		for srows.Next() {
			var c, d, sec, at string
			if srows.Scan(&c, &d, &sec, &at) == nil {
				secrets[c] = struct{ day, secret, at string }{d, sec, at}
			}
		}
		srows.Close()
	}
	// A sampled-out publication is one sampling_decisions row, standing for
	// NOT_PROBED rows that all carry its draw and start at decided_at; it
	// joins the probe rows here as one row of the same shape, which is all
	// the per-promise counts below need of it.
	rows, err := s.st.DB().QueryContext(ctx, `SELECT
			COALESCE(sampling_commitment, '') AS c,
			COALESCE(sampling_binding, '') AS b,
			COALESCE(sampling_p, 1.0) AS p,
			COUNT(DISTINCT promise_hash),
			COUNT(DISTINCT CASE WHEN classification != 'NOT_PROBED' THEN promise_hash END),
			COUNT(DISTINCT CASE WHEN classification = 'NOT_PROBED' AND classification_reason LIKE 'budget:%' THEN promise_hash END)
		FROM (
			SELECT sampling_commitment, sampling_binding, sampling_p, promise_hash, classification, classification_reason
			FROM probes WHERE started_at >= ? AND started_at <= ?
			UNION ALL
			SELECT sampling_commitment, sampling_binding, sampling_p, promise_hash, 'NOT_PROBED', reason
			FROM sampling_decisions WHERE decided_at >= ? AND decided_at <= ?
		)
		GROUP BY c, b, p ORDER BY c, p`, win.startArg(), win.endArg(), win.startArg(), win.endArg())
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	defer rows.Close()
	days := []day{}
	revealed := 0
	for rows.Next() {
		var d day
		if err := rows.Scan(&d.DayCommitment, &d.Binding, &d.P, &d.Publications, &d.Probed, &d.SampledOut); err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		if sec, ok := secrets[d.DayCommitment]; ok {
			dd, ss, at := sec.day, sec.secret, sec.at
			d.Day, d.Secret, d.RevealedAt = &dd, &ss, &at
			revealed++
		}
		days = append(days, d)
	}
	if err := rows.Err(); err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"window":    win,
		"vantage":   s.vantage,
		"decisions": days,
		"how_to_audit": "Each row commits to that day's secret as SHA256(secret). " +
			"The prober publishes a day's secret " + policyRevealNote + " after the day ends (the row's secret field; " +
			"the record is sampling-secrets.jsonl, also in the daily export). With it, recompute " +
			"H(promise_hash || secret) < p * 2^64 for every MsgPayForFibre settled that day: the promise hashes that " +
			"pass are the ones this observer should have probed, and /v1/probes says which ones it did (probes) and which it drew out (sampled_out). " +
			"sentinel-recompute -sampling does this from the export.",
		"days_listed":      len(days),
		"secrets_revealed": revealed,
		"secret_published": revealed > 0,
	})
}

// policyRevealNote is the reveal delay as the prober's default (policy.
// DefaultRevealAfter); the API does not import the policy package.
const policyRevealNote = "seven days"

// ---- probes ----

type probeRow struct {
	Vantage          string `json:"vantage"`
	PromiseHash      string `json:"promise_hash"`
	ValidatorAddress string `json:"validator_address"`
	ValidatorHost    string `json:"validator_host"`
	Assigned         bool   `json:"assigned"`
	// Attested: the settled promise proves this validator stored the blob.
	// false means unproven (so this probe is UNATTESTED and outside the serve
	// rate), null means the measurement predates signature verification.
	Attested         *bool  `json:"attested"`
	AssignedRowCount int    `json:"assigned_row_count"`
	ScheduleLabel    string `json:"schedule_label"`
	ScheduledAt      string `json:"scheduled_at"`
	StartedAt        string `json:"started_at"`
	Phase            string `json:"phase"`
	Outcome          string `json:"outcome"`
	Classification   string `json:"classification"`
	Reason           string `json:"classification_reason"`
	RowsReturned     int    `json:"rows_returned"`
	RowsExpected     int    `json:"rows_expected"`
	TotalDurationMS  int64  `json:"total_duration_ms"`
	TLSOK            bool   `json:"tls_ok"`
	IdentityOK       bool   `json:"identity_ok"`
	RawError         string `json:"raw_error,omitempty"`
	// RetryFirstOutcome is set when this probe was the second attempt after a
	// transport timeout; it is the first attempt's outcome, so a reader can
	// see "the first try timed out" rather than only the final verdict.
	RetryFirstOutcome string `json:"retry_first_outcome,omitempty"`
	ClockOffsetMS     int64  `json:"clock_offset_ms,omitempty"`
	// The evidence behind the verdict, when the row carries it (rows from
	// before schema 9 do not): the row indices returned, a digest of the
	// returned payload, the gRPC status code, the promise whose shard
	// answered instead, and the observer build and chain app version the
	// classification was made under.
	RowIndices    []uint32 `json:"row_indices,omitempty"`
	RowsSHA256    string   `json:"rows_sha256,omitempty"`
	RPCCode       string   `json:"rpc_code,omitempty"`
	ShadowedBy    string   `json:"shadowed_by,omitempty"`
	ObserverBuild string   `json:"observer_build,omitempty"`
	AppVersion    int64    `json:"app_version,omitempty"`
	// RetentionUnverified says this row's publication sits inside an
	// x/fibre params range this observer has not read every height of, so
	// the deadline its phase and verdict were drawn against may not be the
	// one the server used. While it is set, classification reads
	// RETENTION_UNVERIFIED rather than the HEALTHY or FAULT the row was
	// stamped with, and classification_at_probe is not that stamp — see
	// corrected_at, which is set only once a verified range actually moved
	// the row.
	RetentionUnverified bool   `json:"retention_unverified,omitempty"`
	PhaseAtProbe        string `json:"phase_at_probe,omitempty"`
	CorrectedAt         string `json:"corrected_at,omitempty"`
	// ShadowGap says why the verdict on genuine rows no scanned promise
	// assigns was deferred at the probe (the store serves by promise-hash
	// order, so a promise settling after the probe can own them), or that
	// a scan gap makes it permanent. ClassificationAtProbe and AmendedAt
	// are set once the collector's late judgement replaced the verdict
	// (probe_amendments): the row keeps what it was stamped with.
	ShadowGap             string `json:"shadow_gap,omitempty"`
	ClassificationAtProbe string `json:"classification_at_probe,omitempty"`
	AmendedAt             string `json:"amended_at,omitempty"`
	// HostAtSettlement is where the upload went; HostChanged when the host
	// probed differs from it (the validator re-registered during the
	// window). SettlementHostOutcome and SettlementHostServed are the
	// evidence probe of the old host, run when the new one did not serve;
	// they never change the verdict.
	HostAtSettlement      string `json:"host_at_settlement,omitempty"`
	HostChanged           bool   `json:"host_changed,omitempty"`
	SettlementHostOutcome string `json:"settlement_host_outcome,omitempty"`
	SettlementHostServed  *bool  `json:"settlement_host_served,omitempty"`
	// Provisional marks a FAULT younger than verdict.FaultSettling: it
	// counts, and it can still be withdrawn (provisional.go).
	Provisional bool `json:"provisional,omitempty"`
	// ClearedBy names the vantage that fetched this FAULT's rows again
	// within the confirmation window and got them verified: the fault is
	// withdrawn, the row reads PROBE_ERROR and classification_at_probe says
	// FAULT (verdict.ConfirmFault). ConfirmedBy names the vantage that
	// tried and did not get them either; the fault stands.
	ClearedBy   string `json:"cleared_by,omitempty"`
	ConfirmedBy string `json:"confirmed_by,omitempty"`
}

func (s *Server) probeRows(ctx context.Context, where string, limit int, args ...any) ([]probeRow, error) {
	// The effective classification, not the stored one. /v1/probes,
	// /v1/blobs/{hash}.probes and a validator's recent probes are all
	// served from here, and a held row published as a bare "FAULT" beside
	// validator_address is the accusation this whole mechanism exists to
	// withhold — a third party counting classifications would count it.
	// retention_unverified rides along so a reader can see why, and
	// phase_at_probe / classification_at_probe / corrected_at say what the
	// row was stamped with before a correction moved it.
	q := `SELECT vantage, promise_hash, validator_address, validator_host, assigned, attested, assigned_row_count, schedule_label, scheduled_at,
		started_at, phase, outcome, ` + rollup.EffectiveClass("") + `, classification_reason, rows_returned, rows_expected, total_duration_ms, tls_ok, identity_ok, raw_error,
		COALESCE(retry_first_outcome, ''), COALESCE(clock_offset_ms, 0),
		COALESCE(row_indices, ''), COALESCE(rows_sha256, ''), COALESCE(rpc_code, ''), COALESCE(shadowed_by, ''), COALESCE(observer_build, ''), COALESCE(app_version, 0),
		COALESCE(shadow_gap, ''), COALESCE(classification_at_probe, ''), COALESCE(amended_at, ''),
		COALESCE(host_at_settlement, ''), COALESCE(settlement_host_outcome, ''), settlement_host_served,
		retention_unverified, COALESCE(phase_at_probe, ''), COALESCE(corrected_at, ''),
		COALESCE(cleared_by, ''), COALESCE(confirmed_by, '')
		FROM probes`
	if where != "" {
		q += " WHERE " + where
	}
	// One more than asked, so the caller can be told the bound bit rather
	// than left to guess whether 100 rows is all of them. The extra row is
	// trimmed by the caller that reports truncation.
	q += " ORDER BY started_at DESC LIMIT " + strconv.Itoa(limit+1)
	rows, err := s.st.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []probeRow{}
	now := time.Now()
	for rows.Next() {
		var p probeRow
		var assigned, tls, id, held int
		var att sql.NullInt64
		var idxJSON string
		var served sql.NullInt64
		if err := rows.Scan(&p.Vantage, &p.PromiseHash, &p.ValidatorAddress, &p.ValidatorHost, &assigned, &att, &p.AssignedRowCount, &p.ScheduleLabel,
			&p.ScheduledAt, &p.StartedAt, &p.Phase, &p.Outcome, &p.Classification, &p.Reason, &p.RowsReturned, &p.RowsExpected,
			&p.TotalDurationMS, &tls, &id, &p.RawError, &p.RetryFirstOutcome, &p.ClockOffsetMS,
			&idxJSON, &p.RowsSHA256, &p.RPCCode, &p.ShadowedBy, &p.ObserverBuild, &p.AppVersion,
			&p.ShadowGap, &p.ClassificationAtProbe, &p.AmendedAt, &p.HostAtSettlement, &p.SettlementHostOutcome, &served,
			&held, &p.PhaseAtProbe, &p.CorrectedAt, &p.ClearedBy, &p.ConfirmedBy); err != nil {
			return nil, err
		}
		p.RetentionUnverified = held == 1
		p.Provisional = isProvisional(p.Classification, p.ScheduleLabel, p.StartedAt, now)
		p.HostChanged = p.HostAtSettlement != "" && p.ValidatorHost != "" && p.ValidatorHost != p.HostAtSettlement
		if served.Valid {
			b := served.Int64 == 1
			p.SettlementHostServed = &b
		}
		if idxJSON != "" {
			_ = json.Unmarshal([]byte(idxJSON), &p.RowIndices)
		}
		p.Assigned, p.TLSOK, p.IdentityOK = assigned == 1, tls == 1, id == 1
		if att.Valid {
			b := att.Int64 == 1
			p.Attested = &b
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Server) handleProbes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := parseLimit(r, 100, 1000)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	var conds []string
	var args []any
	// The same selection over the sampled-out decisions (store/sampledout.go):
	// each stands for NOT_PROBED rows, started at decided_at, of every
	// validator its publication assigned, at each of its points.
	var dconds []string
	var dargs []any
	decisions := true
	if v := q.Get("validator"); v != "" {
		addr, err := s.resolveAddr(r.Context(), v)
		if err != nil {
			s.writeAddrErr(w, r.URL.Path, err)
			return
		}
		conds, args = append(conds, `validator_address = ?`), append(args, addr)
		dconds = append(dconds, `EXISTS (SELECT 1 FROM assignments a WHERE a.promise_hash = d.promise_hash AND a.validator_address = ? AND a.row_count > 0)`)
		dargs = append(dargs, addr)
	}
	if b := q.Get("blob"); b != "" {
		conds, args = append(conds, `promise_hash = ?`), append(args, strings.ToLower(b))
		dconds, dargs = append(dconds, `d.promise_hash = ?`), append(dargs, strings.ToLower(b))
	}
	if since := q.Get("since"); since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			writeErr(w, 400, "since must be RFC 3339")
			return
		}
		conds, args = append(conds, `started_at >= ?`), append(args, store.TS(t))
		dconds, dargs = append(dconds, `d.decided_at >= ?`), append(dargs, store.TS(t))
	}
	if c := q.Get("class"); c != "" {
		// Filtered on the class the rows are published with, so ?class=FAULT
		// never returns a row that reads RETENTION_UNVERIFIED.
		conds, args = append(conds, rollup.EffectiveClass("")+` = ?`), append(args, strings.ToUpper(c))
		decisions = strings.ToUpper(c) == "NOT_PROBED"
	}
	// served=no: the rows the obligations count as not served
	// (rollup.ObligationClass), including end readings that returned no rows.
	if q.Get("served") == "no" {
		conds = append(conds, rollup.ObligationClass("")+` = 'FAULT'`)
		decisions = false
	}
	// at: one schedule point, exactly as vantage_health.suspect lists it, so
	// the rows behind an incident are one link away.
	if at := q.Get("at"); at != "" {
		// Parsed, not passed through. A timestamp in any other spelling
		// matched nothing after scanning the table for it, which reads to a
		// caller as "no rows at that point" — the opposite of what an
		// exclusion's evidence link is for.
		t, err := time.Parse(store.TimeLayout, at)
		if err != nil {
			if t, err = time.Parse(time.RFC3339, at); err != nil {
				writeErr(w, 400, "at must be a schedule point as vantage_health.suspect prints it (RFC 3339)")
				return
			}
		}
		conds, args = append(conds, `scheduled_at = ?`), append(args, store.TS(t))
		dconds = append(dconds, `EXISTS (SELECT 1 FROM sampling_decision_points pt WHERE pt.vantage = d.vantage AND pt.promise_hash = d.promise_hash AND pt.scheduled_at = ?)`)
		dargs = append(dargs, store.TS(t))
	}
	// before: the upper bound that makes the list walkable. The order is
	// started_at DESC and since is a lower bound, so without this there was
	// no parameter that could reach the rows past the limit — on the one
	// route docs/verdicts.md points a reader at as the evidence behind an
	// exclusion, where a suspect point can hold more rows than the maximum
	// limit allows.
	if before := q.Get("before"); before != "" {
		t, err := time.Parse(time.RFC3339, before)
		if err != nil {
			writeErr(w, 400, "before must be RFC 3339")
			return
		}
		conds, args = append(conds, `started_at < ?`), append(args, store.TS(t))
		dconds, dargs = append(dconds, `d.decided_at < ?`), append(dargs, store.TS(t))
	}
	rows, err := s.probeRows(r.Context(), strings.Join(conds, " AND "), limit, args...)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	rows, truncated := trim(rows, limit)
	sampled := []store.SampledOutDecision{}
	if decisions {
		if sampled, err = s.st.SampledOutDecisions(r.Context(), strings.Join(dconds, " AND "), limit+1, dargs...); err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
	}
	sampled, sampledTruncated := trim(sampled, limit)
	out := map[string]any{"vantage": s.vantage, "probes": rows, "limit": limit, "truncated": truncated,
		// Publications the load policy drew out of the sample, matching the
		// same filters: each is one record standing for a NOT_PROBED row per
		// assigned validator per point (rows), which probes does not repeat.
		"sampled_out": sampled, "sampled_out_truncated": sampledTruncated}
	if truncated && len(rows) > 0 {
		// Where to continue from: everything strictly older than the last row
		// returned. Paired with the same filters it walks the whole selection.
		out["next_before"] = rows[len(rows)-1].StartedAt
	}
	writeJSON(w, 200, out)
}

// trim cuts an over-fetched page back to the limit and says whether there was
// more. A list that stops at its limit without saying so reads as the whole
// answer, which on this API is the difference between "these are the rows" and
// "these are some of the rows".
func trim[T any](rows []T, limit int) ([]T, bool) {
	if len(rows) > limit {
		return rows[:limit], true
	}
	return rows, false
}

// parseLimit reads ?limit= with a default and a maximum; anything that is
// not an integer in [1, max] is a 400, never a silent fallback.
func parseLimit(r *http.Request, def, max int) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return def, nil
	}
	l, err := strconv.Atoi(raw)
	if err != nil || l < 1 || l > max {
		return 0, fmt.Errorf("limit must be an integer between 1 and %d", max)
	}
	return l, nil
}

// ---- exports ----

// exportsDir is where the collector builds the daily exports.
func (s *Server) exportsDir() string {
	if s.dataDir == "" {
		return ""
	}
	return filepath.Join(s.dataDir, "exports")
}

// handleExports lists the daily exports: one tarball per UTC day holding
// every record file's lines for that day, with a manifest of digests. It
// is what a verifier downloads; sentinel-recompute re-derives every verdict
// and every published figure from it.
func (s *Server) handleExports(w http.ResponseWriter, r *http.Request) {
	entries := []export.Entry{}
	if dir := s.exportsDir(); dir != "" {
		var err error
		if entries, err = export.ReadIndex(dir); err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
	}
	out := map[string]any{
		"vantage": s.vantage,
		"exports": entries,
		"how_to_verify": "download /v1/exports/<name>, check its sha256 against the entry (and the .sha256 sidecar), " +
			"untar, check each member against manifest.json, then run sentinel-recompute on the directory: it re-derives " +
			"every row's phase and classification from the row's own fields and the run's recorded configuration, and every " +
			"obligation figure from the rows, and prints what differs from this API's /v1/validators?as_of=<day end>.",
		"rule": "records are assigned to a day by their own timestamp; a record that reached the file after its day's export was built is in the next export, counted as late",
	}
	// Whether exports are signed, by which key, and how to check (see
	// exports_signing.go). An unreadable key record hides the block rather
	// than failing the list: the exports are still the exports.
	if sig, err := s.exportSigning(); err == nil {
		out["signing"] = sig
	}
	writeJSON(w, 200, out)
}

// handleExportFile serves one export or its digest sidecar. Names are
// checked against the export name pattern, so nothing else under the
// directory is reachable.
// handleAvatar serves the Keybase picture the collector holds for an
// identity, from the store: the site's img-src stays 'self' and a reader
// never fetches from Keybase's CDN. A day of caching matches the
// collector's refresh.
func (s *Server) handleAvatar(w http.ResponseWriter, r *http.Request) {
	id := strings.ToUpper(r.PathValue("identity"))
	if !keybase.ValidIdentity(id) {
		writeErr(w, 404, "no such avatar")
		return
	}
	// Avatar matches case-insensitively, so one lookup covers whichever
	// spelling the chain carries and whichever an older build stored.
	ct, data, checked, ok, err := s.st.Avatar(r.Context(), id)
	if err != nil {
		writeErr(w, 500, "avatar lookup failed")
		return
	}
	if !ok {
		writeErr(w, 404, "no such avatar")
		return
	}
	// Re-checked here, not trusted from the row: this is served from the
	// API's own origin, and an active type (image/svg+xml is an image) would
	// run in it. A row written by an older build, or by a build with a wider
	// list, must not decide that.
	if !keybase.InertType(ct) {
		writeErr(w, 404, "no such avatar")
		return
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Disposition", "inline; filename=avatar")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	http.ServeContent(w, r, "", checked, bytes.NewReader(data))
}

func (s *Server) handleExportFile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	dir := s.exportsDir()
	if dir == "" || !export.NamePattern.MatchString(name) {
		writeErr(w, 404, "no such export")
		return
	}
	path := filepath.Join(dir, name)
	f, err := os.Open(path)
	if err != nil {
		writeErr(w, 404, "no such export")
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		writeErr(w, 404, "no such export")
		return
	}
	if strings.HasSuffix(name, ".sha256") {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	} else if strings.HasSuffix(name, ".sig") {
		// The signature over the manifest digest (export/sign.go).
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Disposition", "attachment; filename=\""+name+"\"")
	}
	// An export is written once and never changes; its name carries the day.
	w.Header().Set("Cache-Control", "public, max-age=86400, immutable")
	http.ServeContent(w, r, name, info.ModTime(), f)
}

// latestAssignmentSQL is the newest-assignment query validatorRows runs,
// for every validator or for only; see the comment where it is used.
func latestAssignmentSQL(only string) (string, []any) {
	v := `WITH RECURSIVE v(a) AS (
			SELECT (SELECT MIN(validator_address) FROM assignments)
			UNION ALL
			SELECT (SELECT MIN(validator_address) FROM assignments WHERE validator_address > v.a)
			FROM v WHERE v.a IS NOT NULL)`
	var args []any
	if only != "" {
		v = `WITH v(a) AS (SELECT ?)`
		args = []any{only}
	}
	return v + `,
		m(va, h) AS (SELECT a, (SELECT MAX(settlement_height) FROM assignments WHERE validator_address = v.a) FROM v WHERE a IS NOT NULL)
		SELECT a.validator_address, a.voting_power, a.row_count, a.attested, p.settlement_height, a.promise_hash
		FROM m
		JOIN assignments a ON a.validator_address = m.va AND a.settlement_height = m.h
		JOIN publications p ON p.promise_hash = a.promise_hash AND p.settlement_height = m.h`, args
}
