// observer-api serves the read-only JSON API over the observer store.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/api"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

func main() {
	var (
		dataDir = flag.String("data-dir", "./sentinel-data", "dir holding observer.db")
		dbPath  = flag.String("db", "", "SQLite database path (default <data-dir>/observer.db)")
		listen  = flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
		check   = flag.String("check", "", "health check: GET this URL, exit 0 on HTTP 200 (for container healthchecks; the image has no curl)")
		vantage = flag.String("vantage", "local", "vantage name: whose rows the figures count, the one /v1/meta marks primary, and the snapshot files' owner")
		tipRPC  = flag.String("tip-rpc", os.Getenv("RPC"), "CometBFT RPC the block ticker (/v1/tip) asks for the newest block; default $RPC, the scanner's node in the unit's env file; empty: the scanner's status file")
		// Where this observer watches from. Every reachability observation is
		// a statement about a network path and half that path is ours. Both
		// are operator-declared, and the API no longer publishes them: they
		// feed only the startup warning below.
		vLocation = flag.String("vantage-location", "", `human-readable place, e.g. "Helsinki, Finland"`)
		vProvider = flag.String("vantage-provider", "", `hosting provider, e.g. "Hetzner"`)
		// Accepted and ignored, so a unit written before the observer's
		// addresses and network stopped being published still starts.
		_ = flag.String("vantage-asn", "", "ignored: the autonomous system is no longer published")
		_ = flag.String("vantage-egress", "", "ignored: the observer's addresses are no longer published")
		// Names for publisher accounts, maintained by the operator. The
		// chain has no name for an account, so every label is the operator's
		// word and is published with its source.
		labels = flag.String("publishers", "", "path to publishers.yaml, the publisher label registry (optional)")
		// Where the window snapshots are kept across restarts, and a mode
		// that only fills them: a new build computes its snapshots beside
		// the running API before it replaces it, so it does not start cold
		// (deploy/README.md, "Upgrading a running observer").
		snapDir  = flag.String("snapshot-dir", "", "where the window snapshots are kept across restarts (default <data-dir>/snapshots; with -warm-only <data-dir>/snapshots.next)")
		warmOnly = flag.Bool("warm-only", false, "compute every window snapshot once into -snapshot-dir, reading the database only, then exit; serves nothing, and refuses the live <data-dir>/snapshots")
		// The 7d, 30d and "all" windows are summed from per-day partials
		// kept beside the snapshots (observer/api/dayparts.go). Off, every
		// window is read whole with the shipped statements, as before.
		dayParts = flag.Bool("day-partials", true, "sum the 7d, 30d and all windows from per-day partials kept in -snapshot-dir; false reads every window whole, each statement on its own, as the build before them did")
		// The sealer reads a day at a time from the disk the database is on,
		// which may be shared: after each unit it rests k times as long as
		// the unit took, live and with -warm-only alike.
		partsPace = flag.Float64("day-partials-pace", api.DefaultSealPace, "after each unit of the day partials sealer's work, rest this many times as long as it took (3: sealing reads the disk at most a quarter of the time); 0 does not rest")
		// A sealed day's service-time and transfer-rate histograms stay in
		// its seal file and are read when a window sums the day; the newest
		// days' are kept in memory up to this bound.
		partsCache = flag.Int("day-partials-cache-mb", api.DefaultHistCacheMB, "megabytes of the sealed days' service-time and transfer-rate histograms kept in memory, the newest days first; the rest are read from the seal files as the windows sum them; 0 keeps none")
	)
	flag.Parse()
	if *partsPace < 0 {
		os.Stderr.WriteString("-day-partials-pace must be 0 or more\n")
		os.Exit(2)
	}
	if *partsCache < 0 {
		os.Stderr.WriteString("-day-partials-cache-mb must be 0 or more\n")
		os.Exit(2)
	}
	if *check != "" {
		os.Exit(healthCheck(*check))
	}
	if *dbPath == "" {
		*dbPath = filepath.Join(*dataDir, "observer.db")
	}
	live := filepath.Join(*dataDir, "snapshots")
	switch {
	case *snapDir != "":
	case *warmOnly:
		*snapDir = filepath.Join(*dataDir, "snapshots.next")
	default:
		*snapDir = live
	}
	// The live directory is the one beside the database too: a hand-run that
	// names the database with -db and leaves -data-dir at its default would
	// otherwise not recognise it.
	if *warmOnly && (sameDir(*snapDir, live) || sameDir(*snapDir, filepath.Join(filepath.Dir(*dbPath), "snapshots"))) {
		// The running API reads that directory and rewrites its files on
		// every refresh, under the same temporary names a warm-up writes,
		// so the two could trip over one file; and an older API restarted
		// meanwhile would load market files computed under the new build's
		// rules, whose revision does not carry the methodology version.
		// The warm-up writes beside it and the switch copies the files in
		// with the API stopped (deploy/README.md).
		os.Stderr.WriteString("warm-only: -snapshot-dir " + *snapDir + " is the live snapshot directory; warm into another (default <data-dir>/snapshots.next) and copy the files in with the API stopped\n")
		os.Exit(2)
	}
	log := scan.NewLogger(200)
	// The collector creates the database and its schema; started in the same
	// breath, this process used to lose the race by a few milliseconds and
	// exit. Wait for it instead of insisting on an order between services.
	var st *store.Store
	for attempt := 0; ; attempt++ {
		var err error
		st, err = store.OpenReadOnly(*dbPath)
		if err == nil {
			break
		}
		if attempt >= 60 {
			log.Fatalf("open store: %v", err)
		}
		if attempt == 0 {
			log.Printf("open store: %v; waiting for the collector", err)
		}
		time.Sleep(5 * time.Second)
	}
	defer st.Close()

	info := api.VantageInfo{Name: *vantage, Location: *vLocation, Provider: *vProvider}
	if info.Location == "" || info.Provider == "" {
		// Not fatal: a devnet or a local run has nothing meaningful to say
		// here. The API does not publish the description (/v1/meta names
		// vantages only), so this line is where a public vantage that left
		// it blank shows.
		log.Printf("WARNING: vantage not fully described (location=%q provider=%q); "+
			"a public vantage should set -vantage-location and -vantage-provider",
			info.Location, info.Provider)
	}

	reg, err := api.LoadPublisherLabels(*labels)
	if err != nil {
		log.Fatalf("publisher labels: %v", err)
	}
	if len(reg) > 0 {
		log.Printf("publisher labels: %d from %s", len(reg), *labels)
	}
	opts := []api.Option{api.WithPublisherLabels(reg), api.WithDataDir(*dataDir), api.WithSnapshotDir(*snapDir), api.WithDayParts(*dayParts), api.WithSealPace(*partsPace), api.WithHistCache(*partsCache), api.WithTipRPC(*tipRPC)}
	if *warmOnly {
		// The live API keeps serving meanwhile; this only reads. Every
		// snapshot depends on the vantage (its heartbeats) and the market
		// one on the labels, so the flags must be the unit's own.
		log.Printf("warm-only: computing every window snapshot into %s (vantage=%s db=%s)", *snapDir, *vantage, *dbPath)
		t0 := time.Now()
		if err := api.WarmSnapshots(context.Background(), st, info, log, opts...); err != nil {
			log.Fatalf("warm-only: %v", err)
		}
		log.Printf("warm-only: done in %s", time.Since(t0).Round(time.Second))
		return
	}
	handler := api.NewWithVantage(st, info, log, opts...)

	srv := &http.Server{
		Addr:              *listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		// No single WriteTimeout: it applied to /v1/exports/<name>, a tarball
		// of a whole day's records, and to the pinned-window aggregates the
		// code itself measures at 23 seconds — neither is a 30-second-sized
		// answer, and both were being truncated mid-body. The handler sets a
		// deadline per route instead (api.writeDeadlineFor), so the slow two
		// get the time they need and nothing else gets to hang.
		WriteTimeout: 0,
		IdleTimeout:  60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("api up: listen=%s db=%s vantage=%s", *listen, *dbPath, *vantage)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve: %v", err)
	}
	// What the day partials, the memo and the ledger have come to since
	// their last write, so the next start begins from it.
	keepCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := handler.KeepDerived(keepCtx); err != nil {
		log.Printf("keeping the day partials, the memo and the ledger: %v", err)
	}
	cancel()
	log.Printf("stopped")
}

// sameDir reports whether a and b name one directory: the same path once
// made absolute, or, when both exist, the same directory under two names (a
// symlink, a bind mount).
func sameDir(a, b string) bool {
	aa, errA := filepath.Abs(a)
	bb, errB := filepath.Abs(b)
	if errA == nil && errB == nil && aa == bb {
		return true
	}
	fa, errA := os.Stat(a)
	fb, errB := os.Stat(b)
	return errA == nil && errB == nil && os.SameFile(fa, fb)
}

// healthCheck GETs url and returns a process exit code: 0 on HTTP 200.
func healthCheck(url string) int {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		os.Stderr.WriteString("check: " + err.Error() + "\n")
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Stderr.WriteString("check: HTTP " + resp.Status + "\n")
		return 1
	}
	return 0
}
