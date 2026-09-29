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
		vantage = flag.String("vantage", "local", "vantage name rendered on every response")
		// Where this observer watches from. Every reachability observation is
		// a statement about a network path and half that path is ours, so a
		// reader cannot judge an UNREACHABLE without knowing where it was
		// measured from. Both are operator-declared.
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
		snapDir  = flag.String("snapshot-dir", "", "where the window snapshots are kept across restarts (default <data-dir>/snapshots)")
		warmOnly = flag.Bool("warm-only", false, "compute every window snapshot once into -snapshot-dir, reading the database only, then exit; serves nothing")
	)
	flag.Parse()
	if *check != "" {
		os.Exit(healthCheck(*check))
	}
	if *dbPath == "" {
		*dbPath = filepath.Join(*dataDir, "observer.db")
	}
	if *snapDir == "" {
		*snapDir = filepath.Join(*dataDir, "snapshots")
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
		// here. But a public vantage that leaves it blank is publishing
		// reachability verdicts without saying where they were measured from,
		// and the About page points readers at this endpoint for exactly that.
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
	opts := []api.Option{api.WithPublisherLabels(reg), api.WithDataDir(*dataDir), api.WithSnapshotDir(*snapDir)}
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
	log.Printf("stopped")
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
