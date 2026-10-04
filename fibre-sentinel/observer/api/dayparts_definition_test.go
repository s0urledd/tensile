package api

import (
	"bytes"
	"context"
	"math"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// TestPartsSourcesEmbedded: the partials' definition holds the Go that
// folds and sums them, every non-test dayparts file of the package, so a
// change to how a day is folded refuses the files an older build wrote with
// no version to remember. A comment or a line broken elsewhere is not such
// a change; a token is.
func TestPartsSourcesEmbedded(t *testing.T) {
	files, err := filepath.Glob("dayparts*.go")
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, f := range files {
		if !strings.HasSuffix(f, "_test.go") {
			want = append(want, f)
		}
	}
	ents, err := partsSources.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	srcs := map[string][]byte{}
	for _, e := range ents {
		got = append(got, e.Name())
		if srcs[e.Name()], err = partsSources.ReadFile(e.Name()); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(want)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("the definition embeds %v; the package's dayparts files are %v: list each in the go:embed of partsSources", got, want)
	}
	base := goTokensDigest(got, srcs)
	if base != partsSourcesDigest() {
		t.Fatal("the digest of the embedded sources is not the definition's")
	}
	edit := func(name string, f func([]byte) []byte) string {
		c := map[string][]byte{}
		for k, v := range srcs {
			c[k] = v
		}
		c[name] = f(append([]byte(nil), srcs[name]...))
		return goTokensDigest(got, c)
	}
	if d := edit("dayparts_seal.go", func(b []byte) []byte {
		return bytes.Replace(b, []byte("\n// sealMargin"), []byte("\n// A comment added.\n\n// sealMargin"), 1)
	}); d != base {
		t.Errorf("a comment moved the definition")
	}
	if d := edit("dayparts_seal.go", func(b []byte) []byte {
		return bytes.Replace(b, []byte("const sealMargin = time.Hour"), []byte("const sealMargin = 2 * time.Hour"), 1)
	}); d == base {
		t.Errorf("a constant changed and the definition did not move")
	}
}

// TestAMigrationIsJudgedByWhatItDid: the partials name the store by its
// creation, its chain and how many migrations rewrote rows, and hold the
// definitions of the tables they read in theirs, not the schema version.
// A migration that only adds what they do not read (a table, an index of a
// table they read) leaves them as they are, in a process that runs across
// it and in one that starts from its files after: an upgrade does not cost
// a full sealing in the API still serving and in the warm-up beside it at
// once. One that changes a table they read, or rewrites rows, begins them
// again, and refuses the files written before it.
func TestAMigrationIsJudgedByWhatItDid(t *testing.T) {
	skipUnderRace(t)
	t.Parallel()
	cfg := defaultSimConfig(75)
	cfg.perDay, cfg.days = 8, 4
	s := newSim(t, cfg)
	s.plan()
	srv := s.openAPI()
	ctx := context.Background()
	for at := s.t0.Add(time.Hour); at.Before(s.t0.Add(4*24*time.Hour + 6*time.Hour)); at = at.Add(12 * time.Hour) {
		s.advance(at)
		s.pass()
	}
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(75, 1))
	migrate := func(stmts ...string) {
		t.Helper()
		db := s.st.DB()
		for _, q := range append(stmts, `INSERT INTO schema_migrations (version, applied_at) SELECT MAX(version) + 1, '`+store.TS(s.now)+`' FROM schema_migrations`) {
			if _, err := db.Exec(q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	sealed := func(srv *Server) int {
		srv.parts.mu.Lock()
		defer srv.parts.mu.Unlock()
		return len(srv.parts.cur.rows)
	}
	rows := sealed(srv)

	// A table and an index added.
	migrate(`CREATE TABLE sim_added (k TEXT PRIMARY KEY, v TEXT)`, `CREATE INDEX sim_probes_by_host ON probes (validator_host)`)
	before := srv.parts.rebuilds
	s.compare(srv, rng, 4, "after a migration that added a table and an index")
	if srv.parts.rebuilds != before || sealed(srv) != rows {
		t.Errorf("a migration that added a table and an index: %d rebuild(s) more, %d of %d row days", srv.parts.rebuilds-before, sealed(srv), rows)
	}
	next := s.openAPI()
	s.compare(next, rng, 4, "a start after it")
	if !strings.HasPrefix(next.parts.origin, "loaded") {
		t.Errorf("a start after a migration that added a table and an index: %s", next.parts.origin)
	}

	// A column added to a table the partials read.
	migrate(`ALTER TABLE probes ADD COLUMN sim_extra INTEGER`)
	before = srv.parts.rebuilds
	s.compare(srv, rng, 4, "after a migration that added a column to probes")
	if srv.parts.rebuilds != before+1 {
		t.Errorf("a migration that added a column to probes: %d rebuild(s)", srv.parts.rebuilds-before)
	}
	next = s.openAPI()
	s.compare(next, rng, 4, "a start after it")
	if !strings.Contains(next.parts.origin, "refused: computed with another definition") {
		t.Errorf("a start after a migration that added a column to probes: %s", next.parts.origin)
	}

	// Rows rewritten, as a migration's backfill does it, counted.
	if _, err := srv.sealDue(ctx, math.MaxInt); err != nil {
		t.Fatal(err)
	}
	if err := srv.keepDerived(ctx); err != nil {
		t.Fatal(err)
	}
	migrate(`UPDATE probes SET sim_extra = 1`, `INSERT INTO meta (key, value, updated_at) VALUES ('`+store.MetaMigrationRewrites+`', '1', '')
		ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(meta.value AS INTEGER) + 1 AS TEXT)`)
	before = srv.parts.rebuilds
	s.compare(srv, rng, 4, "after a migration that rewrote rows")
	if srv.parts.rebuilds != before+1 {
		t.Errorf("a migration that rewrote rows: %d rebuild(s)", srv.parts.rebuilds-before)
	}
	next = s.openAPI()
	s.compare(next, rng, 4, "a start after it")
	if !strings.Contains(next.parts.origin, "refused: a migration rewrote rows") {
		t.Errorf("a start after a migration that rewrote rows: %s", next.parts.origin)
	}
}
