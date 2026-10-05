package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/scanner"
	"go/token"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/verdict"
)

// The partials kept across restarts.
//
// A restart would otherwise begin the partials again and read every window
// raw until the sealer had sealed every day once more, which at a year of
// busy days is hours. So they are kept beside the snapshots, with the
// machinery the memo and the ledger use (derived.go): a digest of each
// file's body, a definition, the identity of the store they were read
// from.
//
//   - day-partials.json holds the epoch: every table's marks ladder, raw_from,
//     the vantage, every settlement day's ledger and span, the holds (the
//     holds' counter, a digest of the held rows per promise, and the held
//     publications), the fingerprints and the corrected publications, the
//     anchors, the audit's cursor, every sealed row day but its histograms,
//     and an index of the sealed days naming each one's file and digest;
//   - day-partials/row-<day>.g<n>.json and settle-<day>.g<n>.json hold one
//     sealed day each, whole, written once and never changed; a day sealed
//     again is another file. A row day's is written when it is sealed, and
//     its histograms are read from it by the windows that sum it
//     (dayparts_hist.go).
//
// The definition is a digest of every statement and constant a partial is
// computed with, of the Go that folds and sums them (as tokens), and of the
// tables and views they read as the store defines them, so a build that
// computes any of it differently begins again rather than believing a
// file. The store is named by its creation, its chain and how many
// migrations rewrote rows, not by its schema version: a migration that
// only added what the partials do not read leaves the files good. Loading
// checks the header, every seal file's digest against the index (a seal
// file missing, damaged, or not the one the index names, leaves only its
// own day unsealed; a row day's file is read through for its digest and not
// parsed, the index holding the rest of the day), and then recomputes the
// newest sealed day of each kind and one other at random and compares; the
// catch-up that follows checks every mark and anchor and reads every hold
// again, as a catch-up does when the holds' counter moved. Any other doubt
// begins the partials again, as a start with no file does.
//
// An older build does not know the files and never reads them. Coming back,
// a file written before has marks the store has only grown past since, and
// the catch-up finds, by the same means it finds everything else, every
// row, hold, correction, collapse and prune in between.
const (
	dayPartsFile = "day-partials.json"
	dayPartsDir  = "day-partials"
	// partsVersion names the files' layout; the Go that folds and
	// assembles the partials is in the definition by its own text
	// (partsSourcesDigest).
	partsVersion = 5
)

// partsFile is day-partials.json.
type partsFile struct {
	derivedHeader
	Vantage     string                 `json:"vantage"`
	Marks       map[string][]mark      `json:"marks"`
	RawFrom     string                 `json:"raw_from,omitempty"`
	HeldRev     string                 `json:"held_rev,omitempty"`
	HeldProm    map[string]string      `json:"held_promises,omitempty"`
	HeldPubs    []string               `json:"held_pubs,omitempty"`
	FPs         map[string]string      `json:"fingerprints,omitempty"`
	CorrPub     map[string]string      `json:"corrected_pubs,omitempty"`
	CorrRows    map[string]string      `json:"corrected_rows,omitempty"`
	Reach       []string               `json:"reach,omitempty"`
	Verified    []string               `json:"verified,omitempty"`
	Weird       []string               `json:"weird,omitempty"`
	FirstRow    string                 `json:"first_row,omitempty"`
	LedgerBuilt bool                   `json:"ledger_built"`
	LedgerTo    int64                  `json:"ledger_to"`
	LedgerHi    int64                  `json:"ledger_hi"`
	PubsOdd     bool                   `json:"pubs_odd,omitempty"`
	MSUDue      []string               `json:"msu_due,omitempty"`
	Settle      map[string]*settleDay  `json:"settle"`
	Anchors     map[string]*dayAnchors `json:"anchors,omitempty"`
	// Rows are the sealed row days without their histograms, which their
	// seal files hold.
	Rows  map[string]*rowDay `json:"rows,omitempty"`
	Seals map[string]sealRef `json:"seals,omitempty"`
	// Audit is the audit's cursor per kind of day (auditNext).
	Audit map[string]string `json:"audit,omitempty"`
}

// sealRef names one sealed day's file.
type sealRef struct {
	File   string `json:"file"`
	Digest string `json:"digest"`
}

// sealFile is one sealed day's file.
type sealFile struct {
	derivedHeader
	Day    string      `json:"day"`
	Row    *rowDay     `json:"row,omitempty"`
	Settle *settleSeal `json:"settle,omitempty"`
}

// partsTables are the tables and views the partials read: a migration that
// changes one of them changes the definition read from the store.
var partsTables = []string{
	"probes", "publications", "assignments", "sampling_decisions", "sampling_decision_points", "reachability",
	"probe_amendments", "probe_corrections", "publication_corrections", "param_uncertainty", "meta",
	"probe_rows", "obligation_rows", "sampled_out_rows",
}

// partsDefinition is the digest of what the partials are computed with: the
// statements and constants, the Go that folds and assembles them
// (partsSourcesDigest), and the tables and views they read as q's store
// defines them.
func partsDefinition(ctx context.Context, q store.Querier) (string, error) {
	names, _ := json.Marshal(partsTables)
	rows, err := q.QueryContext(ctx, `SELECT type, name, sql FROM sqlite_master
		WHERE type IN ('table', 'view') AND name IN (SELECT value FROM json_each(?)) ORDER BY type, name`, string(names))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	parts := []string{
		rollup.ObligationBuckets, rollup.ObligationSums, rollup.EffectiveClass(""), rollup.CountedClass("pr"), store.ObligationRowsVerified,
		valClassesSQL(""), valLatencySQL(""), valSeenSQL(""), valBeatsSQL(""), valGapsSQL(""), valLatencyHistSQL(""), valThroughputHistSQL(""),
		obligationPassSQL(""), loadSQL(""), loadSpanSQL(""), loadHeldSQL(""), ledgerLoadSQL, ledgerSigningSQL, ledgerPubsSQL, ledgerRowsSpanSQL,
		signingByValidatorSQL(""), readableSQL(""), readableCountSQL(""), dayReadableSQL, pubCountSQL, daySpanSQL, dayTiesSQL, dayCollapsibleSQL,
		strconv.Itoa(throughputMinBytes), verdict.FaultSettling.String(), rollup.FinalMargin.String(), strconv.FormatFloat(verdict.EndSegmentDivisor, 'g', -1, 64),
		verdict.MethodologyVersion, "parts " + strconv.Itoa(partsVersion), "go " + partsSourcesDigest(),
	}
	for rows.Next() {
		var typ, name string
		var text sql.NullString
		if err := rows.Scan(&typ, &name, &text); err != nil {
			return "", err
		}
		parts = append(parts, typ, name, text.String)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return definitionOf(parts...), nil
}

// The Go that computes the partials is in their definition by its own text,
// so a change to how a day is folded, sealed or summed refuses the files the
// build before it wrote, with no version to remember to bump: the ledger
// and the seals are sums the Go makes of what the statements return, and a
// file of sums made another way is not what this build would compute. The
// text is read as tokens, comments and layout left out, so a comment or a
// gofmt does not cost a sealing again. TestPartsSourcesEmbedded holds the
// list to every dayparts file of the package.
//
//go:embed dayparts.go dayparts_audit.go dayparts_catchup.go dayparts_file.go dayparts_health.go dayparts_hist.go dayparts_rank.go dayparts_seal.go dayparts_sql.go dayparts_window.go
var partsSources embed.FS

var (
	partsSourcesOnce sync.Once
	partsSourcesSum  string
)

// partsSourcesDigest is the digest of the partials' Go as tokens.
func partsSourcesDigest() string {
	partsSourcesOnce.Do(func() {
		ents, _ := partsSources.ReadDir(".")
		var names []string
		srcs := map[string][]byte{}
		for _, ent := range ents { // in name order
			if src, err := partsSources.ReadFile(ent.Name()); err == nil {
				names = append(names, ent.Name())
				srcs[ent.Name()] = src
			}
		}
		partsSourcesSum = goTokensDigest(names, srcs)
	})
	return partsSourcesSum
}

// goTokensDigest is the digest of Go sources as tokens, in the order of
// names: a comment, a blank or a line broken elsewhere leaves it as it is.
func goTokensDigest(names []string, srcs map[string][]byte) string {
	h := sha256.New()
	for _, name := range names {
		src := srcs[name]
		h.Write([]byte(name))
		fset := token.NewFileSet()
		var sc scanner.Scanner
		sc.Init(fset.AddFile(name, -1, len(src)), src, nil, 0)
		for {
			_, tok, lit := sc.Scan()
			if tok == token.EOF {
				break
			}
			if tok == token.SEMICOLON {
				lit = ";" // an inserted one reads "\n"
			}
			h.Write([]byte{0})
			h.Write([]byte(tok.String()))
			h.Write([]byte{1})
			h.Write([]byte(lit))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// readPartsIdentity is the store's identity as the partials name it: its
// creation and chain, and how many migrations rewrote rows
// (store.MetaMigrationRewrites), with no schema version, which it returns
// apart. A migration that changes a table the partials read changes their
// definition instead (partsDefinition).
func readPartsIdentity(ctx context.Context, q store.Querier) (storeIdentity, int, error) {
	id, err := readStoreIdentity(ctx, q)
	if err != nil {
		return id, 0, err
	}
	schema := id.Schema
	id.Schema = 0
	var v string
	if err := q.QueryRowContext(ctx, metaValueSQL, store.MetaMigrationRewrites).Scan(&v); err != nil {
		return id, 0, err
	}
	id.Rewrites, _ = strconv.Atoi(v)
	return id, schema, nil
}

// checkPartsHeader refuses a partials file that is not kind under
// definition for the store q is (readPartsIdentity).
func checkPartsHeader(ctx context.Context, q store.Querier, h derivedHeader, kind, definition string) (refusal, error) {
	switch {
	case h.Kind != kind:
		return refusal("kind " + h.Kind + ", not " + kind), nil
	case h.Format != derivedFormat:
		return "another format", nil
	case h.Definition != definition:
		return "computed with another definition", nil
	}
	id, _, err := readPartsIdentity(ctx, q)
	if err != nil {
		return "", err
	}
	if h.Store != id {
		return refusal(storeChange(h.Store, id)), nil
	}
	return "", nil
}

// sealKey and sealName are a sealed day's key in the index and its file.
func sealName(kind, day string, gen int) string {
	return kind + "-" + day + ".g" + strconv.Itoa(gen) + ".json"
}

// fileOf is the epoch as day-partials.json keeps it.
func (e *epoch) fileOf(vantage string) partsFile {
	f := partsFile{
		Vantage: vantage, Marks: e.marks, RawFrom: e.rawFrom, HeldRev: e.heldRev, HeldProm: e.heldProm, FPs: e.fps, CorrPub: e.corrPub, CorrRows: e.corrRows,
		Weird: e.weird, FirstRow: e.firstRow, LedgerBuilt: e.ledgerBuilt, LedgerTo: e.ledgerTo, LedgerHi: e.ledgerHi,
		PubsOdd: e.pubsOdd, Settle: e.settle, Anchors: e.anchors, Seals: map[string]sealRef{},
	}
	for _, m := range []struct {
		set map[string]bool
		out *[]string
	}{{e.heldPubs, &f.HeldPubs}, {e.reach, &f.Reach}, {e.verified, &f.Verified}, {e.msuDue, &f.MSUDue}} {
		for k := range m.set {
			*m.out = append(*m.out, k)
		}
		sort.Strings(*m.out)
	}
	return f
}

// saveLater writes the partials in the background when what the files keep
// of substance has moved (epoch.content: a day sealed or dropped, the
// ledger grown) and a write is due, or with now whenever it has moved; one
// write at a time. A catch-up that only moved the marks is not a reason to
// write: the next start catches up from the older marks as it would from
// these. Nothing is written once the process reads raw (fallBack).
func (dp *dayParts) saveLater(s *Server, now bool) {
	if dp.file == "" {
		return
	}
	dp.mu.Lock()
	moved := dp.cur != nil && (dp.cur.born != dp.savedBorn || dp.cur.content != dp.saved)
	due := moved && dp.writing == nil && dp.fallback == "" && (now || time.Since(dp.savedAt) >= partsSaveEvery)
	if !due {
		dp.mu.Unlock()
		return
	}
	done := make(chan struct{})
	dp.writing = done
	dp.mu.Unlock()
	go func() {
		defer func() {
			dp.mu.Lock()
			dp.writing = nil
			dp.mu.Unlock()
			close(done)
		}()
		if err := dp.save(context.Background(), s); err != nil && dp.log != nil {
			dp.log("day partials: writing %s: %v", dp.file, err)
		}
	}()
}

// partsSaveEvery is how often the partials are written while they move.
const partsSaveEvery = time.Minute

// wait returns once no write saveLater started is running.
func (dp *dayParts) wait() {
	dp.mu.Lock()
	done := dp.writing
	dp.mu.Unlock()
	if done != nil {
		<-done
	}
}

// saveNow writes the partials now, once any write already running has
// ended, and holds off saveLater while it does: two writes at once could
// each remove the seal files the other names.
func (dp *dayParts) saveNow(ctx context.Context, s *Server) error {
	for {
		dp.mu.Lock()
		if running := dp.writing; running != nil {
			dp.mu.Unlock()
			<-running
			continue
		}
		done := make(chan struct{})
		dp.writing = done
		dp.mu.Unlock()
		err := dp.save(ctx, s)
		dp.mu.Lock()
		dp.writing = nil
		dp.mu.Unlock()
		close(done)
		return err
	}
}

// save writes the newest epoch: every seal file it names that is not on
// disk yet, then the index, then removes the seal files nothing names.
// Nothing is written for a store that is no longer the one the epoch was
// read from.
func (dp *dayParts) save(ctx context.Context, s *Server) error {
	if dp.file == "" {
		return nil
	}
	dp.mu.Lock()
	e, fallback := dp.cur, dp.fallback
	dp.mu.Unlock()
	if e == nil || fallback != "" {
		return nil // nothing yet, or the files are left for whoever looks into them
	}
	err := dp.write(ctx, s, e)
	dp.mu.Lock()
	if err != nil {
		dp.saveErr, dp.saveErrAt = err.Error(), time.Now()
	} else {
		dp.saveErr, dp.saveErrAt = "", time.Time{}
	}
	dp.mu.Unlock()
	return err
}

// write writes epoch e (save).
func (dp *dayParts) write(ctx context.Context, s *Server, e *epoch) error {
	db := s.st.DB()
	id, _, err := readPartsIdentity(ctx, db)
	if err != nil {
		return err
	}
	def, err := partsDefinition(ctx, db)
	if err != nil {
		return err
	}
	if id != e.store || def != e.def {
		return nil // the next catch-up begins them again
	}
	dir := filepath.Join(filepath.Dir(dp.file), dayPartsDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f := e.fileOf(s.vantage)
	f.derivedHeader = derivedHeader{Kind: "day-partials", Format: derivedFormat, Definition: def, Store: e.store}
	dp.mu.Lock()
	f.Audit = make(map[string]string, len(dp.auditAt))
	for k, v := range dp.auditAt {
		f.Audit[k] = v
	}
	dp.mu.Unlock()
	written := map[string]bool{}
	put := func(key, kind, day string, gen int, sf sealFile) error {
		name := sealName(kind, day, gen)
		path := filepath.Join(dir, name)
		written[name] = true
		dp.mu.Lock()
		digest, ok := dp.sealFiles[name]
		dp.mu.Unlock()
		if !ok {
			sf.derivedHeader = derivedHeader{Kind: "day-partial-" + kind, Format: derivedFormat, Definition: def, Store: e.store}
			sf.Day = day
			if err := writeDerived(path, sf); err != nil {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			digest = string(b[len(digestOpen) : len(digestOpen)+64])
			dp.mu.Lock()
			if dp.sealFiles == nil {
				dp.sealFiles = map[string]string{}
			}
			dp.sealFiles[name] = digest
			dp.mu.Unlock()
		}
		f.Seals[key] = sealRef{File: name, Digest: digest}
		return nil
	}
	f.Rows = make(map[string]*rowDay, len(e.rows))
	for d, rd := range e.rows {
		if rd.file != "" {
			// Written whole when it was sealed (writeRowSeal).
			written[rd.file] = true
			f.Seals["row:"+d] = sealRef{File: rd.file, Digest: rd.digest}
			f.Rows[d] = rd
			continue
		}
		if err := put("row:"+d, "row", d, rd.Gen, sealFile{Row: rd}); err != nil {
			return err
		}
		f.Rows[d] = rd.stripped("", "")
	}
	for d, sd := range e.settle {
		if sd.Seal == nil {
			continue
		}
		if err := put("settle:"+d, "settle", d, sd.Seal.Gen, sealFile{Settle: sd.Seal}); err != nil {
			return err
		}
	}
	if err := writeDerived(dp.file, f); err != nil {
		return err
	}
	dp.mu.Lock()
	dp.savedBorn, dp.saved, dp.savedAt = e.born, e.content, time.Now()
	dp.mu.Unlock()
	// The seal files nothing names any more; each is replaced by another
	// generation or dropped. Not those the newest epoch names, sealed since
	// e was taken, nor those the sealer has written and not published yet
	// (pending): they are told apart under the lock the sealer marks a file
	// pending under before it writes it and publishes it under. The
	// histograms kept of a day dropped or sealed again go too. And the
	// temporary files of seal writes that never reached their rename (a
	// process killed mid-write), each the size of its seal, once they are
	// old enough that no write can still be at them (staleDerivedTemp).
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	now := time.Now()
	digests := map[string]bool{}
	dp.mu.Lock()
	named := map[string]bool{}
	if cur := dp.cur; cur != nil {
		for _, rd := range cur.rows {
			named[rd.file] = true
			digests[rd.digest] = true
		}
		for d, sd := range cur.settle {
			if sd.Seal != nil {
				named[sealName("settle", d, sd.Seal.Gen)] = true
			}
		}
	}
	for _, ent := range entries {
		name := ent.Name()
		if strings.HasSuffix(name, ".json") && !written[name] && !named[name] && !dp.pending[name] {
			_ = os.Remove(filepath.Join(dir, name))
			delete(dp.sealFiles, name)
		}
	}
	dp.mu.Unlock()
	dp.hists.retain(digests)
	for _, ent := range entries {
		name := ent.Name()
		if strings.HasSuffix(name, ".tmp") && !ent.IsDir() {
			if fi, err := ent.Info(); err == nil && now.Sub(fi.ModTime()) > staleDerivedTemp {
				_ = os.Remove(filepath.Join(dir, name))
			}
		}
	}
	sweepDerivedTemps(dp.file, now)
	return nil
}

// keptSeals are the seal files a loaded epoch names, by name with their
// digests, and the newest generation of each day in them.
type keptSeals struct {
	files map[string]string
	gens  map[string]int
	// audit is the audit's cursor the file kept.
	audit map[string]string
}

// load reads the kept partials, or refuses them: an epoch that may be
// caught up from and the seal files it names, or nil and why not. It
// changes nothing of dp.
func (dp *dayParts) load(ctx context.Context, s *Server, q store.Querier) (*epoch, keptSeals, refusal, error) {
	kept := keptSeals{files: map[string]string{}, gens: map[string]int{}}
	if dp.file == "" {
		return nil, kept, "", nil
	}
	var f partsFile
	ok, why := readDerived(dp.file, &f)
	if !ok {
		return nil, kept, why, nil
	}
	def, err := partsDefinition(ctx, q)
	if err != nil {
		return nil, kept, "", err
	}
	if why, err := checkPartsHeader(ctx, q, f.derivedHeader, "day-partials", def); why != "" || err != nil {
		return nil, kept, why, err
	}
	if f.Vantage != s.vantage {
		return nil, kept, refusal(fmt.Sprintf("computed for vantage %q, not %q", f.Vantage, s.vantage)), nil
	}
	kept.audit = f.Audit
	e := newEpoch()
	if e.store, e.schema, err = readPartsIdentity(ctx, q); err != nil {
		return nil, kept, "", err
	}
	e.def = def
	e.marks, e.rawFrom, e.fps, e.weird, e.firstRow = f.Marks, f.RawFrom, f.FPs, f.Weird, f.FirstRow
	e.ledgerBuilt, e.ledgerTo, e.ledgerHi, e.pubsOdd = f.LedgerBuilt, f.LedgerTo, f.LedgerHi, f.PubsOdd
	if e.marks == nil {
		e.marks = map[string][]mark{}
	}
	if e.fps == nil {
		e.fps = map[string]string{}
	}
	if f.CorrPub != nil {
		e.corrPub = f.CorrPub
	}
	if f.CorrRows != nil {
		e.corrRows = f.CorrRows
	}
	e.heldRev = f.HeldRev
	if f.HeldProm != nil {
		e.heldProm = f.HeldProm
	}
	for _, m := range []struct {
		list []string
		set  map[string]bool
	}{{f.HeldPubs, e.heldPubs}, {f.Reach, e.reach}, {f.Verified, e.verified}} {
		for _, k := range m.list {
			m.set[k] = true
		}
	}
	for _, d := range f.MSUDue {
		if e.msuDue == nil {
			e.msuDue = map[string]bool{}
		}
		e.msuDue[d] = true
	}
	for d, sd := range f.Settle {
		if sd.Signing == nil {
			sd.Signing = map[string]sigPart{}
		}
		if sd.Load == nil {
			sd.Load = map[string]*loadPart{}
		}
		for _, l := range sd.Load {
			if l.Bytes == nil {
				l.Bytes = map[int]*big.Int{}
			}
		}
		e.settle[d] = sd
	}
	for d, a := range f.Anchors {
		e.anchors[d] = a
	}
	dir := filepath.Join(filepath.Dir(dp.file), dayPartsDir)
	var missing, damaged []string
	for key, ref := range f.Seals {
		// The newest generation of each day named, kept or not, so that a
		// day sealed again never reuses the name of a file left behind.
		var g int
		if _, err := fmt.Sscanf(ref.File[strings.LastIndex(ref.File, ".g")+2:], "%d.json", &g); err == nil && g > kept.gens[key] {
			kept.gens[key] = g
		}
		// A seal file missing, damaged (its digest is not its body's, it
		// does not parse, it cannot be read), or not the one the index names
		// (another process's of the same name), leaves its day unsealed,
		// read raw until it is sealed again: nothing else the index holds
		// rests on it, so a copy that left the seal files behind, a torn
		// write or a bit gone wrong, or another directory's files mixed in,
		// costs those days and not every other one.
		kind, day, _ := strings.Cut(key, ":")
		if kind == "row" {
			// The day but its histograms is in the index; its file is read
			// through for its digest and not parsed, and its histograms are
			// read when a window sums the day (dayparts_hist.go).
			rd := f.Rows[day]
			if rd == nil || rd.Day != day {
				damaged = append(damaged, ref.File+" (the index holds no "+key+")")
				continue
			}
			kept.gens[key] = max(kept.gens[key], rd.Gen)
			gone, why := checkSealFile(filepath.Join(dir, ref.File), ref.Digest)
			if gone {
				missing = append(missing, ref.File)
				continue
			}
			if why != "" {
				damaged = append(damaged, ref.File+" ("+string(why)+")")
				continue
			}
			if rd.Vals == nil {
				rd.Vals = map[string]*rowPart{}
			}
			for _, p := range rd.Vals {
				p.Lat, p.Tput = nil, nil
			}
			rd.file, rd.digest = ref.File, ref.Digest
			e.rows[day] = rd
			kept.files[ref.File] = ref.Digest
			continue
		}
		var sf sealFile
		ok, why := readDerived(filepath.Join(dir, ref.File), &sf)
		if !ok && why == "" {
			missing = append(missing, ref.File)
			continue
		}
		if !ok {
			damaged = append(damaged, ref.File+" ("+string(why)+")")
			continue
		}
		if b, err := os.ReadFile(filepath.Join(dir, ref.File)); err != nil || string(b[len(digestOpen):len(digestOpen)+64]) != ref.Digest {
			missing = append(missing, ref.File)
			continue
		}
		switch {
		case sf.Kind != "day-partial-"+kind || sf.Definition != def || sf.Store != f.Store:
			damaged = append(damaged, ref.File+" (of another kind, definition or store)")
			continue
		case kind == "settle" && sf.Settle != nil && e.settle[day] != nil:
			e.settle[day].Seal = sf.Settle
		default:
			damaged = append(damaged, ref.File+" (does not hold "+key+")")
			continue
		}
		kept.files[ref.File] = ref.Digest
	}
	if len(missing) > 0 {
		dp.logf("day partials: %d of the %d sealed day(s) %s names have no file of theirs in %s; they are read raw until they are sealed again", len(missing), len(f.Seals), dp.file, dir)
	}
	if len(damaged) > 0 {
		sort.Strings(damaged)
		dp.logf("day partials: %d of the %d seal file(s) %s names cannot be used, their days read raw until they are sealed again: %s",
			len(damaged), len(f.Seals), dp.file, strings.Join(damaged, ", "))
	}
	return e, kept, "", nil
}
