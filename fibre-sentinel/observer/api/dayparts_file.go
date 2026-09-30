package api

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
//     the vantage, every settlement day's ledger and span, the holds (an
//     aggregate of the held rows per promise, and the held publications),
//     the fingerprints and the corrected publications, the anchors, and an
//     index of the sealed days naming each one's file and digest;
//   - day-partials/row-<day>.g<n>.json and settle-<day>.g<n>.json hold one
//     sealed day each, written once and never changed; a day sealed again
//     is another file.
//
// The definition is a digest of every statement and constant a partial is
// computed with, and of the views they read as the store holds them, so a
// build that computes any of it differently begins again rather than
// believing a file. Loading checks the header, every seal file's digest
// against the index (a seal file missing, or not the one the index names,
// leaves only its own day unsealed), and then recomputes the newest sealed
// day of each kind and one other at random and compares; the catch-up that
// follows checks every mark and anchor, as it does on every computation.
// Any other doubt begins the partials again, as a start with no file does.
//
// An older build does not know the files and never reads them. Coming back,
// a file written before has marks the store has only grown past since, and
// the catch-up finds, by the same means it finds everything else, every
// row, hold, correction, collapse and prune in between.
const (
	dayPartsFile = "day-partials.json"
	dayPartsDir  = "day-partials"
	// partsVersion names how the partials are folded and assembled; bump it
	// whenever that changes.
	partsVersion = 2
)

// partsFile is day-partials.json.
type partsFile struct {
	derivedHeader
	Vantage     string                 `json:"vantage"`
	Marks       map[string][]mark      `json:"marks"`
	RawFrom     string                 `json:"raw_from,omitempty"`
	HeldGate    heldAgg                `json:"held_gate"`
	HeldProm    map[string]heldAgg     `json:"held_promises,omitempty"`
	HeldPubGate heldAgg                `json:"held_pub_gate"`
	HeldPubs    []string               `json:"held_pubs,omitempty"`
	FPs         map[string]string      `json:"fingerprints,omitempty"`
	Corrected   map[string]corrFP      `json:"corrected,omitempty"`
	CorrStale   []string               `json:"corrected_stale,omitempty"`
	Reach       []string               `json:"reach,omitempty"`
	Verified    []string               `json:"verified,omitempty"`
	Weird       []string               `json:"weird,omitempty"`
	FirstRow    string                 `json:"first_row,omitempty"`
	LedgerBuilt bool                   `json:"ledger_built"`
	LedgerTo    int64                  `json:"ledger_to"`
	LedgerHi    int64                  `json:"ledger_hi"`
	PubsOdd     bool                   `json:"pubs_odd,omitempty"`
	Settle      map[string]*settleDay  `json:"settle"`
	Anchors     map[string]*dayAnchors `json:"anchors,omitempty"`
	Seals       map[string]sealRef     `json:"seals,omitempty"`
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

// partsDefinition is the digest of what the partials are computed with,
// the views they read included, as q's store holds them.
func partsDefinition(ctx context.Context, q store.Querier) (string, error) {
	rows, err := q.QueryContext(ctx, `SELECT name, sql FROM sqlite_master WHERE name IN ('probe_rows', 'obligation_rows', 'sampled_out_rows') ORDER BY name`)
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
		verdict.MethodologyVersion, "parts " + strconv.Itoa(partsVersion),
	}
	for rows.Next() {
		var name string
		var text sql.NullString
		if err := rows.Scan(&name, &text); err != nil {
			return "", err
		}
		parts = append(parts, name, text.String)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return definitionOf(parts...), nil
}

// sealKey and sealName are a sealed day's key in the index and its file.
func sealName(kind, day string, gen int) string {
	return kind + "-" + day + ".g" + strconv.Itoa(gen) + ".json"
}

// fileOf is the epoch as day-partials.json keeps it.
func (e *epoch) fileOf(vantage string) partsFile {
	f := partsFile{
		Vantage: vantage, Marks: e.marks, RawFrom: e.rawFrom, HeldGate: e.heldGate, HeldProm: e.heldProm, HeldPubGate: e.heldPubGate, FPs: e.fps, Corrected: e.corr,
		Weird: e.weird, FirstRow: e.firstRow, LedgerBuilt: e.ledgerBuilt, LedgerTo: e.ledgerTo, LedgerHi: e.ledgerHi,
		PubsOdd: e.pubsOdd, Settle: e.settle, Anchors: e.anchors, Seals: map[string]sealRef{},
	}
	for _, m := range []struct {
		set map[string]bool
		out *[]string
	}{{e.heldPubs, &f.HeldPubs}, {e.reach, &f.Reach}, {e.verified, &f.Verified}, {e.corrStale, &f.CorrStale}} {
		for k := range m.set {
			*m.out = append(*m.out, k)
		}
		sort.Strings(*m.out)
	}
	return f
}

// saveLater writes the partials in the background when they have moved and
// a write is due, or with now whenever they have moved; one write at a
// time.
func (dp *dayParts) saveLater(s *Server, now bool) {
	if dp.file == "" {
		return
	}
	dp.mu.Lock()
	due := dp.cur != nil && dp.cur.seq != dp.saved && dp.writing == nil && (now || time.Since(dp.savedAt) >= partsSaveEvery)
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
	e := dp.cur
	dp.mu.Unlock()
	if e == nil {
		return nil
	}
	db := s.st.DB()
	id, err := readStoreIdentity(ctx, db)
	if err != nil {
		return err
	}
	if id != e.store {
		return nil
	}
	def, err := partsDefinition(ctx, db)
	if err != nil {
		return err
	}
	dir := filepath.Join(filepath.Dir(dp.file), dayPartsDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f := e.fileOf(s.vantage)
	f.derivedHeader = derivedHeader{Kind: "day-partials", Format: derivedFormat, Definition: def, Store: e.store}
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
	for d, rd := range e.rows {
		if err := put("row:"+d, "row", d, rd.Gen, sealFile{Row: rd}); err != nil {
			return err
		}
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
	dp.saved, dp.savedAt = e.seq, time.Now()
	dp.mu.Unlock()
	// The seal files nothing names any more; each is replaced by another
	// generation or dropped. And the temporary files of seal writes that
	// never reached their rename (a process killed mid-write), each the size
	// of its seal, once they are old enough that no write can still be at
	// them (staleDerivedTemp).
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	now := time.Now()
	for _, ent := range entries {
		name := ent.Name()
		switch {
		case strings.HasSuffix(name, ".json") && !written[name]:
			_ = os.Remove(filepath.Join(dir, name))
			dp.mu.Lock()
			delete(dp.sealFiles, name)
			dp.mu.Unlock()
		case strings.HasSuffix(name, ".tmp") && !ent.IsDir():
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
	if why, err := checkHeader(ctx, q, f.derivedHeader, "day-partials", def); why != "" || err != nil {
		return nil, kept, why, err
	}
	if f.Vantage != s.vantage {
		return nil, kept, refusal(fmt.Sprintf("computed for vantage %q, not %q", f.Vantage, s.vantage)), nil
	}
	e := newEpoch()
	e.store = f.Store
	e.marks, e.rawFrom, e.fps, e.weird, e.firstRow = f.Marks, f.RawFrom, f.FPs, f.Weird, f.FirstRow
	e.ledgerBuilt, e.ledgerTo, e.ledgerHi, e.pubsOdd = f.LedgerBuilt, f.LedgerTo, f.LedgerHi, f.PubsOdd
	if e.marks == nil {
		e.marks = map[string][]mark{}
	}
	if e.fps == nil {
		e.fps = map[string]string{}
	}
	if f.Corrected != nil {
		e.corr = f.Corrected
	}
	e.heldGate, e.heldPubGate = f.HeldGate, f.HeldPubGate
	if f.HeldProm != nil {
		e.heldProm = f.HeldProm
	}
	for _, m := range []struct {
		list []string
		set  map[string]bool
	}{{f.HeldPubs, e.heldPubs}, {f.Reach, e.reach}, {f.Verified, e.verified}, {f.CorrStale, e.corrStale}} {
		for _, k := range m.list {
			m.set[k] = true
		}
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
	missing := 0
	for key, ref := range f.Seals {
		var sf sealFile
		// A seal file missing, or not the one the index names (another
		// process's of the same name), leaves its day unsealed, read raw
		// until it is sealed again: nothing else the index holds rests on
		// it, so a copy that left the seal files behind, or mixed in
		// another directory's, costs those days and not every other one.
		ok, why := readDerived(filepath.Join(dir, ref.File), &sf)
		if !ok && why == "" {
			missing++
			continue
		}
		if !ok {
			return nil, kept, refusal(ref.File + ": " + string(why)), nil
		}
		if b, err := os.ReadFile(filepath.Join(dir, ref.File)); err != nil || string(b[len(digestOpen):len(digestOpen)+64]) != ref.Digest {
			missing++
			continue
		}
		if sf.Kind != "day-partial-row" && sf.Kind != "day-partial-settle" || sf.Definition != def || sf.Store != f.Store {
			return nil, kept, refusal(ref.File + " is of another kind, definition or store"), nil
		}
		kind, day, _ := strings.Cut(key, ":")
		switch {
		case kind == "row" && sf.Row != nil && sf.Row.Day == day:
			e.rows[day] = sf.Row
		case kind == "settle" && sf.Settle != nil && e.settle[day] != nil:
			e.settle[day].Seal = sf.Settle
		default:
			return nil, kept, refusal(ref.File + " does not hold " + key), nil
		}
		kept.files[ref.File] = ref.Digest
		gen := 0
		if sf.Row != nil {
			gen = sf.Row.Gen
		} else {
			gen = sf.Settle.Gen
		}
		if gen > kept.gens[key] {
			kept.gens[key] = gen
		}
	}
	if missing > 0 && dp.log != nil {
		dp.log("day partials: %d of the %d sealed day(s) %s names have no file of theirs in %s; they are read raw until they are sealed again", missing, len(f.Seals), dp.file, dir)
	}
	return e, kept, "", nil
}
