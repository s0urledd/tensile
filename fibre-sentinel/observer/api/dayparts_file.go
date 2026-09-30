package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
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
//     the vantage, every settlement day's ledger and span, the held rows
//     and publications, the fingerprints, the anchors, and an index of the
//     sealed days naming each one's file and digest;
//   - day-partials/row-<day>.g<n>.json and settle-<day>.g<n>.json hold one
//     sealed day each, written once and never changed; a day sealed again
//     is another file.
//
// The definition is a digest of every statement and constant a partial is
// computed with, and of the views they read as the store holds them, so a
// build that computes any of it differently begins again rather than
// believing a file. Loading checks the header, every seal file's digest
// against the index, and then recomputes the newest sealed day of each
// kind and one other at random and compares; the catch-up that follows
// checks every mark and anchor, as it does on every computation. Any doubt
// begins the partials again, as a start with no file does.
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
	partsVersion = 1
)

// partsFile is day-partials.json.
type partsFile struct {
	derivedHeader
	Vantage     string                 `json:"vantage"`
	Marks       map[string][]mark      `json:"marks"`
	RawFrom     string                 `json:"raw_from,omitempty"`
	Held        map[string]heldRow     `json:"held,omitempty"`
	HeldPubs    []string               `json:"held_pubs,omitempty"`
	FPs         map[string]string      `json:"fingerprints,omitempty"`
	Covered     []string               `json:"covered,omitempty"`
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
		Vantage: vantage, Marks: e.marks, RawFrom: e.rawFrom, Held: map[string]heldRow{}, FPs: e.fps,
		Weird: e.weird, FirstRow: e.firstRow, LedgerBuilt: e.ledgerBuilt, LedgerTo: e.ledgerTo, LedgerHi: e.ledgerHi,
		PubsOdd: e.pubsOdd, Settle: e.settle, Anchors: e.anchors, Seals: map[string]sealRef{},
	}
	for id, h := range e.held {
		f.Held[strconv.FormatInt(id, 10)] = h
	}
	for _, m := range []struct {
		set map[string]bool
		out *[]string
	}{{e.heldPubs, &f.HeldPubs}, {e.covered, &f.Covered}, {e.verified, &f.Verified}} {
		for k := range m.set {
			*m.out = append(*m.out, k)
		}
		sort.Strings(*m.out)
	}
	return f
}

// saveLater writes the partials in the background when they have moved and
// a write is due; one write at a time.
func (dp *dayParts) saveLater(s *Server) {
	if dp.file == "" {
		return
	}
	dp.mu.Lock()
	due := dp.cur != nil && dp.cur.seq != dp.saved && dp.writing == nil && time.Since(dp.savedAt) >= partsSaveEvery
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
	// generation or dropped.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	for _, ent := range entries {
		name := ent.Name()
		if strings.HasSuffix(name, ".json") && !written[name] {
			_ = os.Remove(filepath.Join(dir, name))
			dp.mu.Lock()
			delete(dp.sealFiles, name)
			dp.mu.Unlock()
		}
	}
	sweepDerivedTemps(dp.file, time.Now())
	return nil
}

// load reads the kept partials, or refuses them: an epoch that may be
// caught up from, or nil and why not.
func (dp *dayParts) load(ctx context.Context, s *Server, q store.Querier) (*epoch, refusal, error) {
	if dp.file == "" {
		return nil, "", nil
	}
	var f partsFile
	ok, why := readDerived(dp.file, &f)
	if !ok {
		return nil, why, nil
	}
	def, err := partsDefinition(ctx, q)
	if err != nil {
		return nil, "", err
	}
	if why, err := checkHeader(ctx, q, f.derivedHeader, "day-partials", def); why != "" || err != nil {
		return nil, why, err
	}
	if f.Vantage != s.vantage {
		return nil, refusal(fmt.Sprintf("computed for vantage %q, not %q", f.Vantage, s.vantage)), nil
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
	for k, h := range f.Held {
		id, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, refusal("a held row keyed " + strconv.Quote(k)), nil
		}
		e.held[id] = h
	}
	for _, m := range []struct {
		list []string
		set  map[string]bool
	}{{f.HeldPubs, e.heldPubs}, {f.Covered, e.covered}, {f.Verified, e.verified}} {
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
	files := map[string]string{}
	for key, ref := range f.Seals {
		var sf sealFile
		ok, why := readDerived(filepath.Join(dir, ref.File), &sf)
		if !ok {
			if why == "" {
				why = "missing"
			}
			return nil, refusal(ref.File + ": " + string(why)), nil
		}
		b, err := os.ReadFile(filepath.Join(dir, ref.File))
		if err != nil || string(b[len(digestOpen):len(digestOpen)+64]) != ref.Digest {
			return nil, refusal(ref.File + " is not the file the index names"), nil
		}
		if sf.Kind != "day-partial-row" && sf.Kind != "day-partial-settle" || sf.Definition != def || sf.Store != f.Store {
			return nil, refusal(ref.File + " is of another kind, definition or store"), nil
		}
		kind, day, _ := strings.Cut(key, ":")
		switch {
		case kind == "row" && sf.Row != nil && sf.Row.Day == day:
			e.rows[day] = sf.Row
		case kind == "settle" && sf.Settle != nil && e.settle[day] != nil:
			e.settle[day].Seal = sf.Settle
		default:
			return nil, refusal(ref.File + " does not hold " + key), nil
		}
		files[ref.File] = ref.Digest
		if dp.gens == nil {
			dp.gens = map[string]int{}
		}
		gen := 0
		if sf.Row != nil {
			gen = sf.Row.Gen
		} else {
			gen = sf.Settle.Gen
		}
		if gen > dp.gens[key] {
			dp.gens[key] = gen
		}
	}
	dp.sealFiles = files
	return e, "", nil
}

// verifyLoaded recomputes, after the first catch-up of a loaded epoch, the
// newest sealed day of each kind and one other of each at random, and
// compares them with what the file held. A difference refuses the file.
func (s *Server) verifyLoaded(ctx context.Context, q store.Querier, e *epoch) (refusal, error) {
	pick := func(days []string) []string {
		if len(days) == 0 {
			return nil
		}
		sort.Strings(days)
		out := []string{days[len(days)-1]}
		if len(days) > 1 {
			out = append(out, days[rand.IntN(len(days)-1)])
		}
		return out
	}
	var rowDays, settleDays []string
	for d := range e.rows {
		rowDays = append(rowDays, d)
	}
	for d, sd := range e.settle {
		if sd.Seal != nil {
			settleDays = append(settleDays, d)
		}
	}
	for _, d := range pick(rowDays) {
		got, err := s.rowSpanParts(ctx, q, dayLo(d), dayHi(d), "")
		if err != nil {
			return "", err
		}
		if !sameJSON(got, e.rows[d].Vals) {
			return refusal("row day " + d + " is not what the store holds"), nil
		}
	}
	for _, d := range pick(settleDays) {
		sd := e.settle[d]
		seal := sd.Seal
		var readable int64
		if err := q.QueryRowContext(ctx, dayReadableSQL, dayLo(d), dayHi(d), sd.HLo, sd.HHi).Scan(&readable); err != nil {
			return "", err
		}
		if readable != seal.Readable {
			return refusal("settlement day " + d + "'s readable count is not what the store holds"), nil
		}
		if seal.Obl == nil || seal.MinStart == "" {
			continue
		}
		rows, err := q.QueryContext(ctx, obligationPassSQL(""), "", "", seal.SealedAt, dayLo(d), dayHi(d), seal.MaxStart, seal.MinStart)
		if err != nil {
			return "", err
		}
		got := map[string]rollup.Obligations{}
		for rows.Next() {
			var addr string
			var r rollup.Obligations
			var n int64
			var young sql.NullString
			if err := rows.Scan(append(append([]any{&addr}, scanObligations(&r)...), &n, &young)...); err != nil {
				rows.Close()
				return "", err
			}
			got[addr] = r
		}
		if err := rows.Close(); err != nil {
			return "", err
		}
		if !reflect.DeepEqual(got, seal.Obl) {
			return refusal("settlement day " + d + "'s obligations are not what the store holds"), nil
		}
	}
	return "", nil
}

// sameJSON reports whether two values encode alike.
func sameJSON(a, b any) bool {
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(x) == string(y)
}
