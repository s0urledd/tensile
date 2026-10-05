package api

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// Day partials.
//
// The 7d, 30d and "all" windows used to be computed from every row of the
// window on every refresh, so their cost grew with the record: a busy day's
// rows cost about 20 seconds of statements, and thirty of them passed the
// five-minute timeout. Nearly all of that work is the same from one refresh
// to the next, because a day that is over does not change. So each figure
// that is a sum, a maximum, a set union or an exact histogram over rows is
// kept per UTC day, and a window is the days it covers wholly, summed in
// memory, plus what it covers partly or cannot use a kept day for, read raw
// with the shipped statement text over those bounds. There is still one
// definition of every figure: a day's partial is the shipped statement over
// the day, and nothing here computes a figure a second way.
//
// Three families, by what a day is:
//
//   - row days (rowDay), by the day a row started: the class tallies, the
//     reading and gap counts, the newest reading and the newest served one,
//     the service-time and transfer-rate histograms and the heartbeats, per
//     validator (the validatorRows statements, rowsql.go);
//   - settlement days (settleDay), by the day a publication settled: the
//     obligations and the publications that are readable, sealed once the
//     day is final (settleSeal), and the row span of the day's promises
//     that bounds a raw statement over them;
//   - and, of the settlement day too, the publication ledger: the count and
//     bytes, and the signing and load figures per validator, from the
//     publication and assignment records, which are written once and never
//     deleted, so the ledger is added to and kept forever.
//
// Attestation stays raw: a (validator, blob) pair is counted once at the
// MAX of its rows in the window, which is not a sum over days.
//
// A kept day is used only while it is exactly what the statement would
// return over it. Every write that can move one (a new row, an amendment, a
// correction, a hold, a collapse, a prune) is found when the next
// computation catches up (dayparts_catchup.go) and the day it touches is
// dropped and read raw until it is sealed again (dayparts_seal.go). The
// state is immutable once published (epoch): each computation takes the
// newest epoch, catches it up inside its own read transaction and computes
// every figure from that transaction, so a day is never both summed and
// read raw, and two computations never see one another's half-made state.
// See docs in dayparts_catchup.go and dayparts_seal.go for when a day is
// sealed and when it is dropped, dayparts_file.go for how the partials are
// kept across restarts, dayparts_hist.go for where a row day's histograms
// are kept, and dayparts_rank.go for how a long window ranks them.

// dayLayout is the day key: the first ten characters of a store.TimeLayout
// timestamp.
const dayLayout = "2006-01-02"

// dayLo and dayHi are a day's first and last timestamps, inclusive, as the
// store writes them.
func dayLo(d string) string { return d + "T00:00:00.000000000Z" }
func dayHi(d string) string { return d + "T23:59:59.999999999Z" }

// dayAdd moves a day key by n days.
func dayAdd(d string, n int) string {
	t, err := time.Parse(dayLayout, d)
	if err != nil {
		return d
	}
	return t.AddDate(0, 0, n).Format(dayLayout)
}

// dayOfTime is the day key of t.
func dayOfTime(t time.Time) string { return t.UTC().Format(dayLayout) }

// isTS reports whether v is exactly a timestamp as the store writes them:
// its day is then v[:10], and it sorts between that day's dayLo and dayHi.
// Anything else sorts somewhere no day's bounds hold it.
func isTS(v string) bool {
	if len(v) != len(store.TimeLayout) {
		return false
	}
	t, err := time.Parse(store.TimeLayout, v)
	return err == nil && store.TS(t) == v
}

// ---- the partials ----

// rowPart is one validator's rows of one span (a day, or a raw span).
type rowPart struct {
	// Classes are the effective classes of the assigned in-window rows
	// (valClassesSQL).
	Classes map[string]int64 `json:"classes,omitempty"`
	// Probes is every row, LastSeen the newest's start, LastServed the
	// newest whose effective class is HEALTHY ("" none) (valSeenSQL).
	Probes     int64  `json:"probes,omitempty"`
	LastSeen   string `json:"last_seen,omitempty"`
	LastServed string `json:"last_served,omitempty"`
	// Gaps are the NOT_PROBED and PROBE_ERROR rows by outcome (valGapsSQL).
	Gaps map[string]int64 `json:"gaps,omitempty"`
	// Lat and Tput are the service-time and transfer-rate histograms
	// (valLatencyHistSQL, valThroughputHistSQL), ascending.
	Lat  hbins `json:"lat,omitempty"`
	Tput hbins `json:"tput,omitempty"`
	// The heartbeats (valBeatsSQL).
	Beats    int64  `json:"beats,omitempty"`
	Up       int64  `json:"up,omitempty"`
	IdentUp  int64  `json:"ident_up,omitempty"`
	LastDown string `json:"last_down,omitempty"`
	LastUp   string `json:"last_up,omitempty"`
}

// hbin is one histogram bin: how many rows had the value.
type hbin [2]int64

// hbins is a histogram's bins, written as JSON's [[value, count], ...]. It
// is read without reflection (UnmarshalJSON): the bins are most of a row
// day's seal file, read whenever a window sums the day, and through the
// decoder's reflection a seal file took twice as long to read.
type hbins []hbin

// UnmarshalJSON reads bins as Marshal writes them, and anything else the
// way the decoder would.
func (h *hbins) UnmarshalJSON(b []byte) error {
	i := 0
	ws := func() {
		for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
			i++
		}
	}
	num := func() (int64, bool) {
		neg := i < len(b) && b[i] == '-'
		if neg {
			i++
		}
		start := i
		var v uint64
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			if v > (math.MaxInt64+1)/10 {
				return 0, false
			}
			v = v*10 + uint64(b[i]-'0')
			i++
		}
		switch {
		case i == start, i-start > 1 && b[start] == '0', i < len(b) && (b[i] == '.' || b[i] == 'e' || b[i] == 'E'):
			return 0, false
		case neg && v <= math.MaxInt64+1:
			return -int64(v-1) - 1, true
		case !neg && v <= math.MaxInt64:
			return int64(v), true
		}
		return 0, false
	}
	slow := func() error {
		var out []hbin
		if err := json.Unmarshal(b, &out); err != nil {
			return err
		}
		*h = out
		return nil
	}
	ws()
	if string(b[i:]) == "null" {
		*h = nil
		return nil
	}
	if i >= len(b) || b[i] != '[' {
		return slow()
	}
	i++
	out := make(hbins, 0, max(0, bytes.Count(b, []byte{'['})-1))
	ws()
	if i < len(b) && b[i] == ']' {
		i++
	} else {
		for {
			ws()
			if i >= len(b) || b[i] != '[' {
				return slow()
			}
			i++
			ws()
			v, ok := num()
			ws()
			if !ok || i >= len(b) || b[i] != ',' {
				return slow()
			}
			i++
			ws()
			n, ok := num()
			ws()
			if !ok || i >= len(b) || b[i] != ']' {
				return slow()
			}
			i++
			out = append(out, hbin{v, n})
			ws()
			if i < len(b) && b[i] == ',' {
				i++
				continue
			}
			if i < len(b) && b[i] == ']' {
				i++
				break
			}
			return slow()
		}
	}
	ws()
	if i != len(b) {
		return slow()
	}
	*h = out
	return nil
}

// rowDay is a sealed row day: every validator's rows started on it.
type rowDay struct {
	Day  string              `json:"day"`
	Vals map[string]*rowPart `json:"vals"`
	// Collapsible are the promises with rows on this day that a collapse
	// would turn into a decision (store.SampledOutReasonSQL): a decision
	// for one of them moves its rows, so the day is dropped.
	Collapsible []string `json:"collapsible,omitempty"`
	SealedAt    string   `json:"sealed_at"`
	Gen         int      `json:"gen"`
	// file and digest name the seal file that holds the day whole, and
	// then Vals holds no histogram: they are read from the file when a
	// window sums the day (dayparts_hist.go). With no file, the partials
	// kept nowhere, Vals holds them.
	file, digest string
}

// sigPart is one validator's signing counts over some promises, in
// signingByValidatorSQL's order: assigned, signed, unknown, no host.
type sigPart [4]int64

// loadPart is one validator's load over some promises (loadSQL's in-window
// sums): the endorsed assignments, their rows, and the row data as exact
// integers. A term of the load bytes is row_count * blob_size /
// original_rows; with original_rows 2^k it is the integer row_count *
// blob_size scaled by 2^-k, and Bytes keeps the integers per k, so the sum
// the statement rounds can be formed exactly (loadBytes).
type loadPart struct {
	Promises int64 `json:"promises"`
	Rows     int64 `json:"rows"`
	// Terms counts the terms SUM adds (those whose original_rows is
	// neither NULL nor zero).
	Terms int64 `json:"terms"`
	// Bytes is, per k, the sum of row_count * blob_size over the terms
	// whose original_rows is 2^k.
	Bytes map[int]*big.Int `json:"bytes,omitempty"`
	// Odd is set when a term is not of that form, or too large for the
	// sum to be exact: then the statement itself is read.
	Odd bool `json:"odd,omitempty"`
}

func (l *loadPart) clone() *loadPart {
	c := *l
	c.Bytes = make(map[int]*big.Int, len(l.Bytes))
	for k, v := range l.Bytes {
		c.Bytes[k] = new(big.Int).Set(v)
	}
	return &c
}

func (l *loadPart) add(o *loadPart) {
	l.Promises += o.Promises
	l.Rows += o.Rows
	l.Terms += o.Terms
	l.Odd = l.Odd || o.Odd
	if l.Bytes == nil {
		l.Bytes = map[int]*big.Int{}
	}
	for k, v := range o.Bytes {
		if cur, ok := l.Bytes[k]; ok {
			cur.Add(cur, v)
		} else {
			l.Bytes[k] = new(big.Int).Set(v)
		}
	}
}

// settleSeal is a sealed settlement day: what its promises' rows add up to
// once the day is final.
type settleSeal struct {
	// Readable is how many of the day's publications have a reading on
	// record (readableSQL).
	Readable int64 `json:"readable"`
	// Obl is the nine obligation counts per validator, the day's promises
	// read with the day's exact row span; nil for a day before raw_from,
	// whose obligations the daily rollup holds.
	Obl map[string]rollup.Obligations `json:"obl,omitempty"`
	// MaxFirstFault is the newest first fault of any obligation of the day:
	// none of them is provisional once the cutoff has passed it.
	MaxFirstFault string `json:"max_first_fault,omitempty"`
	// When it was sealed, and its generation (its file's name).
	SealedAt string `json:"sealed_at"`
	Gen      int    `json:"gen"`
	// Span is the row span the seal was computed with.
	MinStart  string `json:"min_start,omitempty"`
	MaxStart  string `json:"max_start,omitempty"`
	MaxRowMSU string `json:"max_row_msu,omitempty"`
}

// settleDay is one settlement day.
type settleDay struct {
	// The ledger: every publication settled on the day. Added to, never
	// taken from.
	Pubs      int64                `json:"pubs"`
	Bytes     int64                `json:"bytes"`
	HLo       int64                `json:"hlo"`
	HHi       int64                `json:"hhi"`
	MaxPubMSU string               `json:"max_pub_msu,omitempty"`
	Signing   map[string]sigPart   `json:"signing,omitempty"`
	Load      map[string]*loadPart `json:"load,omitempty"`
	// SpanKnown says MinStart..MaxStart holds the start of every row of
	// the day's promises (in both arms of store.ObligationRowsVerified,
	// any phase) and MaxRowMSU is at least every such row's deadline. It
	// may hold more than that (rows since deleted), never less.
	SpanKnown bool   `json:"span_known"`
	MinStart  string `json:"min_start,omitempty"`
	MaxStart  string `json:"max_start,omitempty"`
	MaxRowMSU string `json:"max_row_msu,omitempty"`
	// Seal, while the day's promises are as they were when it was sealed.
	Seal *settleSeal `json:"-"`
}

func (d *settleDay) clone() *settleDay {
	c := *d
	c.Signing = make(map[string]sigPart, len(d.Signing))
	for k, v := range d.Signing {
		c.Signing[k] = v
	}
	c.Load = make(map[string]*loadPart, len(d.Load))
	for k, v := range d.Load {
		c.Load[k] = v // copied on write (addLoad)
	}
	return &c
}

// widen adds a row start and deadline to the span.
func (d *settleDay) widen(start, msu string) {
	if start != "" {
		if d.MinStart == "" || start < d.MinStart {
			d.MinStart = start
		}
		if start > d.MaxStart {
			d.MaxStart = start
		}
	}
	if msu > d.MaxRowMSU {
		d.MaxRowMSU = msu
	}
}

// hasRows reports whether the span holds any row.
func (d *settleDay) hasRows() bool { return d.MinStart != "" }

// ---- the epoch ----

// mark is one entry of a table's marks ladder: a row the catch-up read up
// to, by rowid and by its own key, and the day it started on for a table
// whose rows the prune deletes ("" for one it never does).
type mark struct {
	Rowid int64  `json:"rowid"`
	Key   string `json:"key"`
	Day   string `json:"day,omitempty"`
}

// anchor is one row of a table on a row day, by rowid and key: while it is
// there, the prune has not reached that table's rows of the day (it
// deletes a table's day in one statement).
type anchor struct {
	Rowid int64  `json:"rowid"`
	Key   string `json:"key"`
}

// dayAnchors are a row day's anchors: a probe row, a decision point and
// one of this observer's heartbeats started on it, where it has any.
type dayAnchors struct {
	Probe *anchor `json:"probe,omitempty"`
	Point *anchor `json:"point,omitempty"`
	Beat  *anchor `json:"beat,omitempty"`
}

// epoch is the partials as one catch-up left them: immutable once
// published. A computation reads one epoch; the next catch-up starts from a
// copy of it.
type epoch struct {
	seq uint64
	// born is the seq of the build this epoch descends from: a seal read
	// under one build is never published into another.
	born uint64
	// content counts the changes to what the files keep of substance (a
	// day sealed or dropped, the ledger grown): a save is due only when it
	// moved (dayparts_file.go), not on every catch-up's marks.
	content uint64
	// store is the identity of the store the epoch was built from
	// (partsIdentity), def the definition it was computed under and schema
	// the schema version it last saw: a migration is judged by what it did
	// to the tables the partials read (catchUpFrom), not by its number.
	store  storeIdentity
	def    string
	schema int
	// marks is each table's marks ladder, newest last.
	marks map[string][]mark
	// rawFrom is the first day whose raw rows are all still present
	// (rollup.RawFrom), "" before the first prune.
	rawFrom string
	// heldRev is store.MetaHeldFlagsRev as the last catch-up read it,
	// heldProm a digest of each promise's held rows (their rowids) and
	// heldPubs the held publications (readHolds). The maps are replaced
	// whole, never changed.
	heldRev  string
	heldProm map[string]string
	heldPubs map[string]bool
	// fps is the fingerprint of every decision of a publication a
	// correction can reach (dayparts_catchup.go), by promise hash; reach
	// those publications (a verified params range covers them, or a
	// correction moved them) that had a decision at the last catch-up, and
	// those added since; verified the ranges already counted there.
	fps      map[string]string
	reach    map[string]bool
	verified map[string]bool
	// corrPub is every corrected publication as the last catch-up saw it
	// (its deadline, when a correction last wrote it, its settlement day),
	// replaced whole; corrRows is, by promise, a digest of the rows a
	// correction wrote, for every promise with such a row still in the
	// store (readCorrections).
	corrPub  map[string]string
	corrRows map[string]string
	// weird are the row starts that are not store timestamps, sorted: they
	// sort between days, where no day's bounds hold them.
	weird []string
	// rows are the sealed row days, settle every settlement day the ledger
	// has seen, and anchors the anchored row days.
	rows    map[string]*rowDay
	settle  map[string]*settleDay
	anchors map[string]*dayAnchors
	// ledgerBuilt is set once every publication is in the ledger: the
	// build folds publications up to ledgerHi, the publications mark when
	// it began, ledgerTo being how far it has come; the catch-ups fold
	// those after it as they arrive.
	ledgerBuilt bool
	ledgerTo    int64
	ledgerHi    int64
	// pubsOdd: a publication's settlement_time is not a store timestamp, so
	// it belongs to no day and every settlement figure is read raw.
	pubsOdd bool
	// msuDue are the days whose latest deadline the next catch-up reads
	// again: a step of the ledger build brought them in after a correction
	// moved one (buildLedger). Replaced whole, never changed.
	msuDue map[string]bool
	// firstRow is the first day a row started on, where the sealer begins.
	firstRow string
}

func newEpoch() *epoch {
	return &epoch{
		marks: map[string][]mark{}, heldProm: map[string]string{}, heldPubs: map[string]bool{},
		fps: map[string]string{}, reach: map[string]bool{}, verified: map[string]bool{},
		corrPub: map[string]string{}, corrRows: map[string]string{},
		rows: map[string]*rowDay{}, settle: map[string]*settleDay{}, anchors: map[string]*dayAnchors{},
	}
}

// clone is a copy the next catch-up may change: the maps are copied, the
// values they point at are not (they are replaced, never changed).
func (e *epoch) clone() *epoch {
	c := *e
	c.marks = make(map[string][]mark, len(e.marks))
	for k, v := range e.marks {
		c.marks[k] = append([]mark(nil), v...)
	}
	c.fps = make(map[string]string, len(e.fps))
	for k, v := range e.fps {
		c.fps[k] = v
	}
	c.reach = copySet(e.reach)
	c.corrRows = make(map[string]string, len(e.corrRows))
	for k, v := range e.corrRows {
		c.corrRows[k] = v
	}
	c.verified = copySet(e.verified)
	c.weird = append([]string(nil), e.weird...)
	c.rows = make(map[string]*rowDay, len(e.rows))
	for k, v := range e.rows {
		c.rows[k] = v
	}
	c.settle = make(map[string]*settleDay, len(e.settle))
	for k, v := range e.settle {
		c.settle[k] = v
	}
	c.anchors = make(map[string]*dayAnchors, len(e.anchors))
	for k, v := range e.anchors {
		c.anchors[k] = v
	}
	return &c
}

func copySet(m map[string]bool) map[string]bool {
	c := make(map[string]bool, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// settleMut is day d of the epoch, copied so it may be changed.
func (e *epoch) settleMut(d string) *settleDay {
	if cur, ok := e.settle[d]; ok {
		c := cur.clone()
		e.settle[d] = c
		return c
	}
	c := &settleDay{Signing: map[string]sigPart{}, Load: map[string]*loadPart{}}
	// A day first seen once the ledger holds every publication had none
	// before, so its span is known from its first publication on: every
	// row of its promises is folded in as the publication and the rows
	// arrive.
	c.SpanKnown = e.ledgerBuilt
	e.settle[d] = c
	return c
}

// gapClean reports whether no row start that is not a store timestamp
// lies next to day d, where the bounds of d and of the days beside it
// would all miss it.
func (e *epoch) gapClean(d string) bool {
	if len(e.weird) == 0 {
		return true
	}
	for _, g := range [][2]string{{dayHi(dayAdd(d, -1)), dayLo(d)}, {dayHi(d), dayLo(dayAdd(d, 1))}} {
		i := sort.SearchStrings(e.weird, g[0])
		for ; i < len(e.weird) && e.weird[i] < g[1]; i++ {
			if e.weird[i] > g[0] {
				return false
			}
		}
	}
	return true
}

// addWeird records a row start that is not a store timestamp.
func (e *epoch) addWeird(v string) {
	i := sort.SearchStrings(e.weird, v)
	if i < len(e.weird) && e.weird[i] == v {
		return
	}
	e.weird = append(e.weird, "")
	copy(e.weird[i+1:], e.weird[i:])
	e.weird[i] = v
}

// ---- the server's handle ----

// dayParts is the server's day partials: the newest epoch, the journal of
// what the recent catch-ups dropped (for the sealer), and the files.
type dayParts struct {
	// mu orders catch-ups: an epoch is caught up and published under it,
	// and the read transaction the catch-up reads through is begun under
	// it too, so the epoch a catch-up starts from is never newer than its
	// snapshot.
	mu      sync.Mutex
	cur     *epoch
	journal []journalEntry
	log     logf
	// file is where the partials are kept (dayparts_file.go); "" for
	// nowhere.
	file string
	// persisted state (dayparts_file.go), guarded by mu. loaded is set once
	// the kept partials were loaded, refused or found missing, and loading
	// while that runs (readtx.go); saved is the content generation last
	// written (epoch.content), saveErr the last write's error and when.
	savedBorn uint64
	saved     uint64
	savedAt   time.Time
	saveErr   string
	saveErrAt time.Time
	loaded    bool
	loading   chan struct{}
	origin    string
	writing   chan struct{}
	sealFiles map[string]string
	// pending are the seal files the sealer has written and not published
	// yet (writeRowSeal), which no write of the partials removes; guarded
	// by mu.
	pending map[string]bool
	// rested is how long sealDue has rested for its pace (WithSealPace), in
	// all: the rests a pace adds, measured, so a test can hold them to the
	// work they follow whatever else the machine is doing
	rested atomic.Int64
	// hists keeps the histograms of the sealed row days read last, within
	// its bound (WithHistCache); beforeRead runs before a seal file is read
	// for them, for tests.
	hists      *histCache
	beforeRead func(day string)
	// mergeDays is how many sealed days a window sums its histograms of
	// whole, and bands the bands a longer one ranks in, by the name of the
	// window they are kept for (dayparts_rank.go); bands guarded by mu.
	mergeDays int
	bands     map[string]*bandSet
	// fallback is why the process reads every window raw, "" while it does
	// not, and since when (fallBack); guarded by mu.
	fallback   string
	fallbackAt time.Time
	// auditAt is the audit's cursor per kind of day (auditNext), kept with
	// the partials; audited and auditNote the last audit and what it found,
	// for /v1/health. Guarded by mu.
	auditAt   map[string]string
	audited   time.Time
	auditNote string
	// failures are the units of work that failed, by key, backing off
	// (fail); raw the days kept raw on purpose (keepRaw); dueSince when the
	// sealer first found each day due and unsealed (noteDue). Guarded by mu.
	failures map[string]*failure
	raw      map[string]*keptRaw
	dueSince map[string]time.Time
	// failLoad, when set, fails the load before its check; failUnit, when
	// it returns an error, fails a day's seal with it; beforePublish runs
	// between a seal's read and its publish (sealRow, sealSettle). For
	// tests.
	failLoad      func() error
	failUnit      func(kind, day string) error
	beforePublish func(kind, day string)
	// fullHolds has the next catch-up read every hold again and diff it,
	// whatever the holds' counter says (readHolds): set by the hourly
	// audit, so that a flag moved by a build that does not keep the counter
	// is found within the hour.
	fullHolds bool
	// sealing is held while the sealer runs, so one seals at a time.
	sealing sync.Mutex
	// retry holds the days the sealer did not seal and when to try them
	// again; gens each day's seal generation. Both guarded by mu.
	retry map[string]time.Time
	gens  map[string]int
	// logged are the lines logOnce has logged, by key, and turn whose turn
	// it is while the ledger is built (ledgerTurn); guarded by mu.
	logged map[string]bool
	turn   bool
	// rebuilds counts the times the partials were begun again, dropped the
	// sealed days a catch-up dropped, and unusable the seal files a
	// computation could not use, for tests.
	rebuilds int
	dropped  int
	unusable int
}

// journalEntry is what one catch-up dropped: a sealer that read a day
// before this catch-up does not publish what it read. Besides the days it
// dropped, it names what it found that reaches days by what a seal holds
// rather than by day: the promises a collapse made decisions of (a row
// day's Collapsible), and the row spans a prune took (a settlement day's
// span). A day being sealed is not among the sealed days those are
// matched against, so its seal is matched against them when it is
// published (sealRow, sealSettle).
type journalEntry struct {
	seq       uint64
	rows      map[string]bool
	settle    map[string]bool
	collapsed map[string]bool
	reach     [][2]string
	// msu are the days whose latest deadline a catch-up read again
	// (readMaxPubMSU) while the ledger did not hold them yet.
	msu map[string]bool
	all bool
}

// journalKeep is how many catch-ups the journal remembers. A seal computed
// under an epoch older than the oldest of them is not published.
const journalKeep = 4096

// epochKey carries the epoch a computation reads.
type epochKey struct{}

func withEpoch(ctx context.Context, e *epoch) context.Context {
	return context.WithValue(ctx, epochKey{}, e)
}

// epochOf is the epoch a computation reads, nil when it reads the shipped
// statements over the whole window (the partials are off, or the catch-up
// failed).
func epochOf(ctx context.Context) *epoch {
	e, _ := ctx.Value(epochKey{}).(*epoch)
	return e
}
