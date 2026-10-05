package api

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// The histograms on disk.
//
// A sealed row day's service-time and transfer-rate histograms are exact,
// one bin per distinct value of each validator's readings, and a transfer
// rate is nearly always a value of its own: they are most of what a row day
// weighs, about 14 bytes a probe row, where everything else a row day keeps
// is a few counts per validator. Held in memory for every sealed day, a
// long, busy record would hold gigabytes. So a row day is written whole to
// its seal file the moment it is sealed, before it is published, and the
// epoch keeps it without its histograms (rowDay.file names the file and its
// digest); the index (day-partials.json) keeps the same, so a start reads
// no seal file but to check its digest. A window that sums the day reads
// them from the file (dayHists), checked as the load checks it: the file
// the epoch names, by its digest, of this definition and store, holding
// this day. A seal file is written once and never changed, and a day sealed
// again is another file, so a file whose digest is the one named is the
// day as it was sealed whatever happened since: dropped, sealed again, its
// old file removed by the next write of the partials.
//
// A file that is not there, damaged or not the one named cannot be used.
// The computation that met it reads the day raw instead, which is exactly
// what the seal held, since the epoch it computes from is caught up to the
// snapshot it reads (dayparts_catchup.go); and the day is dropped from the
// newest epoch if that still holds the same seal (dropUnusable), as the
// load leaves a day whose file is missing, to be sealed again. A
// computation of an older epoch whose day was dropped and its file removed
// meanwhile meets a missing file the same way and reads the day raw: the
// newest epoch no longer holds that seal and nothing is dropped. No
// computation ever sums a histogram other than the one sealed for the day
// it holds.
//
// The histograms read last are kept within a bound (histCache,
// observer-api -day-partials-cache-mb), the newest days first: every window
// ends now, so the newest days are in each of them and the oldest only in
// the longest.

// valHists is a validator's histograms of one day.
type valHists struct {
	lat, tput []hbin
}

// dayHists is every validator's histograms of one sealed row day.
type dayHists map[string]*valHists

// histOverhead is what a validator's entry costs beside its bins: the map
// entry, its address and the two slices' headers.
const histOverhead = 160

// size is about how many bytes h holds.
func (h dayHists) size() int64 {
	var n int64
	for _, v := range h {
		n += histOverhead + int64(len(v.lat)+len(v.tput))*16
	}
	return n
}

// histsOf is the histograms a whole row day holds.
func histsOf(rd *rowDay) dayHists {
	h := make(dayHists, len(rd.Vals))
	for addr, p := range rd.Vals {
		if len(p.Lat) > 0 || len(p.Tput) > 0 {
			h[addr] = &valHists{lat: p.Lat, tput: p.Tput}
		}
	}
	return h
}

// stripped is rd without its histograms, held whole in the seal file
// named.
func (rd *rowDay) stripped(file, digest string) *rowDay {
	c := *rd
	c.Vals = make(map[string]*rowPart, len(rd.Vals))
	for addr, p := range rd.Vals {
		q := *p
		q.Lat, q.Tput = nil, nil
		c.Vals[addr] = &q
	}
	c.file, c.digest = file, digest
	return &c
}

// sealDir is the directory of the seal files.
func (dp *dayParts) sealDir() string {
	return filepath.Join(filepath.Dir(dp.file), dayPartsDir)
}

// writeRowSeal writes row day rd, read under epoch e, whole to a seal file
// of a new generation, and returns its name and digest. The file is
// pending until the sealer publishes the day or gives it up (unpend): no
// write of the partials meanwhile removes it as a file nothing names.
func (dp *dayParts) writeRowSeal(e *epoch, rd *rowDay) (name, digest string, err error) {
	dp.mu.Lock()
	rd.Gen = dp.nextGen("row:" + rd.Day)
	name = sealName("row", rd.Day, rd.Gen)
	if dp.pending == nil {
		dp.pending = map[string]bool{}
	}
	dp.pending[name] = true
	dp.mu.Unlock()
	sf := sealFile{
		derivedHeader: derivedHeader{Kind: "day-partial-row", Format: derivedFormat, Definition: e.def, Store: e.store},
		Day:           rd.Day, Row: rd,
	}
	if digest, err = writeDerivedSum(filepath.Join(dp.sealDir(), name), sf); err != nil {
		dp.unpend(name, true)
		return "", "", err
	}
	return name, digest, nil
}

// unpend ends a seal file's pending, and with remove removes it: the
// sealer did not publish its day.
func (dp *dayParts) unpend(name string, remove bool) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	delete(dp.pending, name)
	if remove {
		_ = os.Remove(filepath.Join(dp.sealDir(), name))
	}
}

// errUnusable is a seal file a window could not use: missing, damaged, or
// not the one named.
var errUnusable = errors.New("its seal file cannot be used")

// dayHists is sealed row day rd's histograms, from the cache or from its
// seal file, checked, or from rd itself when it holds them (no file). An
// error wraps errUnusable.
func (dp *dayParts) dayHists(e *epoch, rd *rowDay) (dayHists, error) {
	if rd.file == "" {
		return histsOf(rd), nil
	}
	if h := dp.hists.get(rd.Day, rd.digest); h != nil {
		return h, nil
	}
	if dp.beforeRead != nil {
		dp.beforeRead(rd.Day)
	}
	whole, err := dp.readRowSeal(e, rd)
	if err != nil {
		return nil, err
	}
	h := histsOf(whole)
	dp.hists.put(rd.Day, rd.digest, h)
	return h, nil
}

// readRowSeal reads the seal file rd names, whole: the file whose digest
// rd names, its body what its digest says, of e's definition and store,
// holding rd's day.
func (dp *dayParts) readRowSeal(e *epoch, rd *rowDay) (*rowDay, error) {
	unusable := func(why string) error {
		return fmt.Errorf("%w: %s %s", errUnusable, rd.file, why)
	}
	b, err := os.ReadFile(filepath.Join(dp.sealDir(), rd.file))
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, unusable("is missing")
	case err != nil:
		return nil, unusable("is unreadable: " + err.Error())
	}
	if why := unsealDerived(b); why != "" {
		return nil, unusable("is damaged: " + string(why))
	}
	if string(b[len(digestOpen):len(digestOpen)+2*sha256.Size]) != rd.digest {
		return nil, unusable("is not the one named")
	}
	var sf sealFile
	if err := json.Unmarshal(b, &sf); err != nil {
		return nil, unusable("does not parse: " + err.Error())
	}
	switch {
	case sf.Kind != "day-partial-row" || sf.Format != derivedFormat || sf.Definition != e.def || sf.Store != e.store:
		return nil, unusable("is of another kind, definition or store")
	case sf.Day != rd.Day || sf.Row == nil || sf.Row.Day != rd.Day:
		return nil, unusable("does not hold row day " + rd.Day)
	}
	return sf.Row, nil
}

// wholeRow is rd with its histograms: the counts as the epoch holds them,
// the histograms from the file. An error wraps errUnusable.
func (dp *dayParts) wholeRow(e *epoch, rd *rowDay) (*rowDay, error) {
	if rd.file == "" {
		return rd, nil
	}
	h, err := dp.dayHists(e, rd)
	if err != nil {
		return nil, err
	}
	c := *rd
	c.Vals = make(map[string]*rowPart, len(rd.Vals))
	for addr, p := range rd.Vals {
		q := *p
		if v := h[addr]; v != nil {
			q.Lat, q.Tput = v.lat, v.tput
		}
		c.Vals[addr] = &q
	}
	return &c, nil
}

// unusableSeal is a sealed row day whose file a computation could not use,
// and why.
type unusableSeal struct {
	rd  *rowDay
	err error
}

// dropUnusable drops the sealed row days whose seal files could not be
// used, each only while the newest epoch still holds that very seal: a day
// sealed again since has another file, and stays. Each day dropped is
// logged once.
func (dp *dayParts) dropUnusable(bad []unusableSeal) {
	j := journalEntry{rows: map[string]bool{}}
	var dropped []unusableSeal
	dp.mu.Lock()
	dp.unusable += len(bad)
	dp.mu.Unlock()
	dp.publish(func(cur *epoch, _ []journalEntry) bool {
		for _, b := range bad {
			if c := cur.rows[b.rd.Day]; c != nil && c.digest == b.rd.digest {
				delete(cur.rows, b.rd.Day)
				cur.content++
				j.rows[b.rd.Day] = true
				dropped = append(dropped, b)
			}
		}
		return len(dropped) > 0
	}, 0, j)
	for _, b := range dropped {
		dp.logOnce("unusable:"+b.rd.digest, "day partials: row day %s: %v; it is read raw until it is sealed again", b.rd.Day, b.err)
	}
}

// checkSealFile reads the seal file at path through: missing is set when
// there is none, or one that opens with another digest (another process's
// of the same name), and why says why it is not what its digest says.
// Nothing of it is kept.
func checkSealFile(path, digest string) (missing bool, why refusal) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, ""
	}
	if err != nil {
		return false, refusal("unreadable: " + err.Error())
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<16)
	n := len(digestOpen) + 2*sha256.Size
	head := make([]byte, n+1)
	if _, err := io.ReadFull(r, head); err != nil {
		return false, "no digest where the file opens"
	}
	if string(head[:len(digestOpen)]) != digestOpen || head[n] != '"' {
		return false, "no digest where the file opens"
	}
	if string(head[len(digestOpen):n]) != digest {
		return true, ""
	}
	h := sha256.New()
	h.Write([]byte(digestOpen))
	h.Write(head[n:])
	if _, err := io.Copy(h, r); err != nil {
		return false, refusal("unreadable: " + err.Error())
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return false, "its digest is not its body's"
	}
	return false, ""
}

// ---- the cache ----

// histCache keeps sealed row days' histograms within a bound of bytes, the
// newest days first: one that does not fit is not kept, and room is made
// by letting the oldest go. A day is kept under its seal's digest, so a
// day sealed again is never answered with the histograms of its older
// seal.
type histCache struct {
	mu    sync.Mutex
	max   int64
	bytes int64
	days  map[string]*histEntry
	// hits and misses count the lookups, for tests and measurements.
	hits, misses int64
}

type histEntry struct {
	digest string
	h      dayHists
	size   int64
}

// DefaultHistCacheMB is observer-api's -day-partials-cache-mb: the
// megabytes of sealed row days' histograms kept in memory.
const DefaultHistCacheMB = 64

// WithHistCache bounds the sealed row days' histograms kept in memory to mb
// megabytes (observer-api -day-partials-cache-mb); 0 keeps none, and every
// window reads them from the seal files.
func WithHistCache(mb int) Option {
	return func(s *Server) {
		if mb >= 0 {
			s.histCacheMB = mb
		}
	}
}

func newHistCache(maxBytes int64) *histCache {
	return &histCache{max: maxBytes, days: map[string]*histEntry{}}
}

// get is day's histograms under digest, nil when they are not kept.
func (c *histCache) get(day, digest string) dayHists {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if en := c.days[day]; en != nil && en.digest == digest {
		c.hits++
		return en.h
	}
	c.misses++
	return nil
}

// put keeps day's histograms under digest, in place of any of another
// seal of the day, if they fit with the newer days kept.
func (c *histCache) put(day, digest string, h dayHists) {
	if c == nil || c.max <= 0 {
		return
	}
	size := h.size()
	c.mu.Lock()
	defer c.mu.Unlock()
	if old := c.days[day]; old != nil {
		c.bytes -= old.size
		delete(c.days, day)
	}
	if size > c.max {
		return
	}
	c.days[day] = &histEntry{digest: digest, h: h, size: size}
	c.bytes += size
	for c.bytes > c.max {
		oldest := ""
		for d := range c.days {
			if oldest == "" || d < oldest {
				oldest = d
			}
		}
		c.bytes -= c.days[oldest].size
		delete(c.days, oldest)
	}
}

// retain lets go of every day whose digest keep does not hold: days
// dropped, or sealed again.
func (c *histCache) retain(keep map[string]bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for d, en := range c.days {
		if !keep[en.digest] {
			c.bytes -= en.size
			delete(c.days, d)
		}
	}
}

// held is how many bytes the cache holds, and of how many days.
func (c *histCache) held() (int64, int) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes, len(c.days)
}
