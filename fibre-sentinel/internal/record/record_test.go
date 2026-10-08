package record

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func lineAt(at time.Time, who string, n int) string {
	return fmt.Sprintf(`{"scheduled_at":%q,"w":%q,"n":%d}`+"\n", at.UTC().Format(time.RFC3339Nano), who, n)
}

func readAll(t *testing.T, path string) []byte {
	t.Helper()
	r, err := OpenAll(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func appendLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	a, err := OpenAppender(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	for _, l := range lines {
		if _, err := a.Write([]byte(l)); err != nil {
			t.Fatal(err)
		}
	}
}

func archiveAt(t *testing.T, path string, cutoff time.Time) Result {
	t.Helper()
	res, err := Archive(path, Options{Cutoff: cutoff, TimeField: "scheduled_at", Limit: -1, Now: cutoff})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func skipUnsupported(t *testing.T) {
	if !rotationSupported {
		t.Skip("rotation needs flock")
	}
}

// Archiving moves the old lines into a segment and leaves the rest live;
// read through the archive, the file is byte for byte what was written,
// from any logical offset, and a second run with the same cutoff moves
// nothing.
func TestArchiveKeepsEveryByteAtItsOffset(t *testing.T) {
	skipUnsupported(t)
	path := filepath.Join(t.TempDir(), "measurements.jsonl")
	var want bytes.Buffer
	for d := 0; d < 10; d++ {
		for i := 0; i < 3; i++ {
			l := lineAt(t0.Add(time.Duration(d)*24*time.Hour+time.Duration(i)*time.Hour), "a", d*3+i)
			want.WriteString(l)
			appendLines(t, path, l)
		}
	}
	res := archiveAt(t, path, t0.Add(4*24*time.Hour))
	if res.Lines != 12 || res.Skipped != "" {
		t.Fatalf("first run: %+v", res)
	}
	res = archiveAt(t, path, t0.Add(7*24*time.Hour))
	if res.Lines != 9 || res.Base == 0 {
		t.Fatalf("second run: %+v", res)
	}
	if again := archiveAt(t, path, t0.Add(7*24*time.Hour)); again.Skipped == "" {
		t.Fatalf("a repeated run moved something: %+v", again)
	}
	if got := readAll(t, path); !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("record changed:\n%s\nwant\n%s", got, want.Bytes())
	}
	live, _ := os.ReadFile(path)
	if strings.Count(string(live), "\n") != 9 {
		t.Fatalf("live file keeps %d lines, want 9", strings.Count(string(live), "\n"))
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.End() != int64(want.Len()) || s.Base() != int64(want.Len()-len(live)) || !s.Placed() {
		t.Fatalf("base %d end %d placed %v, want %d %d placed", s.Base(), s.End(), s.Placed(), want.Len()-len(live), want.Len())
	}
	for _, off := range []int64{0, 1, 57, s.Base() - 1, s.Base(), s.Base() + 3, s.End()} {
		r, err := s.ReaderFrom(off)
		if err != nil {
			t.Fatalf("offset %d: %v", off, err)
		}
		got, _ := io.ReadAll(r)
		if !bytes.Equal(got, want.Bytes()[off:]) {
			t.Fatalf("offset %d reads %q", off, got)
		}
	}
	if n, err := Verify(path); err != nil || n != 2 {
		t.Fatalf("verify: %d %v", n, err)
	}
	if since := LiveSince(path); !since.Equal(t0.Add(7 * 24 * time.Hour)) {
		t.Fatalf("live since %s", since)
	}
}

// The cut stops at the first line dated at or after the cutoff, or not
// dated at all, never passes the limit, and always leaves the last line.
func TestArchiveCutRules(t *testing.T) {
	skipUnsupported(t)
	dir := t.TempDir()
	old := func(n int) string { return lineAt(t0, "a", n) }

	p := filepath.Join(dir, "undated.jsonl")
	appendLines(t, p, old(1), old(2), `{"n":3}`+"\n", old(4), lineAt(t0.Add(48*time.Hour), "a", 5))
	if res := archiveAt(t, p, t0.Add(24*time.Hour)); res.Lines != 2 {
		t.Fatalf("an undated line must stop the cut: %+v", res)
	}

	p = filepath.Join(dir, "allold.jsonl")
	appendLines(t, p, old(1), old(2), old(3))
	if res := archiveAt(t, p, t0.Add(24*time.Hour)); res.Lines != 2 {
		t.Fatalf("the last line must stay live: %+v", res)
	}

	p = filepath.Join(dir, "limit.jsonl")
	appendLines(t, p, old(1), old(2), old(3), old(4))
	res, err := Archive(p, Options{Cutoff: t0.Add(24 * time.Hour), TimeField: "scheduled_at", Limit: int64(len(old(1)) * 2), Now: t0})
	if err != nil || res.Lines != 2 {
		t.Fatalf("limit: %+v %v", res, err)
	}
	res, err = Archive(p, Options{Cutoff: t0.Add(24 * time.Hour), TimeField: "scheduled_at", Limit: int64(len(old(1)) * 2), Now: t0})
	if err != nil || res.Skipped == "" {
		t.Fatalf("nothing past the limit may move: %+v %v", res, err)
	}

	p = filepath.Join(dir, "dry.jsonl")
	appendLines(t, p, old(1), old(2), old(3))
	before, _ := os.ReadFile(p)
	res, err = Archive(p, Options{Cutoff: t0.Add(24 * time.Hour), TimeField: "scheduled_at", Limit: -1, DryRun: true})
	after, _ := os.ReadFile(p)
	if err != nil || res.Lines != 2 || !bytes.Equal(before, after) {
		t.Fatalf("dry run: %+v %v", res, err)
	}
	if _, err := os.Stat(filepath.Join(ArchiveDir(p), IndexFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a dry run wrote an index")
	}
}

// Writers append while the archiver rotates, over and over: every line
// written is in the record exactly once, in each writer's own order, and
// the live file never loses a line to the swap.
func TestRotationWhileWritersAppend(t *testing.T) {
	skipUnsupported(t)
	path := filepath.Join(t.TempDir(), "measurements.jsonl")
	const writers, perWriter = 4, 3000
	var clock atomic.Int64 // seconds past t0: lines are dated as they are written
	// Rotations so far. Every writer waits for one more at each checkpoint,
	// so a fast machine cannot finish the writes before the archiver has
	// rotated a few times under them: the test used to fail on a quick CI
	// runner with "only 2 rotations", which proved nothing either way.
	var rot atomic.Int64
	const checkpoint = 750
	var wg sync.WaitGroup
	errs := make(chan error, writers+1)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// one Appender per writer: its own open file, as a process has
			a, err := OpenAppender(path)
			if err != nil {
				errs <- err
				return
			}
			defer a.Close()
			for n := 0; n < perWriter; n++ {
				if n > 0 && n%checkpoint == 0 {
					for until := time.Now().Add(10 * time.Second); rot.Load() < int64(n/checkpoint) && time.Now().Before(until); {
						time.Sleep(time.Millisecond)
					}
				}
				at := t0.Add(time.Duration(clock.Add(1)) * time.Second)
				if _, err := a.Write([]byte(lineAt(at, fmt.Sprint(w), n))); err != nil {
					errs <- err
					return
				}
				if n%50 == 0 {
					if err := a.Sync(); err != nil {
						errs <- err
						return
					}
				}
			}
		}(w)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			time.Sleep(2 * time.Millisecond)
			cut := t0.Add(time.Duration(clock.Load()-200) * time.Second)
			res, err := Archive(path, Options{Cutoff: cut, TimeField: "scheduled_at", Limit: -1, Now: cut})
			if err != nil {
				errs <- err
				return
			}
			if res.Skipped == "" {
				rot.Add(1)
			}
			if clock.Load() >= writers*perWriter {
				return
			}
		}
	}()
	wg.Wait()
	<-done
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	rotations := int(rot.Load())
	if rotations < 3 {
		t.Fatalf("only %d rotations happened; the test proves nothing", rotations)
	}
	last := map[string]int{}
	count := 0
	sc := bufio.NewScanner(bytes.NewReader(readAll(t, path)))
	for sc.Scan() {
		var w string
		var n int
		l := sc.Text()
		i := strings.Index(l, `"w":"`)
		if _, err := fmt.Sscanf(l[i:], `"w":%q,"n":%d}`, &w, &n); err != nil {
			t.Fatalf("line %q: %v", l, err)
		}
		if prev, ok := last[w]; ok && n != prev+1 {
			t.Fatalf("writer %s: line %d after %d (lost or doubled)", w, n, prev)
		} else if !ok && n != 0 {
			t.Fatalf("writer %s starts at %d", w, n)
		}
		last[w] = n
		count++
	}
	if count != writers*perWriter {
		t.Fatalf("%d lines in the record, want %d", count, writers*perWriter)
	}
	if n, err := Verify(path); err != nil || n != rotations {
		t.Fatalf("verify: %d segments (%d rotations) %v", n, rotations, err)
	}
	t.Logf("%d rotations under %d writers", rotations, writers)
}

// A run that stops after any step leaves a record that reads exactly as
// before, and the next run finishes the job and removes the leftovers.
func TestArchiveCrashBetweenSteps(t *testing.T) {
	skipUnsupported(t)
	for _, stop := range []string{"start", "segment", "tail", "index"} {
		t.Run(stop, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "measurements.jsonl")
			var want bytes.Buffer
			add := func(l string) { want.WriteString(l); appendLines(t, path, l) }
			for d := 0; d < 6; d++ {
				add(lineAt(t0.Add(time.Duration(d)*24*time.Hour), "a", d))
			}
			archiveAt(t, path, t0.Add(2*24*time.Hour)) // an earlier generation, so recovery has one to keep
			crash := errors.New("crash")
			_, err := Archive(path, Options{Cutoff: t0.Add(4 * 24 * time.Hour), TimeField: "scheduled_at", Limit: -1, Now: t0,
				hook: func(s string) error {
					if s == stop {
						return crash
					}
					return nil
				}})
			if !errors.Is(err, crash) {
				t.Fatalf("err = %v", err)
			}
			if stop == "index" {
				// the tail copy a real crash would leave behind
				os.WriteFile(path+".rotate.tmp", []byte("partial"), 0o644)
			}
			// readers and writers carry on across the half-done run
			if got := readAll(t, path); !bytes.Equal(got, want.Bytes()) {
				t.Fatalf("after a crash at %s the record reads %q", stop, got)
			}
			add(lineAt(t0.Add(6*24*time.Hour), "a", 6))
			if got := readAll(t, path); !bytes.Equal(got, want.Bytes()) {
				t.Fatalf("append after a crash at %s: %q", stop, got)
			}
			res := archiveAt(t, path, t0.Add(4*24*time.Hour))
			if res.Lines != 2 {
				t.Fatalf("recovery run: %+v", res)
			}
			if got := readAll(t, path); !bytes.Equal(got, want.Bytes()) {
				t.Fatalf("after recovery: %q", got)
			}
			if n, err := Verify(path); err != nil || n != 2 {
				t.Fatalf("verify: %d %v", n, err)
			}
			ents, _ := os.ReadDir(ArchiveDir(path))
			var names []string
			for _, e := range ents {
				names = append(names, e.Name())
			}
			if len(names) != 3 { // two segments and the index
				t.Fatalf("archive dir holds %v", names)
			}
			if _, err := os.Stat(path + ".rotate.tmp"); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("the tail copy was left behind")
			}
		})
	}
}

// A run that fails before its index is saved leaves no segment file behind:
// the segment keeps its temp name until then, and the run takes it back.
// Under its own name it would stay unlisted, since the next run's segment
// of another day has another name, and the backup would copy it.
func TestFailedArchiveLeavesNoSegment(t *testing.T) {
	skipUnsupported(t)
	path := filepath.Join(t.TempDir(), "measurements.jsonl")
	var want bytes.Buffer
	for d := 0; d < 6; d++ {
		l := lineAt(t0.Add(time.Duration(d)*24*time.Hour), "a", d)
		want.WriteString(l)
		appendLines(t, path, l)
	}
	archiveAt(t, path, t0.Add(2*24*time.Hour))
	for _, stop := range []string{"segment", "tail"} {
		_, err := Archive(path, Options{Cutoff: t0.Add(4 * 24 * time.Hour), TimeField: "scheduled_at", Limit: -1, Now: t0,
			hook: func(s string) error {
				if s == stop {
					return errors.New("failed")
				}
				return nil
			}})
		if err == nil {
			t.Fatalf("%s: the run did not fail", stop)
		}
		ents, _ := os.ReadDir(ArchiveDir(path))
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		if len(names) != 2 { // the first run's segment and the index
			t.Fatalf("after a run that failed at %s the archive dir holds %v", stop, names)
		}
	}
	// The next run, on another day, has one segment more and nothing else.
	archiveAt(t, path, t0.Add(5*24*time.Hour))
	ents, _ := os.ReadDir(ArchiveDir(path))
	if len(ents) != 3 {
		t.Fatalf("archive dir holds %d entries, want two segments and the index", len(ents))
	}
	if got := readAll(t, path); !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("the record reads %q", got)
	}
}

// A live file that ends inside a line at the swap is left as it is and the
// run says why in Skipped, not as an error, so the other files' runs and the
// retirement after them go on; its segment is not left behind. Once the
// line is whole (a copier's next pull, a writer's repair), the file is
// archived as usual.
func TestArchiveSkipsATornTail(t *testing.T) {
	skipUnsupported(t)
	path := filepath.Join(t.TempDir(), "reachability.jsonl")
	var want bytes.Buffer
	for d := 0; d < 4; d++ {
		l := lineAt(t0.Add(time.Duration(d)*24*time.Hour), "a", d)
		want.WriteString(l)
		appendLines(t, path, l)
	}
	torn := lineAt(t0.Add(4*24*time.Hour), "a", 4)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(torn[:10])
	f.Close()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Archive(path, Options{Cutoff: t0.Add(2 * 24 * time.Hour), TimeField: "scheduled_at", Limit: -1, Now: t0})
	if err != nil || !strings.HasPrefix(res.Skipped, "ends inside a line") || res.Cut != 0 || res.Segment != "" {
		t.Fatalf("a torn tail: %+v %v", res, err)
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(after, before) {
		t.Fatal("the live file changed")
	}
	if ents, _ := os.ReadDir(ArchiveDir(path)); len(ents) != 0 {
		t.Fatalf("the skipped run left %d file(s) in the archive dir", len(ents))
	}
	if idx, err := LoadIndex(path); err != nil || len(idx.Segments) != 0 {
		t.Fatalf("index: %+v %v", idx, err)
	}
	f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(torn[10:])
	f.Close()
	want.WriteString(torn)
	if res := archiveAt(t, path, t0.Add(2*24*time.Hour)); res.Lines != 2 || res.Skipped != "" {
		t.Fatalf("once the line is whole: %+v", res)
	}
	if got := readAll(t, path); !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("the record reads %q", got)
	}
}

// A run that waits for archive/.lock, which the nightly backup holds shared
// until it has recorded its copy, dates its segment from when it got the
// lock, not from when it began: a segment dated from the run's start would
// look to -retire as if the backup that finished while the run waited had
// copied it.
func TestArchiveDatesTheSegmentWhenItHasTheLock(t *testing.T) {
	skipUnsupported(t)
	path := filepath.Join(t.TempDir(), "measurements.jsonl")
	for d := 0; d < 4; d++ {
		appendLines(t, path, lineAt(t0.Add(time.Duration(d)*24*time.Hour), "a", d))
	}
	if err := os.MkdirAll(filepath.Join(filepath.Dir(path), Dir), 0o755); err != nil {
		t.Fatal(err)
	}
	backup, err := os.OpenFile(filepath.Join(filepath.Dir(path), Dir, LockFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	if err := lockShared(backup); err != nil {
		t.Fatal(err)
	}
	const held = 400 * time.Millisecond
	done := make(chan error, 1)
	go func() {
		_, err := Archive(path, Options{Cutoff: t0.Add(2 * 24 * time.Hour), TimeField: "scheduled_at", Limit: -1, Now: t0})
		done <- err
	}()
	time.Sleep(held)
	select {
	case err := <-done:
		t.Fatalf("the run did not wait for the backup's lock: %v", err)
	default:
	}
	if err := unlock(backup); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	idx, err := LoadIndex(path)
	if err != nil || len(idx.Segments) != 1 {
		t.Fatalf("index: %+v %v", idx, err)
	}
	// Half the hold: the run may have started a little after the lock was
	// taken, never after it was let go.
	if waited := idx.Segments[0].ArchivedAt.Sub(t0); waited < held/2 {
		t.Fatalf("the segment is dated %s after the run began, though it waited about %s for the lock", waited, held)
	}
}

// A live file replaced outside the archiver is not cut, and reads as a file
// of its own (base 0, not placed) rather than at an offset that is not its
// own.
func TestReplacedLiveFileIsRefused(t *testing.T) {
	skipUnsupported(t)
	path := filepath.Join(t.TempDir(), "measurements.jsonl")
	for d := 0; d < 4; d++ {
		appendLines(t, path, lineAt(t0.Add(time.Duration(d)*24*time.Hour), "a", d))
	}
	archiveAt(t, path, t0.Add(2*24*time.Hour))
	os.WriteFile(path, []byte(lineAt(t0, "other", 0)+lineAt(t0, "other", 1)), 0o644)
	if _, err := Archive(path, Options{Cutoff: t0.Add(3 * 24 * time.Hour), TimeField: "scheduled_at", Limit: -1}); err == nil {
		t.Fatal("a replaced file was cut")
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Base() != 0 || s.Placed() {
		t.Fatalf("base %d placed %v", s.Base(), s.Placed())
	}
	if _, err := Verify(path); err == nil {
		t.Fatal("verify passed a live file the index does not describe")
	}
}

// A damaged segment fails verification and a read that needs it.
func TestDamagedSegmentFails(t *testing.T) {
	skipUnsupported(t)
	path := filepath.Join(t.TempDir(), "measurements.jsonl")
	for d := 0; d < 4; d++ {
		appendLines(t, path, lineAt(t0.Add(time.Duration(d)*24*time.Hour), "a", d))
	}
	res := archiveAt(t, path, t0.Add(2*24*time.Hour))
	seg := filepath.Join(ArchiveDir(path), res.Segment)
	os.WriteFile(seg, []byte("not gzip"), 0o644)
	if _, err := Verify(path); err == nil {
		t.Fatal("verify passed a damaged segment")
	}
	if _, err := OpenAll(path); err == nil {
		t.Fatal("a read over a damaged segment succeeded")
	}
}

// RepairTail cuts a torn tail under the lock and leaves a whole file alone.
func TestRepairTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.jsonl")
	os.WriteFile(path, []byte(lineAt(t0, "a", 0)+`{"scheduled_at":"20`), 0o644)
	if n, err := RepairTail(path); err != nil || n != int64(len(`{"scheduled_at":"20`)) {
		t.Fatalf("cut %d %v", n, err)
	}
	if n, err := RepairTail(path); err != nil || n != 0 {
		t.Fatalf("second cut %d %v", n, err)
	}
	if n, err := RepairTail(filepath.Join(t.TempDir(), "none")); err != nil || n != 0 {
		t.Fatalf("missing file: %d %v", n, err)
	}
}

// A write that fails part-way, as one does when the disk fills inside a
// line, is cut back off: the file still ends on its last whole line, and
// the next line written is a line of its own, not glued onto a fragment.
func TestAFailedWriteLeavesNoFragment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reachability.jsonl")
	first := lineAt(t0, "a", 0)
	appendLines(t, path, first)
	full := errors.New("no space left on device")
	fileWrite = func(f *os.File, b []byte) (int, error) {
		n, _ := f.Write(b[:len(b)/2])
		return n, full
	}
	t.Cleanup(func() { fileWrite = (*os.File).Write })
	a, err := OpenAppender(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if n, err := a.Write([]byte(lineAt(t0.Add(time.Hour), "a", 1))); !errors.Is(err, full) || n != 0 {
		t.Fatalf("a write that failed part-way: %d %v", n, err)
	}
	if got, _ := os.ReadFile(path); string(got) != first {
		t.Fatalf("the file after a failed write: %q", got)
	}
	fileWrite = (*os.File).Write
	next := lineAt(t0.Add(2*time.Hour), "a", 2)
	if _, err := a.Write([]byte(next)); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(path); string(got) != first+next {
		t.Fatalf("the next line was not a line of its own: %q", got)
	}

	// A plain file of the collector's own, the same.
	own := filepath.Join(t.TempDir(), "amendments.jsonl")
	f, err := os.OpenFile(own, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := WriteWhole(f, []byte(first)); err != nil {
		t.Fatal(err)
	}
	fileWrite = func(f *os.File, b []byte) (int, error) {
		n, _ := f.Write(b[:3])
		return n, full
	}
	if n, err := WriteWhole(f, []byte(next)); !errors.Is(err, full) || n != 0 {
		t.Fatalf("WriteWhole failing part-way: %d %v", n, err)
	}
	if got, _ := os.ReadFile(own); string(got) != first {
		t.Fatalf("WriteWhole left %q", got)
	}
}

// A writer holding its file across a rotation follows it through the
// Appender; one that writes to a plain O_APPEND descriptor would write into
// the file the rotation replaced, which is why every writer of an archived
// file goes through Appender.
func TestHeldAppenderFollowsRotation(t *testing.T) {
	skipUnsupported(t)
	path := filepath.Join(t.TempDir(), "measurements.jsonl")
	for d := 0; d < 3; d++ {
		appendLines(t, path, lineAt(t0.Add(time.Duration(d)*24*time.Hour), "a", d))
	}
	held, err := OpenAppender(path)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	plain, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	archiveAt(t, path, t0.Add(2*24*time.Hour))
	if _, err := held.Write([]byte(lineAt(t0.Add(72*time.Hour), "held", 0))); err != nil {
		t.Fatal(err)
	}
	plain.WriteString(lineAt(t0.Add(72*time.Hour), "plain", 0))
	got := string(readAll(t, path))
	if !strings.Contains(got, `"w":"held"`) {
		t.Fatalf("the held Appender's line is not in the record:\n%s", got)
	}
	if strings.Contains(got, `"w":"plain"`) {
		t.Fatal("a plain descriptor's line reached the new file; the test no longer shows why the protocol is needed")
	}
}
