package recordcheck

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// A paced check stops only where it holds nothing: once before each member of the tarball (and before the end of
// it), and once every pauseEvery lines of a member it checks against the store. It finds what a check that never
// stops finds, and a pause that fails ends it with the pause's error.
func TestDayPausesBetweenMembersAndLines(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	data := t.TempDir()
	writeFiles(t, data, map[string][]byte{
		"publications.jsonl": file(f.pubLine),
		"measurements.jsonl": file(f.readings...),
		"reachability.jsonl": file(f.reachLines...),
	})
	dir, e := exportDay(t, data, time.Date(2026, 10, 6, 4, 0, 0, 0, time.UTC))
	plain, err := CheckDay(ctx, f.st, dir, e)
	if err != nil || !plain.Reproducible {
		t.Fatalf("%+v %v", plain, err)
	}
	paced := func(every int64, pause Pause) (DayReport, error) {
		t.Helper()
		old := pauseEvery
		pauseEvery = every
		defer func() { pauseEvery = old }()
		return CheckDayPaced(ctx, f.st, dir, e, pause)
	}
	count := func(every int64) int {
		t.Helper()
		n := 0
		r, err := paced(every, func(context.Context) error { n++; return nil })
		if err != nil || !reflect.DeepEqual(r, plain) {
			t.Fatalf("paced every %d lines: %+v %v, want %+v", every, r, err, plain)
		}
		return n
	}

	// One pause per tar header read: each member's, then the end of the archive.
	headers := 0
	tf, err := os.Open(filepath.Join(dir, e.Name))
	if err != nil {
		t.Fatal(err)
	}
	defer tf.Close()
	z, err := gzip.NewReader(tf)
	if err != nil {
		t.Fatal(err)
	}
	for tr := tar.NewReader(z); ; headers++ {
		if _, err := tr.Next(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if got := count(1 << 62); got != headers+1 {
		t.Fatalf("%d pause(s) between members, want %d (%d members, then the end)", got, headers+1, headers)
	}
	// And one per two lines of each checked member.
	lines := 0
	for _, m := range e.Files {
		if Checked(m.Name) {
			lines += int(m.Lines / 2)
		}
	}
	if lines < 4 {
		t.Fatalf("the fixture's members are too short to pause in: %+v", e.Files)
	}
	if got := count(2); got != headers+1+lines {
		t.Fatalf("%d pause(s) every two lines, want %d", got, headers+1+lines)
	}

	stop := errors.New("stop")
	n := 0
	_, err = paced(2, func(context.Context) error {
		if n++; n == headers+2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("a failing pause: %v", err)
	}
}
