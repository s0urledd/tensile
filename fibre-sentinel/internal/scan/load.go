package scan

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
)

// LoadPublications reads a publications.jsonl file (one Publication per line)
// written by the scanner. Blank lines are skipped. A malformed line is a hard
// error (a corrupt record must not be silently dropped), with one exception:
// a malformed FINAL line that has no trailing newline is a write in progress
// or a torn tail, and is ignored.
//
// The whole record is read, archived segments then the live file
// (record.OpenAll): once the file is rotated its older lines are no longer
// in the live file, and a reader of the live file alone would lose them. A
// file never archived reads exactly as it did.
func LoadPublications(path string) ([]Publication, error) {
	f, err := record.OpenAll(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	var out []Publication
	line := 0
	for {
		raw, rerr := r.ReadBytes('\n')
		if rerr != nil && !errors.Is(rerr, io.EOF) {
			return nil, fmt.Errorf("read %s: %w", path, rerr)
		}
		complete := len(raw) > 0 && raw[len(raw)-1] == '\n'
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) > 0 {
			line++
			var p Publication
			if err := json.Unmarshal(trimmed, &p); err != nil {
				if !complete {
					break // torn tail or a write in progress
				}
				return nil, fmt.Errorf("%s line %d: %w", path, line, err)
			}
			out = append(out, p)
		}
		if rerr != nil {
			break
		}
	}
	return out, nil
}

// LoadPayments reads a payments.jsonl file under the same rules as
// LoadPublications, the archived segments included.
func LoadPayments(path string) ([]Payment, error) {
	f, err := record.OpenAll(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	var out []Payment
	line := 0
	for {
		raw, rerr := r.ReadBytes('\n')
		if rerr != nil && !errors.Is(rerr, io.EOF) {
			return nil, fmt.Errorf("read %s: %w", path, rerr)
		}
		complete := len(raw) > 0 && raw[len(raw)-1] == '\n'
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) > 0 {
			line++
			var p Payment
			if err := json.Unmarshal(trimmed, &p); err != nil {
				if !complete {
					break
				}
				return nil, fmt.Errorf("%s line %d: %w", path, line, err)
			}
			out = append(out, p)
		}
		if rerr != nil {
			break
		}
	}
	return out, nil
}
