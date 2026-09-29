// Command sentinel-measure-check asserts that the readings in a
// measurements.jsonl held to the reading rule, including a fault-injection
// run where one fibre server was killed before the blobs were read.
// Test/CI helper.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
)

func main() {
	var (
		dataDir    = flag.String("data-dir", "./sentinel-data", "dir holding measurements.jsonl")
		killedHost = flag.String("killed-host", "", "host:port of the fibre server that was killed before the readings")
		killAtStr  = flag.String("kill-at", "", "RFC3339 time the kill happened (the killed validator's rows started after it must not be served)")
		minProbes  = flag.Int("min-probes", 3, "minimum total rows expected")
		k          = flag.Int("k", 4096, "distinct rows that reconstruct a blob (original_rows)")
	)
	flag.Parse()

	ms, err := probe.LoadMeasurements(filepath.Join(*dataDir, "measurements.jsonl"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "measure-check FATAL: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("measure-check| loaded %d rows\n", len(ms))
	if len(ms) < *minProbes {
		fmt.Fprintf(os.Stderr, "measure-check FATAL: only %d rows, want >= %d\n", len(ms), *minProbes)
		os.Exit(1)
	}
	var killAt time.Time
	if *killAtStr != "" {
		if killAt, err = time.Parse(time.RFC3339, *killAtStr); err != nil {
			fmt.Fprintf(os.Stderr, "measure-check FATAL: bad -kill-at: %v\n", err)
			os.Exit(2)
		}
	}

	fails := 0
	fail := func(format string, a ...any) {
		fmt.Printf("FAIL: "+format+"\n", a...)
		fails++
	}

	byClass := map[probe.Classification]int{}
	readings := map[string][]probe.Measurement{}
	killedAddr := ""
	for _, m := range ms {
		byClass[m.Classification]++
		readings[m.PromiseHash] = append(readings[m.PromiseHash], m)
		if *killedHost != "" && m.ValidatorHost == *killedHost {
			killedAddr = m.ValidatorAddress
		}
	}
	if *killedHost != "" && killedAddr == "" {
		fmt.Printf("measure-check| WARN: no row references killed host %s (the readings did not need it)\n", *killedHost)
	}

	available, killedSeen := 0, false
	hashes := make([]string, 0, len(readings))
	for h := range readings {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	for _, h := range hashes {
		rs := readings[h]
		distinct := map[uint32]bool{}
		result := ""
		for _, m := range rs {
			if m.ScheduleLabel != probe.EndReadLabel || m.Phase != probe.PhaseInWindow {
				fail("%s %s: label %q phase %s, want the one in-window reading", short(h), short(m.ValidatorAddress), m.ScheduleLabel, m.Phase)
			}
			// A validator that did not endorse is asked like the rest, as
			// the client asks the whole set; it serves as UNATTESTED.
			endorsing := m.Attested || m.AttestationUnknown
			if m.Read == nil {
				fail("%s %s: no read record on the row", short(h), short(m.ValidatorAddress))
				continue
			}
			if result == "" {
				result = m.Read.BlobResult
			} else if result != m.Read.BlobResult {
				fail("%s: rows disagree on the reading's result (%s, %s)", short(h), result, m.Read.BlobResult)
			}
			isKilled := killedAddr != "" && m.ValidatorAddress == killedAddr
			switch {
			case isKilled && (killAt.IsZero() || m.StartedAt.After(killAt)):
				killedSeen = true
				if m.Download.CommitmentVerified {
					fail("%s killed %s served rows after the kill", short(h), short(m.ValidatorAddress))
				}
				if m.Classification == probe.ClassFault {
					fail("%s killed %s: FAULT, want an unreachable endpoint (%s)", short(h), short(m.ValidatorAddress), m.Outcome)
				}
			case !isKilled && endorsing && m.Classification != probe.ClassHealthy:
				fail("%s live %s: class=%s outcome=%s (want HEALTHY)", short(h), short(m.ValidatorAddress), m.Classification, m.Outcome)
			case !isKilled && !endorsing && (m.Classification != probe.ClassUnattested || !m.Download.CommitmentVerified):
				fail("%s live %s, not endorsing: class=%s outcome=%s (want its rows, UNATTESTED)", short(h), short(m.ValidatorAddress), m.Classification, m.Outcome)
			}
			if m.Download.CommitmentVerified {
				for _, i := range m.Download.RowIndices {
					distinct[i] = true
				}
			}
		}
		switch result {
		case probe.ReadAvailable:
			available++
			if len(distinct) < *k {
				fail("%s: available with %d distinct rows, want >= %d", short(h), len(distinct), *k)
			}
		default:
			fail("%s: reading came to %q, want available (the live validators hold enough rows)", short(h), result)
		}
		fmt.Printf("measure-check| %s: %s, %d distinct rows from %d validators asked\n", short(h), result, len(distinct), len(rs))
	}

	fmt.Println("measure-check| by classification:")
	for c, n := range byClass {
		fmt.Printf("measure-check|   %-24s %d\n", c, n)
	}
	if available == 0 {
		fail("no available reading")
	}
	if killedAddr != "" && !killedSeen {
		fmt.Println("measure-check| WARN: the killed validator was asked only before the kill")
	}
	fmt.Printf("measure-check| %d checks failed\n", fails)
	if fails > 0 {
		os.Exit(1)
	}
	fmt.Println("measure-check| PASS: every blob read once, available from the live validators; the killed one counted neither way")
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
