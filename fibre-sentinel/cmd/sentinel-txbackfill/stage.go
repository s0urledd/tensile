package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/failedtx"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/txcost"
)

// progressFile, in the staging directory, is how far a staging got.
const progressFile = "progress.json"

// progress is progress.json: the staging's chain and ranges, the first
// height not staged yet, and how many bytes and lines each staged file held
// once every height before it was written. A run starts by cutting each
// file back to its mark, so lines written after the last save, of heights
// not counted yet, are written again rather than twice.
type progress struct {
	ChainID  string `json:"chain_id"`
	From     int64  `json:"from"`
	FailedTo int64  `json:"failed_to"`
	CostsTo  int64  `json:"costs_to"`
	Next     int64  `json:"next"`
	Failed   mark   `json:"failed_txs"`
	Costs    mark   `json:"tx_costs"`
	// Live counts the lines left out because the data directory's file
	// held their key when they were staged.
	Live      int64     `json:"left_out_live"`
	UpdatedAt time.Time `json:"updated_at"`
}

type mark struct {
	Bytes int64 `json:"bytes"`
	Lines int64 `json:"lines"`
}

// last is the last height the staging covers.
func (p *progress) last() int64 { return max(p.FailedTo, p.CostsTo) }

// complete reports a staging that has written every height it covers.
func (p *progress) complete() bool { return p.Next > p.last() }

func loadProgress(dir string) (*progress, error) {
	b, err := os.ReadFile(filepath.Join(dir, progressFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p progress
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(dir, progressFile), err)
	}
	return &p, nil
}

// saveProgress replaces progress.json whole (temp file, fsync, rename,
// directory fsync), so a crash leaves the old one or the new one.
func saveProgress(dir string, p *progress) error {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, progressFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// stateChainID is the chain the data directory's state.json names.
func stateChainID(dataDir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dataDir, "state.json"))
	if err != nil {
		return "", fmt.Errorf("the data directory's state: %w", err)
	}
	var st struct {
		ChainID string `json:"chain_id"`
	}
	if err := json.Unmarshal(b, &st); err != nil || st.ChainID == "" {
		return "", fmt.Errorf("%s names no chain id (%v)", filepath.Join(dataDir, "state.json"), err)
	}
	return st.ChainID, nil
}

// liveKeys is the dedupe key of every whole line of the record file at
// path. The file is only read: while the scanner runs it may be writing the
// last line, and a final line without its newline is not counted. A whole
// line that does not decode is an error, as it is for the scanner's open.
func liveKeys(path string) (map[string]bool, error) {
	keys := map[string]bool{}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return keys, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	for n := 1; ; n++ {
		line, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			return keys, nil // a final line without its newline is still being written
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if len(line) <= 1 {
			continue
		}
		var k struct {
			DedupeKey string `json:"dedupe_key"`
		}
		if err := json.Unmarshal(line, &k); err != nil || k.DedupeKey == "" {
			return nil, fmt.Errorf("%s line %d: no dedupe key (%v)", path, n, err)
		}
		keys[k.DedupeKey] = true
	}
}

// stageConfig is what a staging run is asked to do.
type stageConfig struct {
	From, FailedTo, CostsTo int64
	OutDir, DataDir         string
	Every                   int64
	Out                     io.Writer
}

func (c stageConfig) check() error {
	if c.From < 1 {
		return fmt.Errorf("-from %d: give the first height to stage", c.From)
	}
	if c.FailedTo < 0 || c.CostsTo < 0 || max(c.FailedTo, c.CostsTo) < c.From {
		return fmt.Errorf("-failed-to %d, -costs-to %d: at least one at or above -from %d (0 for none)", c.FailedTo, c.CostsTo, c.From)
	}
	if (c.FailedTo != 0 && c.FailedTo < c.From) || (c.CostsTo != 0 && c.CostsTo < c.From) {
		return fmt.Errorf("-failed-to %d, -costs-to %d: each 0 or at or above -from %d", c.FailedTo, c.CostsTo, c.From)
	}
	return nil
}

// stager writes one staging.
type stager struct {
	cfg                   stageConfig
	p                     *progress
	failedFile, costsFile *os.File
	liveFailed, liveCosts map[string]bool
}

// openStaging opens the staging in cfg.OutDir: a new one when the directory
// holds no progress.json (and none of the staged files), or the one it
// holds, which must be of the same chain and ranges. Each staged file is
// cut back to the mark progress.json gives it. The data directory's chain
// id and the keys its files hold are read here.
func openStaging(cfg stageConfig) (*stager, error) {
	chainID, err := stateChainID(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return nil, err
	}
	p, err := loadProgress(cfg.OutDir)
	if err != nil {
		return nil, err
	}
	failedPath, costsPath := filepath.Join(cfg.OutDir, failedtx.FileName), filepath.Join(cfg.OutDir, txcost.FileName)
	if p == nil {
		for _, path := range []string{failedPath, costsPath} {
			if fi, err := os.Stat(path); err == nil && fi.Size() > 0 {
				return nil, fmt.Errorf("%s holds %s but no %s: not a staging of this tool; use an empty directory", cfg.OutDir, filepath.Base(path), progressFile)
			}
		}
		p = &progress{ChainID: chainID, From: cfg.From, FailedTo: cfg.FailedTo, CostsTo: cfg.CostsTo, Next: cfg.From}
	} else if p.ChainID != chainID || p.From != cfg.From || p.FailedTo != cfg.FailedTo || p.CostsTo != cfg.CostsTo {
		return nil, fmt.Errorf("%s is a staging of %s from %d, failed transactions to %d, costs to %d; this run asks for %s from %d, %d, %d: give the same flags, or another -out-dir",
			cfg.OutDir, p.ChainID, p.From, p.FailedTo, p.CostsTo, chainID, cfg.From, cfg.FailedTo, cfg.CostsTo)
	}
	s := &stager{cfg: cfg, p: p}
	if s.failedFile, err = openAtMark(failedPath, p.Failed); err != nil {
		return nil, err
	}
	if s.costsFile, err = openAtMark(costsPath, p.Costs); err != nil {
		s.failedFile.Close()
		return nil, err
	}
	if s.liveFailed, err = liveKeys(filepath.Join(cfg.DataDir, failedtx.FileName)); err != nil {
		s.close()
		return nil, err
	}
	if s.liveCosts, err = liveKeys(filepath.Join(cfg.DataDir, txcost.FileName)); err != nil {
		s.close()
		return nil, err
	}
	if err := saveProgress(cfg.OutDir, p); err != nil {
		s.close()
		return nil, err
	}
	return s, nil
}

// openAtMark opens a staged file cut back to m.Bytes, positioned at its end.
// A file shorter than its mark lost lines progress.json counts: the staging
// cannot go on from it.
func openAtMark(path string, m mark) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err == nil && fi.Size() < m.Bytes {
		err = fmt.Errorf("%s holds %d bytes, fewer than the %d %s counts: start again in an empty -out-dir", path, fi.Size(), m.Bytes, progressFile)
	}
	if err == nil {
		err = f.Truncate(m.Bytes)
	}
	if err == nil {
		_, err = f.Seek(m.Bytes, io.SeekStart)
	}
	if err == nil {
		err = f.Sync()
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (s *stager) close() {
	if s.failedFile != nil {
		s.failedFile.Close()
	}
	if s.costsFile != nil {
		s.costsFile.Close()
	}
}

func (s *stager) complete() bool { return s.p.complete() }

func (s *stager) report(what string) {
	fmt.Fprintf(s.cfg.Out, "%s: %s heights %d..%d staged through %d; %d failed transaction line(s), %d cost line(s), %d left out as already live\n",
		what, s.p.ChainID, s.p.From, s.p.last(), s.p.Next-1, s.p.Failed.Lines, s.p.Costs.Lines, s.p.Live)
}

// heightLines are one height's staged lines (each without its newline), the
// decode problems met on the way and the number of lines left out as
// already live; or the error that stopped it.
type heightLines struct {
	height        int64
	failed, costs [][]byte
	problems      []string
	live          int64
	err           error
}

// stageHeight reads h and builds its lines with the scanner's functions,
// leaving out the keys the live files hold. A decode problem is printed
// when the height is written, as the scanner logs it, and never stops the
// run.
func (s *stager) stageHeight(ctx context.Context, f *fetcher, h int64) heightLines {
	out := heightLines{height: h}
	blk, res, err := f.read(ctx, h)
	if err != nil {
		out.err = err
		return out
	}
	if blk == nil {
		return out // the results say no line can come from the block
	}
	at := time.Now().UTC()
	if h <= s.p.FailedTo {
		recs, problems := scan.FailedTxRecords(blk, res, at, out.skip(s.liveFailed))
		for _, pr := range problems {
			out.problems = append(out.problems, fmt.Sprintf("h=%d tx=%d: failed tx: %v", h, pr.TxIndex, pr.Err))
		}
		for _, r := range recs {
			b, err := json.Marshal(r)
			if err != nil {
				out.err = fmt.Errorf("h=%d tx=%d: marshal failed tx: %w", h, r.TxIndex, err)
				return out
			}
			out.failed = append(out.failed, b)
		}
	}
	if h <= s.p.CostsTo {
		recs, problems := scan.TxCostRecords(blk, res, at, out.skip(s.liveCosts))
		for _, pr := range problems {
			out.problems = append(out.problems, fmt.Sprintf("h=%d tx=%d: tx cost: %v", h, pr.TxIndex, pr.Err))
		}
		for _, r := range recs {
			b, err := json.Marshal(r)
			if err != nil {
				out.err = fmt.Errorf("h=%d tx=%d: marshal tx cost: %w", h, r.TxIndex, err)
				return out
			}
			out.costs = append(out.costs, b)
		}
	}
	return out
}

// skip is the scanner functions' skip over the keys a live file holds,
// counting each it leaves out.
func (r *heightLines) skip(live map[string]bool) func(string) bool {
	return func(key string) bool {
		if live[key] {
			r.live++
			return true
		}
		return false
	}
}

// put appends one height's lines to the staged files, each file fsynced
// once its lines are in, and moves the marks; then prints the height's
// decode problems.
func (s *stager) put(r heightLines) error {
	if err := appendLines(s.failedFile, r.failed, &s.p.Failed); err != nil {
		return err
	}
	if err := appendLines(s.costsFile, r.costs, &s.p.Costs); err != nil {
		return err
	}
	for _, p := range r.problems {
		fmt.Fprintln(s.cfg.Out, p)
	}
	s.p.Live += r.live
	s.p.Next = r.height + 1
	return nil
}

func appendLines(f *os.File, lines [][]byte, m *mark) error {
	if len(lines) == 0 {
		return nil
	}
	var b []byte
	for _, l := range lines {
		b = append(append(b, l...), '\n')
	}
	if _, err := f.Write(b); err != nil {
		return fmt.Errorf("write %s: %w", f.Name(), err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", f.Name(), err)
	}
	m.Bytes += int64(len(b))
	m.Lines += int64(len(lines))
	return nil
}

// window is how many heights can be asked for ahead of the first one not
// written yet. The lines are written in height order, so the heights done
// out of turn wait here; a height that takes long holds the asking back
// rather than let the wait grow without bound.
const window = 4096

// saveEvery is how many heights progress.json moves by at a time.
const saveEvery = 1000

// run stages every height from progress.json's next to the last, with
// -concurrency workers per endpoint, writing in height order. It saves
// progress.json every saveEvery heights and when it stops, whatever stops
// it: the last height, a signal, or a height no endpoint would serve, which
// is returned.
func (s *stager) run(outer context.Context, f *fetcher) error {
	if s.p.complete() {
		s.report("staging complete, nothing to do")
		return nil
	}
	ctx, cancel := context.WithCancel(outer)
	defer cancel()
	first, last := s.p.Next, s.p.last()
	eps := len(f.archives)
	if f.local != nil {
		eps++
	}
	// Every endpoint's slots are taken by any worker, so as many workers as
	// slots keep every endpoint busy and none past its -concurrency.
	workers := f.conc * eps
	where := fmt.Sprintf("%d archive endpoint(s)", len(f.archives))
	if f.local != nil {
		where = fmt.Sprintf("the local node from %d to %d, %s for the rest", f.base, f.tip, where)
	}
	fmt.Fprintf(s.cfg.Out, "staging %s heights %d..%d (failed transactions to %d, costs to %d) into %s: %d height(s) from %d, %s, %d at once\n",
		s.p.ChainID, s.p.From, last, s.p.FailedTo, s.p.CostsTo, s.cfg.OutDir, last-first+1, first, where, workers)

	jobs := make(chan int64)
	results := make(chan heightLines, workers)
	ahead := make(chan struct{}, window)
	go func() {
		defer close(jobs)
		for h := first; h <= last; h++ {
			select {
			case ahead <- struct{}{}:
			case <-ctx.Done():
				return
			}
			select {
			case jobs <- h:
			case <-ctx.Done():
				return
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range jobs {
				r := s.stageHeight(ctx, f, h)
				select {
				case results <- r:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	started, done := time.Now(), int64(0)
	pending := map[int64]heightLines{}
	var stopErr error
	for r := range results {
		if stopErr != nil {
			continue // draining: the workers stop on the cancelled context
		}
		pending[r.height] = r
		for {
			next, ok := pending[s.p.Next]
			if !ok {
				break
			}
			delete(pending, s.p.Next)
			if next.err != nil {
				stopErr = fmt.Errorf("height %d: %w", next.height, next.err)
				cancel()
				break
			}
			if err := s.put(next); err != nil {
				stopErr = err
				cancel()
				break
			}
			<-ahead
			done++
			if s.p.Next%saveEvery == 0 {
				if err := s.save(); err != nil {
					stopErr = err
					cancel()
					break
				}
			}
			if s.cfg.Every > 0 && done%s.cfg.Every == 0 {
				s.progressLine(started, done, last)
			}
		}
	}
	if err := s.save(); err != nil && stopErr == nil {
		stopErr = err
	}
	switch {
	case stopErr == nil && s.p.complete():
		s.report("staging complete")
		return nil
	case outer.Err() != nil:
		// a signal: whatever the height in turn was, it was cut short
		stopErr = errors.New("stopped by a signal")
	case stopErr == nil:
		stopErr = errors.New("stopped before the last height")
	}
	s.report("stopped")
	return fmt.Errorf("%w; staged through %d: run the same command again to go on from %d", stopErr, s.p.Next-1, s.p.Next)
}

func (s *stager) save() error {
	s.p.UpdatedAt = time.Now().UTC()
	return saveProgress(s.cfg.OutDir, s.p)
}

func (s *stager) progressLine(started time.Time, done, last int64) {
	el := time.Since(started).Seconds()
	rate := float64(done) / max(el, 0.001)
	left := last - s.p.Next + 1
	total := last - s.p.From + 1
	eta := time.Duration(float64(left)/max(rate, 0.001)) * time.Second
	fmt.Fprintf(s.cfg.Out, "h=%d: %d of %d heights staged (%.1f%%), %.1f heights/s, %d failed transaction line(s), %d cost line(s), %d left out as already live, about %s left\n",
		s.p.Next-1, s.p.Next-s.p.From, total, 100*float64(s.p.Next-s.p.From)/float64(total), rate, s.p.Failed.Lines, s.p.Costs.Lines, s.p.Live, eta.Round(time.Minute))
}
