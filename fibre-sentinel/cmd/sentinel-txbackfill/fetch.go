package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// endpoint is one RPC node: the scanner's client for it, and the slots
// that keep it to -concurrency requests at once.
type endpoint struct {
	url   string
	chain *scan.Chain
	slots chan struct{}
}

func newEndpoint(url string, timeout time.Duration, conc int) (*endpoint, error) {
	c, err := scan.NewChain(url, timeout, scan.NewLogger(100))
	if err != nil {
		return nil, err
	}
	return &endpoint{url: url, chain: c, slots: make(chan struct{}, conc)}, nil
}

// attempts is how many times one request is made, across the endpoints a
// height is asked of, before the staging stops on it: twice round four
// archives. A height that fails them all stops the run, which keeps what it
// staged; the same command run again starts from that height.
const attempts = 8

// fetcher asks for the heights: the local node for those it held at the
// start, the archives for the rest, in turn.
type fetcher struct {
	local *endpoint // nil without -local-rpc
	// base and tip are the local node's oldest and newest block at the
	// start. Its oldest block moves up as it prunes, so a height near base
	// may be gone by the time it is asked: that ask fails and goes to an
	// archive, like any failed one.
	base, tip int64
	archives  []*endpoint
	// conc is every endpoint's number of slots (-concurrency).
	conc int
	next atomic.Uint64
	// sleep waits out a backoff; the tests make it instant.
	sleep func(ctx context.Context, d time.Duration) error
}

// dialFetcher opens the endpoints and holds each to chainID, the chain the
// data directory's state.json names: a node of another chain answers every
// height with the wrong block. The local node's oldest and newest block are
// read here, once.
func dialFetcher(ctx context.Context, chainID, localURL string, archiveURLs []string, conc int, timeout time.Duration) (*fetcher, error) {
	if conc < 1 {
		return nil, fmt.Errorf("-concurrency %d: at least 1", conc)
	}
	f := &fetcher{conc: conc, sleep: sleepCtx}
	check := func(ep *endpoint) error {
		id, _, err := ep.chain.Status(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", ep.url, err)
		}
		if id != chainID {
			return fmt.Errorf("%s serves chain %q, the data directory's is %q", ep.url, id, chainID)
		}
		return nil
	}
	if localURL != "" {
		ep, err := newEndpoint(localURL, timeout, conc)
		if err != nil {
			return nil, err
		}
		if err := check(ep); err != nil {
			return nil, fmt.Errorf("-local-rpc %w (an empty -local-rpc asks the archives for every height)", err)
		}
		if f.base, f.tip, err = ep.chain.History(ctx); err != nil {
			return nil, fmt.Errorf("-local-rpc %s: %w", localURL, err)
		}
		f.local = ep
	}
	for _, u := range archiveURLs {
		ep, err := newEndpoint(u, timeout, conc)
		if err != nil {
			return nil, err
		}
		if err := check(ep); err != nil {
			return nil, fmt.Errorf("-rpc %w", err)
		}
		f.archives = append(f.archives, ep)
	}
	if f.local == nil && len(f.archives) == 0 {
		return nil, errors.New("no RPC endpoint: give -rpc, -local-rpc or both")
	}
	return f, nil
}

// localHolds reports a height the local node held at the start.
func (f *fetcher) localHolds(h int64) bool {
	return f.local != nil && h >= f.base && h <= f.tip
}

// candidates are the endpoints h is asked of, in the order the attempts
// take them: the local node first when it held h at the start, then the
// archives from the next one in turn. A height below the local node's
// oldest block is asked of the archives alone.
func (f *fetcher) candidates(h int64) []*endpoint {
	var out []*endpoint
	if f.localHolds(h) {
		out = append(out, f.local)
	}
	if n := len(f.archives); n > 0 {
		start := int(f.next.Add(1) % uint64(n))
		for i := 0; i < n; i++ {
			out = append(out, f.archives[(start+i)%n])
		}
	}
	return out
}

// do makes one request for height h through fn, on the candidates in turn,
// each holding one of its endpoint's slots while it asks. A failure waits
// out a backoff (backoff) and asks the next candidate; after attempts
// failures the last error is returned. A cancelled ctx returns at once.
func (f *fetcher) do(ctx context.Context, h int64, what string, fn func(context.Context, *endpoint) error) error {
	cands := f.candidates(h)
	if len(cands) == 0 {
		return fmt.Errorf("%s %d: no endpoint holds it (below the local node's oldest block %d, and no -rpc)", what, h, f.base)
	}
	var last error
	for a := 0; a < attempts; a++ {
		ep := cands[a%len(cands)]
		select {
		case ep.slots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		err := fn(ctx, ep)
		<-ep.slots
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		last = fmt.Errorf("%s: %w", ep.url, err)
		if a+1 < attempts {
			if err := f.sleep(ctx, backoff(a, err)); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("%s %d: %d attempts failed, the last: %w", what, h, attempts, last)
}

// backoff is the wait after failed attempt a (0-based): a second, doubling
// to half a minute; at least 10 s after a 429, so a node that rations its
// callers is given room. The client reports an HTTP answer that is not JSON
// with its status ("Status: 429 Too Many Requests"), which is how a 429 or
// a 5xx from a proxy in front of a node arrives.
func backoff(a int, err error) time.Duration {
	d := 30 * time.Second
	if a < 5 {
		d = time.Second << a
	}
	if strings.Contains(err.Error(), "429") && d < 10*time.Second {
		d = 10 * time.Second
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// read is one height's block results, and its block when the results say a
// line can come from it (scan.ResultsNeedBlock): blk is nil when they say
// none can. Each answer is held to the height asked, and the block to its
// results' tx count, as processBlock holds them; a node that answers
// otherwise is asked again, on the next endpoint.
func (f *fetcher) read(ctx context.Context, h int64) (*scan.Block, *scan.BlockResults, error) {
	var res *scan.BlockResults
	if err := f.do(ctx, h, "block_results", func(ctx context.Context, ep *endpoint) error {
		r, err := ep.chain.BlockResults(ctx, h)
		if err != nil {
			return err
		}
		if r.Height != h {
			return fmt.Errorf("block_results %d: the answer is of height %d", h, r.Height)
		}
		res = r
		return nil
	}); err != nil {
		return nil, nil, err
	}
	if !scan.ResultsNeedBlock(res) {
		return nil, res, nil
	}
	var blk *scan.Block
	if err := f.do(ctx, h, "block", func(ctx context.Context, ep *endpoint) error {
		b, err := ep.chain.Block(ctx, h)
		if err != nil {
			return err
		}
		if b.Height != h {
			return fmt.Errorf("block %d: the answer is of height %d", h, b.Height)
		}
		if len(b.Txs) != len(res.TxCodes) {
			return fmt.Errorf("block %d: %d txs but %d results", h, len(b.Txs), len(res.TxCodes))
		}
		blk = b
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return blk, res, nil
}
