// observer-heartbeat runs the reachability probe: every interval it dials
// every bonded validator's registered Fibre endpoint (DNS, TCP, TLS 1.3, consensus-key identity)
// and stops there; no DownloadShard, about 3 KB per validator. It is what the
// network overview's "reachable now" and "TLS identity" columns are built
// from on days when a validator has no assignment to probe.
//
// Output: <data-dir>/reachability.jsonl, one probe.Measurement per line with
// an empty promise_hash and schedule_label "heartbeat". observer-collector
// ingests it into the reachability table.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
)

func main() {
	var (
		rpc      = flag.String("rpc", "http://127.0.0.1:26657", "CometBFT RPC endpoint")
		dataDir  = flag.String("data-dir", "./sentinel-data", "reachability.jsonl is written here")
		vantage  = flag.String("vantage", "local", "vantage name recorded on every measurement")
		interval = flag.Duration("interval", 5*time.Minute, "how often every registered endpoint is dialled")
		once     = flag.Bool("once", false, "one round, then exit")
		rpcTO    = flag.Duration("rpc-timeout", 15*time.Second, "per-RPC-call timeout")
		dnsTO    = flag.Duration("dns-timeout", 5*time.Second, "")
		tcpTO    = flag.Duration("tcp-timeout", 5*time.Second, "")
		tlsTO    = flag.Duration("tls-timeout", 10*time.Second, "")
		parallel = flag.Int("parallel", 16, "handshakes in flight at once; one TLS handshake per endpoint per round either way")
		logLines = flag.Int("log-ring", 300, "log lines kept in memory for the crash dump")
	)
	flag.Parse()

	log := scan.NewLogger(*logLines)
	chain, err := scan.NewChain(*rpc, *rpcTO, log)
	if err != nil {
		log.Fatalf("rpc client: %v", err)
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("mkdir: %v", err)
	}
	// Appended through record.Appender: observer-archive may rotate the file,
	// and the appender follows it to the new one without losing a line.
	out, err := record.OpenAppender(filepath.Join(*dataDir, "reachability.jsonl"))
	if err != nil {
		log.Fatalf("open output: %v", err)
	}
	defer out.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	timeouts := probe.StepTimeouts{DNS: *dnsTO, TCP: *tcpTO, TLS: *tlsTO}

	st := status.New(*dataDir, "heartbeat", *vantage, status.BuildRevision())
	cfg := map[string]any{}
	flag.VisitAll(func(f *flag.Flag) { cfg[f.Name] = f.Value.String() })
	st.RecordRuns(cfg)
	st.Start()
	defer st.Stop("exit")
	// A round every -interval, and two minutes for the round itself: a
	// round over every endpoint, a third of them timing out, takes about
	// that. /v1/health fails the heartbeat once its last completed round is
	// a few of these old.
	st.Set(status.CadenceKey, int((*interval+2*time.Minute)/time.Second))

	// A round the chain side refuses writes nothing to reachability.jsonl:
	// there is no validator to attribute a row to. The status file is where
	// the failure goes, dated, so the gap in the file has an explanation.
	inactiveLogged := false
	round := func() {
		chainID, tip, err := chain.Status(ctx)
		if err != nil {
			log.Printf("status: %v", err)
			st.Error(fmt.Sprintf("status: %v", err))
			return
		}
		provs, err := chain.BondedFibreProviders(ctx)
		if err != nil {
			if scan.IsModuleInactive(err) {
				// Before the chain runs app version 10 there is no
				// registry to dial: a round with nothing to do, not a
				// failure. The scanner treats the same answer the same
				// way, and the site says Fibre is not live; a "degraded"
				// banner over that would contradict it.
				if !inactiveLogged {
					log.Printf("x/valaddr is not active on this chain yet (%v); nothing to dial until the v10 upgrade, checking every round", err)
					inactiveLogged = true
				}
				st.OK()
				return
			}
			log.Printf("providers: %v", err)
			st.Error(fmt.Sprintf("providers: %v", err))
			return
		}
		inactiveLogged = false
		members, err := chain.ValidatorSet(ctx, tip)
		if err != nil {
			log.Printf("validator set h=%d: %v", tip, err)
			st.Error(fmt.Sprintf("validator set: %v", err))
			return
		}
		keyByHex := map[string]ed25519.PublicKey{}
		for _, m := range members {
			if len(m.PubKey) == ed25519.PublicKeySize {
				keyByHex[hex.EncodeToString(m.Address)] = ed25519.PublicKey(m.PubKey)
			}
		}
		sort.Slice(provs, func(i, j int) bool { return provs[i].ConsAddressBech32 < provs[j].ConsAddressBech32 })
		scheduled := time.Now().UTC().Truncate(time.Second)
		// Handshakes run a bounded number at a time. Done one after another,
		// a round over eighty endpoints with a third of them timing out took
		// longer than the interval, the ticker dropped ticks, and the "every
		// five minutes" on the dashboard was not true. Writes stay serialised
		// so a crash never leaves a half record for the next round to append
		// after.
		var (
			mu    sync.Mutex
			wg    sync.WaitGroup
			sem   = make(chan struct{}, *parallel)
			n, ok int
		)
		for _, pr := range provs {
			if ctx.Err() != nil {
				break
			}
			_, raw, err := bech32.DecodeAndConvert(pr.ConsAddressBech32)
			if err != nil || len(raw) != 20 {
				log.Printf("bad consensus address %q: %v", pr.ConsAddressBech32, err)
				continue
			}
			addrHex := hex.EncodeToString(raw)
			var a [20]byte
			copy(a[:], raw)
			in := probe.Input{
				Vantage: *vantage, ChainID: chainID,
				Target:        probe.Target{Address: a, AddressHex: addrHex, PubKey: keyByHex[addrHex], Host: pr.Host},
				SchedulePoint: probe.SchedulePoint{At: scheduled, Label: "heartbeat", Phase: probe.PhasePost},
				SkipDownload:  true,
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(in probe.Input, who, host string) {
				defer wg.Done()
				defer func() { <-sem }()
				m := probe.Run(ctx, in, nil, timeouts)
				m.ValidatorSetHeight = tip
				b, err := json.Marshal(m)
				if err != nil {
					log.Fatalf("marshal: %v", err)
				}
				mu.Lock()
				defer mu.Unlock()
				// one write + fsync per record, like the prober's store
				if _, err := out.Write(append(b, '\n')); err != nil {
					log.Fatalf("write: %v", err)
				}
				if err := out.Sync(); err != nil {
					log.Fatalf("sync: %v", err)
				}
				n++
				if m.Outcome == probe.OutcomeReachable {
					ok++
				}
				log.Printf("%s %s -> %s (%d ms)", who, host, m.Outcome, m.TotalDurationMS)
			}(in, pr.ConsAddressBech32[:20], pr.Host)
		}
		wg.Wait()

		log.Printf("round done: h=%d registered=%d probed=%d reachable=%d", tip, len(provs), n, ok)
		st.OK()
		st.Progress(tip)
		st.Set("registered", len(provs))
		st.Set("reachable", ok)
	}

	round()
	if *once {
		return
	}
	tick := time.NewTicker(*interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("stopped (signal)")
			st.Stop("signal")
			return
		case <-tick.C:
			round()
		}
	}
}

var _ = fmt.Sprintf
