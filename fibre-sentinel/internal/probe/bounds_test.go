package probe

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A validator's server writes the text of its own errors, and a row writes
// that text out twice, JSON-escaped at up to six bytes a character. The row
// keeps maxRecordedText of it, and no more than maxHeaderListSize of
// headers comes over the wire at all: one endpoint can neither fill the
// disk this host shares nor write a line the prober's restart cannot read.
func TestAServersErrorTextIsBounded(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	long := strings.Repeat("<", 40_000)
	host, _ := startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		return nil, status.Error(codes.Internal, long)
	}})
	m := Run(context.Background(), probeInput(host, consPub), mustCoder(t), StepTimeouts{})
	if m.Outcome != OutcomeServerError || !strings.Contains(m.RawError, "<<<<") {
		t.Fatalf("outcome %s (%.200q), want SERVER_ERROR with the server's text", m.Outcome, m.RawError)
	}
	if len(m.RawError) > maxRecordedText || len(m.Download.Error) > maxRecordedText {
		t.Fatalf("raw_error %d bytes, download.error %d: want at most %d", len(m.RawError), len(m.Download.Error), maxRecordedText)
	}
	// The row says how much the server sent, however often the text is cut
	// again: once more by Run, and with this observer's words put before it
	// (ownSide, sharedRow).
	const sent = "… (40000 bytes in all)"
	if !strings.HasSuffix(m.RawError, sent) || !strings.HasSuffix(m.Download.Error, sent) {
		t.Fatalf("raw_error ends %q, download.error %q: want %q", tail(m.RawError), tail(m.Download.Error), sent)
	}
	if again := clip(m.RawError); again != m.RawError {
		t.Fatalf("clipped again: %q", tail(again))
	}
	wrapped := clip("this observer's own network: why; on the wire: " + string(m.Outcome) + ": " + m.RawError)
	if len(wrapped) > maxRecordedText || !strings.HasSuffix(wrapped, sent) || !strings.HasPrefix(wrapped, "this observer's own network: why; on the wire: SERVER_ERROR: rpc error: ") {
		t.Fatalf("wrapped: %d bytes, %q … %q", len(wrapped), wrapped[:80], tail(wrapped))
	}
	if b, err := json.Marshal(m); err != nil || len(b) > 64<<10 {
		t.Fatalf("the row is %d bytes of JSON (%v)", len(b), err)
	}

	// Past the header bound the text does not come over at all: the server
	// cannot send it, and its answer is still the server's error.
	huge := strings.Repeat("x", 1<<20)
	host, _ = startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		return nil, status.Error(codes.Internal, huge)
	}})
	m = Run(context.Background(), probeInput(host, consPub), mustCoder(t), StepTimeouts{})
	if m.Outcome != OutcomeServerError {
		t.Fatalf("outcome %s (%.200q), want SERVER_ERROR", m.Outcome, m.RawError)
	}
	if strings.Contains(m.RawError, strings.Repeat("x", 100)) {
		t.Fatalf("a mebibyte of error text came over the wire: %.200q", m.RawError)
	}
}

// Under the client's rules the receive bound is what this validator's shard
// of this blob can weigh, never the protocol's 132 MiB. An answer past it
// may be a genuine shard of this blob held under another promise over the
// commitment, which the client takes: it is asked for again under the bound
// of the largest shard of this blob any validator can hold, when the byte
// budget has room for the difference, and judged on what then comes back.
// Without room it is this observer's gap. Only an answer past that bound
// too, which no shard of this blob can be, is the validator's
// (MALFORMED_SHARD). The protocol's bound still caps both.
func TestTheClientsRulesBoundAnAnswerByItsShard(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	var asked atomic.Int32
	host, _ := startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		asked.Add(1)
		return bigShard(3 << 20), nil
	}})
	in := probeInput(host, consPub)
	in.ClientRules, in.RequestTimeout = true, 10*time.Second
	in.MaxMessageSize, in.ExpectedShardBytes = 100_000_000, 50_000
	own := recvLimitFor(in)
	if own >= in.MaxMessageSize || own < minRecvMsgSize || own >= 3<<20 {
		t.Fatalf("receive bound %d, want this shard's, between %d and %d and under the answer", own, minRecvMsgSize, in.MaxMessageSize)
	}
	var widened []int64
	run := func(largest int64, room bool, widen bool) Measurement {
		asked.Store(0)
		widened = nil
		in := in
		in.LargestShardBytes = largest
		if widen {
			in.widen = func(extra int64) (func(), bool) {
				widened = append(widened, extra)
				return func() {}, room
			}
		}
		return Run(context.Background(), in, mustCoder(t), StepTimeouts{})
	}

	// Within the largest shard of this blob: asked again under its bound,
	// the budget charged the difference, the answer received and judged on
	// what it holds (here a shard no reader can parse).
	m := run(4<<20, true, true)
	in.LargestShardBytes = 4 << 20
	wider := widerRecvLimit(in)
	if asked.Load() != 2 || len(widened) != 1 || widened[0] != int64(wider-own) || m.Download.RecvLimit != wider {
		t.Fatalf("asked %d times, widened by %v, bound %d: want twice, by %d, under %d", asked.Load(), widened, m.Download.RecvLimit, wider-own, wider)
	}
	if m.Outcome != OutcomeMalformedShard || m.Download.RPCCode != "" || !strings.HasPrefix(m.RawError, "shard shape: ") {
		t.Fatalf("outcome=%s code=%s (%s), want the answer received and judged on its shape", m.Outcome, m.Download.RPCCode, m.RawError)
	}
	// A request outside the prober keeps no budget and asks again at once.
	if m := run(4<<20, false, false); asked.Load() != 2 || m.Download.RecvLimit != wider || m.Download.RPCCode != "" {
		t.Fatalf("without a budget: asked %d times, bound %d, code %s", asked.Load(), m.Download.RecvLimit, m.Download.RPCCode)
	}

	// No room in the budget: not asked again, and this observer's gap.
	m = run(4<<20, false, true)
	if asked.Load() != 1 || m.Download.RecvLimit != own || m.Outcome != OutcomeProbeError || m.Classification != ClassProbeError ||
		m.Download.RPCCode != "ResourceExhausted" || !strings.Contains(m.RawError, "no room in this observer's byte budget") {
		t.Fatalf("asked %d times, bound %d, %s / %s code=%s (%s): want this observer's gap", asked.Load(), m.Download.RecvLimit,
			m.Outcome, m.Classification, m.Download.RPCCode, m.RawError)
	}

	// Past the largest shard of this blob too: the validator's.
	m = run(2<<20, true, true)
	in.LargestShardBytes = 2 << 20
	if wider := widerRecvLimit(in); asked.Load() != 2 || m.Download.RecvLimit != wider || wider >= 3<<20 {
		t.Fatalf("asked %d times, bound %d (wider %d)", asked.Load(), m.Download.RecvLimit, wider)
	}
	if m.Outcome != OutcomeMalformedShard || m.Download.RPCCode != "ResourceExhausted" {
		t.Fatalf("outcome=%s code=%s (%s), want MALFORMED_SHARD refused at the wider bound", m.Outcome, m.Download.RPCCode, m.RawError)
	}

	in.LargestShardBytes = 0
	in.ExpectedShardBytes = 200_000_000
	if got := recvLimitFor(in); got != in.MaxMessageSize {
		t.Fatalf("bound %d for a shard past the protocol's %d", got, in.MaxMessageSize)
	}
	in.ExpectedShardBytes = 0
	if got := recvLimitFor(in); got != in.MaxMessageSize {
		t.Fatalf("bound %d with no expectation, want the protocol's %d", got, in.MaxMessageSize)
	}
}

// The byte budget is charged the most a request may receive, not what its
// shard should weigh: what the budget holds is what the requests in flight
// can bring in, whatever their servers send.
func TestTheByteBudgetIsChargedTheReceiveBound(t *testing.T) {
	f := newReadFixture(t, 4, 16, []fakeVal{{rows: rowsOf(0, 4), serve: fakeServes}})
	p := readProber(t, f)
	ms := readNow(t, p, f.pub)
	if len(ms) != 1 || ms[0].ObserverLoad == nil {
		t.Fatalf("%d rows, load %+v", len(ms), ms)
	}
	if got := ms[0].ObserverLoad.ShardBytesInFlight; got < int64(minRecvMsgSize) {
		t.Fatalf("the request was charged %d bytes, want its receive bound (at least %d), not its shard's %d", got, minRecvMsgSize,
			ShardBytes(f.pub.Promise.BlobSize, 4, 4))
	}
}

// tail is the end of s, where clip's mark is.
func tail(s string) string {
	if len(s) > 60 {
		return s[len(s)-60:]
	}
	return s
}
