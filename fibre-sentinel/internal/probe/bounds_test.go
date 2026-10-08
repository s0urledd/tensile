package probe

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
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
	if limit := maxRecordedText + 64; len(m.RawError) > limit || len(m.Download.Error) > limit {
		t.Fatalf("raw_error %d bytes, download.error %d: want at most %d", len(m.RawError), len(m.Download.Error), limit)
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
// of this blob can weigh, never the protocol's 132 MiB: an answer past it,
// which no honest server sends (a padded one), is refused here, and is the
// validator's (MALFORMED_SHARD). The protocol's bound still caps it.
func TestTheClientsRulesBoundAnAnswerByItsShard(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	host, _ := startFibre(t, cert, &fakeFibre{download: func(context.Context, *fibretypes.DownloadShardRequest) (*fibretypes.DownloadShardResponse, error) {
		return bigShard(3 << 20), nil
	}})
	in := probeInput(host, consPub)
	in.ClientRules, in.RequestTimeout = true, 10*time.Second
	in.MaxMessageSize, in.ExpectedShardBytes = 100_000_000, 50_000
	m := Run(context.Background(), in, mustCoder(t), StepTimeouts{})
	if m.Download.RecvLimit >= in.MaxMessageSize || m.Download.RecvLimit < minRecvMsgSize {
		t.Fatalf("receive bound %d, want this shard's, between %d and %d", m.Download.RecvLimit, minRecvMsgSize, in.MaxMessageSize)
	}
	if m.Outcome != OutcomeMalformedShard || m.Download.RPCCode != "ResourceExhausted" {
		t.Fatalf("outcome=%s code=%s (%s), want MALFORMED_SHARD refused at the bound", m.Outcome, m.Download.RPCCode, m.RawError)
	}
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
