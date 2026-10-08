package probe

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// stalls is a connect under which the addresses stall names never answer
// (a blackholed address: the connect waits out its time, then fails as a
// real one does) and the others are dialled for real. calls, when set,
// counts the connects started.
func stalls(stall func(addr string) bool, calls *atomic.Int32) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		if calls != nil {
			calls.Add(1)
		}
		if stall(addr) {
			<-ctx.Done()
			err := error(context.Canceled)
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				err = os.ErrDeadlineExceeded // "i/o timeout", as the dialer says it
			}
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: err}
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

// A validator whose name lists a dead address first and a live one after
// it is read the way celestia-app's client reads it: the client's
// pick_first starts the next address 250 ms after the first, and connects.
// The request is not spent on the dead one, and the row says which address
// served and which was let go.
func TestADeadAddressIsPassedOverAsTheClientPassesIt(t *testing.T) {
	consPub, consPriv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	cert := fibreCert(t, consPriv, "test-chain", now.Add(-time.Hour), now.Add(24*time.Hour))
	live, _ := startFibre(t, cert, &fakeFibre{})
	_, port, _ := net.SplitHostPort(live)
	in := probeInput(net.JoinHostPort("fibre.example", port), consPub)
	in.SkipDownload = true
	in.ClientRules, in.RequestTimeout = true, 5*time.Second
	in.hooks = &netHooks{
		// the dead address first, as a resolver that keeps its order puts it
		lookup: func(context.Context, string) ([]string, error) { return []string{"192.0.2.1", "127.0.0.1"}, nil },
		dial:   stalls(func(addr string) bool { return strings.HasPrefix(addr, "192.0.2.1:") }, nil),
	}
	t0 := time.Now()
	m := Run(context.Background(), in, nil, StepTimeouts{})
	if m.Outcome != OutcomeReachable {
		t.Fatalf("outcome %s (%s), want the live address's answer", m.Outcome, m.RawError)
	}
	if el := time.Since(t0); el > 3*time.Second {
		t.Fatalf("the request took %s: the dead address held it", el)
	}
	if !strings.HasSuffix(m.TCP.Detail, "-> "+live) || !strings.Contains(m.TCP.Detail, "let go 192.0.2.1") {
		t.Fatalf("tcp detail %q", m.TCP.Detail)
	}
}

// The race starts the next address at once when the newest one fails, and
// after the stagger while it is still connecting; the first to connect wins
// and the one still connecting is let go.
func TestRaceDialMovesOnAsPickFirstDoes(t *testing.T) {
	var (
		mu      sync.Mutex
		started []string
		at      = map[string]time.Duration{}
		pipes   []net.Conn
	)
	t0 := time.Now()
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		mu.Lock()
		started = append(started, addr)
		at[addr] = time.Since(t0)
		mu.Unlock()
		switch addr {
		case "a:1":
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
		case "b:1":
			<-ctx.Done()
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: ctx.Err()}
		}
		c, s := net.Pipe()
		mu.Lock()
		pipes = append(pipes, c, s)
		mu.Unlock()
		return c, nil
	}
	conn, tried := raceDial(context.Background(), dial, []string{"a", "b", "c", "d"}, "1", 5*time.Second, 200*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for _, p := range pipes {
		p.Close()
	}
	if conn == nil {
		t.Fatal("nothing connected")
	}
	if fmt.Sprint(started) != "[a:1 b:1 c:1]" {
		t.Fatalf("started %v, want a, then b at once, then c; never d", started)
	}
	if at["b:1"] > 100*time.Millisecond || at["c:1"] < 150*time.Millisecond || at["c:1"] > time.Second {
		t.Fatalf("started b at %s and c at %s, want b at once and c a stagger later", at["b:1"], at["c:1"])
	}
	if len(tried) != 2 || tried[0].addr != "a" || tried[0].err == nil || tried[1].addr != "b" || tried[1].err != nil {
		t.Fatalf("tried %+v, want a failed and b let go", tried)
	}
}

// Outside the client's rules (the reachability heartbeat) a name that
// resolves to hundreds of silent addresses holds the request for its
// connect time once, not once per address, and only the first two of each
// IP version are tried: no heartbeat round waits hours on one validator's
// DNS, and no list of third parties gets a SYN each.
func TestAHeartbeatRequestTriesAFewAddressesWithinItsConnectTime(t *testing.T) {
	var addrs []string
	for i := 1; i <= 250; i++ {
		addrs = append(addrs, fmt.Sprintf("192.0.2.%d", i))
	}
	for i := 1; i <= 50; i++ {
		addrs = append(addrs, fmt.Sprintf("2001:db8::%x", i))
	}
	var calls atomic.Int32
	in := probeInput("many.example:7980", make(ed25519.PublicKey, ed25519.PublicKeySize))
	in.SkipDownload = true
	in.hooks = &netHooks{
		lookup: func(context.Context, string) ([]string, error) { return addrs, nil },
		dial:   stalls(func(string) bool { return true }, &calls),
	}
	t0 := time.Now()
	m := Run(context.Background(), in, nil, StepTimeouts{DNS: time.Second, TCP: 900 * time.Millisecond, TLS: time.Second})
	if el := time.Since(t0); el > 3*time.Second {
		t.Fatalf("the request took %s for a 900ms connect step", el)
	}
	if n := calls.Load(); n == 0 || n > 2*heartbeatAddrsPerFamily {
		t.Fatalf("%d connects started, want at most %d", n, 2*heartbeatAddrsPerFamily)
	}
	if m.Outcome != OutcomeTCPTimeout || !strings.Contains(m.TCP.Error, "more address(es) not tried") {
		t.Fatalf("outcome %s, tcp error %q", m.Outcome, m.TCP.Error)
	}
	if len(m.RawError) > maxRecordedText+64 || len(m.DNS.Detail) > maxRecordedText+64 {
		t.Fatalf("raw error %d bytes, dns detail %d", len(m.RawError), len(m.DNS.Detail))
	}
}

// Under the client's rules, a connect an ICMP unreachable from the path
// turned away is the validator's host not answering ("network is
// unreachable" while this machine has a route out to the address, "host is
// down", "protocol not available", "no route to host"), as the client
// meets it; ownSide then holds it to this observer's own side. "Network is
// unreachable" without a route out never left this machine and stays this
// observer's gap, as it does for the heartbeat.
func TestAnICMPUnreachableFromThePathIsTheHosts(t *testing.T) {
	refused := func(e syscall.Errno) error {
		return &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", e)}
	}
	for _, c := range []struct {
		name           string
		err            error
		routed, client bool
		want           Outcome
	}{
		{"net unreachable, a route out", refused(syscall.ENETUNREACH), true, true, OutcomeTCPUnreachable},
		{"net unreachable, no route out", refused(syscall.ENETUNREACH), false, true, OutcomeProbeError},
		{"host is down", refused(syscall.EHOSTDOWN), true, true, OutcomeTCPUnreachable},
		{"protocol unreachable", refused(syscall.ENOPROTOOPT), true, true, OutcomeTCPUnreachable},
		{"no route to host", refused(syscall.EHOSTUNREACH), true, true, OutcomeTCPUnreachable},
		{"the heartbeat keeps its rule", refused(syscall.ENETUNREACH), true, false, OutcomeProbeError},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := probeInput("192.0.2.9:7980", make(ed25519.PublicKey, ed25519.PublicKeySize))
			in.SkipDownload = true
			in.ClientRules = c.client
			if c.client {
				in.RequestTimeout = 2 * time.Second
			}
			in.hooks = &netHooks{
				dial:   func(context.Context, string) (net.Conn, error) { return nil, c.err },
				routed: func(string) bool { return c.routed },
			}
			m := Run(context.Background(), in, nil, StepTimeouts{TCP: time.Second})
			if m.Outcome != c.want {
				t.Fatalf("outcome %s (%s), want %s", m.Outcome, m.RawError, c.want)
			}
			if fams := fmt.Sprint(m.unreachFamilies); (c.want == OutcomeTCPUnreachable) != (fams == "[IPv4]") {
				t.Fatalf("unreachable over %s", fams)
			}
		})
	}
}

// The addresses a validator can register that lead nowhere on the public
// internet, or into private networks (shared address space: carrier-grade
// NAT and overlays such as Tailscale), are not dialled; a NAT64 address is
// judged by the IPv4 address in it.
func TestSpecialPurposeAddressesAreNotDialled(t *testing.T) {
	for addr, want := range map[string]bool{
		"1.1.1.1": true, "5.9.1.1": true, "100.63.255.255": true, "100.128.0.1": true, "2a01:4f8::1": true,
		"::ffff:5.9.1.1": true, "64:ff9b::505:101": true,
		"100.64.0.1": false, "100.100.1.1": false, "100.127.255.254": false,
		"10.0.0.1": false, "127.0.0.1": false, "169.254.1.1": false, "0.1.2.3": false, "0.0.0.0": false,
		"192.0.0.8": false, "192.0.2.1": false, "198.18.0.1": false, "198.19.255.1": false, "198.51.100.7": false,
		"203.0.113.9": false, "240.0.0.1": false, "255.255.255.255": false, "224.0.0.1": false,
		"64:ff9b::a00:1": false, "64:ff9b::6440:1": false, "64:ff9b:1::1": false, "100::1": false,
		"2001:2::1": false, "2001:10::1": false, "2001:db8::1": false, "3fff::1": false, "fec0::1": false,
		"::a00:1": false, "fc00::1": false, "fe80::1": false, "::1": false, "::": false, "ff02::1": false,
	} {
		if got := RoutableAddr(netip.MustParseAddr(addr)); got != want {
			t.Errorf("RoutableAddr(%s) = %v, want %v", addr, got, want)
		}
		if got := routableIP(net.ParseIP(addr)); got != want {
			t.Errorf("routableIP(%s) = %v, want %v", addr, got, want)
		}
	}
	if c := addrClass(net.ParseIP("100.64.0.1")); c != "shared (carrier-grade NAT)" {
		t.Errorf("class %q", c)
	}
}

// The dial order is each address once, IPv4 first, the families taking
// turns as pick_first interleaves them.
func TestOrderAddrsInterleavesTheFamilies(t *testing.T) {
	got := fmt.Sprint(orderAddrs([]string{"2001:db8::1", "10.0.0.1", "::1", "192.0.2.7", "10.0.0.1", "192.0.2.8"}))
	if want := "[10.0.0.1 2001:db8::1 192.0.2.7 ::1 192.0.2.8]"; got != want {
		t.Fatalf("orderAddrs = %s, want %s", got, want)
	}
	kept, left := firstPerFamily([]string{"10.0.0.1", "::1", "10.0.0.2", "::2", "10.0.0.3", "::3"}, 2)
	if fmt.Sprint(kept) != "[10.0.0.1 ::1 10.0.0.2 ::2]" || left != 2 {
		t.Fatalf("firstPerFamily = %v, %d left", kept, left)
	}
}
