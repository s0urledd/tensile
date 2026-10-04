package scan

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Block headers pushed by the node.
//
// In follow mode the scanner used to learn of a new block only by asking the
// node for its tip, every -poll (a second on the observer). A block the node
// had just committed therefore waited up to that second before the scanner
// read it, and that wait was most of the time between a block and its blob on
// the site. The node can say so itself: CometBFT publishes a NewBlockHeader
// event on its RPC websocket the moment a block is committed and its results
// are stored, which is the moment block and block_results can be read.
//
// heads keeps one subscription to those events and turns each into a wake-up
// of the follow loop, nothing more. The event names a height, but the loop
// does not read that height: it asks /status for the tip at once, as a poll
// would have, and reads from its own cursor up to that tip in order, as it
// always has. So an event can make a read happen sooner and never makes one
// happen twice, out of order, or past what the node says it holds: an
// announcement of a height already read is dropped, a burst of announcements
// is one wake-up holding the newest, and a stale one after a reconnect names
// a height below the cursor.
//
// Headers only. A NewBlock event carries the whole block, and a mainnet
// block is megabytes, which the node would push whether the scanner wanted
// it or not; a header is about a kilobyte and says all this needs.
//
// The tip poll stays, as the safety net. While the subscription is up the
// loop still asks for the tip every -subscribed-poll (five seconds), so an
// event the node dropped costs at most that; while it is down the loop polls
// every -poll, exactly as before the subscription existed. A subscription is
// down when it cannot be made (no websocket on the RPC address, a proxy that
// does not pass upgrades, the node's subscriber limit), when the connection
// drops (a node restart), or when the node cancels it (CometBFT cancels a
// subscriber that does not read fast enough, and says so in an error
// message). It is made again with backoff, 1, 2, 4, 8, 16, then every 30
// seconds, for as long as the scanner runs; nothing about it is ever fatal.
// The log says when it comes up, when it goes down and why, and when it is
// back, one line each, not one per attempt.
//
// One failure says nothing on its own: a connection that stays open and
// answers pings while the node has stopped sending events (it gives up on a
// subscriber whose messages it could not hand over without telling it). That
// shows on the tip: two heights or more past the last one announced, and no
// event for headsQuietAfter, is a subscription that is not delivering, and
// it is dropped and made again.

// headsQuery is the one subscription the scanner makes.
const headsQuery = "tm.event='NewBlockHeader'"

// headsReadWait is how long the connection may be silent before it is taken
// for dead. CometBFT pings every 27 s whether or not there is a block, so a
// minute is two pings missed.
const headsReadWait = time.Minute

// headsReadLimit bounds one message. A header event is about a kilobyte;
// anything near this is not one.
const headsReadLimit = 1 << 20

// headsSteady is how long a subscription has to stay up for its loss to start
// the backoff over: one that drops every few seconds is retried on the
// growing backoff instead of once a second.
const headsSteady = time.Minute

// headsQuietAfter is how long without an event, while the tip has moved two
// heights past the last one announced, before the subscription is taken for
// one that has stopped delivering. A var so tests can shorten it.
var headsQuietAfter = 30 * time.Second

// headsBackoff is the wait before the next try after the attempt-th failure
// in a row: rpcBackoff's 1, 2, 4, 8, 16, 30, 30, ... seconds. A var so tests
// can shorten it.
var headsBackoff = rpcBackoff

// heads is the follow loop's subscription to the node's new block headers.
type heads struct {
	url     string
	header  http.Header
	timeout time.Duration
	// poll and safety are the follow loop's tip poll intervals while the
	// subscription is down (-poll) and while it is up (-subscribed-poll),
	// named in the log lines.
	poll, safety time.Duration
	log          *Logger

	// wake holds at most one announcement the follow loop has not taken: the
	// newest height announced, or 0, which asks for a tip poll at once (the
	// subscription just went down, or an event named no height it could
	// read). notify merges a new one into a pending one, so a burst of
	// events while the scanner reads is one wake-up.
	wake chan int64

	up        atomic.Bool
	announced atomic.Int64 // the highest height announced since the scanner started
	lastEvent atomic.Int64 // unix nanoseconds of the last event, or of the subscription's start

	mu   sync.Mutex
	conn *websocket.Conn // the live connection, for drop to close
	why  string          // the reason drop closed it, for the log
}

// newHeads prepares a subscription to the node at rpcURL. It does not dial.
// An address with no websocket counterpart (a unix socket) is an error, and
// the caller follows by polling alone.
func newHeads(rpcURL string, timeout, poll, safety time.Duration, log *Logger) (*heads, error) {
	u, hdr, err := headsURL(rpcURL)
	if err != nil {
		return nil, err
	}
	return &heads{url: u, header: hdr, timeout: timeout, poll: poll, safety: safety, log: log, wake: make(chan int64, 1)}, nil
}

// headsURL is the websocket address of the node's RPC: the same host and
// port, ws for http and wss for https, at /websocket under the RPC address's
// own path (a node behind a proxy at https://host/celestia is at
// wss://host/celestia/websocket). A user and password in the address go in an
// Authorization header, as CometBFT's own client sends them; the websocket
// library refuses them in the address.
func headsURL(rpcURL string) (string, http.Header, error) {
	u, err := url.Parse(rpcURL)
	if err != nil {
		return "", nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "tcp", "ws":
		u.Scheme = "ws"
	case "https", "wss":
		u.Scheme = "wss"
	default:
		return "", nil, fmt.Errorf("no websocket for the RPC address %q", rpcURL)
	}
	if u.Host == "" {
		return "", nil, fmt.Errorf("no host in the RPC address %q", rpcURL)
	}
	hdr := http.Header{}
	if u.User != nil {
		pw, _ := u.User.Password()
		hdr.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(u.User.Username()+":"+pw)))
		u.User = nil
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/websocket"
	u.RawPath, u.RawQuery, u.Fragment = "", "", ""
	return u.String(), hdr, nil
}

// Up reports whether the subscription is live: the node acknowledged it and
// the connection has not dropped since.
func (h *heads) Up() bool { return h.up.Load() }

// Announced is the highest height an event has named since the scanner
// started, 0 before the first.
func (h *heads) Announced() int64 { return h.announced.Load() }

// notify leaves height for the follow loop, merged with one it has not taken
// yet: the higher of the two, or 0 if either is (a poll at once covers both).
// heads' own goroutine is the only sender, so once the pending one is taken
// out the send cannot find the channel full.
func (h *heads) notify(height int64) {
	select {
	case h.wake <- height:
		return
	default:
	}
	select {
	case old := <-h.wake:
		if old == 0 || height == 0 {
			height = 0
		} else if old > height {
			height = old
		}
	default:
	}
	select {
	case h.wake <- height:
	default:
	}
}

// check is the follow loop's report of the tip it just polled. A
// subscription that is up, has delivered nothing for headsQuietAfter, and is
// two heights or more behind that tip is not delivering: it is dropped, and
// run makes it again.
func (h *heads) check(tip int64) {
	if !h.Up() {
		return
	}
	quiet := time.Since(time.Unix(0, h.lastEvent.Load()))
	if quiet < headsQuietAfter || tip < h.Announced()+2 {
		return
	}
	h.drop(fmt.Sprintf("no block header announced for %s while the tip moved to %d", quiet.Round(time.Second), tip))
}

// drop closes the live connection, if there is one, so run makes the
// subscription again; why is what the log says it was lost to.
func (h *heads) drop(why string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn == nil {
		return
	}
	h.why = why
	_ = h.conn.Close()
}

// run keeps the subscription up until ctx ends: it subscribes, reads events
// until the connection drops or the node cancels, and subscribes again after
// a backoff. Every time it goes down it asks the follow loop for a tip poll
// at once, so the loop does not sit out a safety interval before it falls
// back to -poll.
func (h *heads) run(ctx context.Context) {
	state := "" // "" before the first outcome, then "up" or "down"
	var downSince time.Time
	failures := 0
	for {
		started := time.Now()
		var upAt time.Time
		err := h.session(ctx, func() {
			upAt = time.Now()
			switch state {
			case "":
				h.log.Printf("block subscription up: each block is read as soon as the node announces its header (%s at %s); the tip is still polled every %s behind it", headsQuery, h.url, h.safety)
			case "down":
				h.log.Printf("block subscription up again after %s", time.Since(downSince).Round(time.Second))
			}
			state = "up"
		})
		h.up.Store(false)
		if ctx.Err() != nil {
			return
		}
		h.mu.Lock()
		if h.why != "" {
			err = errors.New(h.why)
			h.why = ""
		}
		h.mu.Unlock()
		h.notify(0)
		switch state {
		case "":
			h.log.Printf("WARNING: block subscription unavailable (%v): following by polling the tip every %s, and trying again with backoff", err, h.poll)
			downSince = started
		case "up":
			h.log.Printf("WARNING: block subscription lost (%v): following by polling the tip every %s until it is back", err, h.poll)
			downSince = time.Now()
			if time.Since(upAt) >= headsSteady {
				failures = 0
			}
		}
		state = "down"
		wait := headsBackoff(failures)
		failures++
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// session is one subscription: dial, subscribe, and read until the connection
// fails. onUp is called once, when the node acknowledges the subscription.
func (h *heads) session(ctx context.Context, onUp func()) error {
	d := websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: h.timeout}
	conn, resp, err := d.DialContext(ctx, h.url, h.header)
	if err != nil {
		if resp != nil {
			err = fmt.Errorf("%w (HTTP %s)", err, resp.Status)
		}
		return err
	}
	defer conn.Close()
	h.mu.Lock()
	h.conn, h.why = conn, ""
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.conn = nil
		h.mu.Unlock()
	}()
	// A read blocks until a message or the read deadline; the scanner
	// stopping closes the connection under it.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	conn.SetReadLimit(headsReadLimit)
	// The node's pings keep the connection open from its side (it drops a
	// client silent for 30 s), and are the sign of life on this one between
	// blocks: each pushes the read deadline on.
	conn.SetPingHandler(func(data string) error {
		_ = conn.SetReadDeadline(time.Now().Add(headsReadWait))
		err := conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(h.timeout))
		var ne net.Error
		if err == websocket.ErrCloseSent || errors.As(err, &ne) && ne.Timeout() {
			return nil
		}
		return err
	})
	_ = conn.SetWriteDeadline(time.Now().Add(h.timeout))
	req := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "subscribe", "params": map[string]string{"query": headsQuery}}
	if err := conn.WriteJSON(req); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	_ = conn.SetWriteDeadline(time.Time{})
	// The acknowledgement is due within one RPC timeout.
	_ = conn.SetReadDeadline(time.Now().Add(h.timeout))
	acked := false
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if !acked {
				return fmt.Errorf("subscribe: %w", err)
			}
			return err
		}
		var m headsMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			return fmt.Errorf("unreadable message from the node: %w", err)
		}
		if m.Error != nil {
			// a refused subscription (the node's subscriber limits), or one
			// the node cancelled: either way this connection carries no more
			// events
			return fmt.Errorf("the node: %s", m.Error)
		}
		if m.Result == nil {
			continue
		}
		_ = conn.SetReadDeadline(time.Now().Add(headsReadWait))
		h.lastEvent.Store(time.Now().UnixNano())
		if !acked {
			acked = true
			h.up.Store(true)
			onUp()
		}
		if m.Result.Data.Type == "" {
			continue // the acknowledgement itself: an empty result
		}
		height, err := m.Result.Data.Value.Header.Height.Int64()
		if err != nil || height <= 0 {
			// a header whose height this cannot read still says a block
			// arrived: the follow loop polls the tip
			h.notify(0)
			continue
		}
		for {
			cur := h.announced.Load()
			if height <= cur || h.announced.CompareAndSwap(cur, height) {
				break
			}
		}
		h.notify(height)
	}
}

// headsMessage is a JSON-RPC message on the subscription: the acknowledgement
// (an empty result), an event, or an error.
type headsMessage struct {
	Result *struct {
		Data struct {
			Type  string `json:"type"`
			Value struct {
				Header struct {
					// a string in CometBFT's JSON ("123"), which
					// json.Number reads as readily as a bare number
					Height json.Number `json:"height"`
				} `json:"header"`
			} `json:"value"`
		} `json:"data"`
	} `json:"result"`
	Error *headsError `json:"error"`
}

type headsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data"`
}

func (e *headsError) String() string {
	if e.Data != "" {
		return fmt.Sprintf("%s: %s", e.Message, e.Data)
	}
	return e.Message
}
