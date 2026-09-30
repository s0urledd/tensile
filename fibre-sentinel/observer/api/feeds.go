package api

// Atom feeds: GET /v1/validators/{addr}/feed.atom for one validator's
// endpoint history, and GET /v1/feed.atom for the network's registrations
// and first not-served readings. Nothing is stored for them; every entry is
// derived from rows the store already holds (observer/feed has the rules),
// and every entry ID is built from what happened and when, so a feed reader
// polling the URL never sees the same change twice.
//
// What goes in, per validator:
//
//   - the chain's own record of Fibre host registrations and changes
//     (host_events, source "event": a set_fibre_provider_info transaction);
//   - joining and leaving the bonded provider list, as this observer's
//     endpoint poll saw it (endpoints). There is no unregister message on
//     chain, so "left the list" is jailing, unbonding or the like, and the
//     entry says the registration itself remains;
//   - the first heartbeat that completed a TLS handshake;
//   - becoming unreachable after feedConfirmBeats consecutive failed
//     heartbeats, and recovering; the certificate stopping being endorsed
//     by the validator's key (expired, or not its key) and being put right;
//   - the first reading on record that counts as not served (its rows did
//     not come back, and the blob was Unavailable).
//
// And for the network: every registration and host change, bonded-list
// joins and departures after the observer's first poll, and each
// validator's first not-served reading. Per-validator reachability
// transitions are not in the network feed: across a hundred validators they
// are the per-validator feeds' job, and walking every heartbeat of a month
// per request is not.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/feed"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// feedConfirmBeats is how many consecutive heartbeats must agree before a
// reachability or identity change is published: three at the five-minute
// heartbeat, a quarter of an hour. One failed handshake from one vantage is
// a statement about a path as much as about a server.
const feedConfirmBeats = 3

// feedTagDate is the date part of every tag: URI (RFC 4151). It must never
// change: it is part of every entry's identity.
const feedTagDate = "2026"

// feedTTL is how long a rendered feed is reused before it is rebuilt. Feed
// readers poll every few minutes to hours; the events are minutes-grained by
// construction. Past it the feed is still served as it stands while its
// replacement is built behind it (serveFeed), so only the first reader after
// a start waits for a build, about two seconds for the network feed on the
// observer's store in September 2026.
const feedTTL = 5 * time.Minute

// feedBuildTimeout bounds a rebuild in the background, which has no request
// whose context could end it.
const feedBuildTimeout = 2 * time.Minute

type cachedFeed struct {
	body     []byte
	etag     string
	modified time.Time
	at       time.Time
}

// feedCache holds rendered feeds per server, per URL and per public host
// (the host is in every entry ID). Package-level rather than a Server
// field so this feature adds nothing to the Server struct; keyed by the
// server's address so two servers in one process (tests) never share.
var feedCache = struct {
	sync.Mutex
	m map[string]cachedFeed
	// refreshing marks the keys being rebuilt in the background, so a burst
	// of readers of a stale feed starts one rebuild, not one each.
	refreshing map[string]bool
}{m: map[string]cachedFeed{}, refreshing: map[string]bool{}}

const feedCacheMax = 2048

// feedBuilder builds one feed; status is http.StatusOK or the status to
// answer with instead (a validator the observer has not seen).
type feedBuilder func(ctx context.Context, authority string, now time.Time) (*feed.Feed, int, error)

func (s *Server) serveFeed(w http.ResponseWriter, r *http.Request, build feedBuilder) {
	authority := feedAuthority(r)
	key := fmt.Sprintf("%p|%s|%s", s, authority, r.URL.Path)
	now := time.Now()
	feedCache.Lock()
	c, ok := feedCache.m[key]
	start := ok && now.Sub(c.at) > feedTTL && !feedCache.refreshing[key]
	if start {
		feedCache.refreshing[key] = true
	}
	feedCache.Unlock()
	switch {
	case !ok:
		// Nothing to serve: this reader waits for the build, as only the
		// first reader of each feed after a start does.
		var status int
		var err error
		c, status, err = renderFeed(r.Context(), build, authority, now)
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		if status != http.StatusOK {
			writeErr(w, status, validatorNotSeen)
			return
		}
		storeFeed(key, c)
	case start:
		// Stale: served as it stands, rebuilt behind it. A feed a few
		// minutes past its TTL says nothing false (every entry is dated, and
		// so is the feed, by its newest entry); it can lack an entry from
		// those minutes, which the next poll brings, as it would have
		// brought one from the minutes after a fresh build. The reader who
		// found it stale used to wait for the two-second build.
		s.refreshFeed(key, build, authority)
	}
	h := w.Header()
	h.Set("Content-Type", "application/atom+xml; charset=utf-8")
	h.Set("Cache-Control", "public, max-age=300")
	h.Set("ETag", c.etag)
	if !c.modified.IsZero() {
		h.Set("Last-Modified", c.modified.UTC().Format(http.TimeFormat))
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && etagMatch(inm, c.etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(c.body)
	}
}

// renderFeed builds and renders one feed as the cache holds it.
func renderFeed(ctx context.Context, build feedBuilder, authority string, now time.Time) (cachedFeed, int, error) {
	f, status, err := build(ctx, authority, now)
	if err != nil || status != http.StatusOK {
		return cachedFeed{}, status, err
	}
	body, err := f.Render()
	if err != nil {
		return cachedFeed{}, 0, err
	}
	sum := sha256.Sum256(body)
	c := cachedFeed{body: body, etag: `"` + hex.EncodeToString(sum[:12]) + `"`, at: now, modified: f.Updated}
	for _, e := range f.Entries {
		if e.At.After(c.modified) {
			c.modified = e.At
		}
	}
	return c, http.StatusOK, nil
}

func storeFeed(key string, c cachedFeed) {
	feedCache.Lock()
	defer feedCache.Unlock()
	if len(feedCache.m) >= feedCacheMax {
		feedCache.m = map[string]cachedFeed{} // crude, bounded; a refill costs one query set per feed
	}
	feedCache.m[key] = c
}

// refreshFeed rebuilds key in the background; the caller has marked it
// refreshing. A failed rebuild, a panicking one included (rebuildFeed),
// keeps the feed being served and is logged, and the next reader past the
// TTL tries again. A feed that now answers otherwise than 200 (the validator
// is no longer on record) is dropped, so the next reader gets that answer
// rather than the old feed.
// feedRebuilds bounds the background rebuilds. They run off the request, so
// the site server's caps on requests in flight no longer bound them, and a
// feed kept under many spellings of one path could start a rebuild each. A
// few at a time is plenty for feeds that change in minutes; a stale feed
// whose turn has not come is served as it stands until the next reader past
// the TTL asks again.
var feedRebuilds = make(chan struct{}, 2)

func (s *Server) refreshFeed(key string, build feedBuilder, authority string) {
	select {
	case feedRebuilds <- struct{}{}:
	default:
		feedCache.Lock()
		delete(feedCache.refreshing, key)
		feedCache.Unlock()
		return
	}
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		defer func() { <-feedRebuilds }()
		ctx, cancel := context.WithTimeout(context.Background(), feedBuildTimeout)
		defer cancel()
		c, status, err := rebuildFeed(ctx, build, authority, time.Now())
		switch {
		case err != nil:
			if s.log != nil {
				s.log.Printf("api: feed refresh %s: %v", key, err)
			}
		case status != http.StatusOK:
			feedCache.Lock()
			delete(feedCache.m, key)
			feedCache.Unlock()
		default:
			storeFeed(key, c)
		}
		feedCache.Lock()
		delete(feedCache.refreshing, key)
		feedCache.Unlock()
	}()
}

// rebuildFeed is renderFeed for a rebuild in the background. On a request's
// goroutine net/http recovers a panic and fails only that request; on a
// goroutine of its own a panic would end the whole API and leave the feed
// marked refreshing. It is a failed rebuild instead: logged, and the feed
// served as it stands until the next reader past the TTL tries again.
func rebuildFeed(ctx context.Context, build feedBuilder, authority string, now time.Time) (c cachedFeed, status int, err error) {
	defer func() {
		if p := recover(); p != nil {
			c, status, err = cachedFeed{}, 0, fmt.Errorf("panic: %v\n%s", p, debug.Stack())
		}
	}()
	return renderFeed(ctx, build, authority, now)
}

func etagMatch(header, etag string) bool {
	for _, t := range strings.Split(header, ",") {
		t = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(t), "W/"))
		if t == etag || t == "*" {
			return true
		}
	}
	return false
}

// feedAuthority is the tag: URI authority: the public host name the feed
// was requested at (the site's proxy passes Host through), without a port.
// Feed readers subscribe at one URL, so from any one reader's point of view
// it never changes; and it is a name the operator of this deployment
// controls, which is what RFC 4151 asks of a tag authority.
//
// A deployment should fix it with $TENSILE_PUBLIC_HOST: taken from the
// request, any client could send a different X-Forwarded-Host each time, and
// since the authority is part of the cache key every such request missed the
// cache and rebuilt the feed (a full pass over the probes), and minted entry
// IDs under a name the operator does not control.
const feedAuthorityEnv = "TENSILE_PUBLIC_HOST"

func feedAuthority(r *http.Request) string {
	if v := strings.ToLower(strings.TrimSpace(os.Getenv(feedAuthorityEnv))); v != "" {
		return v
	}
	h := r.Host
	if fh := r.Header.Get("X-Forwarded-Host"); fh != "" {
		h = strings.TrimSpace(strings.Split(fh, ",")[0])
	}
	if hh, _, err := net.SplitHostPort(h); err == nil {
		h = hh
	}
	h = strings.ToLower(strings.Trim(h, "[]"))
	if h == "" {
		h = "localhost"
	}
	return h
}

func (s *Server) handleValidatorFeed(w http.ResponseWriter, r *http.Request) {
	addr, err := s.resolveAddr(r.Context(), r.PathValue("addr"))
	if err != nil {
		s.writeAddrErr(w, r.URL.Path, err)
		return
	}
	s.serveFeed(w, r, func(ctx context.Context, authority string, now time.Time) (*feed.Feed, int, error) {
		return s.validatorFeed(ctx, addr, authority, now)
	})
}

func (s *Server) handleNetworkFeed(w http.ResponseWriter, r *http.Request) {
	s.serveFeed(w, r, func(ctx context.Context, authority string, now time.Time) (*feed.Feed, int, error) {
		f, err := s.networkFeed(ctx, authority, now)
		return f, http.StatusOK, err
	})
}

// feedMeta is what both feeds need to name things.
type feedMeta struct {
	chainID string
	// firstPoll is the observer's first endpoint poll: every endpoint row
	// opened then was already listed before anyone was watching, and saying
	// it "joined" then would be inventing an event.
	firstPoll string
}

func (s *Server) readFeedMeta(ctx context.Context) (feedMeta, error) {
	var m feedMeta
	db := s.st.DB()
	if err := db.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM meta WHERE key = 'chain_id'), '')`).Scan(&m.chainID); err != nil {
		return m, err
	}
	if m.chainID == "" {
		m.chainID = "chain"
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MIN(first_seen_at), '') FROM endpoints`).Scan(&m.firstPoll); err != nil {
		return m, err
	}
	return m, nil
}

func parseTS(s string) time.Time {
	t, err := time.Parse(store.TimeLayout, s)
	if err != nil {
		t, _ = time.Parse(time.RFC3339Nano, s)
	}
	return t.UTC()
}

// idTime is the time component of an entry ID: whole seconds, UTC, fixed.
func idTime(t time.Time) string { return t.UTC().Format("20060102T150405Z") }

// validatorLink is the site's page for a validator, by its operator address
// (celestiavaloper1…) when the staking set names one, as every link on the
// site is, else by the consensus address the rows carry. The page takes
// either. Only the link: an entry's ID keeps the consensus address it was
// minted with, so a reader never sees an entry twice.
func validatorLink(hexAddr, operator string) string {
	if operator != "" {
		return "/validator/?addr=" + operator
	}
	return "/validator/?addr=" + hexAddr
}

// validatorFeed builds one validator's feed. status is 404 when nothing
// about the address is on record at all.
func (s *Server) validatorFeed(ctx context.Context, addr, authority string, now time.Time) (*feed.Feed, int, error) {
	db := s.st.DB()
	bech, _ := consBech(addr)
	var moniker, operator string
	var known int
	if err := db.QueryRowContext(ctx, `SELECT
			COALESCE((SELECT moniker FROM validator_identities WHERE cons_address = ?), ''),
			COALESCE((SELECT operator_address FROM validator_identities WHERE cons_address = ?), ''),
			(SELECT COUNT(*) FROM validator_identities WHERE cons_address = ?)
			+ (SELECT COUNT(*) FROM endpoints WHERE validator_cons_address = ?)
			+ (SELECT COUNT(*) FROM (SELECT 1 FROM reachability WHERE validator_address = ? LIMIT 1))
			+ (SELECT COUNT(*) FROM host_events WHERE cons_address = ?)`,
		addr, addr, addr, bech, addr, addr).Scan(&moniker, &operator, &known); err != nil {
		return nil, 0, err
	}
	if known == 0 {
		return nil, http.StatusNotFound, nil
	}
	fm, err := s.readFeedMeta(ctx)
	if err != nil {
		return nil, 0, err
	}
	name := moniker
	if name == "" {
		name = addr[:12] + "…"
	}
	id := func(parts ...string) string {
		return feed.TagID(authority, feedTagDate, append([]string{"tensile", fm.chainID, addr}, parts...)...)
	}
	link := validatorLink(addr, operator)
	ops := map[string]string{addr: operator}
	var es []feed.Entry

	// Registrations and host changes, from the chain's own events.
	regs, err := s.registrationEntries(ctx, addr, fm, authority, map[string]string{addr: name}, ops)
	if err != nil {
		return nil, 0, err
	}
	es = append(es, regs...)

	// Bonded provider list, as this observer's poll saw it.
	bl, err := s.bondedListEntries(ctx, bech, addr, name, fm, authority, false, ops)
	if err != nil {
		return nil, 0, err
	}
	es = append(es, bl...)

	// First completed handshake on record.
	var first string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MIN(started_at), '') FROM reachability
		WHERE validator_address = ? AND tcp_ok = 1 AND tls_ok = 1 AND outcome <> 'PROBE_ERROR' AND +vantage = ?`, addr, s.vantage).Scan(&first); err != nil {
		return nil, 0, err
	}
	if first != "" {
		at := parseTS(first)
		// No time in the ID: there is only ever one first. (Past the raw
		// retention the earliest row moves, but that is ninety days back,
		// long outside the feed's thirty.)
		es = append(es, feed.Entry{ID: id("first-reachable"), Kind: "first-reachable", At: at, Link: link,
			Title:   name + ": Fibre endpoint reachable for the first time",
			Summary: "The first heartbeat on record that completed a TLS handshake with this validator's registered Fibre host."})
	}

	// Reachability and identity transitions over the feed's span, with a
	// day before it as the baseline so the first change in the span is a
	// change and not the starting state.
	// This observer's own heartbeats, as the entries say: another vantage's
	// rows interleaved here would read as flapping.
	rows, err := db.QueryContext(ctx, `SELECT started_at, validator_host, tcp_ok, tls_ok, identity_ok, identity_reason
		FROM reachability WHERE validator_address = ? AND started_at >= ? AND outcome <> 'PROBE_ERROR' AND +vantage = ?
		ORDER BY started_at`, addr, store.TS(now.Add(-feed.MaxAge-24*time.Hour)), s.vantage)
	if err != nil {
		return nil, 0, err
	}
	var beats []feed.Beat
	for rows.Next() {
		var at, host, reason string
		var tcp, tls, idok int
		if err := rows.Scan(&at, &host, &tcp, &tls, &idok, &reason); err != nil {
			rows.Close()
			return nil, 0, err
		}
		st := reachState{tcpOK: tcp == 1, tlsOK: tls == 1, identityOK: idok == 1, identityReason: reason}
		st.reachable = st.tcpOK && st.tlsOK
		idw := identityStatus(&st)
		if idw != "verified" && idw != "expired" && idw != "mismatch" {
			idw = "" // not judged
		}
		beats = append(beats, feed.Beat{At: parseTS(at), Host: host, Reachable: st.reachable, Identity: idw})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	for _, ev := range feed.Transitions(beats, feedConfirmBeats) {
		e := feed.Entry{Kind: ev.Kind, At: ev.At, Link: link, ID: id(ev.Kind, idTime(ev.At))}
		switch ev.Kind {
		case feed.KindUnreachable:
			e.Title = name + ": Fibre endpoint unreachable"
			e.Summary = fmt.Sprintf("%s did not complete a TLS handshake on %d consecutive heartbeats starting %s. "+
				"An endpoint check, not a reading of any shard.", ev.Host, ev.Beats, ev.At.Format(time.RFC3339))
		case feed.KindRecovered:
			e.Title = name + ": Fibre endpoint reachable again"
			e.Summary = fmt.Sprintf("%s completed TLS handshakes again from %s", ev.Host, ev.At.Format(time.RFC3339))
			if !ev.Since.IsZero() {
				e.Summary += fmt.Sprintf(", after %s unreachable", feed.Span(ev.At.Sub(ev.Since)))
			}
			e.Summary += "."
		case feed.KindIdentityProblem:
			if ev.Identity == "expired" {
				e.Title = name + ": Fibre certificate endorsement expired"
				e.Summary = fmt.Sprintf("%s answered with a certificate whose endorsement by the validator's consensus key has lapsed, from %s. A client will refuse it.", ev.Host, ev.At.Format(time.RFC3339))
			} else {
				e.Title = name + ": Fibre certificate not endorsed by this validator's key"
				e.Summary = fmt.Sprintf("%s answered with a certificate this validator's consensus key does not endorse, from %s. A client will refuse it.", ev.Host, ev.At.Format(time.RFC3339))
			}
		case feed.KindIdentityRestored:
			e.Title = name + ": Fibre certificate endorsed again"
			e.Summary = fmt.Sprintf("%s presented a certificate endorsed by the validator's consensus key again from %s", ev.Host, ev.At.Format(time.RFC3339))
			if !ev.Since.IsZero() {
				e.Summary += fmt.Sprintf(", after %s", feed.Span(ev.At.Sub(ev.Since)))
			}
			e.Summary += "."
		}
		es = append(es, e)
	}

	// The first FAULT on record.
	if ffs, err := s.firstFaults(ctx, addr, now); err != nil {
		return nil, 0, err
	} else if fe, ok := ffs[addr]; ok {
		fe.ID, fe.Link = id("first-fault"), "/blob/?hash="+fe.Link
		fe.Title = name + ": " + fe.Title
		es = append(es, fe)
	}

	f := &feed.Feed{
		ID:       feed.TagID(authority, feedTagDate, "tensile", fm.chainID, addr),
		Title:    "Tensile · " + name + " · Fibre endpoint events",
		Subtitle: "State changes of this validator's Fibre endpoint on " + fm.chainID + ", as Tensile observed them. Newest 50 of the last 30 days.",
		SelfHref: "feed.atom",
		AltHref:  link,
		Author:   "Tensile observer (" + s.vantage + ")",
		Entries:  feed.Bound(es, now),
	}
	f.Updated = feedUpdated(f.Entries, now)
	return f, http.StatusOK, nil
}

// feedUpdated is the feed-level updated time: the newest entry's, or, for a
// feed with none, the start of the current day, so an empty feed's bytes
// (and ETag) change once a day rather than on every render.
func feedUpdated(es []feed.Entry, now time.Time) time.Time {
	if len(es) > 0 {
		return es[0].At
	}
	return now.UTC().Truncate(24 * time.Hour)
}

// registrationEntries turns the chain's host_events into "registered" and
// "changed host" entries. Only rows the scanner read from a transaction
// (source "event") are events; seed rows set the previous host without an
// entry, because a seed is the registry as it already stood, not a change.
// only limits to one validator (hex); empty means all. ops holds each
// validator's operator address for its link (operatorAddrs).
func (s *Server) registrationEntries(ctx context.Context, only string, fm feedMeta, authority string, names, ops map[string]string) ([]feed.Entry, error) {
	q := `SELECT cons_address, host, source, time, from_height, from_tx_index FROM host_events`
	var args []any
	if only != "" {
		q += ` WHERE cons_address = ?`
		args = append(args, only)
	}
	q += ` ORDER BY cons_address, from_height, from_tx_index`
	rows, err := s.st.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	prev := map[string]string{}
	var out []feed.Entry
	for rows.Next() {
		var addr, host, source, at string
		var h, tx int64
		if err := rows.Scan(&addr, &host, &source, &at, &h, &tx); err != nil {
			return nil, err
		}
		before, had := prev[addr]
		prev[addr] = host
		if source != "event" || (had && before == host) {
			continue
		}
		name := names[addr]
		if name == "" {
			name = addr[:12] + "…"
		}
		e := feed.Entry{At: parseTS(at), Link: validatorLink(addr, ops[addr]),
			ID: feed.TagID(authority, feedTagDate, "tensile", fm.chainID, addr, "registration", fmt.Sprintf("%d-%d", h, tx))}
		if !had || before == "" {
			e.Kind = "registered"
			e.Title = name + ": registered Fibre host " + host
			e.Summary = fmt.Sprintf("set_fibre_provider_info at height %d registered %s as this validator's Fibre host (chain record).", h, host)
		} else {
			e.Kind = "host-changed"
			e.Title = name + ": Fibre host changed to " + host
			e.Summary = fmt.Sprintf("set_fibre_provider_info at height %d changed this validator's Fibre host from %s to %s (chain record). Shards uploaded before the change stay owed.", h, before, host)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// bondedListEntries turns endpoint rows into joined/left entries. bech
// limits to one validator; empty means all (then names comes from the
// identities table). skipFirstPoll leaves out the rows opened by the
// observer's first poll entirely (the network feed); otherwise they are
// published once, worded as what they are. ops holds each validator's
// operator address for its link.
func (s *Server) bondedListEntries(ctx context.Context, bech, hexAddr, name string, fm feedMeta, authority string, skipFirstPoll bool, ops map[string]string) ([]feed.Entry, error) {
	q := `SELECT e.validator_cons_address, e.host, e.first_seen_at, e.first_seen_height, e.closed_at, e.closed_height,
		COALESCE(e.closed_reason, ''), COALESCE(i.moniker, '')
		FROM endpoints e LEFT JOIN validator_identities i ON i.cons_address = ?`
	args := []any{hexAddr}
	if bech != "" {
		q += ` WHERE e.validator_cons_address = ?`
		args = append(args, bech)
	}
	if bech == "" {
		// Network-wide: the join key is per row, so it is done in Go below.
		q = `SELECT validator_cons_address, host, first_seen_at, first_seen_height, closed_at, closed_height,
			COALESCE(closed_reason, ''), '' FROM endpoints WHERE first_seen_at >= ? OR closed_at >= ?`
		cut := store.TS(time.Now().Add(-feed.MaxAge))
		args = []any{cut, cut}
	}
	rows, err := s.st.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	type ep struct {
		bech, host, first, reason, moniker string
		fh                                 int64
		closed                             sql.NullString
		ch                                 sql.NullInt64
	}
	var eps []ep
	for rows.Next() {
		var e ep
		if err := rows.Scan(&e.bech, &e.host, &e.first, &e.fh, &e.closed, &e.ch, &e.reason, &e.moniker); err != nil {
			rows.Close()
			return nil, err
		}
		eps = append(eps, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	names := map[string]string{}
	if bech == "" {
		names, err = s.monikers(ctx)
		if err != nil {
			return nil, err
		}
	}
	var out []feed.Entry
	for _, e := range eps {
		addr := hexAddr
		if bech == "" {
			a, err := consHex(e.bech)
			if err != nil {
				continue
			}
			addr = a
		}
		nm := name
		if bech == "" {
			nm = names[addr]
			if nm == "" {
				nm = addr[:12] + "…"
			}
		}
		base := []string{"tensile", fm.chainID, addr}
		atStart := e.first == fm.firstPoll
		if !(atStart && skipFirstPoll) {
			at := parseTS(e.first)
			en := feed.Entry{Kind: "bonded-joined", At: at, Link: validatorLink(addr, ops[addr]),
				ID: feed.TagID(authority, feedTagDate, append(base, "bonded-joined", idTime(at))...)}
			if atStart {
				en.Title = nm + ": listed as a bonded Fibre provider (" + e.host + ")"
				en.Summary = "In the bonded provider list at this observer's first poll, at height " + fmt.Sprint(e.fh) + "; it may have been listed long before."
			} else {
				en.Title = nm + ": joined the bonded Fibre provider list (" + e.host + ")"
				en.Summary = fmt.Sprintf("First seen in AllBondedFibreProviders at height %d with host %s.", e.fh, e.host)
			}
			out = append(out, en)
		}
		if e.closed.Valid {
			at := parseTS(e.closed.String)
			en := feed.Entry{Kind: "bonded-left", At: at, Link: validatorLink(addr, ops[addr]),
				ID:    feed.TagID(authority, feedTagDate, append(base, "bonded-left", idTime(at))...),
				Title: nm + ": left the bonded Fibre provider list"}
			en.Summary = fmt.Sprintf("%s stopped appearing in AllBondedFibreProviders at height %d", e.host, e.ch.Int64)
			if e.reason != "" && e.reason != "left_bonded_provider_list" {
				en.Summary += " (" + e.reason + ")"
			}
			en.Summary += ". There is no unregister message: the registration stays on chain, and jailing or unbonding also removes a validator from the list."
			out = append(out, en)
		}
	}
	return out, nil
}

func (s *Server) monikers(ctx context.Context) (map[string]string, error) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT cons_address, moniker FROM validator_identities WHERE moniker <> ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var a, m string
		if err := rows.Scan(&a, &m); err != nil {
			return nil, err
		}
		out[strings.ToLower(a)] = m
	}
	return out, rows.Err()
}

// firstFaults finds each validator's first not-served reading on record
// (addr's alone when addr is set): a reading whose rows did not come back on
// a blob that was Unavailable (rollup.CountedClass, the same rule every
// figure applies). The returned entries have no ID; Link holds the promise
// hash for the caller to turn into a URL.
//
// The entry ID has no time in it, so it must name the same reading for good:
//
//   - a reading still settling (verdict.FaultSettling) is left out: a params
//     range can still withdraw it;
//   - a tie on started_at goes to the lower promise hash, not to row order;
//   - a validator with a not-served obligation rolled up on an earlier day
//     had its first one already, pruned from the raw rows since, and gets
//     none.
func (s *Server) firstFaults(ctx context.Context, addr string, now time.Time) (map[string]feed.Entry, error) {
	db := s.st.DB()
	faultWhere := `assigned = 1 AND ` + rollup.NotServedSQL("probes")
	var args []any
	if addr != "" {
		faultWhere += ` AND validator_address = ?`
		args = append(args, addr)
	}
	q := `SELECT validator_address, MIN(day) FROM obligation_daily WHERE broken > 0`
	if addr != "" {
		q += ` AND validator_address = ?`
	}
	rows, err := db.QueryContext(ctx, q+` GROUP BY validator_address`, args...)
	if err != nil {
		return nil, err
	}
	rolled := map[string]string{}
	for rows.Next() {
		var a, d string
		if err := rows.Scan(&a, &d); err != nil {
			rows.Close()
			return nil, err
		}
		rolled[a] = d
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = db.QueryContext(ctx, `SELECT validator_address, promise_hash, scheduled_at, started_at, schedule_label FROM probes
		WHERE `+faultWhere+` AND started_at <= ?
		ORDER BY validator_address, started_at, promise_hash`, append(args, provisionalCutoff(now))...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]feed.Entry{}
	seen := map[string]bool{}
	for rows.Next() {
		var a, hash, sched, at, label string
		if err := rows.Scan(&a, &hash, &sched, &at, &label); err != nil {
			return nil, err
		}
		if seen[a] {
			continue
		}
		seen[a] = true
		t := parseTS(at)
		if d, ok := rolled[a]; ok && d < t.Format("2006-01-02") {
			continue
		}
		out[a] = feed.Entry{Kind: "first-fault", At: t, Link: hash,
			Title: "first not-served reading on record",
			Summary: fmt.Sprintf("At the reading of blob %s (%s, %s) the validator did not hand over the rows it endorsed, "+
				"and the blob could not be reconstructed from the rows the other validators returned.", hash, label, sched)}
	}
	return out, rows.Err()
}

// networkFeed builds /v1/feed.atom.
func (s *Server) networkFeed(ctx context.Context, authority string, now time.Time) (*feed.Feed, error) {
	fm, err := s.readFeedMeta(ctx)
	if err != nil {
		return nil, err
	}
	names, err := s.monikers(ctx)
	if err != nil {
		return nil, err
	}
	ops, err := s.operatorAddrs(ctx)
	if err != nil {
		return nil, err
	}
	var es []feed.Entry
	regs, err := s.registrationEntries(ctx, "", fm, authority, names, ops)
	if err != nil {
		return nil, err
	}
	es = append(es, regs...)
	bl, err := s.bondedListEntries(ctx, "", "", "", fm, authority, true, ops)
	if err != nil {
		return nil, err
	}
	es = append(es, bl...)

	// Each validator's first not-served reading, when it falls in the
	// feed's span.
	cut := store.TS(now.Add(-feed.MaxAge))
	ffs, err := s.firstFaults(ctx, "", now)
	if err != nil {
		return nil, err
	}
	for a, fe := range ffs {
		if store.TS(fe.At) < cut {
			continue
		}
		nm := names[a]
		if nm == "" {
			nm = a[:12] + "…"
		}
		fe.ID = feed.TagID(authority, feedTagDate, "tensile", fm.chainID, a, "first-fault")
		fe.Link, fe.Title = "/blob/?hash="+fe.Link, nm+": "+fe.Title
		es = append(es, fe)
	}

	f := &feed.Feed{
		ID:       feed.TagID(authority, feedTagDate, "tensile", fm.chainID, "network"),
		Title:    "Tensile · " + fm.chainID + " · Fibre network events",
		Subtitle: "Fibre host registrations, bonded provider list changes and first not-served readings on " + fm.chainID + ", from vantage " + s.vantage + ". Newest 50 of the last 30 days.",
		SelfHref: "feed.atom",
		AltHref:  "/",
		Author:   "Tensile observer (" + s.vantage + ")",
		Entries:  feed.Bound(es, now),
	}
	f.Updated = feedUpdated(f.Entries, now)
	return f, nil
}
