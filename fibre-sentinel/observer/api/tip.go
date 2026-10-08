package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
)

// tipResponse is the newest block, for the site's block ticker: the one figure
// on the page that moves with every block, so a reader can see the observer is
// following the chain without a status paragraph telling them so. With a node
// to ask (WithTipRPC) it is the node's newest committed block, asked when the
// cached answer is a quarter of a second old; otherwise the newest the scanner
// has read.
type tipResponse struct {
	Height int64 `json:"height"`
	// BlockTime is the block's own timestamp; absent while the scanner is
	// catching up, when the block it last read is not the tip.
	BlockTime   *time.Time `json:"block_time,omitempty"`
	FibreActive bool       `json:"fibre_active"`
	// LatestBlob is the newest blob the store holds, in /v1/blobs' own order;
	// absent while it holds none. It is here so a page can tell that a blob
	// arrived from the stream it already reads every second, and ask
	// /v1/blobs only then, rather than every few seconds in case one did.
	LatestBlob *tipBlob  `json:"latest_blob,omitempty"`
	ServerTime time.Time `json:"server_time"`
}

// tipBlob names a blob: the marker a page compares with the blobs it holds,
// and the height it settled at, so the marker can be told from an older one
// without a second request.
type tipBlob struct {
	PromiseHash      string `json:"promise_hash"`
	SettlementHeight int64  `json:"settlement_height"`
}

// latestBlobSQL is the newest publication in /v1/blobs' order (settlement
// height, then position in the block, both descending). The highest height
// comes from publications_settlement's last entry, which SQLite reads without
// walking the index (its min/max optimisation), and only that block's
// publications are then sought through the same index and sorted: a handful of
// rows however many millions the table holds. The page's own statement with
// LIMIT 1 would read the index from its top and stop just as soon, but its plan
// prints as a SCAN, the shape TestHotQueriesUseIndexes exists to refuse.
const latestBlobSQL = `SELECT promise_hash, settlement_height FROM publications
	WHERE settlement_height = (SELECT MAX(settlement_height) FROM publications)
	ORDER BY settlement_tx_index DESC LIMIT 1`

// tipStoreTimeout bounds the store's reads of one answer: those before the
// node is asked together, and the collector's meta row after it, when that
// answers, together again. A read in WAL mode is never blocked by the
// collector writing, but it waits for a connection of the pool while long
// reads hold them all; the cache's mutex is held while readTip runs, and
// every reader waits on it.
const tipStoreTimeout = 500 * time.Millisecond

// tipCache keeps one answer for a quarter of a second: every open page polls
// this route every second, so however many readers there are, the node and the
// store are each asked at most four times a second, and a new block (or a new
// blob) shows within that of the node committing it (or the collector storing
// it).
type tipCache struct {
	mu sync.Mutex
	at time.Time
	v  tipResponse
	// blobErrAt is when a failure to read the newest blob was last logged,
	// so a store that keeps failing is said once a minute, not four times a
	// second.
	blobErrAt time.Time
}

const tipTTL = 250 * time.Millisecond

// tipRPCTimeout bounds one ask of the node; past it the scanner's file answers.
const tipRPCTimeout = 800 * time.Millisecond

func (s *Server) handleTip(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	s.tip.mu.Lock()
	if now.Sub(s.tip.at) >= tipTTL {
		s.tip.v = s.readTip(now)
		s.tip.at = now
	}
	v := s.tip.v
	s.tip.mu.Unlock()
	v.ServerTime = now.UTC()
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, v)
}

// readTip asks the node first (WithTipRPC): its newest committed block, the
// moment it has it. Without a node, or when it does not answer in time, the
// scanner's status file answers (written within a second of each block while
// it follows the tip, after reading it), and the collector's meta row (a poll
// behind) when the scanner has written nothing. The newest blob comes from the
// store whichever of them answers.
//
// The store is read within tipStoreTimeout, never through Store.Meta, which
// takes no context and waited for a free connection for as long as the reads
// ahead of it ran, with the cache's mutex held. A store that does not answer
// in time costs this answer its store figures, not the answer: Fibre's state
// stays the last answer's, which the store has not been seen to change, and
// so does the block when the collector's row is the one left to name it.
// Called with s.tip.mu held.
func (s *Server) readTip(now time.Time) tipResponse {
	var out tipResponse
	ctx, cancel := context.WithTimeout(context.Background(), tipStoreTimeout)
	defer cancel()
	if active, err := s.metaWithin(ctx, "fibre_active"); err == nil {
		out.FibreActive = active == "yes"
	} else {
		out.FibreActive = s.tip.v.FibreActive
	}
	out.LatestBlob = s.latestBlob(ctx, now)
	cancel()
	if s.tipRPC != "" {
		if h, t, ok := nodeTip(s.tipRPC); ok {
			out.Height, out.BlockTime = h, &t
			return out
		}
	}
	if s.dataDir != "" {
		if r, ok := status.ReadOne(filepath.Join(s.dataDir, "status"), "scanner"); ok {
			out.Height = r.Height
			if v, ok := r.Detail["chain_tip"].(float64); ok && int64(v) > out.Height {
				out.Height = int64(v)
			}
			if v, ok := r.Detail["tip_block_time"].(string); ok {
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					out.BlockTime = &t
				}
			}
			if out.Height > 0 {
				return out
			}
		}
	}
	// The meta row is read within a timeout of its own, begun only now: a
	// node that does not answer takes tipRPCTimeout to say so, longer than
	// the reads above were given.
	mctx, mcancel := context.WithTimeout(context.Background(), tipStoreTimeout)
	defer mcancel()
	v, err := s.metaWithin(mctx, "chain_height")
	if err != nil {
		out.Height, out.BlockTime = s.tip.v.Height, s.tip.v.BlockTime
		return out
	}
	if v != "" {
		out.Height, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, _ := s.metaWithin(mctx, "chain_tip_time"); v != "" {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			out.BlockTime = &t
		}
	}
	return out
}

// metaWithin is Store.Meta within ctx: one meta value, "" when the key is
// not there.
func (s *Server) metaWithin(ctx context.Context, key string) (string, error) {
	var v string
	err := s.st.DB().QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// latestBlob is the newest blob the store holds (latestBlobSQL); nil when it
// holds none, or when the store could not say within ctx, in which case the
// answer leaves the field out and a page reads /v1/blobs at its slow pace, as
// it would from an API that never published it. Called with s.tip.mu held.
func (s *Server) latestBlob(ctx context.Context, now time.Time) *tipBlob {
	var b tipBlob
	if err := s.st.DB().QueryRowContext(ctx, latestBlobSQL).Scan(&b.PromiseHash, &b.SettlementHeight); err != nil {
		if !errors.Is(err, sql.ErrNoRows) && s.log != nil && now.Sub(s.tip.blobErrAt) >= time.Minute {
			s.tip.blobErrAt = now
			s.log.Printf("tip: newest blob: %v", err)
		}
		return nil
	}
	return &b
}

// nodeTip asks a CometBFT RPC's /status for its newest committed block.
func nodeTip(rpc string) (int64, time.Time, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), tipRPCTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(rpc, "/")+"/status", nil)
	if err != nil {
		return 0, time.Time{}, false
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, time.Time{}, false
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, time.Time{}, false
	}
	var body struct {
		Result struct {
			SyncInfo struct {
				Height string `json:"latest_block_height"`
				Time   string `json:"latest_block_time"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return 0, time.Time{}, false
	}
	h, err := strconv.ParseInt(body.Result.SyncInfo.Height, 10, 64)
	if err != nil || h <= 0 {
		return 0, time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, body.Result.SyncInfo.Time)
	if err != nil {
		return 0, time.Time{}, false
	}
	return h, t.UTC(), true
}
