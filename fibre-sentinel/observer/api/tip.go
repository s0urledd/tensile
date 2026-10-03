package api

import (
	"context"
	"encoding/json"
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
	ServerTime  time.Time  `json:"server_time"`
}

// tipCache keeps one answer for a quarter of a second: every open page polls
// this route every second, so however many readers there are, the node is asked
// at most four times a second, and a new block shows within that of the node
// committing it.
type tipCache struct {
	mu sync.Mutex
	at time.Time
	v  tipResponse
}

const tipTTL = 250 * time.Millisecond

// tipRPCTimeout bounds one ask of the node; past it the scanner's file answers.
const tipRPCTimeout = 800 * time.Millisecond

func (s *Server) handleTip(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
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
// behind) when the scanner has written nothing.
func (s *Server) readTip(now time.Time) tipResponse {
	var out tipResponse
	active, _ := s.st.Meta("fibre_active")
	out.FibreActive = active == "yes"
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
	if v, _ := s.st.Meta("chain_height"); v != "" {
		out.Height, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, _ := s.st.Meta("chain_tip_time"); v != "" {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			out.BlockTime = &t
		}
	}
	return out
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
