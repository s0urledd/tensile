package api

import (
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
)

// tipResponse is the newest block this observer has read, for the site's
// block ticker: the one figure on the page that moves every few seconds, so
// a reader can see the observer is following the chain without a status
// paragraph telling them so.
type tipResponse struct {
	Height int64 `json:"height"`
	// BlockTime is the block's own timestamp; absent while the scanner is
	// catching up, when the block it last read is not the tip.
	BlockTime   *time.Time `json:"block_time,omitempty"`
	FibreActive bool       `json:"fibre_active"`
	ServerTime  time.Time  `json:"server_time"`
}

// tipCache keeps one answer for a second: every open page polls this route,
// and the file behind it changes at most once a second anyway.
type tipCache struct {
	mu sync.Mutex
	at time.Time
	v  tipResponse
}

const tipTTL = time.Second

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

// readTip prefers the scanner's status file, written within a second of each
// block while it follows the tip; the collector's meta row (a poll behind)
// stands in when the scanner has written nothing.
func (s *Server) readTip(now time.Time) tipResponse {
	var out tipResponse
	active, _ := s.st.Meta("fibre_active")
	out.FibreActive = active == "yes"
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
