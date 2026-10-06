package api

import (
	"net/http"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/store"
)

// namespaceRow is one namespace's settled Fibre publications: every figure is
// a count or a sum over the chain's own records (MsgPayForFibre), nothing
// measured.
type namespaceRow struct {
	Namespace string `json:"namespace"` // hex, as the chain carries it
	Blobs     int64  `json:"blobs"`
	Bytes     int64  `json:"bytes"` // padded blob size, as charged
	Blobs24h  int64  `json:"blobs_24h"`
	Bytes24h  int64  `json:"bytes_24h"`
	Accounts  int64  `json:"accounts"` // distinct publishers: the escrow owner that paid, not the tx submitter
	FirstSeen string `json:"first_seen"`
	LastBlob  string `json:"last_blob"`
}

// handleNamespaces lists namespaces by their newest settled publication.
func (s *Server) handleNamespaces(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r, 100, 500)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	since := store.TS(s.now().UTC().Add(-24 * time.Hour))
	rows, err := s.st.DB().QueryContext(r.Context(), `SELECT pub.namespace, COUNT(*), COALESCE(SUM(pub.blob_size), 0),
			COALESCE(SUM(CASE WHEN pub.settlement_time >= ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN pub.settlement_time >= ? THEN pub.blob_size ELSE 0 END), 0),
			COUNT(DISTINCT COALESCE(pay.publisher, pub.signer)), MIN(pub.settlement_time), MAX(pub.settlement_time)
		FROM publications pub
		LEFT JOIN payments pay ON pay.promise_hash = pub.promise_hash AND pay.kind = 'settlement'
		GROUP BY pub.namespace
		ORDER BY MAX(pub.settlement_height) DESC, pub.namespace LIMIT ?`, since, since, limit+1)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	defer rows.Close()
	out := []namespaceRow{}
	for rows.Next() {
		var n namespaceRow
		if err := rows.Scan(&n.Namespace, &n.Blobs, &n.Bytes, &n.Blobs24h, &n.Bytes24h, &n.Accounts, &n.FirstSeen, &n.LastBlob); err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	out, truncated := trim(out, limit)
	writeJSON(w, 200, map[string]any{"namespaces": out, "limit": limit, "truncated": truncated})
}
