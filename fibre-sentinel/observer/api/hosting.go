package api

// Hosting: which network and country every registered Fibre host resolves
// into, and how concentrated the registered set is. The lookups are the
// collector's (observer/hosting, from local database files and the
// addresses the heartbeat already recorded); this file only reads them,
// attaches them to the validator rows and summarises them.
//
// Nothing here is a verdict. Every figure is "as resolved from this
// vantage" and is published with the sources and the caveat beside it.

import (
	"context"
	"net/http"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/hosting"
)

// registerExtraRoutes adds the routes that live outside api.go: the hosting
// summary and the Atom feeds. Called once from NewWithVantage.
func (s *Server) registerExtraRoutes() {
	s.mux.HandleFunc("GET /v1/hosting", s.handleHosting)
	s.mux.HandleFunc("GET /v1/feed.atom", s.handleNetworkFeed)
	s.mux.HandleFunc("GET /v1/validators/{addr}/feed.atom", s.handleValidatorFeed)
}

// attachHosting sets Hosting on every row whose open endpoint the collector
// has looked up. A lookup for a host the validator no longer has open (it
// re-registered since the last pass) is not attached: the row's host and
// its hosting must describe the same endpoint.
//
// Hosting is the current lookup, not a windowed figure, so a pinned
// (?as_of=) window gets none rather than today's answer under an old date.
func (s *Server) attachHosting(ctx context.Context, rows []validatorRow, win Window) error {
	if win.AsOf || len(rows) == 0 {
		return nil
	}
	cur, err := hosting.Current(ctx, s.st.DB())
	if err != nil {
		return err
	}
	for i := range rows {
		in, ok := cur[rows[i].Address]
		if !ok || rows[i].Host == "" || in.Host != rows[i].Host {
			continue
		}
		rows[i].Hosting = &in // a fresh variable per iteration (Go 1.22 loop semantics)
	}
	return nil
}

// hostingResponse is /v1/hosting.
type hostingResponse struct {
	Vantage string          `json:"vantage"`
	Sources hosting.Sources `json:"sources"`
	// Summary is absent when the feature is off (no database file on the
	// collector's host): an empty summary would read as "nothing
	// concentrated", which is not what off means.
	Summary *hosting.Summary `json:"summary,omitempty"`
	// ProviderASNs is the AS-number list every provider bucket is drawn
	// from, so a reader can check a bucket without reading the code.
	ProviderASNs []hosting.ASNProvider `json:"provider_asns"`
	// StakeBasis says where the stake figures come from: the validator
	// list's voting_power (the latest assignment's, else the staking
	// module's), over the validators with an open Fibre endpoint.
	StakeBasis string `json:"stake_basis"`
	ComputedAt string `json:"computed_at"`
}

func (s *Server) handleHosting(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.URL.Query().Get("as_of") != "" {
		writeErr(w, 400, "hosting is the current lookup only; as_of is not supported here")
		return
	}
	src, err := hosting.ReadSources(ctx, s.st.DB())
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	out := hostingResponse{
		Vantage: s.vantage, Sources: src, ProviderASNs: hosting.ProviderASNs(),
		StakeBasis: "voting_power of each validator with an open Fibre endpoint, as on /v1/validators",
		ComputedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if src.Enabled {
		// The validator list's own snapshot supplies the population and the
		// stake, so the shares here are over exactly the rows the table
		// shows; hosting.Current supplies the placements, fresh.
		// The 24h list: the window only moves the rates, never who has an
		// endpoint or how much stake it holds.
		now := time.Now()
		win := Window{Name: "24h", Span: windows["24h"], Start: now.Add(-windows["24h"]), End: now}
		snap, _, _, err := s.vals.get(ctx, s.logf(), win)
		if err != nil {
			s.writeSnapshotErr(w, r, win, err)
			return
		}
		cur, err := hosting.Current(ctx, s.st.DB())
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		var members []hosting.Member
		for _, v := range snap.Rows {
			if v.Host == "" {
				continue // no open endpoint: not a registered host
			}
			m := hosting.Member{Validator: v.Address, Stake: v.VotingPower}
			if in, ok := cur[v.Address]; ok && in.Host == v.Host {
				m.Info = &in
			}
			members = append(members, m)
		}
		sum := hosting.Concentrate(members)
		out.Summary = &sum
	}
	w.Header().Set("Cache-Control", "public, max-age=60")
	writeJSON(w, 200, out)
}
