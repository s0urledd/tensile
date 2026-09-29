package api

import (
	"net/http"
	"time"
)

// GET /v1/validators/{addr}/status: one validator's state in a few hundred
// bytes, for a tool that polls it and alerts. The validator page's answer
// carries four periods and fifty readings; this carries the endpoint's
// state, the chain's word on the validator, the served and not-served
// counts of one period, its endorsements, and the newest endpoint check.
//
// Everything but the endpoint check is the validator's row in the snapshot
// /v1/validators serves, so this costs a lookup, never an aggregate. The
// check is one indexed row.

// validatorStatus is the answer.
type validatorStatus struct {
	Address     string `json:"address"`
	ConsAddress string `json:"cons_address"`
	Operator    string `json:"operator_address,omitempty"`
	Moniker     string `json:"moniker,omitempty"`
	Host        string `json:"host"`
	// Window is the period the counts are over, and ComputedAt when the
	// snapshot they come from was taken.
	Window     Window `json:"window"`
	ComputedAt string `json:"computed_at"`
	// Jailed and BondStatus are the chain's word; the rest is this
	// observer's.
	Jailed          bool    `json:"jailed"`
	BondStatus      string  `json:"bond_status,omitempty"`
	EndpointState   string  `json:"endpoint_state,omitempty"`
	Reachable       *bool   `json:"reachable"`
	IdentityStatus  string  `json:"identity_status"`
	LastReachableAt *string `json:"last_reachable_at"`
	// Obligations is served and not served over the window, and the
	// service rate between them (see obligationStats).
	Obligations       statusObligations  `json:"obligations"`
	ProvisionalFaults *provisionalFaults `json:"provisional_faults,omitempty"`
	Signing           statusSigning      `json:"signing"`
	// LastEndpointCheck is this observer's newest heartbeat against the
	// host; null before the first.
	LastEndpointCheck *statusCheck `json:"last_endpoint_check"`
}

type statusObligations struct {
	Served int64 `json:"served"`
	Broken int64 `json:"broken"`
	Rate   Rate  `json:"rate"`
}

type statusSigning struct {
	Assigned       int64   `json:"assigned"`
	Signed         int64   `json:"signed"`
	LastEndorsedAt *string `json:"last_endorsed_at"`
}

type statusCheck struct {
	At      string `json:"at"`
	Outcome string `json:"outcome"`
}

func statusOf(v validatorRow, win Window, at time.Time, check *endpointCheck) validatorStatus {
	out := validatorStatus{
		Address: v.Address, ConsAddress: v.ConsAddress, Operator: v.Operator, Moniker: v.Moniker, Host: v.Host,
		Window: win, ComputedAt: at.UTC().Format(time.RFC3339Nano),
		Jailed: v.Jailed, BondStatus: v.BondStatus, EndpointState: v.EndpointState, Reachable: v.Reachable,
		IdentityStatus: v.IdentityStatus, LastReachableAt: v.LastReachableAt,
		Obligations:       statusObligations{Served: v.Obligations.Served, Broken: v.Obligations.Broken, Rate: v.Obligations.Rate},
		ProvisionalFaults: v.ProvisionalFaults,
		Signing:           statusSigning{Assigned: v.Signing.Assigned, Signed: v.Signing.Signed, LastEndorsedAt: v.Signing.LastEndorsedAt},
	}
	if check != nil {
		out.LastEndpointCheck = &statusCheck{At: check.At, Outcome: check.Outcome}
	}
	return out
}

func (s *Server) handleValidatorStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	addr, err := s.resolveAddr(ctx, r.PathValue("addr"))
	if err != nil {
		s.writeAddrErr(w, r.URL.Path, err)
		return
	}
	if r.URL.Query().Get("as_of") != "" {
		writeErr(w, 400, "status is the current state only; as_of is not supported here")
		return
	}
	win, err := parseWindow(r, time.Now())
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	snap, at, _, err := s.vals.get(ctx, s.logf(), win)
	if err != nil {
		s.writeSnapshotErr(w, r, win, err)
		return
	}
	row, ok := snapshotRow(snap.Rows, addr)
	if !ok {
		known, err := s.validatorKnown(ctx, addr)
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		if !known {
			writeErr(w, 404, validatorNotSeen)
			return
		}
		// On record, and not in the snapshot yet: it appeared after the
		// snapshot was taken, and the next one lists it.
		s.writeSnapshotErr(w, r, win, errComputing)
		return
	}
	check, err := s.lastEndpointCheck(ctx, addr, win)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	writeJSON(w, 200, statusOf(row, snap.Window, at, check))
}
