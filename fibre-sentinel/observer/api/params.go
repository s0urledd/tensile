package api

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	celfibre "github.com/celestiaorg/celestia-app/v10/fibre"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	assign "github.com/plsgiveup/fibre/fibre-assign"
)

// GET /v1/params: the x/fibre parameters as this observer has them, and the
// protocol constants it pins.
//
// Two kinds of number, kept apart because they are different claims:
//
//   - chain: the five x/fibre params (withdrawal_delay, payment_promise_
//     timeout, payment_promise_height_window, shard_retention,
//     full_stake_storage_budget), from params_history. The scanner seeds that
//     table with the values in force at the height it started from and adds
//     an entry for every EventUpdateFibreParams it reads after; the collector
//     dates each entry with its height's block time. Nothing here is
//     hard-coded: a changed param shows up only because the chain said so.
//   - pinned: constants compiled into the celestia-app build this observer
//     pins (fibre/protocol_params.go, x/fibre/types/params.go at
//     assign.PinnedCelestiaAppCommit). No node or Fibre server exposes them
//     over RPC, so the only honest source is the pinned code itself, and
//     the response names the commit. They are read from the compiled
//     dependency, not retyped, so they cannot drift from the code the
//     assignment and the probe budget actually use.

// paramEntry is one params value and where in chain history it takes effect.
type paramEntry struct {
	EffectiveFromHeight  int64 `json:"effective_from_height"`
	EffectiveFromTxIndex int   `json:"effective_from_tx_index"`
	// EffectiveFromTime is the block time of EffectiveFromHeight; null
	// until the collector has read that header, and for good if the node
	// pruned it.
	EffectiveFromTime *string `json:"effective_from_time"`
	// Source: seed (read from state where the scan began; in force since
	// at least then, possibly much earlier), event (an
	// EventUpdateFibreParams in a transaction, effective for promises
	// settled after that transaction), or finalize (the same event at end
	// of block, e.g. governance, effective from the next block).
	Source                     string `json:"source"`
	WithdrawalDelayS           int64  `json:"withdrawal_delay_s"`
	PaymentPromiseTimeoutS     int64  `json:"payment_promise_timeout_s"`
	PaymentPromiseHeightWindow int64  `json:"payment_promise_height_window"`
	ShardRetentionS            int64  `json:"shard_retention_s"`
	FullStakeStorageBudget     int64  `json:"full_stake_storage_budget_bytes"`
	// Changed names the params that differ from the previous entry; empty
	// on the first entry.
	Changed []string `json:"changed"`
}

// derivedParams are what the observer computes from the current entry.
type derivedParams struct {
	// MustServeWindowS = max(payment_promise_timeout, shard_retention): a
	// signing validator's serving obligation runs from the promise's
	// creation for this long (the must_serve_until every verdict uses).
	MustServeWindowS int64 `json:"must_serve_window_s"`
	// PromiseSettleableS = withdrawal_delay: how long after creation a
	// promise can still be settled or timed out
	// (x/fibre/types/params.go, MinWithdrawalDelay's comment).
	PromiseSettleableS int64 `json:"promise_settleable_s"`
	// ProcessedPaymentRetentionS = withdrawal_delay + MaxPromiseClockSkew
	// (Params.PaymentPromiseRetentionWindow).
	ProcessedPaymentRetentionS int64 `json:"processed_payment_retention_s"`
}

// paramBound is one x/fibre param's validation range in the pinned code.
type paramBound struct {
	MinS *int64 `json:"min_s,omitempty"`
	MaxS int64  `json:"max_s"`
}

// protocolConstants are read from celestia-app's fibre/protocol_params.go
// (DefaultProtocolParams) and x/fibre/types/params.go at the pinned commit.
type protocolConstants struct {
	PinnedCelestiaApp        string `json:"pinned_celestia_app_commit"`
	PinnedCelestiaAppVersion string `json:"pinned_celestia_app_version"`

	// Erasure coding, blob version 0.
	OriginalRows  int     `json:"original_rows"`
	ParityRows    int     `json:"parity_rows"`
	TotalRows     int     `json:"total_rows"`
	EncodingRatio float64 `json:"encoding_ratio"`
	MinRowSize    int     `json:"min_row_size_bytes"`
	MaxBlobSize   int     `json:"max_blob_size_bytes"`
	MaxRowSize    int     `json:"max_row_size_bytes"`

	// Assignment.
	MinRowsPerValidator   int    `json:"min_rows_per_validator"`
	MaxRowsPerValidator   int    `json:"max_rows_per_validator"`
	MaxValidatorCount     int    `json:"max_validator_count"`
	LivenessThreshold     string `json:"liveness_threshold"`
	SafetyThreshold       string `json:"safety_threshold"`
	UniqueDecodingBits    int    `json:"unique_decoding_security_bits"`
	MaxShardSize          int    `json:"max_shard_size_bytes"`
	MaxMessageSize        int    `json:"max_message_size_bytes"`
	AssignmentFingerprint string `json:"assignment_fingerprint"`
	// ScannerFingerprint is the fingerprint the scanner stamped on its
	// state; it must equal AssignmentFingerprint, and FingerprintMatches
	// says whether it does (null before the scanner has written one).
	ScannerFingerprint string `json:"scanner_fingerprint,omitempty"`
	FingerprintMatches *bool  `json:"fingerprint_matches"`

	// x/fibre param validation ranges and fixed timing constants.
	MaxPromiseClockSkewS        int64                 `json:"max_promise_clock_skew_s"`
	MinTimeoutSettlementWindowS int64                 `json:"min_timeout_settlement_window_s"`
	Bounds                      map[string]paramBound `json:"param_bounds"`
}

// paramsResponse is /v1/params. current and history are the x/fibre params
// as the chain recorded them in params_history: the value in force where
// the scan began (a seed entry, which says nothing about when it took
// effect), then every EventUpdateFibreParams after it, dated from block
// headers. The params can also change without an event (a chain upgrade's
// migration); the scanner re-reads them from state and publishes any
// disagreement as a params-uncertainty range (/v1/meta param_uncertainty)
// rather than an entry here. The protocol constants are compiled into the
// pinned celestia-app build and exposed by no RPC, so they are read from
// that code, and the commit is named.
type paramsResponse struct {
	// Current is the latest entry: the params in force at the scanner's
	// checkpoint as far as the event record goes. Null before the scanner
	// has seeded the history (Fibre not live yet).
	Current  *paramEntry       `json:"current"`
	Derived  *derivedParams    `json:"derived,omitempty"`
	History  []paramEntry      `json:"history"`
	Changes  int               `json:"changes"`
	Protocol protocolConstants `json:"protocol"`
	// PriceFormula is x/fibre's charge for a blob, from which every fee on
	// /v1/market, /v1/publishers and /v1/blobs is recomputed: no chain
	// event carries the amount.
	PriceFormula priceFormula `json:"price_formula"`
}

func secs(d interface{ Seconds() float64 }) int64 { return int64(d.Seconds()) }

func pinnedProtocol() protocolConstants {
	p := celfibre.DefaultProtocolParams
	minWD, minTO, minSR := secs(fibretypes.MinWithdrawalDelay), secs(fibretypes.MinPaymentPromiseTimeout), secs(fibretypes.MinShardRetention)
	return protocolConstants{
		PinnedCelestiaApp:        assign.PinnedCelestiaAppCommit,
		PinnedCelestiaAppVersion: assign.PinnedCelestiaAppVersion,

		OriginalRows:  p.Rows,
		ParityRows:    p.ParityRows(),
		TotalRows:     p.TotalRows(),
		EncodingRatio: p.EncodingRatio,
		MinRowSize:    p.MinRowSize,
		MaxBlobSize:   p.MaxBlobSize,
		MaxRowSize:    p.MaxRowSize(0),

		MinRowsPerValidator:   p.MinRowsPerValidator(),
		MaxRowsPerValidator:   p.MaxRowsPerValidator(),
		MaxValidatorCount:     p.MaxValidatorCount,
		LivenessThreshold:     fmt.Sprintf("%d/%d", p.LivenessThreshold.Numerator, p.LivenessThreshold.Denominator),
		SafetyThreshold:       fmt.Sprintf("%d/%d", p.SafetyThreshold.Numerator, p.SafetyThreshold.Denominator),
		UniqueDecodingBits:    p.UniqueDecodingSecurityBits,
		MaxShardSize:          p.MaxShardSize(),
		MaxMessageSize:        p.MaxMessageSize(),
		AssignmentFingerprint: assign.ParamsV10BlobV0.Fingerprint(),

		MaxPromiseClockSkewS:        secs(fibretypes.MaxPromiseClockSkew),
		MinTimeoutSettlementWindowS: secs(fibretypes.MinTimeoutSettlementWindow),
		Bounds: map[string]paramBound{
			"withdrawal_delay":        {MinS: &minWD, MaxS: secs(fibretypes.MaxWithdrawalDelay)},
			"payment_promise_timeout": {MinS: &minTO, MaxS: secs(fibretypes.MaxPaymentPromiseTimeout)},
			"shard_retention":         {MinS: &minSR, MaxS: secs(fibretypes.MaxShardRetention)},
		},
	}
}

// paramHistory reads params_history oldest first, with each entry's changes
// against the one before.
func (s *Server) paramHistory(ctx context.Context) ([]paramEntry, error) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT effective_from_height, effective_from_tx_index, effective_from_time, source,
			withdrawal_delay_s, payment_promise_timeout_s, payment_promise_height_window, shard_retention_s, full_stake_storage_budget
		FROM params_history ORDER BY effective_from_height, effective_from_tx_index`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []paramEntry{}
	for rows.Next() {
		var e paramEntry
		var at sql.NullString
		if err := rows.Scan(&e.EffectiveFromHeight, &e.EffectiveFromTxIndex, &at, &e.Source,
			&e.WithdrawalDelayS, &e.PaymentPromiseTimeoutS, &e.PaymentPromiseHeightWindow, &e.ShardRetentionS, &e.FullStakeStorageBudget); err != nil {
			return nil, err
		}
		e.EffectiveFromTime = nullStr(at)
		e.Changed = []string{}
		if n := len(out); n > 0 {
			prev := out[n-1]
			for _, f := range []struct {
				name string
				a, b int64
			}{
				{"withdrawal_delay", prev.WithdrawalDelayS, e.WithdrawalDelayS},
				{"payment_promise_timeout", prev.PaymentPromiseTimeoutS, e.PaymentPromiseTimeoutS},
				{"payment_promise_height_window", prev.PaymentPromiseHeightWindow, e.PaymentPromiseHeightWindow},
				{"shard_retention", prev.ShardRetentionS, e.ShardRetentionS},
				{"full_stake_storage_budget", prev.FullStakeStorageBudget, e.FullStakeStorageBudget},
			} {
				if f.a != f.b {
					e.Changed = append(e.Changed, f.name)
				}
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Server) handleParams(w http.ResponseWriter, r *http.Request) {
	hist, err := s.paramHistory(r.Context())
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	resp := paramsResponse{History: hist, Protocol: pinnedProtocol(), PriceFormula: formula}
	if n := len(hist); n > 0 {
		cur := hist[n-1]
		resp.Current = &cur
		resp.Changes = n - 1
		window := cur.PaymentPromiseTimeoutS
		if cur.ShardRetentionS > window {
			window = cur.ShardRetentionS
		}
		resp.Derived = &derivedParams{
			MustServeWindowS:           window,
			PromiseSettleableS:         cur.WithdrawalDelayS,
			ProcessedPaymentRetentionS: cur.WithdrawalDelayS + resp.Protocol.MaxPromiseClockSkewS,
		}
	}
	if fp, err := s.st.Meta("protocol_params_fingerprint"); err == nil && fp != "" {
		resp.Protocol.ScannerFingerprint = fp
		m := fp == resp.Protocol.AssignmentFingerprint
		resp.Protocol.FingerprintMatches = &m
	}
	writeJSON(w, 200, resp)
}
