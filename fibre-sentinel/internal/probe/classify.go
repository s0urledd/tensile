package probe

// Outcome is the raw, mechanism-level result of a single probe. It says what
// happened on the wire, not whether that is acceptable — that is Classification.
type Outcome string

const (
	OutcomeServedOK       Outcome = "SERVED_OK"       // shard returned, rows verify against commitment AND assignment
	OutcomeNotFound       Outcome = "NOT_FOUND"       // server answered "no such shard"
	OutcomeWrongRows      Outcome = "WRONG_ROWS"      // rows returned but not the assigned set (ShardMap.Verify failed)
	OutcomeInvalidRows    Outcome = "INVALID_ROWS"    // rows returned but fail commitment verification
	OutcomePartial        Outcome = "PARTIAL"         // fewer rows than assigned, otherwise valid
	OutcomeDNSFail        Outcome = "DNS_FAIL"        // host name did not resolve
	OutcomeTCPRefused     Outcome = "TCP_REFUSED"     // connection actively refused
	OutcomeTCPTimeout     Outcome = "TCP_TIMEOUT"     // TCP connect timed out
	OutcomeTCPUnreachable Outcome = "TCP_UNREACHABLE" // network/host unreachable, DNS ok
	OutcomeTLSFail        Outcome = "TLS_HANDSHAKE_FAIL"
	OutcomeIdentityFail   Outcome = "IDENTITY_FAIL"   // handshake ok, consensus-key binding rejected
	OutcomeRPCUnavailable Outcome = "RPC_UNAVAILABLE" // gRPC Unavailable after a good TLS handshake
	// OutcomeServerError: the validator was reached, completed TLS, proved its
	// identity and answered the RPC with an application error (gRPC Internal
	// or Unknown). It is NOT a reachability failure: calling it "unreachable"
	// would be factually wrong about a server the observer just talked to.
	OutcomeServerError Outcome = "SERVER_ERROR"
	// OutcomeThrottled: the validator was reached, proved its identity and
	// refused the request with ResourceExhausted that is not the observer's
	// own receive bound: a server-side limit. Celestia has said a per-peer
	// rate limiter is coming to the Fibre server; a probe it turns away says
	// nothing about the shard, and filing it as the observer's own error
	// would hide a limit set tight enough to keep real clients out.
	OutcomeThrottled Outcome = "RPC_THROTTLED"
	// OutcomeRPCDeadline: the download did not finish within the observer's
	// own deadline (base + size-scaled), on the earlier schedule. Never a
	// verdict about the validator.
	OutcomeRPCDeadline Outcome = "RPC_DEADLINE"
	// OutcomeRPCTimeout: the request, dial and DownloadShard together, did
	// not finish within the RPCTimeout celestia-app's Fibre client gives it
	// (15 s), after the connection was made. The client moves on without
	// the rows, and so does the reading.
	OutcomeRPCTimeout Outcome = "RPC_TIMEOUT"
	// OutcomeMalformedShard: the server answered with something the Fibre
	// client cannot use as a shard: empty, unparseable, rows outside the
	// code, or a reply larger than the protocol's message bound. The client
	// skips such a shard, and so does the reading.
	OutcomeMalformedShard Outcome = "MALFORMED_SHARD"
	// OutcomeNoHost: the validator has no fibre host registered in x/valaddr,
	// so nobody can fetch its rows.
	OutcomeNoHost Outcome = "NO_REGISTERED_HOST"
	// OutcomeBadHost: the registered host is an address this observer will
	// not connect to — loopback, a private or link-local range, or the
	// unspecified address. The chain validates only the host:port shape, so
	// any of those can be registered, and a public observer that dialled
	// them would be a port scanner and a DNS resolver driven from the chain,
	// publishing the result. Nothing was attempted, so it is no more a
	// statement about the shard than NO_REGISTERED_HOST is.
	OutcomeBadHost    Outcome = "UNROUTABLE_HOST"
	OutcomeRPCError   Outcome = "RPC_ERROR"   // some other gRPC error
	OutcomeProbeError Outcome = "PROBE_ERROR" // the probe itself failed (bug / config), not the target
	OutcomeMissed     Outcome = "MISSED"      // scheduled point elapsed before the prober could run it
	OutcomeReachable  Outcome = "REACHABLE"   // DNS/TCP/TLS/identity all fine; download deliberately skipped (heartbeat or policy backoff)
)

// AllOutcomes is every outcome the prober can record. The taxonomy test walks
// it so a new outcome cannot reach a default arm unnoticed.
var AllOutcomes = []Outcome{
	OutcomeServedOK, OutcomeNotFound, OutcomeWrongRows, OutcomeInvalidRows, OutcomePartial,
	OutcomeDNSFail, OutcomeTCPRefused, OutcomeTCPTimeout, OutcomeTCPUnreachable, OutcomeTLSFail,
	OutcomeIdentityFail, OutcomeRPCUnavailable, OutcomeServerError, OutcomeThrottled, OutcomeRPCDeadline,
	OutcomeRPCTimeout, OutcomeMalformedShard,
	OutcomeNoHost, OutcomeBadHost, OutcomeRPCError, OutcomeProbeError, OutcomeMissed, OutcomeReachable,
}

// Classification is the Sentinel's verdict on one measurement, given the probe
// phase and whether the validator was assigned this shard. It is a fact about
// this one probe, not a score — aggregation is a separate, later view.
type Classification string

const (
	// ClassHealthy: assigned validator served its exact rows while under
	// obligation, or still in grace; after the window the same answer is
	// ClassServedPastWindow.
	ClassHealthy Classification = "HEALTHY"
	// ClassFault: an identity-verified endpoint, while the promise held,
	// said it has no such shard, returned bytes that do not verify against
	// the commitment, or returned rows outside this promise's assignment,
	// for a shard the chain proves it stored. This is the only class that
	// counts against a validator, and every part of the sentence is load-
	// bearing: a validator the observer could not reach is not in here, a
	// server that answered with an error is not in here, and an endpoint
	// whose certificate is wrong is not in here — each of those is a
	// different statement with its own class.
	ClassFault Classification = "FAULT"
	// ClassUnreachable: assigned and attested, under obligation, and the
	// observer could not complete a conversation with the endpoint at all.
	// Recorded in full and shown next to the serve rate, but kept out of it:
	// from one vantage this is indistinguishable from a route, firewall or
	// peering problem on the observer's own side, and publishing it as a
	// retention failure would be an accusation the evidence does not support.
	ClassUnreachable Classification = "UNREACHABLE"
	// ClassNotRegistered: the validator has no Fibre host in x/valaddr at the
	// moment of the probe. Nobody can fetch its rows, but this is a registry
	// state (jailing and unbonding remove a provider from the bonded list;
	// the chain keeps the entry until the validator leaves staking state or
	// has been jailed and unbonded for a week past its unbonding time), not
	// a refusal to serve.
	ClassNotRegistered Classification = "NOT_REGISTERED"
	// ClassShadowedShard: the rows that came back are genuine rows of this
	// blob — they verify against the commitment — but their indices are not
	// the ones this promise assigns. DownloadShard is addressed by commitment
	// alone and a validator's store keeps one shard per commitment, so a
	// second promise over the same blob makes an honest validator answer with
	// the other promise's rows. Never a fault: the validator cannot tell the
	// two promises apart, because the protocol gives it no way to.
	ClassShadowedShard Classification = "SHADOWED_SHARD"
	// ClassUnmatchedGenuine: the rows that came back are genuine rows of
	// this blob, are not this promise's assignment, and no settled promise
	// over the commitment assigns them either. Under hash-order serving
	// that is not an accusation the evidence supports: a shard uploaded
	// for a promise that never settled sits on disk until its prune,
	// never appears on chain, and answers when its hash sorts first, and
	// a validator serving it honestly is serving a genuine piece of the
	// blob. Held out of the rate and counted beside it; the row carries
	// the indices. FAULT stays reserved for what hash order cannot excuse:
	// nothing served (NOT_FOUND in window), bytes that do not verify
	// (INVALID_ROWS), or rows that fail the commitment.
	ClassUnmatchedGenuine Classification = "UNMATCHED_GENUINE"
	// ClassIdentityExpired: the TLS certificate is endorsed by the right
	// consensus key, but the signed validity window has lapsed or has not
	// started. That is endpoint hygiene, not impersonation and not a
	// retention failure, so it is reported on its own rather than folded into
	// the serve rate.
	ClassIdentityExpired Classification = "IDENTITY_EXPIRED"
	// ClassIdentityMismatch: the TLS certificate is not endorsed by this
	// validator's consensus key at all. No client will download from the
	// endpoint, so it is as unusable as one that does not answer — and like
	// UNREACHABLE it is a statement about the endpoint, not about a shard.
	// It is judged before attestation (a certificate is a property of the
	// endpoint) and held out of the serve rate: the rate speaks about shards
	// the chain proves were stored, and a wrong certificate proves nothing
	// about any shard. It is surfaced as the endpoint's status and in the
	// endorsement rate instead.
	ClassIdentityMismatch Classification = "IDENTITY_MISMATCH"
	// ClassServerError: the endpoint was reached, proved its identity and
	// answered the RPC with an application error (gRPC Internal, Unknown,
	// DataLoss, Aborted) instead of the shard, while the promise held. The
	// server did not say it lacks the shard; it said it could not answer.
	// From one probe that is not distinguishable from a transient fault —
	// an overloaded process, a disk hiccup — so it is recorded and shown
	// beside the rate, never inside it. A server that errors at every probe
	// point is visible as such on its own page.
	ClassServerError Classification = "SERVER_ERROR"
	// ClassThrottled: the endpoint was reached, proved its identity and
	// answered the download with a rate limit. Shown beside the rate under
	// its own name, never inside it, and the probe policy backs off from a
	// validator that says so.
	ClassThrottled Classification = "THROTTLED"
	// ClassTolerated: NOT_FOUND / unreachable in the grace window right after
	// must_serve_until. Within measured prune lag; not held against the
	// validator.
	ClassTolerated Classification = "TOLERATED"
	// ClassExpectedGone: NOT_FOUND in the post window — the blob is supposed to
	// be pruned by now.
	ClassExpectedGone Classification = "EXPECTED_GONE"
	// ClassExpectedUnassigned: NOT_FOUND from a validator that was never
	// assigned this shard. Normal.
	ClassExpectedUnassigned Classification = "EXPECTED_UNASSIGNED"
	// ClassServedPastWindow: assigned validator still serving well after the
	// obligation ended. Not a fault; noted because it affects disk accounting.
	ClassServedPastWindow Classification = "SERVED_PAST_WINDOW"
	// ClassServingUnassigned: a NOT-assigned validator returned a shard.
	// Unexpected; flagged for review (misassignment or a validator over-serving).
	ClassServingUnassigned Classification = "SERVING_UNASSIGNED"
	// ClassUnreachablePostWindow: assigned validator unreachable after its
	// obligation ended. Recorded, not a retention fault.
	ClassUnreachablePostWindow Classification = "UNREACHABLE_POST_WINDOW"
	// ClassProbeError: the probe could not be carried out (our side).
	ClassProbeError Classification = "PROBE_ERROR"
	// ClassNotProbed: the scheduled point elapsed before it could be probed.
	ClassNotProbed Classification = "NOT_PROBED"

	// ClassUnattested: the validator is assigned rows for this blob, but the
	// settled promise carries no verified signature from it, so there is no
	// proof it ever received the shard. Absence of a signature is not proof
	// that it did not: the publisher stops collecting at the safety threshold
	// and keeps delivering in the background. These probes are reported
	// separately and are counted neither for nor against the validator.
	ClassUnattested Classification = "UNATTESTED"

	// ClassRetentionUnverified: an x/fibre params change that emitted no
	// event landed somewhere in a range of heights covering this
	// publication's upload, and the observer has not read the params at
	// every height in that range. must_serve_until is computed from those
	// params, the phase from must_serve_until, and this row's place in the
	// serve rate from the phase, so the rate cannot speak for this row
	// until the range is read.
	//
	// Classify never returns it. It is applied afterwards, over the stored
	// row, by rollup.EffectiveClass and verdict.Row.EffectiveClass: the
	// measurement record says what happened on the wire and is append-only,
	// while whether the observer trusts its own deadline is a judgement
	// that has to be revisable when the range closes. Both directions are
	// withheld, the FAULT it would otherwise publish and the HEALTHY it
	// would otherwise credit, because withholding only the accusations
	// would raise every rate it touched. Never a statement about the
	// validator.
	ClassRetentionUnverified Classification = "RETENTION_UNVERIFIED"
)

// AllClassifications is every class this observer publishes. All but one
// are returned by Classify, and the product test walks the whole evidence
// space to check that every cell lands on one of them with a reason, so a
// new class or a new default arm cannot go unnoticed.
//
// The exception is ClassRetentionUnverified, which is applied over a stored
// row rather than derived from one probe's evidence; see its comment. It is
// in this list because it appears in published class tallies and a reader
// must be able to find it named.
var AllClassifications = []Classification{
	ClassHealthy, ClassFault, ClassUnreachable, ClassNotRegistered, ClassShadowedShard, ClassUnmatchedGenuine,
	ClassIdentityExpired, ClassIdentityMismatch, ClassServerError, ClassThrottled, ClassTolerated, ClassExpectedGone,
	ClassExpectedUnassigned, ClassServedPastWindow, ClassServingUnassigned, ClassUnreachablePostWindow,
	ClassProbeError, ClassNotProbed, ClassUnattested, ClassRetentionUnverified,
}

// EndReadLabel is the schedule label of the one reading of a blob, 10
// minutes before its retention window ends (ScheduleConfig.EndReadOffset).
const EndReadLabel = "end"

// OwnAnswer reports whether a row is a validator's own answer at a reading:
// in the window, and either rows that verified against the commitment, or
// anything but this observer's own gap (NOT_PROBED: the request was never
// made; PROBE_ERROR: it failed on this observer's side before it reached
// the validator, a local resolver or no route out, or this build could not
// handle the answer). A reading without one did not happen: not a single
// request reached a validator, and the blob was not read by Tensile. The
// prober's result (blobread.go) and the verdict's (observer/verdict,
// rollup's Answered) are both built from it.
func OwnAnswer(phase Phase, c Classification, verified bool) bool {
	if phase != PhaseInWindow {
		return false
	}
	return verified || (c != ClassNotProbed && c != ClassProbeError)
}

// VerdictDeferred reports whether a row's verdict is still to be drawn by
// the collector (verdict.LateShadow): rows that verified against the
// commitment and are not this promise's assignment, written PROBE_ERROR
// with a shadow gap until the scanner has read far enough to say which
// promise, if any, they belong to. The rows verified, so for the reading
// they came back whatever that verdict comes to.
func VerdictDeferred(m Measurement) bool {
	return m.Classification == ClassProbeError && m.Download.ShadowGap != "" && m.Download.CommitmentVerified &&
		(m.Outcome == OutcomeWrongRows || m.Outcome == OutcomePartial)
}

// DeadlineDerivedClasses is every classification whose membership of the
// serve rate depends on which side of must_serve_until the probe fell. It
// is exactly the two the rate is built from: HEALTHY is SERVED_PAST_WINDOW
// on the other side of the deadline, and FAULT is EXPECTED_GONE.
//
// Everything else the phase switch produces only changes its name across
// the boundary — UNREACHABLE becomes UNREACHABLE_POST_WINDOW, THROTTLED
// becomes TOLERATED — and is held out of the rate on both sides, so
// withholding it would move no published figure.
//
// rollup.DeadlineDerivedSQL is the same list for the SQL twin, held to this
// one by TestTheSQLAndTheGoTwinHoldTheSameRows.
var DeadlineDerivedClasses = []Classification{ClassHealthy, ClassFault}

// DeadlineDerived reports whether a row's verdict would change if
// must_serve_until moved.
//
// INVALID_ROWS is carved out: bytes that do not verify against the blob
// commitment are a FAULT in the window, in the grace phase and after it
// alike (classify.go, all three phase arms), so no deadline can rescue that
// fault and no hold should discard it.
func DeadlineDerived(c Classification, o Outcome) bool {
	if o == OutcomeInvalidRows {
		return false
	}
	for _, d := range DeadlineDerivedClasses {
		if c == d {
			return true
		}
	}
	return false
}

// CountsAgainst reports whether a class is held against the validator. Exactly
// one class is, and every rate in the API is built from this predicate rather
// than from a list repeated at each call site.
func (c Classification) CountsAgainst() bool { return c == ClassFault }

// CountsFor reports whether a class is evidence the validator kept its promise.
func (c Classification) CountsFor() bool { return c == ClassHealthy }

// Rated reports whether a class belongs in the serve rate at all. Everything
// else is published beside the rate with its own name, never folded in.
func (c Classification) Rated() bool { return c.CountsFor() || c.CountsAgainst() }

// served reports whether an outcome means "the shard came back".
func (o Outcome) served() bool {
	return o == OutcomeServedOK || o == OutcomePartial
}

// reachFailure reports whether an outcome means the observer never got a
// usable answer out of the endpoint. SERVER_ERROR is deliberately absent: the
// validator was reached and answered.
func (o Outcome) reachFailure() bool {
	switch o {
	case OutcomeDNSFail, OutcomeTCPRefused, OutcomeTCPTimeout, OutcomeTCPUnreachable,
		OutcomeTLSFail, OutcomeRPCUnavailable, OutcomeRPCError, OutcomeRPCTimeout:
		return true
	}
	return false
}

// answeredWrong reports an outcome where the endpoint was reached and
// answered with something other than the shard: an application error, or a
// shard the client cannot use.
func (o Outcome) answeredWrong() bool {
	return o == OutcomeServerError || o == OutcomeMalformedShard
}

// Evidence is everything the taxonomy needs about one probe. It is a struct
// rather than a list of bare booleans so that adding a field cannot silently
// reorder an existing call.
type Evidence struct {
	// Assigned: fibre-assign gives this validator rows for this commitment.
	Assigned bool
	// Attested: the settled promise carries a signature from this validator
	// that the observer verified against its consensus key, which is the only
	// on-chain proof that it ever stored the shard.
	Attested bool
	// AttestationUnknown: the publication record predates signature
	// verification, so Attested carries no evidence either way. Such a blob
	// is judged under the older rules, where assignment alone was the
	// obligation; it is never filed as UNATTESTED, which would turn "not
	// recorded" into "did not attest".
	AttestationUnknown bool
	// Phase is derived from the ACTUAL probe start time.
	Phase Phase
	// Outcome is what happened on the wire.
	Outcome Outcome
	// CommitmentVerified: the rows that came back are genuine rows of this
	// blob. With WRONG_ROWS or PARTIAL it is half the question: rows that do
	// not verify are corrupt data; rows that verify but carry the wrong
	// indices are either another promise's shard (Shadowed) or an incomplete
	// delivery of this one.
	CommitmentVerified bool
	// Shadowed: another settled promise over the same commitment assigns
	// this validator exactly the row set it returned. Only then is
	// "answered from a different promise, which it has no way to avoid" a
	// finding rather than a guess. Without it, verified rows that are not
	// this promise's assignment are UNMATCHED_GENUINE (held out of the
	// rate, never a fault: an upload whose promise never settled can
	// answer under hash-order serving), and the row carries the indices
	// so anyone can check.
	Shadowed bool
	// ShadowUncertain: no known promise assigns the returned rows, but a
	// scan gap overlaps the interval in which a promise whose shard could
	// still be on disk would have settled. The observer knows its own
	// blindness; accusing across it is what the methodology forbids.
	ShadowUncertain bool
	// IdentityStale: the certificate is endorsed by the right consensus key
	// but its signed validity window has lapsed or not yet started. Only
	// meaningful when Outcome is IDENTITY_FAIL.
	IdentityStale bool
	// RowsSubsetOfOwn: the returned indices are all ones this promise
	// assigns this validator, and fewer than it owes. Still held out of the
	// rate — the validator signs after writing whatever it received, so a
	// short shard may be the publisher's doing — but the row says so, rather
	// than citing hash-order serving, which cannot produce a part of this
	// promise's own assignment.
	RowsSubsetOfOwn bool
	// PinStale: the chain's app version is above the celestia-app major the
	// assignment constants are pinned to. Which rows this validator owes
	// may then be computed wrongly, so nothing that depends on assignment
	// is judged; the row is an observer gap, never a verdict.
	PinStale bool
}

// Classify applies the taxonomy.
func Classify(in Evidence) (Classification, string) {
	o := in.Outcome

	// Observer-side first: none of these say anything about the validator.
	if o == OutcomeProbeError {
		return ClassProbeError, "probe could not be carried out"
	}
	if o == OutcomeMissed {
		return ClassNotProbed, "scheduled point elapsed before the prober ran it"
	}
	if o == OutcomeRPCDeadline {
		return ClassProbeError, "download did not finish within the observer's deadline; no retention verdict"
	}
	if o == OutcomeReachable {
		// The endpoint answered and proved its identity; no retention verdict
		// was attempted. Recorded as an observer-side gap for the shard.
		return ClassNotProbed, "reachable; download skipped by policy"
	}

	// Identity is a property of the endpoint, not of one shard, so it is
	// judged before anything that depends on holding this blob.
	if o == OutcomeIdentityFail {
		if in.IdentityStale {
			return ClassIdentityExpired, "certificate is endorsed by the right consensus key but its signed validity window has lapsed"
		}
		return ClassIdentityMismatch, "TLS certificate is not endorsed by this validator's consensus key; no client can download from this endpoint"
	}

	// No registered host is a registry state, not a refusal. Judged before
	// assignment because it is true of the endpoint either way.
	if o == OutcomeNoHost {
		if !in.Assigned {
			return ClassExpectedUnassigned, "validator not assigned this shard and has no registered Fibre host"
		}
		return ClassNotRegistered, "no Fibre host registered for this validator at the time of the probe, so nobody could fetch its rows"
	}
	// An unroutable registered host is the same kind of statement: the
	// endpoint as published cannot be reached from the public internet, by
	// this observer or by anyone. No connection was made, so there is
	// nothing to hold against the shard.
	if o == OutcomeBadHost {
		if !in.Assigned {
			return ClassExpectedUnassigned, "validator not assigned this shard, and its registered Fibre host is not a public address"
		}
		return ClassNotRegistered, "the registered Fibre host is not a public address, so no client on the internet could fetch its rows; this observer does not connect to it"
	}

	// A stale assignment pin is the observer's problem: after a chain
	// upgrade this build may assign rows the chain does not, and every
	// NOT_FOUND or WRONG_ROWS it then produces would be a false accusation
	// of the whole set at once. Identity and registry above are still
	// judged, because neither depends on assignment.
	if in.PinStale {
		return ClassProbeError, "assignment pin stale: the chain runs an app version above the pinned celestia-app major, so which rows this validator owes cannot be computed by this build; no retention verdict"
	}

	if in.Assigned && !in.Attested && !in.AttestationUnknown {
		// No verified signature over this promise, so nothing proves this
		// validator was ever sent the shard. Serving it anyway proves it has
		// it; failing to serve it proves nothing at all. Counted neither for
		// nor against: excluding only the failures would inflate every rate.
		if o.served() {
			return ClassUnattested, "served the shard although the settled promise carries no verified signature from this validator"
		}
		return ClassUnattested, "assigned rows, but the settled promise carries no verified signature from this validator: nothing proves it ever received the shard"
	}

	if !in.Assigned {
		switch {
		case o == OutcomeNotFound:
			return ClassExpectedUnassigned, "validator not assigned this shard; NOT_FOUND expected"
		case o.served() || o == OutcomeWrongRows || o == OutcomeInvalidRows:
			return ClassServingUnassigned, "validator returned data for a shard it was not assigned — misassignment or over-serving"
		case o.reachFailure() || o.answeredWrong() || o == OutcomeThrottled:
			return ClassExpectedUnassigned, "validator not assigned this shard; reachability not required"
		default:
			// An outcome the taxonomy does not know says nothing, not even
			// that nothing was expected. The in-window arm below reads the
			// same case the same way.
			return ClassProbeError, "unrecognised probe outcome; no retention verdict"
		}
	}

	// Rows came back and verify against the commitment, but their indices are
	// not this promise's assignment, and another settled promise over the
	// same blob assigns exactly those rows. The validator is answering from
	// that promise and has no way to tell the two apart, in any phase.
	// Never a fault. Without a matching promise the same wire result falls
	// through to the phase arms below, where genuine rows no promise
	// assigns are UNMATCHED_GENUINE, held out of the rate.
	if (o == OutcomeWrongRows || o == OutcomePartial) && in.CommitmentVerified && in.Shadowed {
		return ClassShadowedShard, "returned rows of this blob that verify against the commitment and are exactly another settled promise's assignment for this validator; DownloadShard is addressed by commitment alone, so that promise answers in this one's place"
	}
	// The same wire result while the observer knows it has not seen every
	// promise that could own a shard over this commitment: a scan gap
	// overlaps the lifetime such a shard would have. Not a fault, not a
	// shadow: a gap in observation, re-classifiable once the gap is scanned.
	if (o == OutcomeWrongRows || o == OutcomePartial) && in.CommitmentVerified && in.ShadowUncertain {
		return ClassProbeError, "returned genuine rows of this blob that no promise this observer has scanned assigns; the store serves the first shard by promise-hash order, so a promise settling after this probe may own them: verdict deferred until the scanner passes probe time + payment_promise_timeout (probe_amendments), permanent when a scan gap covers the interval"
	}

	// assigned and attested validator.
	switch in.Phase {
	case PhaseInWindow:
		switch {
		case o == OutcomeServedOK:
			return ClassHealthy, "served assigned rows, verified against commitment and assignment"
		case o == OutcomeNotFound:
			return ClassFault, "assigned shard not found while under retention obligation"
		case o == OutcomeInvalidRows:
			return ClassFault, "returned bytes that do not verify against the blob commitment"
		case (o == OutcomeWrongRows || o == OutcomePartial) && in.CommitmentVerified && in.RowsSubsetOfOwn:
			return ClassUnmatchedGenuine, "returned genuine rows of this blob that this promise does assign this validator, but fewer than it owes; the shard on disk is short, which the validator signed for after writing whatever it received, so this observer cannot tell a validator that lost rows from a publisher that uploaded them incomplete: held out of the rate, counted beside it, indices on the row"
		case (o == OutcomeWrongRows || o == OutcomePartial) && in.CommitmentVerified:
			return ClassUnmatchedGenuine, "returned genuine rows of this blob, but not the set this promise assigns, and no settled promise over this commitment assigns them; the store serves the first shard by promise-hash order and a shard uploaded for a promise that never settled is never on chain, so this is not an accusation the evidence supports: held out of the rate, counted beside it, indices on the row"
		case o == OutcomeWrongRows || o == OutcomePartial:
			return ClassFault, "returned rows that verify against neither the commitment nor this promise's assignment"
		case o == OutcomeServerError:
			return ClassServerError, "endpoint reached and identity verified; the server answered with an application error instead of the shard, which from one probe is not distinguishable from a transient fault"
		case o == OutcomeMalformedShard:
			return ClassServerError, "endpoint reached and identity verified; the server answered with a shard the Fibre client cannot use (empty, unparseable, or larger than the protocol's message bound)"
		case o == OutcomeThrottled:
			return ClassThrottled, "endpoint reached and identity verified; the server refused the download with a rate limit, which says nothing about the shard"
		case o.reachFailure():
			return ClassUnreachable, "could not complete a conversation with the endpoint while it was under obligation; from one vantage this is not distinguishable from a problem on the observer's own path"
		default:
			// An outcome the taxonomy does not know cannot be an accusation.
			return ClassProbeError, "unrecognised probe outcome; no retention verdict"
		}

	case PhaseGrace:
		switch {
		case o == OutcomeServedOK:
			return ClassHealthy, "still serving just past must_serve_until"
		case o == OutcomeNotFound:
			return ClassTolerated, "NOT_FOUND within prune-lag tolerance after must_serve_until"
		case o == OutcomeInvalidRows:
			return ClassFault, "returned bytes that do not verify against the blob commitment"
		case (o == OutcomeWrongRows || o == OutcomePartial) && in.CommitmentVerified && in.RowsSubsetOfOwn:
			return ClassUnmatchedGenuine, "returned genuine rows of this blob that this promise does assign this validator, but fewer than it owes; a short shard is not an accusation this observer can make, since the validator signed for whatever it received: held out of the rate, indices on the row"
		case (o == OutcomeWrongRows || o == OutcomePartial) && in.CommitmentVerified:
			return ClassUnmatchedGenuine, "returned genuine rows of this blob, but not the set this promise assigns, and no settled promise over this commitment assigns them; not an accusation the evidence supports under hash-order serving: held out of the rate, indices on the row"
		case o == OutcomeWrongRows || o == OutcomePartial:
			return ClassFault, "returned rows that verify against neither the commitment nor this promise's assignment"
		case o.answeredWrong():
			return ClassTolerated, "server error within prune-lag tolerance after must_serve_until"
		case o == OutcomeThrottled:
			return ClassTolerated, "rate limited within prune-lag tolerance after must_serve_until"
		case o.reachFailure():
			return ClassTolerated, "unreachable within prune-lag tolerance"
		default:
			// An outcome the taxonomy does not name is the observer's gap
			// in every phase: "we have not taught the observer about this"
			// is not evidence, for or against.
			return ClassProbeError, "unrecognised outcome " + string(o) + " in the grace phase; no verdict"
		}

	default: // PhasePost
		switch {
		case o == OutcomeNotFound:
			return ClassExpectedGone, "blob pruned as expected after must_serve_until + tolerance"
		case o == OutcomeServedOK:
			return ClassServedPastWindow, "still serving well after the obligation ended"
		case o == OutcomeInvalidRows:
			return ClassFault, "returned bytes that do not verify against the blob commitment, after the window"
		case o == OutcomeWrongRows:
			// DownloadShard performs no assignment check at all: assignment is
			// enforced only at upload. Accusing a validator of serving rows
			// outside an assignment the download path never enforces, after
			// its obligation has ended, is not a claim the protocol supports.
			return ClassServedPastWindow, "served rows of this blob outside this promise's assignment after the obligation ended; DownloadShard enforces no assignment, so this is not a rule the validator broke"
		case o == OutcomePartial:
			return ClassServedPastWindow, "still serving (part of the shard) after the obligation ended"
		case o.answeredWrong():
			return ClassUnreachablePostWindow, "server error after the obligation ended"
		case o == OutcomeThrottled:
			return ClassUnreachablePostWindow, "rate limited after the obligation ended"
		case o.reachFailure():
			return ClassUnreachablePostWindow, "unreachable after the obligation ended"
		default:
			return ClassProbeError, "unrecognised outcome " + string(o) + " after the window; no verdict"
		}
	}
}
