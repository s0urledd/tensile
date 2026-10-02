package api

import (
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/export"
	"github.com/plsgiveup/fibre/fibre-sentinel/observer/hosting"
)

// The shapes the routes publish, where they are not the rows they are built
// from.
//
// A validator's row is computed once per window and kept whole: the
// snapshot, its file on disk, the validator page, the feeds and /v1/hosting
// all read it. What a route publishes is what someone reading that route
// uses. The rest of a row is copies (serve_rate_by_obligation repeats
// obligations.rate, assigned_rows_last load.rows_per_blob, keybase_identity
// the avatar's path), tallies that count the validators a reading never
// needed to ask, or figures only the validator page shows, which the list
// leaves to it.

// networkOut is /v1/network: the network's figures over the window. The
// snapshot keeps more (the reading tallies, attestation, latency, the
// previous span), which the page does not show and the rows of /v1/probes
// state better; publications and their bytes are /v1/market's settlements
// and bytes.
type networkOut struct {
	AsOfNote             string                `json:"as_of_note,omitempty"`
	RolledUp             *rolledUp             `json:"rolled_up,omitempty"`
	RetentionUncertainty *retentionUncertainty `json:"retention_uncertainty,omitempty"`
	Window               Window                `json:"window"`
	ComputedAt           string                `json:"computed_at,omitempty"`
	ComputeMs            int64                 `json:"compute_ms,omitempty"`
	RecordThrough        *recordThrough        `json:"record_through,omitempty"`
	Excluded             []string              `json:"excluded,omitempty"`
	// ExcludedOperators is the operator address of each validator in
	// Excluded the staking set names, keyed by the address in Excluded.
	ExcludedOperators   map[string]string  `json:"excluded_operator_addresses,omitempty"`
	ExcludeNote         string             `json:"exclude_note,omitempty"`
	RegisteredEndpoints int64              `json:"registered_endpoints"`
	Reachability        Rate               `json:"reachability"`
	ReachabilityWindow  Rate               `json:"reachability_window"`
	Obligations         obligationStats    `json:"obligations"`
	Reconstructable     reconstructSummary `json:"reconstructable"`
	ProvisionalFaults   *provisionalFaults `json:"provisional_faults,omitempty"`
}

func networkOutOf(r *networkResponse) networkOut {
	return networkOut{
		AsOfNote: r.AsOfNote, RolledUp: r.RolledUp, RetentionUncertainty: r.RetentionUncertainty, Window: r.Window,
		ComputedAt: r.ComputedAt, ComputeMs: r.ComputeMs, RecordThrough: r.RecordThrough, Excluded: r.Excluded, ExcludeNote: r.ExcludeNote,
		RegisteredEndpoints: r.RegisteredEndpoints, Reachability: r.Reachability, ReachabilityWindow: r.ReachabilityWindow,
		Obligations: r.Obligations, Reconstructable: r.Reconstructable, ProvisionalFaults: r.ProvisionalFaults,
	}
}

// publisherListRow is a publisher as /v1/publishers lists it: the row, with
// its queue as last read but not the read's height and time, which are the
// same on every row and are /v1/market's withdrawal_queue.pending's.
type publisherListRow struct {
	publisherRow
	PendingWithdrawals *pendingQueue `json:"pending_withdrawals"`
}

// publisherList is the rows of /v1/publishers, never nil.
func publisherList(rows []publisherRow) []publisherListRow {
	out := make([]publisherListRow, len(rows))
	for i, p := range rows {
		out[i].publisherRow = p
		if p.PendingWithdrawals != nil {
			q := p.PendingWithdrawals.pendingQueue
			out[i].PendingWithdrawals = &q
		}
	}
	return out
}

// recentBlob is one of a publisher's newest blobs as its page lists them:
// the blob's identity, when it settled, what it was charged and whether it
// was available. The whole row is /v1/blobs/{promise_hash}.
type recentBlob struct {
	PromiseHash        string        `json:"promise_hash"`
	Commitment         string        `json:"commitment"`
	Namespace          string        `json:"namespace"`
	BlobSize           int64         `json:"blob_size"`
	SettlementHeight   int64         `json:"settlement_height"`
	SettlementTime     string        `json:"settlement_time"`
	ValidatorsWithRows int           `json:"validators_with_rows"`
	Charge             *recentCharge `json:"charge"`
	Reconstructable    *recentStatus `json:"reconstructable"`
}

type recentCharge struct {
	FeeUtia int64 `json:"fee_utia"`
}

type recentStatus struct {
	Status string `json:"status"`
}

func recentBlobs(rows []blobRow) []recentBlob {
	out := make([]recentBlob, len(rows))
	for i, b := range rows {
		out[i] = recentBlob{
			PromiseHash: b.PromiseHash, Commitment: b.Commitment, Namespace: b.Namespace, BlobSize: b.BlobSize,
			SettlementHeight: b.SettlementHeight, SettlementTime: b.SettlementTime, ValidatorsWithRows: b.ValidatorsWithRows,
		}
		if b.Charge != nil {
			out[i].Charge = &recentCharge{FeeUtia: b.Charge.FeeUtia}
		}
		if b.Reconstructable != nil {
			out[i].Reconstructable = &recentStatus{Status: b.Reconstructable.Status}
		}
	}
	return out
}

// exportOut is one daily export as /v1/exports lists it: the index entry
// without the rule every entry repeats (the answer states it once), the
// public key the signature block copies from signing.current, and the
// byte ranges of the observer's own source files each member was read
// from. The manifest inside the tarball keeps all of it.
type exportOut struct {
	Name        string            `json:"name"`
	Bytes       int64             `json:"bytes"`
	SHA256      string            `json:"sha256"`
	Day         string            `json:"day"`
	GeneratedAt time.Time         `json:"generated_at"`
	Build       string            `json:"build"`
	Methodology string            `json:"methodology_version,omitempty"`
	Files       []exportMemberOut `json:"files"`
	State       *exportMemberOut  `json:"state,omitempty"`
	Signature   *exportSigOut     `json:"signature,omitempty"`
}

// exportMemberOut is one file inside an export.
type exportMemberOut struct {
	Name        string `json:"name"`
	TimeField   string `json:"time_field"`
	Lines       int64  `json:"lines"`
	LateLines   int64  `json:"late_lines"`
	Bytes       int64  `json:"bytes"`
	SHA256      string `json:"sha256"`
	SkewedLines int64  `json:"skewed_lines,omitempty"`
}

// exportSigOut is the signature over an export's manifest; the key it
// verifies against is /v1/exports/pubkey (signing.current).
type exportSigOut struct {
	Algorithm      string `json:"algorithm"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Message        string `json:"message"`
	Signature      string `json:"signature"`
	KeyFingerprint string `json:"key_fingerprint"`
}

func exportMemberOf(m export.Member) exportMemberOut {
	return exportMemberOut{Name: m.Name, TimeField: m.TimeField, Lines: m.Lines, LateLines: m.LateLines, Bytes: m.Bytes, SHA256: m.SHA256, SkewedLines: m.SkewedLines}
}

// exportList is the entries of /v1/exports, never nil.
func exportList(entries []export.Entry) []exportOut {
	out := make([]exportOut, len(entries))
	for i, e := range entries {
		o := exportOut{
			Name: e.Name, Bytes: e.Bytes, SHA256: e.SHA256, Day: e.Day, GeneratedAt: e.GeneratedAt, Build: e.Build,
			Methodology: e.Methodology, Files: make([]exportMemberOut, len(e.Files)),
		}
		for j, m := range e.Files {
			o.Files[j] = exportMemberOf(m)
		}
		if e.State != nil {
			st := exportMemberOf(*e.State)
			o.State = &st
		}
		if sg := e.Signature; sg != nil {
			o.Signature = &exportSigOut{Algorithm: sg.Algorithm, ManifestSHA256: sg.ManifestSHA256, Message: sg.Message, Signature: sg.Signature, KeyFingerprint: sg.KeyFingerprint}
		}
		out[i] = o
	}
	return out
}

// validatorOut is one validator as /v1/validators lists it.
type validatorOut struct {
	Address          string             `json:"address"`
	ConsAddress      string             `json:"cons_address"`
	Moniker          string             `json:"moniker,omitempty"`
	Operator         string             `json:"operator_address,omitempty"`
	AvatarURL        string             `json:"avatar_url,omitempty"`
	Jailed           bool               `json:"jailed"`
	BondStatus       string             `json:"bond_status,omitempty"`
	SignaledUpgrade  *bool              `json:"signaled_upgrade,omitempty"`
	Host             string             `json:"host"`
	LastHost         string             `json:"last_host,omitempty"`
	EndpointClosedAt *string            `json:"endpoint_closed_at,omitempty"`
	VotingPower      int64              `json:"voting_power"`
	LastSeenAt       *string            `json:"last_seen_at"`
	LastServedAt     *string            `json:"last_served_at"`
	Reachable        *bool              `json:"reachable"`
	EndpointState    string             `json:"endpoint_state,omitempty"`
	IdentityStatus   string             `json:"identity_status"`
	IdentityReason   string             `json:"identity_reason,omitempty"`
	ConfirmedFrom    string             `json:"confirmed_from,omitempty"`
	AlsoFailedFrom   string             `json:"also_failed_from,omitempty"`
	Reachability     Rate               `json:"reachability_window"`
	LastReachableAt  *string            `json:"last_reachable_at"`
	Obligations      obligationStats    `json:"obligations"`
	Signing          signingOut         `json:"signing"`
	Load             loadOut            `json:"load"`
	Hosting          *hostingOut        `json:"hosting,omitempty"`
	Provisional      *provisionalFaults `json:"provisional_faults,omitempty"`
}

// validatorDetailOut is the validator object of /v1/validators/{addr}: the
// list's fields and what only the validator page shows. Attestation is the
// page's Endorsements figure for a record from before signatures were
// counted per settlement (signing.assigned is 0 there).
type validatorDetailOut struct {
	validatorOut
	Website           string           `json:"website,omitempty"`
	EndpointSince     *string          `json:"endpoint_since"`
	ProviderSince     *string          `json:"provider_since,omitempty"`
	LastUnreachableAt *string          `json:"last_unreachable_at"`
	Attestation       attestationStats `json:"attestation"`
	BytesPerSecond    *int64           `json:"serve_bytes_per_second"`
	ThroughputSample  int64            `json:"serve_throughput_sample"`
	TimeoutsEnforced  int64            `json:"timeouts_enforced"`
}

// signingOut is signingStats without its rate, which is signed over
// assigned; Recent only on the validator page.
type signingOut struct {
	Assigned       int64              `json:"assigned"`
	Signed         int64              `json:"signed"`
	Unknown        int64              `json:"unknown"`
	NoHost         int64              `json:"no_host"`
	LastEndorsedAt *string            `json:"last_endorsed_at"`
	Recent         *recentEndorsement `json:"recent,omitempty"`
}

// loadOut is loadStats without the row count, which bytes already sizes.
type loadOut struct {
	Promises    int64 `json:"promises"`
	Bytes       int64 `json:"bytes"`
	StoredBytes int64 `json:"stored_bytes"`
	RowsPerBlob int64 `json:"rows_per_blob"`
}

// hostingOut is the endpoint's hosting as a validator row publishes it:
// the lookup's answer, without when and how the lookup ran, and with the
// per-address list only when the addresses fall in more than one network
// (otherwise it repeats the fields above it).
type hostingOut struct {
	Status        string            `json:"status"`
	Host          string            `json:"host"`
	IP            string            `json:"ip,omitempty"`
	ASN           uint32            `json:"asn,omitempty"`
	ASOrg         string            `json:"as_org,omitempty"`
	Country       string            `json:"country,omitempty"`
	CountryBasis  string            `json:"country_basis,omitempty"`
	City          string            `json:"city,omitempty"`
	Lat           *float64          `json:"lat,omitempty"`
	Lon           *float64          `json:"lon,omitempty"`
	Provider      string            `json:"provider"`
	Addresses     []hosting.Address `json:"addresses,omitempty"`
	MixedNetworks bool              `json:"mixed_networks,omitempty"`
}

func hostingOf(h *hosting.Info) *hostingOut {
	if h == nil {
		return nil
	}
	out := &hostingOut{
		Status: h.Status, Host: h.Host, IP: h.IP, ASN: h.ASN, ASOrg: h.ASOrg, Country: h.Country, CountryBasis: h.CountryBasis,
		City: h.City, Lat: h.Lat, Lon: h.Lon, Provider: h.Provider, MixedNetworks: h.MixedNetworks,
	}
	if h.MixedNetworks {
		out.Addresses = h.Addresses
	}
	return out
}

// listOf is a row as /v1/validators lists it.
func listOf(v validatorRow) validatorOut {
	return validatorOut{
		Address: v.Address, ConsAddress: v.ConsAddress, Moniker: v.Moniker, Operator: v.Operator, AvatarURL: v.AvatarURL,
		Jailed: v.Jailed, BondStatus: v.BondStatus, SignaledUpgrade: v.SignaledUpgrade,
		Host: v.Host, LastHost: v.LastHost, EndpointClosedAt: v.EndpointClosedAt, VotingPower: v.VotingPower, LastSeenAt: v.LastSeenAt,
		LastServedAt: v.LastServedAt,
		Reachable:    v.Reachable, EndpointState: v.EndpointState, IdentityStatus: v.IdentityStatus, IdentityReason: v.IdentityReason,
		ConfirmedFrom: v.ConfirmedFrom, AlsoFailedFrom: v.AlsoFailedFrom, Reachability: v.Reachability, LastReachableAt: v.LastReachableAt,
		Obligations: v.Obligations,
		Signing: signingOut{Assigned: v.Signing.Assigned, Signed: v.Signing.Signed, Unknown: v.Signing.Unknown, NoHost: v.Signing.NoHost,
			LastEndorsedAt: v.Signing.LastEndorsedAt},
		Load:        loadOut{Promises: v.Load.Promises, Bytes: v.Load.Bytes, StoredBytes: v.Load.StoredBytes, RowsPerBlob: v.Load.RowsPerBlob},
		Hosting:     hostingOf(v.Hosting),
		Provisional: v.ProvisionalFaults,
	}
}

// listOfRows is listOf over a snapshot's rows, never nil.
func listOfRows(rows []validatorRow) []validatorOut {
	out := make([]validatorOut, len(rows))
	for i, v := range rows {
		out[i] = listOf(v)
	}
	return out
}

// detailOf is a row as the validator page reads it.
func detailOf(v validatorRow) validatorDetailOut {
	out := validatorDetailOut{
		validatorOut: listOf(v), Website: v.Website, EndpointSince: v.EndpointSince, ProviderSince: v.ProviderSince,
		LastUnreachableAt: v.LastUnreachableAt, Attestation: v.Attestation, BytesPerSecond: v.BytesPerSecond,
		ThroughputSample: v.ThroughputSample, TimeoutsEnforced: v.TimeoutsEnforced,
	}
	recent := v.Signing.Recent
	out.Signing.Recent = &recent
	return out
}

// validatorReading is one of a validator's newest readings as its page
// lists them: the reading's own fields, without the validator's address and
// host, which are the page's (host_at_settlement and host_changed say when
// the upload went elsewhere), and without the build and clock of the run
// that made it, which the exports carry. Vantage and scheduled_at key the
// row.
type validatorReading struct {
	Vantage           string `json:"vantage"`
	PromiseHash       string `json:"promise_hash"`
	Attested          *bool  `json:"attested"`
	ScheduleLabel     string `json:"schedule_label"`
	ScheduledAt       string `json:"scheduled_at"`
	StartedAt         string `json:"started_at"`
	Phase             string `json:"phase"`
	Outcome           string `json:"outcome"`
	Classification    string `json:"classification"`
	Reason            string `json:"classification_reason"`
	RowsReturned      int    `json:"rows_returned"`
	RowsExpected      int    `json:"rows_expected"`
	TotalDurationMS   int64  `json:"total_duration_ms"`
	RawError          string `json:"raw_error,omitempty"`
	RetryFirstOutcome string `json:"retry_first_outcome,omitempty"`
	RPCCode           string `json:"rpc_code,omitempty"`
	ShadowedBy        string `json:"shadowed_by,omitempty"`
	HostAtSettlement  string `json:"host_at_settlement,omitempty"`
	HostChanged       bool   `json:"host_changed,omitempty"`
	Service           string `json:"service,omitempty"`
	Provisional       bool   `json:"provisional,omitempty"`
	Attempt           int    `json:"attempt,omitempty"`
	NextAttemptDue    string `json:"next_attempt_due,omitempty"`
	// RowsSubsetOfAssignment: see probeRow.
	RowsSubsetOfAssignment *bool `json:"rows_subset_of_assignment,omitempty"`
}

func validatorReadings(rows []probeRow) []validatorReading {
	out := make([]validatorReading, len(rows))
	for i, p := range rows {
		out[i] = validatorReading{
			Vantage: p.Vantage, PromiseHash: p.PromiseHash, Attested: p.Attested, ScheduleLabel: p.ScheduleLabel, ScheduledAt: p.ScheduledAt,
			StartedAt: p.StartedAt, Phase: p.Phase, Outcome: p.Outcome, Classification: p.Classification, Reason: p.Reason,
			RowsReturned: p.RowsReturned, RowsExpected: p.RowsExpected, TotalDurationMS: p.TotalDurationMS, RawError: p.RawError,
			RetryFirstOutcome: p.RetryFirstOutcome, RPCCode: p.RPCCode, ShadowedBy: p.ShadowedBy, HostAtSettlement: p.HostAtSettlement,
			HostChanged: p.HostChanged, Service: p.Service, Provisional: p.Provisional, Attempt: p.Attempt,
			NextAttemptDue: p.NextAttemptDue, RowsSubsetOfAssignment: p.RowsSubsetOfAssignment,
		}
	}
	return out
}

// blobReading is one validator's reading of a blob as the blob page lists
// it: what it answered, and the phase, schedule and class the page picks
// each validator's reading by. The assignment beside it already names the
// host, the rows owed and the endorsement; the row indices and their
// digest come with ?rows=1.
type blobReading struct {
	ValidatorAddress string   `json:"validator_address"`
	OperatorAddress  string   `json:"operator_address,omitempty"`
	ScheduleLabel    string   `json:"schedule_label"`
	StartedAt        string   `json:"started_at"`
	Phase            string   `json:"phase"`
	Outcome          string   `json:"outcome"`
	Classification   string   `json:"classification"`
	RowsReturned     int      `json:"rows_returned"`
	RowsExpected     int      `json:"rows_expected"`
	TotalDurationMS  int64    `json:"total_duration_ms"`
	RawError         string   `json:"raw_error,omitempty"`
	RowIndices       []uint32 `json:"row_indices,omitempty"`
	RowsSHA256       string   `json:"rows_sha256,omitempty"`
	RPCCode          string   `json:"rpc_code,omitempty"`
	Service          string   `json:"service,omitempty"`
	Attempt          int      `json:"attempt,omitempty"`
	NextAttemptDue   string   `json:"next_attempt_due,omitempty"`
	// RowsSubsetOfAssignment: see probeRow.
	RowsSubsetOfAssignment *bool `json:"rows_subset_of_assignment,omitempty"`
}

func blobReadings(rows []probeRow) []blobReading {
	out := make([]blobReading, len(rows))
	for i, p := range rows {
		out[i] = blobReading{
			ValidatorAddress: p.ValidatorAddress, OperatorAddress: p.OperatorAddress, ScheduleLabel: p.ScheduleLabel, StartedAt: p.StartedAt, Phase: p.Phase,
			Outcome: p.Outcome, Classification: p.Classification, RowsReturned: p.RowsReturned, RowsExpected: p.RowsExpected,
			TotalDurationMS: p.TotalDurationMS, RawError: p.RawError, RowIndices: p.RowIndices, RowsSHA256: p.RowsSHA256,
			RPCCode: p.RPCCode, Service: p.Service, Attempt: p.Attempt, NextAttemptDue: p.NextAttemptDue,
			RowsSubsetOfAssignment: p.RowsSubsetOfAssignment,
		}
	}
	return out
}
