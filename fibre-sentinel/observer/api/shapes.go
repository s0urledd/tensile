package api

import (
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
		Reachable: v.Reachable, EndpointState: v.EndpointState, IdentityStatus: v.IdentityStatus, IdentityReason: v.IdentityReason,
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
}

func validatorReadings(rows []probeRow) []validatorReading {
	out := make([]validatorReading, len(rows))
	for i, p := range rows {
		out[i] = validatorReading{
			Vantage: p.Vantage, PromiseHash: p.PromiseHash, Attested: p.Attested, ScheduleLabel: p.ScheduleLabel, ScheduledAt: p.ScheduledAt,
			StartedAt: p.StartedAt, Phase: p.Phase, Outcome: p.Outcome, Classification: p.Classification, Reason: p.Reason,
			RowsReturned: p.RowsReturned, RowsExpected: p.RowsExpected, TotalDurationMS: p.TotalDurationMS, RawError: p.RawError,
			RetryFirstOutcome: p.RetryFirstOutcome, RPCCode: p.RPCCode, ShadowedBy: p.ShadowedBy, HostAtSettlement: p.HostAtSettlement,
			HostChanged: p.HostChanged, Service: p.Service, Provisional: p.Provisional,
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
}

func blobReadings(rows []probeRow) []blobReading {
	out := make([]blobReading, len(rows))
	for i, p := range rows {
		out[i] = blobReading{
			ValidatorAddress: p.ValidatorAddress, ScheduleLabel: p.ScheduleLabel, StartedAt: p.StartedAt, Phase: p.Phase,
			Outcome: p.Outcome, Classification: p.Classification, RowsReturned: p.RowsReturned, RowsExpected: p.RowsExpected,
			TotalDurationMS: p.TotalDurationMS, RawError: p.RawError, RowIndices: p.RowIndices, RowsSHA256: p.RowsSHA256,
			RPCCode: p.RPCCode, Service: p.Service,
		}
	}
	return out
}
