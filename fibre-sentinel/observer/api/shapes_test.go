package api

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/hosting"
)

// fullRow is a validator row with every field set, so a field the
// projections drop or mangle cannot hide behind a zero value.
func fullRow() validatorRow {
	s := func(v string) *string { return &v }
	n := func(v int64) *int64 { return &v }
	f := func(v float64) *float64 { return &v }
	yes, no := true, false
	return validatorRow{
		Address: "aa", ConsAddress: "celestiavalcons1aa", Moniker: "m", Operator: "celestiavaloper1aa", KeybaseIdentity: "K",
		AvatarURL: "/v1/avatars/K", Website: "https://example.org", Jailed: true, BondStatus: "BOND_STATUS_BONDED", SignaledUpgrade: &yes,
		Host: "h:7980", EndpointSince: s("2026-09-01T00:00:00Z"), ProviderSince: s("2026-08-01T00:00:00Z"), LastHost: "old:7980",
		EndpointClosedAt: s("2026-09-02T00:00:00Z"), VotingPower: 7, LastSeenAt: s("2026-09-03T00:00:00Z"), LastServedAt: s("2026-09-02T12:00:00Z"), Reachable: &no,
		EndpointState: "unreachable", IdentityStatus: "no_tls", IdentityReason: "r", ConfirmedFrom: "de-1", AlsoFailedFrom: "de-2",
		Reachability: rate(3, 4), IdentityValid: rate(1, 3), LastUnreachableAt: s("2026-09-04T00:00:00Z"), LastReachableAt: s("2026-09-05T00:00:00Z"),
		Obligations:  obligationStats{Total: 9, Served: 5, Broken: 1, HeldParamUnverified: 1, NotCounted: 1, Pending: 1, Rate: rate(5, 6)},
		ByObligation: rate(5, 6),
		Attestation:  attestationStats{AttestedBlobs: 2, UnattestedBlobs: 1, UnknownBlobs: 1, BlobCoverage: rate(2, 3)},
		ProbeCount:   4, Classes: classCounts{"HEALTHY": 4}, AssignedRowsLast: 148, ExpectedLoadBand: "floor",
		LatencyP50: n(10), LatencyP95: n(20), LatencySample: 4, BytesPerSecond: n(1000), ThroughputSample: 3,
		AttestedLast: &yes, AssignmentHeight: 99, TimeoutsEnforced: 2,
		Signing: signingStats{Assigned: 4, Signed: 3, Rate: rate(3, 4), Unknown: 1, NoHost: 1, LastEndorsedAt: s("2026-09-06T00:00:00Z"),
			Recent: recentEndorsement{Assigned: 20, Endorsed: 18}},
		Load: loadStats{Promises: 3, Rows: 444, Bytes: 111, StoredBytes: 37, RowsPerBlob: 148},
		Hosting: &hosting.Info{Status: "ok", Host: "h:7980", IP: "1.2.3.4", ASN: 24940, ASOrg: "HETZNER-AS", Country: "DE",
			CountryBasis: "geolocation", City: "Falkenstein", Region: "Saxony", Lat: f(50.4), Lon: f(12.3), Provider: "Hetzner",
			Addresses:  []hosting.Address{{IP: "1.2.3.4", ASN: 24940, Provider: "Hetzner", Connected: true}},
			ResolvedAt: "2026-09-07T00:00:00Z", ResolvedBy: "heartbeat", LookedUpAt: "2026-09-07T00:00:00Z"},
		ProvisionalFaults: &provisionalFaults{Obligations: 1, Until: "2026-09-08T00:00:00Z", SettlingSeconds: 1800, Note: "n"},
	}
}

func jsonMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func keys(m map[string]any) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sameAs checks that every key of got carries the row's value, and that
// got has exactly want's keys.
func sameAs(t *testing.T, what string, got, row map[string]any, want []string) {
	t.Helper()
	sort.Strings(want)
	if k := keys(got); !reflect.DeepEqual(k, want) {
		t.Errorf("%s keys = %v, want %v", what, k, want)
	}
	for k, v := range got {
		if !reflect.DeepEqual(v, row[k]) {
			t.Errorf("%s %s = %v, the row says %v", what, k, v, row[k])
		}
	}
}

// The list and the validator page publish the row's own values under the
// row's own names: a projection leaves fields out and never changes one.
// The list leaves out what only the validator page shows (the heartbeat
// rate over the window, what is held now and the rows per blob), and
// neither publishes signing.no_host, which nothing shows.
func TestValidatorProjectionsCopyTheRow(t *testing.T) {
	row := fullRow()
	whole := jsonMap(t, row)
	// the top-level fields, whole; signing, load and hosting are subsets,
	// checked below
	flat := []string{"address", "cons_address", "moniker", "operator_address", "avatar_url", "jailed", "bond_status",
		"signaled_upgrade", "host", "last_host", "endpoint_closed_at", "voting_power", "last_seen_at", "last_served_at", "reachable", "endpoint_state",
		"identity_status", "identity_reason", "confirmed_from", "also_failed_from", "last_reachable_at",
		"obligations", "provisional_faults"}
	split := func(m map[string]any) (signing, load, hosting map[string]any) {
		signing, load, hosting = m["signing"].(map[string]any), m["load"].(map[string]any), m["hosting"].(map[string]any)
		delete(m, "signing")
		delete(m, "load")
		delete(m, "hosting")
		return
	}

	list := jsonMap(t, listOf(row))
	listSigning, listLoad, listHosting := split(list)
	sameAs(t, "list", list, whole, flat)

	detail := jsonMap(t, detailOf(row))
	detailSigning, detailLoad, detailHosting := split(detail)
	sameAs(t, "detail", detail, whole, append(flat, "reachability_window", "website", "endpoint_since", "provider_since",
		"last_unreachable_at", "attestation", "serve_bytes_per_second", "serve_throughput_sample", "timeouts_enforced"))

	signing, load, hostingRow := whole["signing"].(map[string]any), whole["load"].(map[string]any), whole["hosting"].(map[string]any)
	sameAs(t, "list signing", listSigning, signing, []string{"assigned", "signed", "unknown", "last_endorsed_at"})
	sameAs(t, "detail signing", detailSigning, signing, []string{"assigned", "signed", "unknown", "last_endorsed_at", "recent"})
	sameAs(t, "list load", listLoad, load, []string{"promises", "bytes"})
	sameAs(t, "detail load", detailLoad, load, []string{"promises", "bytes", "stored_bytes", "rows_per_blob"})
	hostingKeys := []string{"status", "host", "ip", "asn", "as_org", "country", "country_basis", "city", "lat", "lon", "provider"}
	for name, m := range map[string]map[string]any{"list": listHosting, "detail": detailHosting} {
		sameAs(t, name+" hosting", m, hostingRow, hostingKeys)
	}

	// The per-address list goes out only where it says something the fields
	// above it do not: when the addresses fall in more than one network.
	row.Hosting.MixedNetworks = true
	mixed := jsonMap(t, listOf(row))["hosting"].(map[string]any)
	sameAs(t, "mixed hosting", mixed, jsonMap(t, row)["hosting"].(map[string]any), append(hostingKeys, "addresses", "mixed_networks"))
}

// A validator that left the set keeps its row in the snapshot (its page, its
// status route and the feeds read it) but is listed only in a window that
// holds something of it: the 24h list every overview viewer re-reads grows
// with the set, not with every validator that ever held an endpoint. One
// with an open endpoint or a bond is listed whatever the window, and each
// figure alone lists a former one.
func TestAFormerValidatorIsListedOnlyWhereItHasFigures(t *testing.T) {
	closedAt := "2026-09-02T00:00:00Z"
	former := validatorRow{Address: "ff", BondStatus: "BOND_STATUS_UNBONDED", LastHost: "old:7980", EndpointClosedAt: &closedAt}
	rows := []validatorRow{
		{Address: "aa", BondStatus: "BOND_STATUS_BONDED"},
		{Address: "bb", Host: "h:7980"},
		former,
	}
	listed := func(rows []validatorRow) []string {
		var out []string
		for _, v := range listedRows(rows) {
			out = append(out, v.Address)
		}
		return out
	}
	if got := listed(rows); !reflect.DeepEqual(got, []string{"aa", "bb"}) {
		t.Fatalf("listed %v, want the bonded and the open one", got)
	}
	figures := map[string]func(v *validatorRow){
		"an obligation":            func(v *validatorRow) { v.Obligations.Total = 1 },
		"a pending obligation":     func(v *validatorRow) { v.Obligations.Pending = 1 },
		"an assignment":            func(v *validatorRow) { v.Signing.Assigned = 1 },
		"one assigned, no host":    func(v *validatorRow) { v.Signing.NoHost = 1 },
		"an endorsement on record": func(v *validatorRow) { v.Attestation.UnknownBlobs = 1 },
		"row data":                 func(v *validatorRow) { v.Load.Promises = 1 },
		"data held now":            func(v *validatorRow) { v.Load.StoredBytes = 1 },
		"a reading":                func(v *validatorRow) { v.ProbeCount = 1 },
		"an endpoint check":        func(v *validatorRow) { v.Reachability = rate(0, 1) },
		"a settling fault":         func(v *validatorRow) { v.ProvisionalFaults = &provisionalFaults{Obligations: 1} },
		"a timeout it enforced":    func(v *validatorRow) { v.TimeoutsEnforced = 1 },
	}
	for what, set := range figures {
		v := former
		set(&v)
		if got := listed([]validatorRow{v}); len(got) != 1 {
			t.Errorf("a former validator with %s in the window is not listed", what)
		}
	}
}
