/**
 * Hosting: the network (origin AS, provider bucket) and country each
 * registered Fibre host resolves into, and how concentrated the set is.
 * Types mirror observer/hosting (per validator, on /v1/validators) and
 * /v1/hosting (the concentration summary with its sources).
 *
 * Everything here is "as resolved from this vantage": the site never turns
 * it into a verdict about an operator.
 */

export type HostingAddress = { ip: string; asn?: number; as_org?: string; country?: string; provider: string; connected?: boolean };

export type Hosting = {
  status: "ok" | "no_asn" | "unresolved";
  host: string;
  ip?: string;
  asn?: number;
  as_org?: string;
  country?: string;
  /** geolocation (DB-IP's estimate) | as_registry (where the AS is registered) */
  country_basis?: "geolocation" | "as_registry";
  /** city-level geolocation, when the collector has a city database: the map places the host here */
  city?: string;
  lat?: number;
  lon?: number;
  provider: string;
  /** every address the host resolved to, sent only when they fall in more than one network */
  addresses?: HostingAddress[];
  mixed_networks?: boolean;
};

export type DBSource = { name: string; url: string; license: string; license_url: string; attribution?: string };

/** the lookup's databases, each with the licence and the attribution its use requires; city_db only with a city file */
export type HostingSources = { enabled: boolean; asn_db?: DBSource; country_db?: DBSource; city_db?: DBSource; looked_up_at?: string };

export type HostingBucket = { key: string; label?: string; provider?: string; hosts: number; host_share: number; stake: number; stake_share: number };

export type Nakamoto = { count: number | null; entities: string[]; share: number; note: string };

export type HostingSummary = {
  registered_hosts: number;
  resolved_hosts: number;
  unresolved_hosts: number;
  total_stake: number;
  resolved_stake: number;
  resolved_stake_share: number;
  by_provider: HostingBucket[];
  by_country: HostingBucket[];
  by_asn: HostingBucket[];
  nakamoto_third: { provider: Nakamoto; asn: Nakamoto; country: Nakamoto };
  basis: "stake" | "hosts";
};

export type HostingResponse = {
  sources: HostingSources;
  summary?: HostingSummary;
  provider_asns: { asn: number; provider: string }[];
  computed_at: string;
};

/** the providers the Foundation Delegation Program names; marked, never judged */
export const FDP_NAMED = new Set(["Hetzner", "OVH"]);

/** "AS24940 HETZNER-AS" */
export function asLabel(h: { asn?: number; as_org?: string }): string {
  if (!h.asn) return "";
  return `AS${h.asn}${h.as_org ? ` ${h.as_org}` : ""}`;
}

/** the one-line tooltip for a validator's hosting cell */
export function hostingTitle(h: Hosting): string {
  if (h.status === "unresolved") return `${h.host}: no recent address on record, so its network is unknown.`;
  const parts = [
    `${h.host} → ${h.ip}`,
    h.asn ? asLabel(h) : "no known network for this address",
    h.country ? `${h.country} (${h.country_basis === "geolocation" ? "geolocation estimate" : "where the network is registered"})` : "",
    h.mixed_networks ? `resolves into ${new Set((h.addresses ?? []).map((a) => a.asn).filter(Boolean)).size} networks; shown: the address Tensile connected to` : "",
  ];
  return parts.filter(Boolean).join(" · ");
}
