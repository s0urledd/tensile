"use client";
import { useApi, int, ago, utcWord } from "@/lib/api";
import { Flag, countryName } from "./Flag";
import { type Hosting, type HostingResponse, type HostingBucket, type Nakamoto, FDP_NAMED, hostingTitle } from "@/lib/hosting";

/**
 * Hosting on the site: a compact cell for the validators table and the
 * concentration panel on the overview. Both read what the collector looked
 * up from local database files (observer/hosting); neither judges anyone.
 * The data sources are credited wherever a figure from them is shown, as
 * DB-IP's licence requires.
 */

/** the validators-table cell: the country's flag and the provider, details in the tooltip */
export function HostingCell({ h }: { h?: Hosting }) {
  if (!h) return <span className="muted" title="Not looked up: no open endpoint, or the lookup has not reached this host yet.">—</span>;
  if (h.status === "unresolved") return <span className="muted" title={hostingTitle(h)}>unresolved</span>;
  const prov = h.status === "no_asn" ? "no network" : h.provider === "Other" ? (h.as_org ? shortOrg(h.as_org) : "Other") : h.provider;
  return (
    <span className="hosting" title={hostingTitle(h)}>
      {h.country ? <Flag cc={h.country} label /> : <span className="flag-none" aria-hidden="true" />}
      <span>{prov}</span>
      {h.mixed_networks && <span className="cc" aria-label="resolves into several networks">+</span>}
    </span>
  );
}

/** "HETZNER-AS" / "EXAMPLE-NET Example B.V." → a short readable handle */
function shortOrg(org: string): string {
  // Registry handles are shouted and suffixed ("SERVETHEWORLD-AS", "AS-30083-US-VELIA-NET",
  // "IS-AS-1"): drop the AS markers and numbers, and bring long all-caps words to a readable
  // case beside the named providers. Short tokens (RL5, IP, IS) stay as they are. The full
  // registry name is in the cell's tooltip.
  let w = (org.split(/\s+/)[0] ?? org).replace(/^AS-/i, "").replace(/-ASN?(-\d+)?$/i, "").replace(/-\d+$/, "");
  w = w.split("-").map((p) => (p.length <= 3 || p !== p.toUpperCase() ? p : p.charAt(0) + p.slice(1).toLowerCase())).join("-");
  if (!w) w = org;
  return w.length > 16 ? w.slice(0, 15) + "…" : w;
}

const pct = (v: number) => (v * 100 >= 10 || v === 0 ? (v * 100).toFixed(0) : (v * 100).toFixed(1)) + "%";

function colorOf(b: HostingBucket, i: number): string {
  if (b.key === "Unknown" || b.key === "") return "var(--pending)";
  if (b.key === "Other") return "var(--cat-other)";
  // Five categorical hues, largest providers first; the rest share one
  // neutral rather than repeat a hue and make two providers look like one.
  return i < 5 ? `var(--cat-${i + 1})` : "var(--neutral)";
}

function StakeBar({ buckets, label }: { buckets: HostingBucket[]; label: string }) {
  let named = 0;
  const colored = buckets.map((b) => {
    const special = b.key === "Other" || b.key === "Unknown" || b.key === "";
    return { b, color: special ? colorOf(b, 0) : colorOf(b, named++) };
  });
  return (
    <div className="bar" role="img" aria-label={`${label}: ` + buckets.map((b) => `${b.key || "unknown"} ${pct(b.stake_share)}`).join(", ")}>
      {colored.filter(({ b }) => b.stake_share > 0).map(({ b, color }) => (
        <i key={b.key || "none"} style={{ width: `${(b.stake_share * 100).toFixed(2)}%`, background: color }} title={`${b.key || "unknown"}: ${pct(b.stake_share)} of stake, ${int(b.hosts)} host${b.hosts === 1 ? "" : "s"}`} />
      ))}
    </div>
  );
}

function nakWord(n: Nakamoto, unit: [string, string]): { value: string; title: string } {
  if (n.count === null) return { value: "—", title: `The identifiable ${unit[1]} never add up to more than a third. ${n.note}.` };
  return { value: int(n.count), title: `${n.entities.join(", ")}: ${pct(n.share)} of stake together. ${n.note}.` };
}

/**
 * The provider / concentration panel. Not rendered on any page for now (taken
 * off the overview on 2026-09-25, kept for a later layout); the API figures
 * behind it (/v1/hosting) are unchanged. Renders nothing while the lookup is
 * off: an empty panel would read as "nothing concentrated", which is not what
 * off means.
 */
export function Concentration() {
  const { data } = useApi<HostingResponse>("/v1/hosting", 120000);
  if (!data || !data.sources.enabled || !data.summary) return null;
  const s = data.summary;
  if (s.registered_hosts === 0) return null;
  const src = data.sources;
  let named = 0;
  const provs = s.by_provider.map((b) => ({ b, color: b.key === "Other" || b.key === "Unknown" ? colorOf(b, 0) : colorOf(b, named++) }));
  const countries = s.by_country.filter((b) => b.key !== "").slice(0, 6);
  const unknownCountry = s.by_country.find((b) => b.key === "");
  const np = nakWord(s.nakamoto_third.provider, ["provider", "providers"]);
  const na = nakWord(s.nakamoto_third.asn, ["network", "networks"]);
  const nc = nakWord(s.nakamoto_third.country, ["country", "countries"]);
  const basis = s.basis === "hosts" ? "hosts" : "stake";

  return (
    <section className="band hostingband" id="hosting">
      <div>
        <h2>Where Fibre hosts run</h2>
        <p className="sub">{int(s.registered_hosts)} registered hosts · share of {basis}</p>
        <StakeBar buckets={s.by_provider} label="Share of stake by provider" />
        <div className="key hkey">
          {provs.map(({ b, color }) => (
            <div key={b.key} title={b.key === "Other" ? "Every network not in the provider list, together. Not one entity." : b.key === "Unknown" ? "No address recorded for the host, or no routed network for it." : undefined}>
              <span className="sw" style={{ background: color }} />{b.key}
              {FDP_NAMED.has(b.key) && <span className="ours" title="The Foundation Delegation Program asks delegation recipients not to run their infrastructure on Hetzner or OVH.">FDP</span>}
              <span className="v">{pct(s.basis === "hosts" ? b.host_share : b.stake_share)}<span className="den"> · {int(b.hosts)}</span></span>
            </div>
          ))}
        </div>
      </div>
      <div>
        <h2>Concentration</h2>
        <p className="sub">Fewest entities holding over ⅓ of registered {basis}</p>
        <div className="nak">
          <div title={np.title}><span className="n">{np.value}</span>provider{np.value === "1" ? "" : "s"}</div>
          <div title={na.title}><span className="n">{na.value}</span>network{na.value === "1" ? "" : "s"} (AS)</div>
          <div title={nc.title}><span className="n">{nc.value}</span>countr{nc.value === "1" ? "y" : "ies"}</div>
        </div>
        <p className="blobs ctry">
          {countries.map((c, i) => <span key={c.key} className={i ? "sepd" : undefined} title={`${countryName(c.key)}: ${int(c.hosts)} host${c.hosts === 1 ? "" : "s"}`}><Flag cc={c.key} />{c.key} <b>{pct(s.basis === "hosts" ? c.host_share : c.stake_share)}</b></span>)}
          {unknownCountry && unknownCountry.hosts > 0 && <span className="sepd soft">unknown {pct(s.basis === "hosts" ? unknownCountry.host_share : unknownCountry.stake_share)}</span>}
        </p>
      </div>
    </section>
  );
}

/** the validator page's hosting fact: the country's flag, provider, network, place */
export function HostingFact({ h }: { h: Hosting }) {
  if (h.status === "unresolved") return <span className="soft" title={hostingTitle(h)}>unresolved</span>;
  const prov = h.provider === "Other" || h.status === "no_asn" ? (h.as_org ? shortOrg(h.as_org) : "unknown network") : h.provider;
  const place = h.country ? countryName(h.country) : "";
  return (
    <span className="vd-hosting" title={hostingTitle(h)}>
      {h.country && <Flag cc={h.country} />}
      {prov}
      {h.asn ? <span className="soft"> · AS{h.asn}</span> : null}
      {/* the place keeps one line; a city that is its country ("Hong Kong") is named once */}
      {h.country && <span className="soft"> · <span className="nw">{h.city && h.city !== place ? `${h.city}, ` : ""}{place}</span></span>}
    </span>
  );
}
