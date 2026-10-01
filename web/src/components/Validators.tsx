"use client";
import Link from "next/link";
import { useMemo, useState, type ReactNode } from "react";
import { useRouter } from "next/navigation";
import { type Validator, int, pctOf, bytes, ago, utcWord, shortMid } from "@/lib/api";
import { periodName } from "@/lib/window";
import Avatar from "./Avatar";
import Info from "./Info";
import { HostingCell } from "./Hosting";
import { SELF_VALIDATOR } from "@/lib/site";
import { isOperatorAccount, validatorHref } from "@/lib/addr";
import { openRow } from "@/lib/row";

/**
 * The validators table: who, whether the endpoint answers right now, how
 * much stake, and what the chain records of its endorsements in the period:
 * the shard data it stored and endorsed, and how often it endorsed. Default order
 * is voting power, descending, which is the chain's own order and never a
 * performance rank; the filters are for inspection. Serving, throughput and
 * the raw rows are on the validator's page.
 */

const bonded = (v: Validator) => !v.jailed && (!v.bond_status || v.bond_status === "BOND_STATUS_BONDED");

// Whether a row is the validator this observer's own operator runs. It marks
// and nothing else: no filter, no exclusion, no adjustment.
function isSelf(v: Validator): boolean {
  if (!SELF_VALIDATOR) return false;
  return [v.address, v.cons_address, v.operator_address].some((a) => !!a && a.toLowerCase() === SELF_VALIDATOR);
}

type Filter = "all" | "unreachable" | "nohost";
type SortKey = "power" | "shard" | "signed" | "last";

/**
 * A rejected certificate, in the words of the Fibre TLS identity spec
 * (specs/src/fibre_tls_identity.md): the reason a client refuses it. The
 * verifier splits the spec's outside_validity_window into expired and not yet
 * valid; no_certificate is a peer that sent none, before the spec's checks.
 */
function certificate(reason: string | undefined): { word: string; spec: string } {
  switch (reason) {
    case "cert_expired": return { word: "Certificate expired", spec: "outside_validity_window" };
    case "cert_not_yet_valid": return { word: "Certificate not yet valid", spec: "outside_validity_window" };
    case "no_certificate": return { word: "No certificate", spec: "" };
    case undefined: case "": return { word: "Wrong certificate", spec: "" };
    default: return { word: "Wrong certificate", spec: reason };
  }
}

/** the endpoint right now: the chain's own words first, then the newest handshake */
export function endpoint(v: Validator): { dot: string; word: string; title: string; warn?: boolean } {
  if (v.jailed) return { dot: "none", word: "Jailed", title: "Jailed by the chain: out of the bonded provider list, so no handshake is attempted. Shards it signed for are still owed." };
  if (!bonded(v)) return { dot: "none", word: "Not bonded", title: `${v.bond_status!.replace("BOND_STATUS_", "").toLowerCase()} by the chain: out of the bonded provider list, so no handshake is attempted.` };
  if (!v.host) return { dot: "none", word: "No endpoint", title: v.last_host ? `No open Fibre endpoint. Last registered ${v.last_host}; the registration stays on chain.` : "No Fibre provider registered in x/valaddr." };
  if (v.reachable === null) return { dot: "none", word: "Not checked yet", title: `${v.host}: no handshake attempted yet.` };
  const checked = v.last_seen_at ? ` · checked ${ago(v.last_seen_at)}` : "";
  if (v.reachable === false) return { dot: "hold", word: "Unreachable", title: `${v.host}: no TLS handshake in the last two checks${v.last_reachable_at ? `; last reachable ${ago(v.last_reachable_at)}` : ""}${checked}`, warn: true };
  if (v.identity_status === "no_tls") return { dot: "hold", word: "No TLS", title: `${v.host}: answered TCP, but no TLS handshake completed${checked}`, warn: true };
  if (v.identity_status === "expired" || v.identity_status === "mismatch") {
    const c = certificate(v.identity_reason);
    return { dot: "hold", word: c.word, title: `${v.host}: answered TLS with a certificate a client rejects${c.spec ? ` (${c.spec}, Fibre TLS identity)` : ""}${checked}`, warn: true };
  }
  if (v.identity_status && v.identity_status !== "verified") return { dot: "hold", word: "Reachable, unverified", title: `${v.host}: answered TLS; no certificate check recorded yet${checked}`, warn: true };
  // Reachable is dated by the last handshake this location completed (one
  // failed check after it still counts as reachable); a host only a second
  // location reached has no handshake of ours to date.
  const when = v.confirmed_from ? ", reached from a second location" : v.last_reachable_at ? ` · last handshake ${ago(v.last_reachable_at)}` : checked;
  return { dot: "ok", word: "Reachable", title: `${v.host}: TLS with this validator's key${when}` };
}

const time = (s: string | null | undefined) => (s ? new Date(s).getTime() : null);

/** the shard data of the settled blobs it endorsed in the period, as the validator page's Shard data; none before Fibre is live */
const shardBytes = (v: Validator, notLive?: boolean) => (!notLive && v.load && v.load.bytes > 0 ? v.load.bytes : null);

function sortValue(v: Validator, k: SortKey, notLive?: boolean): number | null {
  switch (k) {
    case "power": return v.voting_power;
    // A validator that endorsed nothing in the period stored nothing for it and sorts last.
    case "shard": return shardBytes(v, notLive);
    // Every validator with a settlement in the period is ranked by its share;
    // one with none has no share and sorts last.
    case "signed": { const s = v.signing; return s && s.assigned > 0 ? s.signed / s.assigned : null; }
    case "last": return time(v.signing?.last_endorsed_at);
  }
}

/** `periodSwitch`, the page's period control, sits beside the heading: the period it selects is the Shard data and Endorsements columns', named in their headings */
export default function Validators({ rows, window: win, notLive, loading, periodSwitch }: { rows: Validator[]; window: string; notLive?: boolean; loading?: boolean; periodSwitch?: ReactNode }) {
  const [q, setQ] = useState("");
  const [filter, setFilter] = useState<Filter>("all");
  const [sort, setSort] = useState<{ key: SortKey; dir: 1 | -1 }>({ key: "power", dir: -1 });
  const router = useRouter();
  const go = (_: React.MouseEvent, to: string) => router.push(to);

  const counts = useMemo(() => ({
    all: rows.length,
    unreachable: rows.filter((v) => bonded(v) && !!v.host && v.reachable === false).length,
    nohost: rows.filter((v) => bonded(v) && !v.host).length,
  }), [rows]);

  // The hosting column only exists while the lookup is on (observer/hosting):
  // a column of dashes would read as "nobody knows", which is not what off means.
  const showHosting = useMemo(() => rows.some((v) => !!v.hosting), [rows]);
  const needle = q.trim().toLowerCase();
  const list = useMemo(() => {
    const pool = rows.filter((v) => {
      switch (filter) {
        case "unreachable": return bonded(v) && !!v.host && v.reachable === false;
        case "nohost": return bonded(v) && !v.host;
        default: return true;
      }
    }).filter((v) => !needle
      || v.address.toLowerCase().includes(needle)
      || (v.cons_address ?? "").toLowerCase().includes(needle)
      || (v.moniker ?? "").toLowerCase().includes(needle)
      || (v.operator_address ?? "").toLowerCase().includes(needle)
      || isOperatorAccount(needle, v.operator_address)
      || (v.host || v.last_host || "").toLowerCase().includes(needle)
      || (v.hosting ? [v.hosting.provider, v.hosting.as_org, v.hosting.country, v.hosting.asn ? "as" + v.hosting.asn : ""].join(" ").toLowerCase().includes(needle) : false));
    return [...pool].sort((a, b) => {
      const av = sortValue(a, sort.key, notLive), bv = sortValue(b, sort.key, notLive);
      if (av === null && bv === null) return b.voting_power - a.voting_power;
      if (av === null) return 1;
      if (bv === null) return -1;
      // Equal shares: the one resting on more settlements first (8 of 8 before 1 of 1).
      const more = sort.key === "signed" ? (b.signing?.assigned ?? 0) - (a.signing?.assigned ?? 0) : 0;
      return (av - bv) * sort.dir || more || b.voting_power - a.voting_power;
    });
  }, [rows, filter, needle, sort, notLive]);

  const clickSort = (k: SortKey, dflt: 1 | -1) => setSort((s) => (s.key === k ? { key: k, dir: (s.dir * -1) as 1 | -1 } : { key: k, dir: dflt }));
  // A heading with a definition carries it behind an (i), opened by a click:
  // nothing appears on hover. The (i) sits outside the label's box so the
  // label stays centred over its column. A heading that counts the selected
  // period names it after the label, in its own case: "Shard data (24h)".
  const period = periodName(win);
  const Th = ({ k, dflt, label, title, info, col, per }: { k: SortKey; dflt: 1 | -1; label: string; title?: string; info?: string; col: string; per?: boolean }) => (
    <th className={"num " + col + (info ? " has-i" : "")} title={title}>
      <span className="thi">
        <button type="button" className="sort" aria-pressed={sort.key === k} onClick={() => clickSort(k, dflt)}>
          {label}{per && <> <span className="per">({period})</span></>}{sort.key === k && <span className="arrow" aria-hidden="true">{sort.dir === -1 ? "↓" : "↑"}</span>}
        </button>
        {info && <span className="thi-i"><Info label={label}><p>{info}</p></Info></span>}
      </span>
    </th>
  );
  const href = (v: Validator, hash = "") => validatorHref(v.operator_address, v.address, `${win !== "24h" ? `&window=${win}` : ""}${hash}`);

  // The endorsement share, with the counts behind it in the title; a dash
  // with its reason when nothing was assigned. Never a fault colour.
  const signed = (v: Validator) => {
    const s = v.signing;
    if (!s || s.assigned === 0) return <span className="muted" title={s && s.unknown > 0 ? `${int(s.unknown)} assigned promise${s.unknown === 1 ? "" : "s"} recorded before signatures were verified: nothing to say either way.` : "No settled promise assigned this validator rows in this period."}>—</span>;
    return <span className="rate share endorsed" title={`${int(s.signed)} of ${int(s.assigned)} settled promises endorsed`}>{pctOf(s.signed, s.assigned)}</span>;
  };
  // The shard data its endorsements stand for, with the count of blobs behind it in the title.
  const shard = (v: Validator) => {
    const n = shardBytes(v, notLive);
    if (n === null) return <span className="muted" title="No settled blob endorsed by this validator in this period.">—</span>;
    const p = v.load!.promises;
    return <span title={`${int(p)} endorsed blob${p === 1 ? "" : "s"} in the period`}>{bytes(n)}</span>;
  };
  const last = (v: Validator) => {
    const at = v.signing?.last_endorsed_at;
    return at ? <span title={utcWord(at)}>{ago(at)}</span> : <span className="muted">—</span>;
  };

  return (
    <section>
      <div className="vhead">
        <div>
          <div className="vtitle"><h2>Validators</h2>{periodSwitch}</div>
          <p className="sub">{notLive ? "Bonded set" : "Endorsements over the selected period"}</p>
        </div>
        <div className="tools">
          <label className="search"><span className="sr-only">Search validators</span><input type="search" placeholder="Search name or address" value={q} onChange={(e) => setQ(e.target.value)} /></label>
          <label className="select"><span className="sr-only">Filter</span>
            <select value={filter} onChange={(e) => setFilter(e.target.value as Filter)}>
              <option value="all">All validators · {counts.all}</option>
              <option value="unreachable">Unreachable now · {counts.unreachable}</option>
              <option value="nohost">No endpoint · {counts.nohost}</option>
            </select>
          </label>
        </div>
      </div>
      <div className="tablewrap framed">
        <table className="vt">
          <thead>
            <tr>
              <th className="col-pin">Validator</th>
              <th className="c-ep" title="Whether the validator's Fibre server answered our latest check.">Endpoint now</th>
              {showHosting && <th className="c-host" title="Where the validator's Fibre server is hosted.">Hosting</th>}
              <Th col="c-power" k="power" dflt={-1} label="Voting power" title="The default order. Not a performance ranking." />
              <Th col="c-shard" k="shard" dflt={-1} label="Shard data" per info="Row data of the shards this validator stored and endorsed over the period's settled blobs: blob_size / 4096 per row, padding included. The row proofs stored beside them are not counted." />
              <Th col="c-end" k="signed" dflt={-1} label="Endorsements" per info="How often this validator’s signature is in the settlement, counted while it had a Fibre provider. A settlement needs signatures from ⅔ of the stake, and the first validators to respond fill it." />
              <Th col="c-last" k="last" dflt={-1} label="Last endorsement" info="The last time this validator signed a blob, in any period." />
            </tr>
          </thead>
          <tbody>
            {list.length === 0 && (
              <tr className="empty"><td colSpan={6 + (showHosting ? 1 : 0)}>
                {loading && rows.length === 0 ? "Loading…"
                  : rows.length === 0 ? "No validators on record yet."
                  : needle ? `Nothing matches “${q}”.`
                  : filter === "unreachable" ? "Every registered endpoint answered its newest check."
                  : "Every bonded validator has registered a Fibre provider."}
              </td></tr>
            )}
            {list.map((v) => {
              const e = endpoint(v);
              return (
                // The whole row opens the validator; links keep their own target.
                <tr key={v.address} className={e.warn ? "warn" : undefined}
                  onClick={(ev) => openRow(ev, href(v), go)} onAuxClick={(ev) => openRow(ev, href(v), go)}>
                  <td className="id col-pin">
                    <span className="who">
                      <Avatar v={v} />
                      <span>
                        <Link className="mon" href={href(v)}>{v.moniker || shortMid(v.operator_address || v.cons_address || v.address, 18, 4)}</Link>
                        {isSelf(v) && <span className="ours" title="Huginn Tech runs both this validator and Tensile. It is measured like every other row.">runs Tensile</span>}
                        {notLive && v.signaled_upgrade === true && <span className="ours" title="Signalled for the app version that brings Fibre (x/signal, a chain record).">signalled</span>}
                        {notLive && v.signaled_upgrade === false && <span className="ours" title="Has not signalled for the app version that brings Fibre (x/signal, a chain record).">not signalled</span>}
                        {/* The operator address is the one operators and delegators know (explorers list it); the consensus address stays in the tooltip and on the validator page. */}
                        {/* Without a moniker the name above already is the operator address; the line then gives the consensus address rather than the same one twice. */}
                        {(v.moniker || v.operator_address) && <span className="addr mono" title={[v.operator_address, v.cons_address || v.address].filter(Boolean).join(" · ")}>{shortMid(v.moniker ? (v.operator_address || v.cons_address || v.address) : (v.cons_address || v.address), 18, 4)}</span>}
                      </span>
                    </span>
                  </td>
                  <td><span className="state" title={e.title}><i className={"dot " + e.dot} />{e.word}</span></td>
                  {showHosting && <td><HostingCell h={v.hosting} /></td>}
                  <td className="num fig">{int(v.voting_power)}</td>
                  <td className="num fig">{shard(v)}</td>
                  <td className="num soft-col fig">{signed(v)}</td>
                  <td className="num">{last(v)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
