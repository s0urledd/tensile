"use client";
import Link from "next/link";
import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { type Validator, int, pctOf, ago, utcWord, shortMid, MIN_RATED, provisionalNow, rateTone, loadTitle } from "@/lib/api";
import Avatar from "./Avatar";
import { HostingCell } from "./Hosting";
import { SELF_VALIDATOR } from "@/lib/site";
import { isOperatorAccount } from "@/lib/addr";

/**
 * The validators table: who, whether the endpoint answers right now, how
 * much stake, what happened to the obligations in the selected period, and
 * when the newest evidence is from. Default order is voting power,
 * descending, which is the chain's own order and never a performance rank;
 * the filters are for inspection. Throughput, per-point rates and the raw
 * rows are on the validator's page.
 */

const bonded = (v: Validator) => !v.jailed && (!v.bond_status || v.bond_status === "BOND_STATUS_BONDED");

// Whether a row is the validator this observer's own operator runs. It marks
// and nothing else: no filter, no exclusion, no adjustment.
function isSelf(v: Validator): boolean {
  if (!SELF_VALIDATOR) return false;
  return [v.address, v.cons_address, v.operator_address].some((a) => !!a && a.toLowerCase() === SELF_VALIDATOR);
}

type Filter = "all" | "broken" | "unreachable" | "collecting" | "nohost";
type SortKey = "power" | "load" | "kept" | "broken" | "pending" | "signed" | "seen";

/** the endpoint right now: the chain's own words first, then the newest handshake */
export function endpoint(v: Validator): { dot: string; word: string; title: string; warn?: boolean } {
  if (v.jailed) return { dot: "none", word: "Jailed", title: "Jailed by the chain: out of the bonded provider list, so no handshake is attempted. Shards it signed for are still owed." };
  if (!bonded(v)) return { dot: "none", word: "Not bonded", title: `${v.bond_status!.replace("BOND_STATUS_", "").toLowerCase()} by the chain: out of the bonded provider list, so no handshake is attempted.` };
  if (!v.host) return { dot: "none", word: "No endpoint", title: v.last_host ? `No open Fibre endpoint. Last registered ${v.last_host}; the registration stays on chain.` : "No Fibre endpoint registered in x/valaddr. Not a fault: nothing can be asked of it." };
  if (v.reachable === null) return { dot: "none", word: "Not checked yet", title: `${v.host}: no handshake attempted yet.` };
  const checked = v.last_seen_at ? ` · checked ${ago(v.last_seen_at)}` : "";
  if (v.reachable === false) return { dot: "hold", word: "Unreachable", title: `${v.host}: no TLS handshake in the last two checks${v.last_reachable_at ? `; last reachable ${ago(v.last_reachable_at)}` : ""}${checked}`, warn: true };
  if (v.endpoint_state === "flaky") return { dot: "flaky", word: "Flaky", title: `${v.host}: the last check failed, the one before passed${checked}` };
  if (v.identity_status && v.identity_status !== "verified") {
    const w = v.identity_status === "expired" ? "Certificate expired" : v.identity_status === "mismatch" ? "Wrong certificate" : v.identity_status === "no_tls" ? "No TLS" : "Reachable, unverified";
    return { dot: "hold", word: w, title: v.identity_reason || `${v.host} answered, but its certificate is not one a client would accept. Not a fault.`, warn: true };
  }
  return { dot: "ok", word: "Reachable", title: `${v.host}: TLS with this validator's key${v.confirmed_from ? ", from a second location" : ""}${checked}` };
}

function sortValue(v: Validator, k: SortKey): number | null {
  const o = v.obligations;
  switch (k) {
    case "power": return v.voting_power;
    case "kept": { const d = o ? o.served + o.broken : 0; return d >= MIN_RATED ? o.served / d : null; }
    case "broken": return o?.broken ?? 0;
    case "pending": return o && o.total > 0 ? o.pending : null;
    // Signing is descriptive, and below the sample floor it is not ranked either.
    case "load": return v.load && v.load.rows_per_blob > 0 ? v.load.rows_per_blob : null;
    case "signed": { const s = v.signing; return s && s.assigned >= MIN_RATED ? s.signed / s.assigned : null; }
    case "seen": return v.last_seen_at ? new Date(v.last_seen_at).getTime() : null;
  }
}

export default function Validators({ rows, window: win, notLive, loading }: { rows: Validator[]; window: string; notLive?: boolean; loading?: boolean }) {
  const [q, setQ] = useState("");
  const [filter, setFilter] = useState<Filter>("all");
  const [sort, setSort] = useState<{ key: SortKey; dir: 1 | -1 }>({ key: "power", dir: -1 });
  const router = useRouter();

  const counts = useMemo(() => ({
    all: rows.length,
    broken: rows.filter((v) => (v.obligations?.broken ?? 0) > 0).length,
    unreachable: rows.filter((v) => bonded(v) && !!v.host && v.reachable === false).length,
    collecting: rows.filter((v) => { const o = v.obligations; return !!o && o.total > 0 && o.served + o.broken < MIN_RATED; }).length,
    nohost: rows.filter((v) => bonded(v) && !v.host).length,
  }), [rows]);

  // The hosting column only exists while the lookup is on (observer/hosting):
  // a column of dashes would read as "nobody knows", which is not what off means.
  const showHosting = useMemo(() => rows.some((v) => !!v.hosting), [rows]);
  // The score columns are the product: they stay on screen from the first visit, dashes
  // included, so a reader sees what Tensile measures before the first blob settles.
  const showScores = true;
  const needle = q.trim().toLowerCase();
  const list = useMemo(() => {
    const pool = rows.filter((v) => {
      const o = v.obligations;
      switch (filter) {
        case "broken": return (o?.broken ?? 0) > 0;
        case "unreachable": return bonded(v) && !!v.host && v.reachable === false;
        case "collecting": return !!o && o.total > 0 && o.served + o.broken < MIN_RATED;
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
      const av = sortValue(a, sort.key), bv = sortValue(b, sort.key);
      if (av === null && bv === null) return b.voting_power - a.voting_power;
      if (av === null) return 1;
      if (bv === null) return -1;
      return (av - bv) * sort.dir || b.voting_power - a.voting_power;
    });
  }, [rows, filter, needle, sort]);

  const clickSort = (k: SortKey, dflt: 1 | -1) => setSort((s) => (s.key === k ? { key: k, dir: (s.dir * -1) as 1 | -1 } : { key: k, dir: dflt }));
  const Th = ({ k, dflt, label, title, col }: { k: SortKey; dflt: 1 | -1; label: string; title: string; col: string }) => (
    <th className={"num " + col} title={title}>
      <button type="button" className="sort" aria-pressed={sort.key === k} onClick={() => clickSort(k, dflt)}>
        {label}{sort.key === k && <span className="arrow" aria-hidden="true">{sort.dir === -1 ? "↓" : "↑"}</span>}
      </button>
    </th>
  );
  const href = (v: Validator, hash = "") => `/validator/?addr=${v.address}${win !== "24h" ? `&window=${win}` : ""}${hash}`;

  // The service rate cell, in one form whatever the sample: the share and
  // the counts behind it, so 100% · 9/9 shows its own size. A dash with the
  // reason in its title before anything is assessed. The sample floor only
  // decides ranking (sortValue), never whether the figure is printed.
  const rate = (v: Validator) => {
    const o = v.obligations;
    if (!o || o.total === 0) return <span className="muted" title="No observations: the settled promises prove no serving obligation for this validator in this period.">—</span>;
    const d = o.served + o.broken;
    if (d === 0) return <span className="muted" title={`Awaiting results: ${int(o.total)} obligation${o.total === 1 ? "" : "s"} in this period, none assessed yet (pending or inconclusive).`}>—</span>;
    return <span title={d < MIN_RATED ? `Fewer than ${MIN_RATED} assessed obligations: shown, not ranked.` : undefined}><span className="pair"><span className={"rate " + (rateTone(o.served, d) ?? "")}>{pctOf(o.served, d)}</span><span className="den">{int(o.served)}/{int(d)}</span></span></span>;
  };
  // The signing cell, in the service rate's form: share and counts, a dash
  // with its reason when nothing was assigned. Never a fault colour: a
  // missing signature is the two-thirds quorum closing, not a missed duty.
  const signed = (v: Validator) => {
    const s = v.signing;
    if (!s || s.assigned === 0) return <span className="muted" title={s && s.unknown > 0 ? `${int(s.unknown)} assigned promise${s.unknown === 1 ? "" : "s"} recorded before signatures were verified: nothing to say either way.` : "No settled promise assigned this validator rows in this period."}>—</span>;
    const noHost = s.no_host ? ` ${int(s.no_host)} promise${s.no_host === 1 ? "" : "s"} from before its Fibre host was registered are left out: it could not endorse them.` : "";
    const why = `Endorsed ${int(s.signed)} of ${int(s.assigned)} promises. Publishers stop at ⅔ of stake, so a low rate is normal, not a fault.${noHost}`;
    return <span title={s.assigned < MIN_RATED ? `${why} Fewer than ${MIN_RATED} promises: shown, not ranked.` : why}><span className="pair endorsed"><span className="rate">{pctOf(s.signed, s.assigned)}</span><span className="den">{int(s.signed)}/{int(s.assigned)}</span></span></span>;
  };
  const count = (v: Validator, n: number, kind: "broken" | "pending") => {
    const o = v.obligations;
    if (!o || o.total === 0) return <span className="muted">—</span>;
    if (n === 0) return <span className="muted">0</span>;
    if (kind === "broken") {
      // Counted either way; the badge says how many are still settling.
      const prov = provisionalNow(v.provisional_faults);
      return <><Link className="fault" href={href(v, "#evidence")} title={`${int(n)} obligation${n === 1 ? "" : "s"} broken: the validator answered and did not hand over a shard it had signed for. Opens the evidence.`}>{int(n)}</Link>
        {prov > 0 && <span className="ours" title={`${int(prov)} of these rest only on failed probes younger than the settling period. Counted now; final unless evidence still arriving withdraws them.`}>{prov === n ? "provisional" : `${int(prov)} provisional`}</span>}</>;
    }
    return <Link className="plain" href={href(v, "#outcomes")} title={`${int(n)} obligation${n === 1 ? "" : "s"} whose retention window has not ended: no verdict yet.`}>{int(n)}</Link>;
  };

  return (
    <section>
      <div className="vhead">
        <div><h2>Validators</h2><p className="sub">{notLive ? "Bonded set" : "Reachability and service, selected period"}</p></div>
        <div className="tools">
          <label className="search"><span className="sr-only">Search validators</span><input type="search" placeholder="Search name or address" value={q} onChange={(e) => setQ(e.target.value)} /></label>
          <label className="select"><span className="sr-only">Filter</span>
            <select value={filter} onChange={(e) => setFilter(e.target.value as Filter)}>
              <option value="all">All validators · {counts.all}</option>
              <option value="broken">Broken obligations · {counts.broken}</option>
              <option value="unreachable">Unreachable now · {counts.unreachable}</option>
              <option value="collecting">Small sample · {counts.collecting}</option>
              <option value="nohost">No endpoint · {counts.nohost}</option>
            </select>
          </label>
        </div>
      </div>
      <div className="tablewrap">
        <table className="vt">
          <thead>
            <tr>
              <th className="col-pin">Validator</th>
              <th className="c-ep" title="The newest handshake with the registered endpoint; the chain's own words (jailed, not bonded) come first.">Endpoint now</th>
              {showHosting && <th className="c-host" title="Network provider and country of the endpoint. Hover a cell for the network (AS) and address.">Hosting</th>}
              <Th col="c-power" k="power" dflt={-1} label="Voting power" title="From the staking module. The default order, and never a performance rank." />
              <Th col="c-rows" k="load" dflt={-1} label="Rows / blob" title="Rows of every settled blob this validator is assigned, by stake: its share of the load, to receive and keep for the retention window. From the chain, nothing measured." />
              {showScores && <Th col="c-rate" k="kept" dflt={1} label="Service rate" title="Share of assessed obligations fulfilled in the selected period." />}
              {showScores && <Th col="c-broken" k="broken" dflt={-1} label="Broken" title="Obligations the validator was reached for and did not keep. The only count held against a validator." />}
              {showScores && <Th col="c-pend" k="pending" dflt={-1} label="Pending" title="Obligations whose retention window has not ended: no verdict yet." />}
              {showScores && <Th col="c-end" k="signed" dflt={-1} label="Endorsed ⅔" title="Promises carrying this validator’s endorsement. Publishers stop at ⅔ of stake, so a low rate is normal, not a fault." />}
            </tr>
          </thead>
          <tbody>
            {list.length === 0 && (
              <tr className="empty"><td colSpan={3 + (showHosting ? 1 : 0) + (showScores ? 4 : 0)}>
                {loading && rows.length === 0 ? "Loading…"
                  : rows.length === 0 ? "No validators on record yet."
                  : needle ? `Nothing matches “${q}”.`
                  : filter === "broken" ? "No broken obligation in this period."
                  : filter === "unreachable" ? "Every registered endpoint answered its newest check."
                  : filter === "collecting" ? `Every validator with obligations has ${MIN_RATED} or more assessed.`
                  : "Every bonded validator has registered a Fibre endpoint."}
              </td></tr>
            )}
            {list.map((v) => {
              const e = endpoint(v);
              const o = v.obligations;
              return (
                // A click that lands on a titled figure (above the cover link) opens the page too; links keep their own target.
                <tr key={v.address} className={e.warn ? "warn" : undefined}
                  onClick={(ev) => { if (!(ev.target as HTMLElement).closest("a") && !window.getSelection()?.toString()) router.push(href(v)); }}>
                  <td className="id col-pin">
                    <span className="who">
                      <Avatar v={v} />
                      <span>
                        <Link className="mon" href={href(v)}>{v.moniker || shortMid(v.cons_address || v.address, 18, 4)}</Link>
                        {isSelf(v) && <span className="ours" title="Huginn Tech runs both this validator and Tensile. It is measured by the same code as every other row, never filtered or adjusted.">runs Tensile</span>}
                        {notLive && v.signaled_upgrade === true && <span className="ours" title="Signalled for the app version that brings Fibre (x/signal, a chain record).">signalled</span>}
                        {notLive && v.signaled_upgrade === false && <span className="ours" title="Has not signalled for the app version that brings Fibre (x/signal, a chain record).">not signalled</span>}
                        {/* The operator address is the one operators and delegators know (explorers list it); the consensus address stays in the tooltip and on the validator page. */}
                        <span className="addr mono" title={[v.operator_address, v.cons_address || v.address].filter(Boolean).join(" · ")}>{shortMid(v.operator_address || v.cons_address || v.address, 18, 4)}</span>
                      </span>
                    </span>
                  </td>
                  <td><Link className="rowcover" href={href(v)} tabIndex={-1} aria-hidden="true" /><span className="state" title={e.title}><i className={"dot " + e.dot} />{e.word}</span></td>
                  {showHosting && <td><HostingCell h={v.hosting} /></td>}
                  <td className="num">{int(v.voting_power)}</td>
                  <td className="num">{v.load && v.load.rows_per_blob > 0 ? <span title={loadTitle(v.load)}>{int(v.load.rows_per_blob)}</span> : <span className="muted" title="Not in the validator set of the newest settled blob.">—</span>}</td>
                  {showScores && <>
                    <td className="num">{rate(v)}</td>
                    <td className="num">{count(v, o?.broken ?? 0, "broken")}</td>
                    <td className="num">{count(v, o?.pending ?? 0, "pending")}</td>
                    <td className="num soft-col">{signed(v)}</td>
                  </>}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
