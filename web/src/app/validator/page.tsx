"use client";
import { Suspense, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useApi, type Validator, type Probe, type SampledOut, type Window, type Rate, type RecordThrough, type Obligations, type ClassCounts, type Meta, type EndpointCheck, int, pctOf, bytes, utcWord, hhmmss, dateUTC, whenUTC, shortMid, notFound, rateTone, leftOutText, badRequest, MIN_RATED, API_BASE, provisionalNow, type ProvisionalFaults, type NetworkReference } from "@/lib/api";
import { useWindow, WindowSwitch, windowLabel } from "@/lib/window";
import StatusLine from "@/components/StatusLine";
import { Metric, Metrics } from "@/components/Metrics";
import Copy from "@/components/Copy";
import Avatar from "@/components/Avatar";
import { endpoint } from "@/components/Validators";
import { HostingFact } from "@/components/Hosting";
import { SELF_VALIDATOR } from "@/lib/site";
import PreLive from "@/components/PreLive";
import Diagnosis from "@/components/Diagnosis";

type Span = { window: Window; serve_rate: Rate; probe_count: number; obligations: Obligations; classes: ClassCounts; provisional_faults?: ProvisionalFaults };
type Detail = {
  window: Window;
  record_through?: RecordThrough;
  validator: Validator;
  windows: Span[];
  recent_probes: Probe[];
  recent_probes_truncated?: boolean;
  /** blobs assigned to this validator that the load policy sampled out: one record each, not a row per point */
  recent_sampled_out?: SampledOut[];
  recent_sampled_out_truncated?: boolean;
  /** set when this window rests partly on the daily rollup (the "all" window past the raw retention) */
  rolled_up?: { raw_from: string; days: number; note: string };
  /** schedule points, over all time, that no rate counts: the observer's own correlated failures */
  suspect_points: { at: string; label: string; reason: string }[];
  /** the newest heartbeat against this validator's host, stage by stage */
  last_endpoint_check?: EndpointCheck;
  /** the network's service rate over the same window from the same vantage; absent on a pinned window */
  network_reference?: NetworkReference;
};

/** a fraction as the site prints a share */
const pctFrac = (f: number) => (f >= 1 ? "100%" : `${(f * 100).toFixed(1)}%`);

/** The overview's Endorsements sentence, so the two pages explain the figure one way. */
const ENDORSE_TITLE = "How often this validator’s signature is in the settlement, counted while it had a Fibre provider. A settlement needs signatures from ⅔ of the stake, and the first validators to respond fill it.";

/** the verdict as a word and a mark; the classification is the observer's, never re-derived here */
const WORDS: Record<string, [string, string]> = {
  HEALTHY: ["Served", "ok"], FAULT: ["Not served", "fault"], UNATTESTED: ["Not endorsed", "unsigned"], NOT_PROBED: ["Not read", "gone"],
  EXPECTED_GONE: ["Expected gone", "gone"], UNREACHABLE: ["Unreachable", "other"], NOT_REGISTERED: ["No endpoint", "none"],
  IDENTITY_EXPIRED: ["Certificate expired", "other"], IDENTITY_MISMATCH: ["Wrong certificate", "other"], THROTTLED: ["Rate limited", "other"],
  SERVER_ERROR: ["Server error", "other"], RETENTION_UNVERIFIED: ["Deadline unverified", "gone"], TOLERATED: ["Tolerated", "other"],
  UNREACHABLE_POST_WINDOW: ["Unreachable after window", "gone"], SERVED_PAST_WINDOW: ["Served after window", "gone"],
  SHADOWED_SHARD: ["Shadowed", "other"], UNMATCHED_GENUINE: ["Unmatched genuine rows", "other"], PROBE_ERROR: ["Read failed", "gone"],
  EXPECTED_UNASSIGNED: ["Unassigned", "gone"], SERVING_UNASSIGNED: ["Serving unassigned", "other"],
};
/** "Sep 25" this year, "Sep 25, 2025" before it: the facts grid has room for one short date */
function shortDate(ts: string): string {
  const d = dateUTC(ts);
  return d.endsWith(`, ${new Date().getUTCFullYear()}`) ? d.slice(0, d.lastIndexOf(",")) : d;
}

/**
 * The website a validator put in its staking description, as a link we are
 * willing to render: anyone can write anything there, so only http(s) URLs
 * pass, and a bare domain ("example.io") gets https:// in front.
 */
function safeSite(raw?: string): string | null {
  const s = (raw ?? "").trim();
  if (/^https?:\/\/[^\s"'<>]+$/i.test(s)) return s;
  if (/^[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}(\/[^\s"'<>]*)?$/i.test(s)) return `https://${s}`;
  return null;
}

const wordOf = (cls: string): [string, string] => WORDS[cls] ?? [cls.toLowerCase().replace(/_/g, " "), "other"];
/** an unsigned probe is not rated either way; the word says what came back, and nothing more */
const probeWord = (p: Probe): [string, string] => {
  // a fault the second location fetched and verified: withdrawn, not counted either way
  if (p.cleared_by) return ["Cleared", "gone"];
  // the end-of-window reading counts as a reader of the chain's client meets it
  if (endNoRows(p)) return [`Not served · ${wordOf(p.classification)[0].toLowerCase()}`, "fault"];
  if (endGenuine(p)) return ["Served, genuine rows", "ok"];
  if (p.classification === "UNATTESTED") return (p.outcome === "SERVED_OK" || p.outcome === "PARTIAL") ? ["Served, not endorsed", "unsigned"] : ["Not endorsed", "unsigned"];
  return wordOf(p.classification);
};
/** the earlier schedule's readings, named in the row's title; the end reading needs no name */
const POINT: Record<string, string> = {
  w1: "w1, at 12% of the window", w2: "w2, at 45%", w3: "w3, at 72%", w4: "w4, within 2 min 30 s of the deadline",
  grace: "grace, 30 s after the deadline", post: "post, 3.5 min after the deadline",
};

/**
 * The end-of-window reading (schedule label "end") counts as a reader of the
 * chain's own client meets the validator: no rows back (no answer, a
 * certificate the client rejects, an error, a rate limit, no endpoint) is
 * not served, genuine rows of the blob are served. The observer publishes
 * the same rule (probe.EndReadClass); this is only its wording.
 */
const END_NO_ROWS = new Set(["UNREACHABLE", "IDENTITY_MISMATCH", "IDENTITY_EXPIRED", "SERVER_ERROR", "THROTTLED", "NOT_REGISTERED"]);
const END_GENUINE = new Set(["SHADOWED_SHARD", "UNMATCHED_GENUINE"]);
const endNoRows = (p: Probe) => p.schedule_label === "end" && END_NO_ROWS.has(p.classification);
const endGenuine = (p: Probe) => p.schedule_label === "end" && END_GENUINE.has(p.classification);
const identityWord: Record<string, string> = { verified: "verified", expired: "expired", mismatch: "not this validator’s key", no_tls: "no TLS", unverified: "unverified", unreachable: "unreachable" };

/**
 * What the newest readings add up to, by what came back.
 *
 * This replaced a line that read "No failed probe among the newest 50 rows"
 * over fifty rows of "tcp refused", next to a Broken figure of six. Both were
 * true in the narrow sense, and together they told an operator their endpoint
 * was fine when it had not answered in a day. So the rows are counted by
 * outcome group, every group that occurs is named, and "not served" is always
 * named, zero included.
 *
 * The groups are read off the observer's own classification and the wire
 * outcome, never re-judged: not served is what the obligations count as not
 * served, outside a suspect point, and nothing else.
 */
const REACH_FAIL = new Set(["DNS_FAIL", "TCP_REFUSED", "TCP_TIMEOUT", "TCP_UNREACHABLE", "TLS_HANDSHAKE_FAIL", "RPC_UNAVAILABLE", "RPC_ERROR"]);
const GROUPS = ["served", "unreachable", "certificate rejected", "no endpoint", "not found, not endorsed", "answered with an error", "not counted", "other", "not served"] as const;
type Group = (typeof GROUPS)[number];
function groupOf(p: Probe, suspect: boolean): Group {
  if (suspect) return "not counted";
  if (p.cleared_by) return "not counted";
  if (p.classification === "FAULT" || endNoRows(p)) return "not served";
  if (p.classification === "HEALTHY" || p.outcome === "SERVED_OK" || endGenuine(p)) return "served";
  if (p.classification === "NOT_REGISTERED") return "no endpoint";
  if (p.classification.startsWith("IDENTITY_")) return "certificate rejected";
  if (REACH_FAIL.has(p.outcome)) return "unreachable";
  if (p.outcome === "NOT_FOUND" && p.classification === "UNATTESTED") return "not found, not endorsed";
  if (p.classification === "SERVER_ERROR" || p.classification === "THROTTLED") return "answered with an error";
  return "other";
}
/** "Newest 50 readings: 43 served, 7 unreachable, 0 not served" */
function evidenceSummary(rows: { g: Group }[]): string {
  const n = new Map<Group, number>();
  for (const r of rows) n.set(r.g, (n.get(r.g) ?? 0) + 1);
  const parts = GROUPS.filter((g) => g === "not served" || (n.get(g) ?? 0) > 0).map((g) => `${int(n.get(g) ?? 0)} ${g}`);
  return `Newest ${int(rows.length)} reading${rows.length === 1 ? "" : "s"}: ${parts.join(", ")}`;
}
/** " · 12 recent blobs sampled out": listed once each, not as a row per point */
function sampledText(d: Detail): string {
  const n = d.recent_sampled_out?.length ?? 0;
  if (n === 0) return "";
  return ` · ${int(n)}${d.recent_sampled_out_truncated ? "+" : ""} recent blob${n === 1 ? "" : "s"} sampled out`;
}

function Page() {
  const addr = useSearchParams().get("addr") ?? "";
  const [win, setWin] = useWindow("24h");
  const [onlyNotServed, setOnlyNotServed] = useState(false);
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  const d = useApi<Detail>(addr ? `/v1/validators/${addr}?window=${win}` : null);
  const notLive = !!meta?.app_version && !meta.fibre_active;
  if (!addr) return <p className="notice">Open a validator from the <Link href="/">overview</Link>, or add <code>?addr=</code> with its consensus, operator (<code>celestiavaloper1…</code>) or account address to the address.</p>;
  const data = d.data;
  if (!data) {
    return (
      <>
        <div className="head"><div><p className="crumb"><Link href="/">Validators</Link> › …</p><h1>{notFound(d) ? "Validator not found" : badRequest(d) ? "Not a validator address" : d.error ? "Validator" : "Loading…"}</h1></div></div>
        <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: d.error, fetchedAt: d.fetchedAt, status: d.status }} />
        {notFound(d) && <p className="notice">No validator with the address <span className="mono">{addr}</span> is on record: neither in the staking set nor in any probe. Check the address, or open one from the <Link href="/">overview</Link>.</p>}
        {badRequest(d) && <p className="notice"><span className="mono">{addr}</span> is not a validator address ({d.error}). Open a validator from the <Link href="/">overview</Link>.</p>}
      </>
    );
  }
  const v = data.validator;
  const o = v.obligations;
  const decided = o ? o.served + o.broken : 0;
  const e = endpoint(v);
  const bonded = !v.jailed && (!v.bond_status || v.bond_status === "BOND_STATUS_BONDED");
  const rw = v.reachability_window;
  // failed readings a second location fetched and verified: withdrawn, in no count
  const cleared = v.faults_cleared ?? 0;
  const clearedText = cleared > 0 ? ` · ${int(cleared)} cleared from a second location` : "";
  const self = !!SELF_VALIDATOR && [v.address, v.cons_address, v.operator_address].some((a) => !!a && a.toLowerCase() === SELF_VALIDATOR);
  const suspect = new Map((data.suspect_points ?? []).map((s) => [s.at, s.reason.replace(",", " and ")]));
  // Readings inside the retention window: the earlier schedule's checks after
  // the deadline (grace, post) count in no rate and stay in the full history.
  const probes = data.recent_probes.filter((p) => p.phase === "in_window").sort((a, b) => b.started_at.localeCompare(a.started_at));
  // Each row with its outcome group, once: the summary counts them and the
  // filter selects on the same judgement, so the two cannot disagree.
  const grouped = probes.map((p) => ({ p, g: groupOf(p, suspect.has(p.scheduled_at)) }));
  const notServedRows = grouped.filter((r) => r.g === "not served");
  const shown = onlyNotServed ? notServedRows : grouped;
  const lastNotServed = notServedRows[0]?.p;
  const lastServed = grouped.find((r) => r.g === "served")?.p;
  // Every not-served row of the period, for when they are older than the
  // newest rows this page carries: served=no is the obligations' own rule.
  const notServedHref = `${API_BASE}/v1/probes?validator=${v.address}&served=no${data.window.start ? `&since=${encodeURIComponent(data.window.start)}` : ""}&limit=1000`;
  const showNotServed = () => setOnlyNotServed(true);
  const measuring = !!o && o.total > 0 && decided < MIN_RATED && o.pending > 0;
  const att = v.attestation;
  const sig = v.signing;
  const load = v.load && v.load.rows_per_blob > 0 ? v.load : null;
  const site = safeSite(v.website);
  // Not-served obligations whose readings are all younger than the settling
  // period: counted, and still able to be withdrawn. provisionalNow drops
  // them once `until` passes, so a cached answer does not keep the badge.
  const prov = provisionalNow(v.provisional_faults);
  const ref = data.network_reference;
  const refText = ref && ref.median_rate != null ? `network median ${pctFrac(ref.median_rate)}` : ref && ref.pooled_rate.den > 0 ? `network ${pctOf(ref.pooled_rate.num, ref.pooled_rate.den)}` : "";

  return (
    <>
      <div className="head">
        <div>
          <p className="crumb"><Link href={win === "24h" ? "/" : `/?window=${win}`}>Validators</Link> › {v.moniker || shortMid(v.cons_address || v.address, 18, 4)}</p>
          <h1><Avatar v={v} />{v.moniker || <span className="mono">{shortMid(v.cons_address || v.address, 22, 6)}</span>}{self && <span className="ours" title="Huginn Tech runs both this validator and Tensile. It is measured by the same code as every other validator, never filtered or adjusted.">runs Tensile</span>}</h1>
          <div className="chips">
            <span className="state" title={e.title}><i className={"dot " + e.dot} />{e.word}</span>
            {v.host && <span title={v.identity_reason || "The consensus-key check on the newest handshake."}>TLS identity <b className="word">{identityWord[v.identity_status] ?? v.identity_status}</b></span>}
          </div>
        </div>
        <WindowSwitch value={win} onChange={setWin} />
      </div>
      <dl className="facts">
        {v.host
          ? <div><dt>Endpoint</dt><dd><span className="mono">{v.host}</span>{v.endpoint_since && <span className="soft"> · since {shortDate(v.endpoint_since)}</span>}</dd></div>
          : v.last_host && <div title="The registration stays on chain; the validator left the bonded provider list."><dt>Last endpoint</dt><dd><span className="mono">{v.last_host}</span>{v.endpoint_closed_at && <span className="soft"> · left {dateUTC(v.endpoint_closed_at)}</span>}</dd></div>}
        {v.host && v.hosting && <div><dt>Hosting</dt><dd><HostingFact h={v.hosting} /></dd></div>}
        {v.operator_address && <div><dt>Operator address</dt><dd title={[v.operator_address, v.cons_address && `consensus ${v.cons_address}`, `hex ${v.address}`].filter(Boolean).join(" · ")}><span className="mono">{shortMid(v.operator_address, 18, 6)}</span><Copy text={v.operator_address} label="operator address" /></dd></div>}
        <div><dt>Links</dt><dd>
          {site && <><a href={site} rel="nofollow noopener noreferrer" target="_blank">{site.replace(/^https?:\/\//, "").replace(/\/$/, "")}</a><span className="soft"> · </span></>}
          <a href={`${API_BASE}/v1/validators/${v.address}/feed.atom`} type="application/atom+xml" title="Endpoint changes of this validator, as an Atom feed">Atom feed</a>
        </dd></div>
      </dl>
      <PreLive meta={meta} />
      <StatusLine meta={meta} metaError={metaErr} snap={{ record_through: data.record_through, window: data.window }} client={{ error: d.error, fetchedAt: d.fetchedAt, status: d.status }} measuring={measuring} />

      <section className="group" id="chain">
        <div className="vhead"><div><h2>On chain</h2><p className="sub">Read from the chain, nothing measured.</p></div></div>
        <Metrics>
          {sig && sig.assigned > 0
            ? <Metric label="Endorsements" value={pctOf(sig.signed, sig.assigned)} help={`${int(sig.signed)} of ${int(sig.assigned)} settlements`} title={ENDORSE_TITLE} />
            : att && att.blob_coverage.den > 0
              ? <Metric label="Endorsements" value={pctOf(att.attested_blobs, att.blob_coverage.den)} help={`${int(att.attested_blobs)} of ${int(att.blob_coverage.den)} blobs`} title={ENDORSE_TITLE} />
              : <Metric label="Endorsements" value="—" tone="absent" help="no settlement assigned it rows" title={ENDORSE_TITLE} />}
          {load && <>
            <Metric label="Rows per blob" value={int(load.rows_per_blob)} help="of every settled blob, by stake"
              title="Rows the assignment gives this validator on the newest settled blob. Rows follow stake, not blob size." />
            <Metric label="Committed" value={notLive ? "—" : bytes(load.bytes)} tone={notLive ? "absent" : undefined}
              help={`${int(load.promises)} endorsed blobs in the period`}
              title="Row data of the settled blobs this validator endorsed in the period: what its signature undertook to store." />
            <Metric label="Held now" value={bytes(load.stored_bytes)} help="retention window still running"
              title="Row data this validator must hold at this moment: endorsed blobs whose retention window has not ended." />
          </>}
          {(v.timeouts_enforced ?? 0) > 0 && <Metric label="Timeouts reported" value={int(v.timeouts_enforced)} help="payment promise timeouts"
            title="MsgPaymentPromiseTimeout submitted by this validator’s operator account in the period." />}
        </Metrics>
      </section>

      <section className="group" id="observed">
        <div className="vhead"><div><h2>Observed by Tensile</h2><p className="sub">Each endorsed shard is read once, near the end of its retention window.</p></div></div>
        {/* the conclusion before the figures; before activation there is nothing to conclude, and StatusLine says so */}
        {!notLive && <Diagnosis v={v} check={data.last_endpoint_check} meta={meta} decided={decided} provisional={prov}
          failedShown={notServedRows.length} onShowFailed={showNotServed} failedHref={notServedHref} />}
        <Metrics>
          <Metric label="Service rate"
            value={notLive || !o || o.total === 0 || decided === 0 ? "—" : pctOf(o.served, decided)}
            tone={notLive || !o || decided === 0 ? "absent" : rateTone(o.served, decided)}
            help={notLive ? " " : !o || o.total === 0 ? "nothing endorsed in this period" : decided === 0 ? "not read yet" : `${int(o.served)} / ${int(decided)} read${refText ? ` · ${refText}` : ""}`}
            title="Endorsed shards served at the end reading, over those read." />
          <Metric label="Not served"
            value={notLive ? "—" : int(o?.broken ?? 0)} tone={notLive ? "absent" : (o?.broken ?? 0) > 0 ? "fault" : !o || o.total === 0 ? "absent" : undefined}
            help={notLive ? " " : (o?.broken ?? 0) > 0 ? (prov > 0 ? `${int(prov)} provisional${clearedText}` : `endorsed shards not read back${clearedText}`) : `none in this period${clearedText}`}
            title={prov > 0 ? `${int(prov)} of these are younger than ${Math.round((v.provisional_faults?.settling_seconds ?? 1800) / 60)} minutes: counted, and final at ${whenUTC(v.provisional_faults!.until)} unless withdrawn.` : "Endorsed shards whose rows did not come back at the end reading."} />
          <Metric label="In retention window" value={notLive ? "—" : int(o?.pending ?? 0)} tone={notLive || !o || o.total === 0 ? "absent" : undefined}
            help={notLive ? " " : (leftOutText(o) || "read at the end of the window")} title="Endorsed shards whose retention window has not ended." />
          <Metric label="Reachability"
            value={!bonded ? "—" : rw && rw.den > 0 ? pctOf(rw.num, rw.den) : "—"}
            tone={!bonded || !rw || rw.den === 0 ? "absent" : undefined}
            help={!bonded ? "out of the bonded list" : rw && rw.den > 0 ? `${int(rw.num)} / ${int(rw.den)} checks` : "no handshake yet"}
            title="Completed handshakes with the registered endpoint, over attempts. Not signing uptime." />
          <Metric label="Throughput"
            value={v.serve_bytes_per_second == null ? "—" : `${bytes(v.serve_bytes_per_second)}/s`} tone={v.serve_bytes_per_second == null ? "absent" : undefined}
            help={v.serve_bytes_per_second == null ? (v.serve_throughput_sample > 0 ? `${int(v.serve_throughput_sample)} of 3 large-shard downloads` : "no large-shard download yet") : `${int(v.serve_throughput_sample)} shards ≥ 2 MiB`}
            title="Median download speed over shards of 2 MiB or more, shown from three." />
        </Metrics>

        <div className="band">
          <div>
            <table className="periods">
              <thead><tr><th>Period</th><th>Service rate</th><th>Not served</th><th>In retention window</th></tr></thead>
              <tbody>
                {data.windows.map((w) => {
                  const wo = w.obligations, wd = wo.served + wo.broken;
                  const name = w.window.name;
                  const wp = provisionalNow(w.provisional_faults);
                  return (
                    <tr key={name} className={name === win ? "on" : undefined}>
                      <td><button type="button" className="rowlink" aria-pressed={name === win} onClick={() => setWin(name as typeof win)} title={`show the ${windowLabel(name)} period`}>{windowLabel(name)}</button></td>
                      <td>{notLive || wd === 0 ? "—" : <><span className={rateTone(wo.served, wd)}>{pctOf(wo.served, wd)}</span><span className="den"> · {int(wo.served)}/{int(wd)}</span></>}</td>
                      <td>{notLive ? "—" : wo.broken > 0 ? <><span className="word fault">{int(wo.broken)}</span>{wp > 0 && <span className="den" title="Counted, and still settling."> · {int(wp)} provisional</span>}</> : "0"}</td>
                      <td>{notLive ? "—" : int(wo.pending)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {data.rolled_up && <p className="rolled">Before {data.rolled_up.raw_from}, from the daily rollup.</p>}
          </div>
          <div>
            {data.last_endpoint_check ? <EndpointCheckLine c={data.last_endpoint_check} /> : <p className="errs">No endpoint check yet.</p>}
            <p className="errs">
              Last served <b>{lastServed ? whenUTC(lastServed.started_at) : "—"}</b>
              <br />Last not served {lastNotServed
                ? <><b>{whenUTC(lastNotServed.started_at)}</b> · blob <Link className="mono" href={`/blob/?hash=${lastNotServed.promise_hash}`}>{lastNotServed.promise_hash.slice(0, 10)}…</Link></>
                : (o?.broken ?? 0) > 0 ? <>older than the readings below · <a href={notServedHref}>in the API →</a></> : <b>none on record</b>}
              <br />Last failed handshake <b>{v.last_unreachable_at ? whenUTC(v.last_unreachable_at) : "none on record"}</b>
            </p>
          </div>
        </div>
      </section>

      <section id="evidence">
        <div className="vhead">
          <div><h2>Readings</h2><p className="sub">{probes.length > 0 ? evidenceSummary(grouped) : "No reading yet"}{data.recent_probes_truncated ? " · the rest in the API" : ""}{sampledText(data)}</p></div>
          <div className="tools">
            {probes.length > 0 && (
              <div className="seg" role="group" aria-label="show rows">
                <button type="button" aria-pressed={!onlyNotServed} onClick={() => setOnlyNotServed(false)}>All <span className="n">{int(probes.length)}</span></button>
                <button type="button" aria-pressed={onlyNotServed} onClick={() => setOnlyNotServed(true)}>Not served <span className="n">{int(notServedRows.length)}</span></button>
              </div>
            )}
            <a className="dis" href={onlyNotServed ? notServedHref : `${API_BASE}/v1/probes?validator=${v.address}&limit=1000`}>Full history →</a>
          </div>
        </div>
        <div className="tablewrap">
          <table className="marks">
            <thead><tr><th>Time</th><th>Blob</th><th>Result</th><th className="num">Rows</th><th className="num">ms</th><th className="go" /></tr></thead>
            <tbody>
              {probes.length === 0 && <tr className="empty"><td colSpan={6}>No reading of this validator on record yet.</td></tr>}
              {probes.length > 0 && shown.length === 0 && <tr className="empty"><td colSpan={6}>
                No not-served reading among the newest {int(probes.length)}.{(o?.broken ?? 0) > 0 && <> Older ones are <a href={notServedHref}>in the API →</a></>}</td></tr>}
              {shown.map(({ p, g }) => {
                const sus = suspect.get(p.scheduled_at);
                // at a point the observer does not trust itself at, nothing is this validator's: no red, no verdict word
                const [word0, mk] = sus ? ["Not counted", "gone"] : probeWord(p);
                // a not-served reading younger than the settling period counts, and can still be withdrawn
                const provisional = !sus && g === "not served" && !!p.provisional;
                const word = provisional ? `${word0} · provisional` : word0;
                const earlier = p.schedule_label !== "end";
                const notes = [
                  earlier && `earlier schedule: ${POINT[p.schedule_label] ?? p.schedule_label}`,
                  `outcome: ${p.outcome.toLowerCase().replace(/_/g, " ")}`,
                  g === "not served" && p.raw_error,
                  provisional && "provisional: counted, and can still be withdrawn",
                  p.attested === false && "not endorsed by this validator, so outside the rate",
                  p.retry_first_outcome && `first attempt ${p.retry_first_outcome}, retried once`,
                  p.host_changed && `re-registered during the window: the upload went to ${p.host_at_settlement}`,
                  p.cleared_by && `cleared: ${p.cleared_by} fetched the same rows minutes later and they verified`,
                  p.confirmed_by && `confirmed: ${p.confirmed_by} did not get the rows either`,
                  p.rpc_code && `gRPC ${p.rpc_code}`,
                  p.shadowed_by && `answered from promise ${p.shadowed_by.slice(0, 10)}…`,
                ].filter(Boolean).join(" · ");
                return (
                  <tr key={`${p.vantage}|${p.promise_hash}|${p.scheduled_at}`} className={sus ? "suspect" : g === "not served" ? "fault-row" : undefined}
                    title={sus ? `At this point ${sus} of the validators read failed at once, which looks like Tensile's own failure: nothing here counts.` : notes}>
                    <td title={utcWord(p.started_at)}>{hhmmss(p.started_at)}<span className="soft"> · {dateUTC(p.started_at)}</span></td>
                    <td><Link className="mono" href={`/blob/?hash=${p.promise_hash}`}>{p.promise_hash.slice(0, 10)}…</Link></td>
                    <td title={p.classification_reason || undefined}><span className={"mk " + mk} /> <span className={"word" + (mk === "fault" ? " fault" : "")}>{word}</span>{earlier && <span className="soft"> · {p.schedule_label}</span>}</td>
                    <td className="num">{p.rows_expected ? `${int(p.rows_returned)} / ${int(p.rows_expected)}` : "—"}</td>
                    <td className="num">{int(p.total_duration_ms)}</td>
                    <td className="go"><Link href={`/blob/?hash=${p.promise_hash}`} aria-label="open the blob">→</Link></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

export default function ValidatorPage() {
  return <Suspense fallback={<p className="crumb" style={{ paddingTop: 22 }}>Loading…</p>}><Page /></Suspense>;
}

/**
 * The newest handshake with this validator's Fibre host, stage by stage:
 * what an operator setting up a server needs, and cannot see from their own
 * machine — whether the name resolves, the port opens, TLS completes and the
 * certificate carries this validator's key, and the error it stopped on.
 */
function EndpointCheckLine({ c }: { c: EndpointCheck }) {
  const stages: [string, boolean, string][] = [
    ["DNS", c.dns_ok, ""],
    ["TCP", c.tcp_ok, c.tcp_ok ? `${c.tcp_ms} ms` : ""],
    ["TLS", c.tls_ok, c.tls_ok ? `${c.tls_ms} ms` : ""],
    ["identity", c.identity_ok, c.identity_ok ? "" : c.identity_reason ?? ""],
  ];
  // stages after the first failure were never reached; say so rather than "failed"
  const first = stages.findIndex(([, ok]) => !ok);
  return (
    <p className="errs endpoint-check">
      Endpoint check <b>{whenUTC(c.at)}</b> · <span className="mono">{c.host}</span>
      <br />
      {stages.map(([name, ok, note], i) => (
        <span key={name} className={"stage " + (ok ? "ok" : first >= 0 && i > first ? "skip" : "bad")}>
          {name} {ok ? "✓" : first >= 0 && i > first ? "–" : "✗"}{note && <> {note}</>}{i < stages.length - 1 && " · "}
        </span>
      ))}
      {c.raw_error && <><br /><code>{c.raw_error}</code></>}
    </p>
  );
}
