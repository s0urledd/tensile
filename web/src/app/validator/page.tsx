"use client";
import { Suspense, useCallback, useState, type CSSProperties } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useApi, type ValidatorDetail, type ValidatorReading, type Window, type RecordThrough, type Obligations, type Meta, type EndpointCheck, int, pctOf, bytes, utcWord, dateUTC, whenUTC, shortMid, notFound, rateTone, notCountedText, badRequest, MIN_RATED, API_BASE, provisionalNow, type ProvisionalFaults, type NetworkReference,
  endOfWindow, fullReading, ownGap, attemptsOf, judged, askedTimes, FULL_READ_SINCE_WORDS } from "@/lib/api";
import { useWindow, WindowSwitch, windowLabel, periodName } from "@/lib/window";
import StatusLine from "@/components/StatusLine";
import { PanelFig, Eye } from "@/components/Metrics";
import Warn from "@/components/Warn";
import { unit } from "@/components/Unit";
import { age, monthDayTime } from "@/components/BlobsDeck";
import { openRow } from "@/lib/row";
import Copy from "@/components/Copy";
import Avatar from "@/components/Avatar";
import { endpoint } from "@/components/Validators";
import { HostingFact } from "@/components/Hosting";
import { SELF_VALIDATOR } from "@/lib/site";
import PreLive from "@/components/PreLive";
import Diagnosis from "@/components/Diagnosis";
import { pageAddr } from "@/lib/addr";

type Span = { window: Window; obligations: Obligations; provisional_faults?: ProvisionalFaults };
type Detail = {
  window: Window;
  record_through?: RecordThrough;
  validator: ValidatorDetail;
  windows: Span[];
  recent_probes: ValidatorReading[];
  recent_probes_truncated?: boolean;
  /** set when this window rests partly on the daily rollup (the "all" window past the raw retention) */
  rolled_up?: { raw_from: string; days: number; note: string };
  /** the newest heartbeat against this validator's host, stage by stage */
  last_endpoint_check?: EndpointCheck;
  /** the network's service rate over the same window from the same vantage; absent on a pinned window */
  network_reference?: NetworkReference;
  /** endorsed shards whose retention window has not ended, from the chain's record */
  in_retention_window?: number;
};

/** a fraction as the site prints a share */
const pctFrac = (f: number) => (f >= 1 ? "100%" : `${(f * 100).toFixed(1)}%`);

/** The overview's Endorsements sentence, so the two pages explain the figure one way. */
const ENDORSE_TITLE = "How often this validator’s signature is in the settlement, counted while it had a Fibre provider. A settlement needs signatures from ⅔ of the stake, and the first validators to respond fill it.";

/** the verdict as a word and a mark; the classification is the observer's, never re-derived here */
const WORDS: Record<string, [string, string]> = {
  HEALTHY: ["Served", "ok"], FAULT: ["Not served", "fault"], UNATTESTED: ["Not endorsed", "unsigned"], NOT_PROBED: ["Not read by Tensile", "gone"],
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
 * An address shortened in the middle, as shortMid(s, 18, 6) prints it, in two
 * parts: in a narrow card the head gives way first, so the tail that tells
 * two addresses apart stays in view.
 */
function MidAddr({ s }: { s: string }) {
  if (s.length <= 18 + 6 + 1) return <span className="mono">{s}</span>;
  return <span className="mono mid"><span>{s.slice(0, 18)}</span><span>…{s.slice(-6)}</span></span>;
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
/** what came back, in a few words: the outcome of a FAULT, else the class */
const whatCame = (p: ValidatorReading): string => {
  if (p.classification === "FAULT" || p.classification === "HEALTHY") {
    return ({ NOT_FOUND: "not found", INVALID_ROWS: "rows do not verify", PARTIAL: "short shard", WRONG_ROWS: "wrong rows", SERVED_OK: "served" } as Record<string, string>)[p.outcome]
      ?? p.outcome.toLowerCase().replace(/_/g, " ");
  }
  if (p.outcome === "PARTIAL") return "short shard";
  return wordOf(p.classification)[0].toLowerCase();
};
/**
 * The word for one reading. The observer says what it counts as (service):
 * served; not served (at a full reading, its own rows did not come back, at
 * the reading and each time it was asked again; at an earlier one, they did
 * not come back from a blob that could not be reconstructed); or neither
 * (Tensile's own gap, a blob not read by Tensile, and at an earlier reading
 * a failure on a blob that was available all the same); the page only
 * words it.
 */
const probeWord = (p: ValidatorReading): [string, string] => {
  if (p.service === "not_served") return [`Not served · ${whatCame(p)}`, "fault"];
  if (p.service === "served") return ["Served", "ok"];
  if (p.classification === "UNATTESTED") return (p.outcome === "SERVED_OK" || p.outcome === "PARTIAL") ? ["Served, not endorsed", "unsigned"] : ["Not endorsed", "unsigned"];
  if (p.classification === "NOT_PROBED" || p.classification === "PROBE_ERROR") return wordOf(p.classification);
  const w = whatCame(p);
  return [`${w[0].toUpperCase()}${w.slice(1)} · not counted`, "gone"];
};
const identityWord: Record<string, string> = { verified: "verified", expired: "expired", mismatch: "not this validator’s key", no_tls: "no TLS", unverified: "unverified", unreachable: "unreachable" };

/**
 * What came back, in groups, read off the observer's own service word,
 * classification and wire outcome and never re-judged: not served is what the
 * obligations count as not served, and nothing else. The Not served tab selects
 * on the same judgement as the lane's word, so the two cannot disagree.
 */
const REACH_FAIL = new Set(["DNS_FAIL", "TCP_REFUSED", "TCP_TIMEOUT", "TCP_UNREACHABLE", "TLS_HANDSHAKE_FAIL", "RPC_UNAVAILABLE", "RPC_ERROR", "RPC_TIMEOUT"]);
const GROUPS = ["served", "unreachable", "certificate rejected", "no endpoint", "not found", "answered with an error", "not counted", "other", "not served"] as const;
type Group = (typeof GROUPS)[number];
function groupOf(p: ValidatorReading): Group {
  if (p.service === "not_served") return "not served";
  if (p.service === "served" || p.classification === "HEALTHY" || p.outcome === "SERVED_OK") return "served";
  if (p.classification === "NOT_REGISTERED") return "no endpoint";
  if (p.classification.startsWith("IDENTITY_")) return "certificate rejected";
  if (REACH_FAIL.has(p.outcome)) return "unreachable";
  if (p.outcome === "NOT_FOUND") return "not found";
  if (p.classification === "SERVER_ERROR" || p.classification === "THROTTLED") return "answered with an error";
  return "other";
}
const plural = (n: number, w: string) => `${int(n)} ${w}${n === 1 ? "" : "s"}`;
const lower = (w: string) => w.charAt(0).toLowerCase() + w.slice(1);
/**
 * groups where the validator's side failed and it counted neither way: at a full reading, beside a gap of Tensile's
 * own; at an earlier one, while the blob was available from the others
 */
const HELD = new Set<Group>(["unreachable", "certificate rejected", "no endpoint", "not found", "answered with an error"]);
type Tone = "ok" | "fault" | "hold" | "quiet";
/**
 * One line of the readings: a validator's requests at one reading of a blob (at a full reading the reading's own and
 * any made again after an answer that did not serve), worded by the last that reached the validator, which gives the
 * reason (a request of Tensile's own that failed never speaks for it). made: how many reached it; at: when the reading
 * asked it first.
 */
type Line = { p: ValidatorReading; tries: ValidatorReading[]; made: number; g: Group; full: boolean; at: string };
function linesOf(rows: ValidatorReading[]): Line[] {
  const by = new Map<string, ValidatorReading[]>();
  for (const p of rows) {
    const k = `${p.vantage}|${p.promise_hash}|${p.scheduled_at}`;
    by.set(k, [...(by.get(k) ?? []), p]);
  }
  const out: Line[] = [];
  for (const group of by.values()) {
    const { last, tries } = attemptsOf(group);
    const made = tries.filter((t) => !ownGap(t.classification));
    // what the requests count as together: a later answer replaces an earlier one, and a gap of Tensile's own among
    // them leaves the validator counted neither way (the observer's service word on each says which)
    const p: ValidatorReading = { ...(made[made.length - 1] ?? last), service: judged(tries) || undefined };
    out.push({ p, tries, made: made.length, g: groupOf(p), full: fullReading(last.schedule_label, last.started_at), at: tries[0].started_at });
  }
  // the newest requests are a cut of 50: a reading whose own request fell outside it would show its later attempts
  // alone, so it is left out (a later attempt never follows a request that was not made)
  return out.filter((l) => (l.tries[0].attempt ?? 0) === 0).sort((a, b) => b.at.localeCompare(a.at));
}
/** one request's answer in a few words, for the list of a validator's requests at a reading */
const answerWord = (p: ValidatorReading): string =>
  p.service === "served" || p.outcome === "SERVED_OK" ? "served"
  : p.classification === "NOT_PROBED" ? "not made in time"
  : p.classification === "PROBE_ERROR" ? "Tensile's own error" : whatCame(p);
/**
 * The lane's word for one reading, in the Blobs list's tones: served green,
 * not served red, a failure that did not count amber with its dot (the rows
 * did not come back, beside a gap of Tensile's own at a full reading, or
 * from a blob that was available all the same at an earlier one), and the
 * readings that count neither way for a reason of their own quiet.
 */
function resultOf(p: ValidatorReading, g: Group): { word: string; tone: Tone } {
  if (g === "not served") return { word: `not served · ${whatCame(p)}${p.provisional ? " · provisional" : ""}`, tone: "fault" };
  if (p.service === "served") return { word: "served", tone: "ok" };
  const [w, mk] = probeWord(p);
  if (mk === "unsigned" || p.classification === "NOT_PROBED" || p.classification === "PROBE_ERROR") return { word: lower(w), tone: "quiet" };
  if (HELD.has(g)) return { word: g === "certificate rejected" || g === "answered with an error" ? lower(wordOf(p.classification)[0]) : g, tone: "hold" };
  return { word: lower(w.replace(/ · not counted$/, "")), tone: "quiet" };
}

function Page() {
  const addr = useSearchParams().get("addr") ?? "";
  const [win, setWin] = useWindow("24h");
  const [onlyNotServed, setOnlyNotServed] = useState(false);
  const router = useRouter();
  const onOpen = useCallback((e: React.MouseEvent, href: string) => {
    // a plain click stays in the app; a modified or middle click is the browser's
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  }, [router]);
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
        {notFound(d) && <p className="notice">No validator with the address <span className="mono">{addr}</span> is on record: neither in the staking set nor in any reading. Check the address, or open one from the <Link href="/">overview</Link>.</p>}
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
  const self = !!SELF_VALIDATOR && [v.address, v.cons_address, v.operator_address].some((a) => !!a && a.toLowerCase() === SELF_VALIDATOR);
  // The address the page names the validator by, and its API links carry: the
  // operator address when the staking set names one, with the consensus
  // address under it in the same fact.
  const own = pageAddr(v.operator_address, v.address);
  const cons = v.cons_address || v.address;
  // Readings inside the retention window: the earlier schedule's checks after
  // the deadline count in nothing and stay in the full history. One line per
  // reading of a blob, its requests together, with its outcome group, once:
  // the summary counts them and the filter selects on the same judgement, so
  // the two cannot disagree.
  const grouped = linesOf(data.recent_probes.filter((p) => p.phase === "in_window"));
  const probes = grouped.map((r) => r.p);
  const notServedRows = grouped.filter((r) => r.g === "not served");
  const shown = onlyNotServed ? notServedRows : grouped;
  const lastNotServed = notServedRows[0]?.p;
  const lastServed = grouped.find((r) => r.g === "served")?.p;
  // Every not-served row of the period, for when they are older than the
  // newest rows this page carries: served=no is the obligations' own rule.
  const notServedHref = `${API_BASE}/v1/probes?validator=${own}&served=no${data.window.start ? `&since=${encodeURIComponent(data.window.start)}` : ""}&limit=1000`;
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

  // Readings in this period that did not count although something on the validator's side failed (a wrong
  // certificate, an endpoint that did not answer): beside a gap of Tensile's own at a full reading, or while the blob
  // was available from the others at an earlier one. The rate cannot show them, so its figure carries a dot that names
  // them.
  const since = data.window.start ? Date.parse(data.window.start) : 0;
  const held = grouped.filter((r) => resultOf(r.p, r.g).tone === "hold" && Date.parse(r.p.started_at) >= since);
  const heldBy = new Map<string, number>();
  for (const r of held) { const w = resultOf(r.p, r.g).word; heldBy.set(w, (heldBy.get(w) ?? 0) + 1); }
  const heldWhy = [...heldBy].map(([w, n]) => `${w} (${int(n)})`).join(", ");
  const heldFull = held.filter((r) => r.full).length;
  const heldText = heldFull === held.length ? "The rows did not come back, but one of Tensile’s own requests at the reading failed or was not made in time."
    : heldFull === 0 ? "The rows did not come back, and the blob was available from other validators."
    : `The rows did not come back, but one of Tensile’s own requests at the reading failed or was not made in time, or, before ${FULL_READ_SINCE_WORDS}, the blob was available from other validators.`;
  const tone = o && decided > 0 ? rateTone(o.served, decided) : undefined;
  const now = Date.now();
  const per = periodName(data.window.name ?? win);
  const cells = 4 + ((v.timeouts_enforced ?? 0) > 0 ? 1 : 0);

  return (
    <>
      <div className="head">
        <div>
          <p className="crumb"><Link href={win === "24h" ? "/" : `/?window=${win}`}>Validators</Link> › {v.moniker || shortMid(v.operator_address || cons, 18, 4)}</p>
          <h1><Avatar v={v} />{v.moniker || <span className="mono">{shortMid(v.operator_address || cons, 22, 6)}</span>}{self && <span className="ours" title="Huginn Tech runs both this validator and Tensile. It is measured by the same code as every other validator, never filtered or adjusted.">runs Tensile</span>}</h1>
          <div className="chips">
            <span className="state" title={e.title}><i className={"dot " + e.dot} />{e.word}</span>
            {v.host && <span title={v.identity_reason || "The consensus-key check on the newest handshake."}>TLS identity <b className="word">{identityWord[v.identity_status] ?? v.identity_status}</b></span>}
            {v.provider_since && <span title={`When this validator first appeared as a Fibre provider, whatever endpoint it had then: ${utcWord(v.provider_since)}`}>Fibre provider since <b className="word">{shortDate(v.provider_since)}</b></span>}
          </div>
        </div>
        <WindowSwitch value={win} onChange={setWin} />
      </div>

      {/* who and where: the publisher page's light frame of facts, one row each */}
      <dl className="pb-meta vd-meta">
        {v.host
          ? <><dt>Endpoint</dt><dd><span className="mono">{v.host}</span>{v.endpoint_since && <em>since {shortDate(v.endpoint_since)}</em>}</dd></>
          : v.last_host && <><dt>Last endpoint</dt><dd title="The registration stays on chain; the validator left the bonded provider list."><span className="mono">{v.last_host}</span>{v.endpoint_closed_at && <em>left {dateUTC(v.endpoint_closed_at)}</em>}</dd></>}
        {v.host && v.hosting && <><dt>Hosting</dt><dd><HostingFact h={v.hosting} /></dd></>}
        {v.operator_address && <><dt>Operator</dt><dd className="vd-addr" title={v.operator_address}><MidAddr s={v.operator_address} /><Copy text={v.operator_address} label="operator address" /></dd></>}
        <dt>Consensus</dt><dd className="vd-addr" title={[v.cons_address && `consensus ${v.cons_address}`, `hex ${v.address}`].filter(Boolean).join(" · ")}><MidAddr s={cons} /><Copy text={cons} label="consensus address" /></dd>
        <dt>Links</dt><dd>
          {site && <><a href={site} rel="nofollow noopener noreferrer" target="_blank">{site.replace(/^https?:\/\//, "").replace(/\/$/, "")}</a><em>·</em></>}
          <a href={`${API_BASE}/v1/validators/${own}/feed.atom`} type="application/atom+xml" title="Endpoint changes of this validator, as an Atom feed">Atom feed</a>
        </dd>
      </dl>
      <PreLive meta={meta} />
      <StatusLine meta={meta} metaError={metaErr} snap={{ record_through: data.record_through, window: data.window }} client={{ error: d.error, fetchedAt: d.fetchedAt, status: d.status }} measuring={measuring} />
      {/* the conclusion before the figures; before activation there is nothing to conclude, and StatusLine says so */}
      {!notLive && <Diagnosis v={v} check={data.last_endpoint_check} meta={meta} decided={decided} />}

      {/* the period's figures in two framed panels, as the publisher pages frame theirs: a label and a figure in each
          cell, what qualifies a figure on hover, a dot where something needs a look */}
      <div className="vd-figs">
        <section className="pan vp" id="chain">
          <div className="vp-h"><h2 className="vp-t" title="Read from the chain, nothing measured.">On chain <span className="per">({per})</span></h2></div>
          <dl className="vp-cells" style={{ "--n": cells } as CSSProperties}>
            {sig && sig.assigned > 0
              ? <PanelFig label="Endorsements" value={pctOf(sig.signed, sig.assigned)} title={`${int(sig.signed)} of ${int(sig.assigned)} settlements. ${ENDORSE_TITLE}`} />
              : att && att.blob_coverage.den > 0
                ? <PanelFig label="Endorsements" value={pctOf(att.attested_blobs, att.blob_coverage.den)} title={`${int(att.attested_blobs)} of ${int(att.blob_coverage.den)} blobs. ${ENDORSE_TITLE}`} />
                : <PanelFig label="Endorsements" value="—" className="na" title={`No settlement assigned it rows. ${ENDORSE_TITLE}`} />}
            <PanelFig label="Rows per blob" value={load ? int(load.rows_per_blob) : "—"} className={load ? undefined : "na"}
              title="Rows the assignment gives this validator on the newest settled blob, of every settled blob, by stake. Rows follow stake, not blob size." />
            <PanelFig label="Shard data" value={load && !notLive ? unit(bytes(load.bytes)) : "—"} className={load && !notLive ? undefined : "na"}
              title={`${load ? `${int(load.promises)} endorsed blobs in the period. ` : ""}Row data of the shards this validator stored and endorsed over the period's settled blobs: blob_size / 4096 per row, padding included. The row proofs stored beside them are not counted.`} />
            <PanelFig label="Held now" value={load ? unit(bytes(load.stored_bytes)) : "—"} className={load ? undefined : "na"}
              title="Shard data this validator must hold at this moment: endorsed blobs whose retention window has not ended." />
            {(v.timeouts_enforced ?? 0) > 0 && <PanelFig label="Timeouts reported" value={int(v.timeouts_enforced)}
              title="MsgPaymentPromiseTimeout submitted by this validator’s operator account in the period." />}
          </dl>
        </section>

        <section className="pan vp" id="observed">
          <div className="vp-h">
            <h2 className="vp-t" title="Each blob is read once, 10 minutes before its retention window ends, and every validator that endorsed it is asked for its own rows. A validator is not served when its own rows did not come back, at the reading and each time it was asked again."><Eye />Observed by Tensile <span className="per">({per})</span></h2>
          </div>
          <dl className="vp-cells" style={{ "--n": 5 } as CSSProperties}>
            <PanelFig label="Service rate" className={notLive || decided === 0 ? "na" : tone === "r-bad" ? "bad" : tone === "r-warn" ? "warn" : undefined}
              value={<>{notLive || !o || decided === 0 ? "—" : pctOf(o.served, decided)}{!notLive && held.length > 0 && <Warn text={`${plural(held.length, "reading")} in this period did not count: ${heldWhy}. ${heldText} The readings below show each one.`} />}</>}
              title={notLive ? undefined : !o || o.total === 0 ? ((v.signing?.signed ?? 0) > 0 ? "Not read yet." : "Nothing endorsed in this period.")
                : decided === 0 ? (o.not_counted > 0 ? `Read, none counted: ${notCountedText(o)}.` : "Not read yet.")
                : `${int(o.served)} of ${int(decided)} counted readings served${refText ? `; ${refText}` : ""}. Endorsed shards served, over served plus not served. A shard whose request failed on Tensile’s side or could not be made in time, or whose reading Tensile did not make or that reached no server, counts neither way; before ${FULL_READ_SINCE_WORDS}, so did a shard not asked for, or one that failed on a blob that was available.`} />
            <PanelFig label="Not served" className={notLive ? "na" : (o?.broken ?? 0) > 0 ? "bad" : undefined}
              value={<>{notLive ? "—" : int(o?.broken ?? 0)}{!notLive && prov > 0 && <Warn text={`${int(prov)} of these ${prov === 1 ? "is" : "are"} younger than ${Math.round((v.provisional_faults?.settling_seconds ?? 1800) / 60)} minutes: counted, and final at ${whenUTC(v.provisional_faults!.until)} unless withdrawn.`} />}</>}
              title={`Endorsed shards whose own rows did not come back, at the reading and each time they were asked again. Before ${FULL_READ_SINCE_WORDS}: rows that did not come back from a blob that could not be reconstructed.`} />
            <PanelFig label="In retention window" value={notLive || data.in_retention_window == null ? "—" : int(data.in_retention_window)}
              className={notLive || data.in_retention_window == null ? "na" : undefined}
              title={`Endorsed shards whose retention window has not ended. Each is read 10 minutes before its window ends: the result shows in the readings below at once, and enters the counts when the window closes.${!notLive && notCountedText(o) ? ` Not counted in the period: ${notCountedText(o)}.` : ""}`} />
            <PanelFig label="Reachability" value={bonded && rw && rw.den > 0 ? pctOf(rw.num, rw.den) : "—"} className={bonded && rw && rw.den > 0 ? undefined : "na"}
              title={!bonded ? "Out of the bonded list: not checked." : rw && rw.den > 0 ? `${int(rw.num)} of ${int(rw.den)} handshakes completed with the registered endpoint. Not signing uptime.` : "No handshake yet."} />
            <PanelFig label="Throughput" value={v.serve_bytes_per_second == null ? "—" : unit(`${bytes(v.serve_bytes_per_second)}/s`)} className={v.serve_bytes_per_second == null ? "na" : undefined}
              title={v.serve_bytes_per_second == null
                ? (v.serve_throughput_sample > 0 ? `${int(v.serve_throughput_sample)} of the 3 large-shard downloads it is shown from.` : "No large-shard download yet.")
                : `Median download speed over ${int(v.serve_throughput_sample)} shards of 2 MiB or more.`} />
          </dl>
        </section>
      </div>

      {/* every period at once, beside the endpoint as the newest handshake found it */}
      <div className="vd-band">
        <div className="lg-tw vd-per">
          <table className="vd-pt">
            <thead><tr><th className="p-n">Period</th><th>Served</th><th>Not served</th><th>Service rate</th></tr></thead>
            <tbody>
              {data.windows.map((w) => {
                const wo = w.obligations, wd = wo.served + wo.broken;
                const name = w.window.name;
                const wp = provisionalNow(w.provisional_faults);
                const wt = wd > 0 ? rateTone(wo.served, wd) : undefined;
                return (
                  <tr key={name} className={name === win ? "on" : undefined} onClick={() => setWin(name as typeof win)}
                    title={name === "all" && data.rolled_up ? `Before ${data.rolled_up.raw_from}, from the daily rollup.` : undefined}>
                    <td className="p-n"><button type="button" aria-pressed={name === win} onClick={() => setWin(name as typeof win)} title={`show the ${windowLabel(name)} period`}>{windowLabel(name)}</button></td>
                    <td>{notLive ? "—" : int(wo.served)}</td>
                    <td className={!notLive && wo.broken > 0 ? "bad" : undefined}>{notLive ? "—" : int(wo.broken)}{!notLive && wp > 0 && <Warn text={`${int(wp)} of these ${wp === 1 ? "is" : "are"} still settling: counted, and can still be withdrawn.`} />}</td>
                    <td className={wt === "r-bad" ? "bad" : wt === "r-warn" ? "warn" : undefined}>{notLive || wd === 0 ? "—" : pctOf(wo.served, wd)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <EndpointBox v={v} c={data.last_endpoint_check} lastServed={lastServed} lastNotServed={lastNotServed} olderNotServed={(o?.broken ?? 0) > 0 ? notServedHref : null} />
      </div>

      {/* the readings in the Blobs list's own rows: the whole row opens the blob, and what Tensile found sits in its
          lane at the end */}
      <section id="evidence" className="listing lg-list vd-list">
        <div className="list-head">
          <div className="tabs" role="group" aria-label="readings">
            <button type="button" aria-pressed={!onlyNotServed} onClick={() => setOnlyNotServed(false)}>Readings{probes.length > 0 && <span className="n">{int(probes.length)}</span>}</button>
            {probes.length > 0 && <button type="button" aria-pressed={onlyNotServed} onClick={() => setOnlyNotServed(true)}>Not served <span className="n">{int(notServedRows.length)}</span></button>}
          </div>
          <a className="vd-api" href={onlyNotServed ? notServedHref : `${API_BASE}/v1/probes?validator=${own}&limit=1000`}
            title={data.recent_probes_truncated ? "The newest readings are here; every one is in the API." : "Every reading, in the API."}>Full history →</a>
        </div>
        <div className="lg-tw">
          <table className="lg-t vr-t">
            <thead>
              <tr>
                <th className="c-t">Read <span className="per">(UTC)</span></th>
                <th className="c-b">Blob</th>
                <th className="c-r">Rows</th>
                <th className="c-d">Duration</th>
                <th className="gap" aria-hidden="true" />
                <th className="tn" title="What came back when Tensile read this validator's rows of the blob."><span><Eye />Result</span></th>
              </tr>
            </thead>
            <tbody>
              {probes.length === 0 && <tr className="lg-empty"><td colSpan={6}>No reading of this validator on record yet.</td></tr>}
              {probes.length > 0 && shown.length === 0 && <tr className="lg-empty"><td colSpan={6}>
                No not-served reading among the newest {int(probes.length)}.{(o?.broken ?? 0) > 0 && <> Older ones are <a href={notServedHref}>in the API →</a></>}</td></tr>}
              {shown.map(({ p, g, tries, made, full, at: readAt }) => {
                // the end reading's result shows as soon as it is in; the record takes it when the window closes, and
                // only then can a not-served one be provisional
                const open = endOfWindow(p.schedule_label) && Date.parse(p.scheduled_at) + 10 * 60_000 > now;
                const r = resultOf(open ? { ...p, provisional: false } : p, g);
                const href = `/blob/?hash=${p.promise_hash}`;
                const t = Date.parse(readAt);
                const rows = p.rows_expected ? <><b>{int(p.rows_returned)}</b><span className="u"> / {int(p.rows_expected)}</span></> : "—";
                const ms = <>{int(p.total_duration_ms)}<span className="u"> ms</span></>;
                // a validator asked more than once says so after the word (on a line of its own where the lane is narrow)
                const at = made > 1 ? <span className="at">{askedTimes(made)}</span> : null;
                const notes = [
                  r.tone === "hold" && (full
                    ? (tries.some((x) => ownGap(x.classification))
                      ? "not counted: the rows did not come back, but one of Tensile’s own requests at the reading failed or was not made in time"
                      : "not counted: the rows did not come back, but one of Tensile’s own requests at the reading failed or was not made in time, or no request of the reading reached any server")
                    : "not counted: the rows did not come back, and the blob was available from other validators"),
                  tries.length > 1 && `requests in order: ${tries.map(answerWord).join(", ")}${judged(tries) ? "; the last answer carries the result" : ""}`,
                  open && "the retention window is still open: final when it closes",
                  !endOfWindow(p.schedule_label) && "read on the earlier schedule",
                  `outcome: ${p.outcome.toLowerCase().replace(/_/g, " ")}`,
                  g === "not served" && p.raw_error,
                  r.tone === "hold" && p.raw_error,
                  g === "not served" && p.provisional && !open && "provisional: counted, and can still be withdrawn",
                  p.attested === false && "not endorsed by this validator, so outside the rate",
                  p.retry_first_outcome && `first answer ${p.retry_first_outcome.toLowerCase().replace(/_/g, " ")}, dialled again at once`,
                  p.host_changed && `re-registered during the window: the upload went to ${p.host_at_settlement}`,
                  p.rpc_code && `gRPC ${p.rpc_code}`,
                  p.shadowed_by && `answered from promise ${p.shadowed_by.slice(0, 10)}…`,
                ].filter(Boolean).join(" · ");
                return (
                  <tr key={`${p.vantage}|${p.promise_hash}|${p.scheduled_at}`} className="row"
                    onClick={(ev) => openRow(ev, href, onOpen)} onAuxClick={(ev) => openRow(ev, href, onOpen)}>
                    <td className="c-t"><span title={utcWord(readAt)}><span className="tm">{monthDayTime(readAt)}</span><span className="ag">{age(now - t)}</span></span></td>
                    <td className="c-b"><Link href={href} title={p.promise_hash}>{p.promise_hash.slice(0, 6)}<span className="el">…</span>{p.promise_hash.slice(-4)}</Link></td>
                    <td className="c-r">{rows}</td>
                    <td className="c-d">{ms}</td>
                    <td className="gap" aria-hidden="true" />
                    <td className="tn"><span className={r.tone} title={[p.classification_reason, notes].filter(Boolean).join(" · ")}>{r.word}{at}</span></td>
                    <td className="c-m"><span className={"rs " + r.tone}>{r.word}{at}</span><span className="sep rs-sep">·</span><span>{rows}</span><span className="sep">·</span><span>{ms}</span></td>
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
 * certificate carries this validator's key, and the error it stopped on —
 * with the last served, not served and failed handshake under it, in a framed
 * box of facts beside the periods.
 */
function EndpointBox({ v, c, lastServed, lastNotServed, olderNotServed }: {
  v: ValidatorDetail; c?: EndpointCheck; lastServed?: ValidatorReading; lastNotServed?: ValidatorReading; olderNotServed: string | null;
}) {
  const stages: [string, boolean, string][] = c ? [
    ["DNS", c.dns_ok, ""],
    ["TCP", c.tcp_ok, c.tcp_ok ? `${c.tcp_ms} ms` : ""],
    ["TLS", c.tls_ok, c.tls_ok ? `${c.tls_ms} ms` : ""],
    ["Identity", c.identity_ok, c.identity_ok ? "" : c.identity_reason ?? ""],
  ] : [];
  // stages after the first failure were never reached; say so rather than "failed"
  const first = stages.findIndex(([, ok]) => !ok);
  return (
    <section className="pan vp vd-chk">
      <div className="vp-h">
        <h2 className="vp-t">Endpoint check</h2>
        {c && <span className="vp-at" title={utcWord(c.at)}>{whenUTC(c.at)}</span>}
      </div>
      <dl className="vd-kv">
        {c
          ? <>
            {c.host !== v.host && <><dt>Host</dt><dd><span className="mono">{c.host}</span></dd></>}
            <dt>Handshake</dt>
            <dd className="vd-stages">{stages.map(([name, ok, note], i) => {
              const skip = first >= 0 && i > first;
              return <span key={name} className={ok ? "ok" : skip ? "skip" : "bad"}>{name} <i aria-hidden="true">{ok ? "✓" : skip ? "–" : "✗"}</i>{note && <em>{note}</em>}</span>;
            })}</dd>
            {c.raw_error && <><dt>Error</dt><dd><code>{c.raw_error}</code></dd></>}
          </>
          : <><dt>Handshake</dt><dd><em>no check yet</em></dd></>}
        <dt>Last served</dt><dd>{lastServed ? <b title={utcWord(lastServed.started_at)}>{whenUTC(lastServed.started_at)}</b> : <em>none among the readings below</em>}</dd>
        <dt>Last not served</dt>
        <dd>{lastNotServed
          ? <><b title={utcWord(lastNotServed.started_at)}>{whenUTC(lastNotServed.started_at)}</b><Link className="mono" href={`/blob/?hash=${lastNotServed.promise_hash}`}>{lastNotServed.promise_hash.slice(0, 6)}…{lastNotServed.promise_hash.slice(-4)}</Link></>
          : olderNotServed ? <><em>older than the readings below</em><a href={olderNotServed}>in the API →</a></> : <em>none on record</em>}</dd>
        <dt>Last failed handshake</dt><dd>{v.last_unreachable_at ? <b title={utcWord(v.last_unreachable_at)}>{whenUTC(v.last_unreachable_at)}</b> : <em>none on record</em>}</dd>
      </dl>
    </section>
  );
}
