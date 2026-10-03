"use client";
import { Fragment, Suspense, useCallback, useEffect, useRef, useState, type CSSProperties } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useApi, type ValidatorDetail, type ValidatorReading, type Window, type RecordThrough, type Obligations, type Meta, type EndpointCheck, int, pctOf, bytes, utcWord, dateUTC, whenUTC, shortMid, notFound, rateTone, notCountedText, badRequest, MIN_RATED, API_BASE, provisionalNow, type ProvisionalFaults, type NetworkReference,
  endOfWindow, fullReading, ownGap, ownSide, sharedAnswer, rawErrorWords, foreignRows, hhmm, attemptsOf, judged, askedTimes, FULL_READ_SINCE_WORDS } from "@/lib/api";
import { useWindow, WindowSwitch, periodName } from "@/lib/window";
import StatusLine from "@/components/StatusLine";
import { Eye } from "@/components/Metrics";
import Warn from "@/components/Warn";
import { unit } from "@/components/Unit";
import { age, monthDayTime } from "@/components/BlobsDeck";
import { openRow } from "@/lib/row";
import { CopyMark } from "@/components/Ledger";
import Avatar from "@/components/Avatar";
import { endpoint } from "@/components/Validators";
import { HostingFact } from "@/components/Hosting";
import { SELF_VALIDATOR } from "@/lib/site";
import PreLive from "@/components/PreLive";
import { diagnose } from "@/components/Diagnosis";
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
  if (foreignRows(p)) return "other rows of the blob";
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
  // nothing owed: whatever an earlier reading got from it, a blob it did not endorse is not served or not
  if (p.classification === "UNATTESTED") return ["Not endorsed", "unsigned"];
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
  : p.classification === "PROBE_ERROR" ? `Tensile's own ${ownSide(p.raw_error) || "error"}`
  // a later attempt answered by another request to the same endpoint, which failed before any blob was asked for
  : sharedAnswer(p.raw_error) ? `same answer as its request for another blob at ${hhmm(p.started_at)}: ${whatCame(p)}`
  : whatCame(p);
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


const cap = (w: string) => w.charAt(0).toUpperCase() + w.slice(1);
/** a blob's hash as the lists print it: the first six and the last four */
const shortHash = (h: string) => `${h.slice(0, 6)}…${h.slice(-4)}`;

/**
 * A host as registered, free to wrap after each dot: a long name breaks
 * between its parts, and the port stays with the last one.
 */
function Host({ s }: { s: string }) {
  const parts = s.split(".");
  return <span className="mono">{parts.map((part, i) => <Fragment key={i}>{i > 0 && <wbr />}{part}{i < parts.length - 1 && "."}</Fragment>)}</span>;
}

/** the addresses' mark: a chevron that turns when they show */
const Chevron = () => (
  <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="m4.5 6.5 3.5 3.5 3.5-3.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
);

/** the checks table shows this many rows at first, and this many more at each "Show more" */
const PAGE = 12;

/**
 * One cell of the latest-checks strip: one reading of a blob, its requests together, worded and toned as its row in
 * the table below (resultOf), so the two cannot disagree. open: the retention window is still open, so the result is
 * not final yet.
 */
type Cell = { k: string; at: string; hash: string; tone: Tone; word: string; made: number; open: boolean };

/**
 * The strip's key: each kind of cell it holds, in this order, with how many. Counted neither way takes the amber of a
 * failure on the validator's side and the grey of a reason of its own; a cell whose window is open is drawn hollow,
 * whatever its colour.
 */
function keyOf(cells: Cell[]): { sw: string[]; word: string; n: number }[] {
  const n = (f: (c: Cell) => boolean) => cells.filter(f).length;
  const hold = n((c) => !c.open && c.tone === "hold"), quiet = n((c) => !c.open && c.tone === "quiet");
  return [
    { sw: ["ok"], word: "Served", n: n((c) => !c.open && c.tone === "ok") },
    { sw: ["fault"], word: "Not served", n: n((c) => !c.open && c.tone === "fault") },
    { sw: [hold > 0 ? "hold" : "", quiet > 0 ? "quiet" : ""].filter(Boolean), word: "Counted neither way", n: hold + quiet },
    { sw: ["quiet open"], word: "In retention window", n: n((c) => c.open) },
  ].filter((k) => k.n > 0);
}

/** the strip's tooltip width, so it can be centred on its cell */
const TIP_W = 216;

/**
 * The latest checks, oldest to newest: one small square per check, coloured by its result, in one line across the
 * frame where there is room for it and in lines of a fixed count where there is not. Not a timeline: the cells are
 * evenly spaced whatever the time between the checks, and nothing marks time along it. A cell names its check (when,
 * which blob, what came back) on hover, on focus and on a tap; a finger can slide along the strip to move from check
 * to check, and the arrow keys do the same.
 */
function Strip({ cells }: { cells: Cell[] }) {
  const refs = useRef<(HTMLButtonElement | null)[]>([]);
  const box = useRef<HTMLDivElement>(null);
  // the tooltip names its check by key, so a refresh that shifts the strip never puts another check's words on it
  const [tip, setTip] = useState<{ k: string; top: number; left: number; below: boolean } | null>(null);
  const [focusK, setFocusK] = useState<string | null>(null);
  // when a finger last slid to another cell: the focus and the click that can end the slide land on the cell it started
  // on, and are dropped
  const slidAt = useRef(0);
  const sliding = () => performance.now() - slidAt.current < 700;
  const tabK = focusK && cells.some((x) => x.k === focusK) ? focusK : cells[cells.length - 1]?.k;
  const show = useCallback((i: number) => {
    const r = refs.current[i]?.getBoundingClientRect();
    if (!r) return;
    // above the cell, clear of the finger that taps it; under it when the strip is near the top of the window and there
    // is no room above
    const below = r.top < 110;
    setTip({ k: cells[i].k, top: below ? r.bottom + 8 : r.top - 8, below,
      left: Math.max(12, Math.min(r.left + r.width / 2 - TIP_W / 2, window.innerWidth - 12 - TIP_W)) });
  }, [cells]);
  const scrub = (e: React.PointerEvent) => {
    if (e.pointerType === "mouse") return;
    const el = (document.elementFromPoint(e.clientX, e.clientY) as HTMLElement | null)?.closest(".vd-c");
    const i = refs.current.findIndex((b) => b === el);
    if (i < 0 || cells[i].k === tip?.k) return;
    slidAt.current = performance.now();
    show(i);
  };
  const hide = useCallback(() => setTip(null), []);
  // closed rather than moved when the page scrolls or the window changes size; a tap elsewhere closes it too
  useEffect(() => {
    if (!tip) return;
    const key = (e: KeyboardEvent) => { if (e.key === "Escape") hide(); };
    const away = (e: PointerEvent) => { if (!box.current?.contains(e.target as Node)) hide(); };
    window.addEventListener("scroll", hide, true);
    window.addEventListener("resize", hide);
    window.addEventListener("keydown", key);
    window.addEventListener("pointerdown", away);
    return () => {
      window.removeEventListener("scroll", hide, true); window.removeEventListener("resize", hide);
      window.removeEventListener("keydown", key); window.removeEventListener("pointerdown", away);
    };
  }, [tip, hide]);
  const move = (e: React.KeyboardEvent, i: number) => {
    const to = e.key === "ArrowLeft" ? i - 1 : e.key === "ArrowRight" ? i + 1 : e.key === "Home" ? 0 : e.key === "End" ? cells.length - 1 : null;
    if (to == null || to < 0 || to >= cells.length) return;
    e.preventDefault();
    refs.current[to]?.focus();
  };
  const c = tip ? cells.find((x) => x.k === tip.k) : undefined;
  return (
    <div ref={box}>
      <div className="vd-strip" role="group" aria-label="Latest checks, oldest to newest"
        onPointerMove={scrub}>
        {cells.map((x, i) => (
          <button key={x.k} ref={(el) => { refs.current[i] = el; }} type="button"
            className={"vd-c " + x.tone + (x.open ? " open" : "") + (tip?.k === x.k ? " on" : "")} tabIndex={x.k === tabK ? 0 : -1}
            aria-label={`${monthDayTime(x.at)} UTC, blob ${shortHash(x.hash)}: ${x.word}${x.made > 1 ? `, ${askedTimes(x.made)}` : ""}${x.open ? ", retention window still open" : ""}`}
            onPointerEnter={(e) => { if (e.pointerType === "mouse") show(i); }}
            onPointerLeave={(e) => { if (e.pointerType === "mouse") hide(); }}
            onFocus={() => { setFocusK(x.k); if (!sliding()) show(i); }} onBlur={() => { if (!sliding()) hide(); }}
            onClick={() => { if (!sliding()) show(i); }} onKeyDown={(e) => move(e, i)} />
        ))}
      </div>
      {c && tip && (
        <div className={"vd-tip" + (tip.below ? " below" : "")} role="tooltip" style={{ top: tip.top, left: tip.left }}>
          <span className="tip-x">{monthDayTime(c.at)} UTC</span>
          <span className="tip-b">Blob <span className="mono">{shortHash(c.hash)}</span></span>
          <span className={"tip-r " + c.tone}>{cap(c.word)}{c.made > 1 && <span className="tip-q"> · {askedTimes(c.made)}</span>}</span>
          {(c.tone === "hold" || c.tone === "quiet") && !c.open && <span className="tip-n">Counted neither way</span>}
          {c.open && <span className="tip-n">The retention window is still open: final when it closes</span>}
        </div>
      )}
    </div>
  );
}

function Page() {
  const addr = useSearchParams().get("addr") ?? "";
  const [win, setWin] = useWindow("24h");
  const [onlyNotServed, setOnlyNotServed] = useState(false);
  // how many rows of the checks table show: a page at first, a page more at each "Show more"
  const [rowsShown, setRowsShown] = useState(PAGE);
  const router = useRouter();
  const onOpen = useCallback((e: React.MouseEvent, href: string) => {
    // a plain click stays in the app; a modified or middle click is the browser's
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  }, [router]);
  // another validator starts at the top of its own list
  useEffect(() => { setRowsShown(PAGE); setOnlyNotServed(false); }, [addr]);
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
  const broken = o?.broken ?? 0;
  const e = endpoint(v);
  const bonded = !v.jailed && (!v.bond_status || v.bond_status === "BOND_STATUS_BONDED");
  const rw = v.reachability_window;
  const self = !!SELF_VALIDATOR && [v.address, v.cons_address, v.operator_address].some((a) => !!a && a.toLowerCase() === SELF_VALIDATOR);
  // The address the page names the validator by, and its API links carry: the
  // operator address when the staking set names one, with the consensus
  // address beside it under Addresses.
  const own = pageAddr(v.operator_address, v.address);
  const cons = v.cons_address || v.address;
  // Readings inside the retention window: the earlier schedule's checks after
  // the deadline count in nothing and stay in the full history. One line per
  // reading of a blob, its requests together, with its outcome group, once:
  // the strip, the summary and the filter all read the same judgement, so
  // they cannot disagree.
  const grouped = linesOf(data.recent_probes.filter((p) => p.phase === "in_window"));
  const probes = grouped.map((r) => r.p);
  const notServedRows = grouped.filter((r) => r.g === "not served");
  const shown = onlyNotServed ? notServedRows : grouped;
  // Every not-served row of the period, for when they are older than the
  // newest rows this page carries: served=no is the obligations' own rule.
  const notServedHref = `${API_BASE}/v1/probes?validator=${own}&served=no${data.window.start ? `&since=${encodeURIComponent(data.window.start)}` : ""}&limit=1000`;
  const measuring = !!o && o.total > 0 && decided < MIN_RATED && o.pending > 0;
  const att = v.attestation;
  const sig = v.signing;
  const load = v.load && v.load.rows_per_blob > 0 ? v.load : null;
  const site = safeSite(v.website);
  // the endpoint's date only when it says something the provider date beside the name does not: a re-registration
  const endpointSince = v.endpoint_since && (!v.provider_since || shortDate(v.endpoint_since) !== shortDate(v.provider_since)) ? v.endpoint_since : null;
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
  const gapText = "each counted neither way: one of Tensile’s requests failed on its side or was not made in time, rows of the blob came back that are not the validator’s own, no request reached any server, or a request Tensile still owed is not on record";
  const heldText = heldFull === held.length ? `Its own rows did not come back, but ${gapText}.`
    : heldFull === 0 ? "The rows did not come back, and the blob was available from other validators."
    : `The rows did not come back, but ${gapText}; or, before ${FULL_READ_SINCE_WORDS}, the blob was available from other validators.`;
  const tone = o && decided > 0 ? rateTone(o.served, decided) : undefined;
  const now = Date.now();
  const per = periodName(data.window.name ?? win);
  // the end reading's result shows as soon as it is in; the record takes it when the window closes, and only then can
  // a not-served one be provisional
  const isOpen = (p: ValidatorReading) => endOfWindow(p.schedule_label) && Date.parse(p.scheduled_at) + 10 * 60_000 > now;
  // the not-served checks this page carries that the period's count holds: when the count is larger, the rest are
  // older than these checks, and the Not served tab says where they are
  const nsInPeriod = notServedRows.filter((r) => !isOpen(r.p) && Date.parse(r.at) >= since).length;

  // the strip: every check this page carries, oldest to newest, worded as its row
  const cells: Cell[] = [...grouped].reverse().map(({ p, g, made, at }) => {
    const op = isOpen(p);
    const r = resultOf(op ? { ...p, provisional: false } : p, g);
    return { k: `${p.vantage}|${p.promise_hash}|${p.scheduled_at}`, at, hash: p.promise_hash, tone: r.tone, word: r.word, made, open: op };
  });
  const key = keyOf(cells);

  // the endpoint as the newest handshake found it, stage by stage; stages after the first failure were never reached
  const c = data.last_endpoint_check;
  const stages: [string, boolean, string][] = c ? [
    ["DNS", c.dns_ok, ""],
    ["TCP", c.tcp_ok, c.tcp_ok ? `${c.tcp_ms} ms` : ""],
    ["TLS", c.tls_ok, c.tls_ok ? `${c.tls_ms} ms` : ""],
    ["Identity", c.identity_ok, c.identity_ok ? "" : c.identity_reason ?? ""],
  ] : [];
  const firstFail = stages.findIndex(([, ok]) => !ok);
  // what state the endpoint is in, in words, when it needs a look: before activation there is nothing to conclude,
  // and StatusLine says so
  const diag = notLive ? null : diagnose(v, c, decided);
  // a bonded validator that never registered an endpoint: the state chip already says so, and the Endpoint fact carries
  // the words on hover and the guide link
  const noEndpoint = !v.host && !v.last_host && bonded;

  // the not-served figure opens its records: the checks table, filtered to them
  const showNotServed = () => {
    setOnlyNotServed(true);
    setRowsShown(PAGE);
    document.getElementById("evidence")?.scrollIntoView({ block: "start" });
  };
  const setFilter = (only: boolean) => { setOnlyNotServed(only); setRowsShown(PAGE); };
  // a page more; focus moves to the blob link of the first row it adds, so it is not lost when the button goes with the
  // last page
  const showMore = () => {
    const first = Math.min(rowsShown, shown.length);
    setRowsShown((n) => n + PAGE);
    requestAnimationFrame(() => document.querySelectorAll<HTMLAnchorElement>("#evidence tr.row .c-b a")[first]?.focus({ preventScroll: true }));
  };

  // signing participation, as the chain records it: settlements signed of those assigned, or before signing was
  // counted per settlement, blobs endorsed
  const endorse = sig && sig.assigned > 0
    ? { n: sig.signed, of: sig.assigned, title: `${int(sig.signed)} of ${int(sig.assigned)} settlements. ${ENDORSE_TITLE}` }
    : att && att.blob_coverage.den > 0
      ? { n: att.attested_blobs, of: att.blob_coverage.den, title: `${int(att.attested_blobs)} of ${int(att.blob_coverage.den)} blobs. ${ENDORSE_TITLE}` }
      : null;
  const timeouts = v.timeouts_enforced ?? 0;
  const reach = bonded && rw && rw.den > 0 ? rw : null;

  const rateCls = notLive || decided === 0 ? "na" : tone === "r-bad" ? "bad" : tone === "r-warn" ? "warn" : "";
  // why the period has no rate, when it has none: under the dash, in the words its title starts with
  const noRate = !o || o.total === 0 ? ((v.signing?.signed ?? 0) > 0 ? "Not read yet" : "Nothing endorsed in this period")
    : o.not_counted > 0 ? "Read, none counted" : "Not read yet";
  const rateTitle = notLive ? undefined : (!o || o.total === 0 ? ((v.signing?.signed ?? 0) > 0 ? "Not read yet." : "Nothing endorsed in this period.")
    : decided === 0 ? (o.not_counted > 0 ? `Read, none counted: ${notCountedText(o)}.` : "Not read yet.")
    : `${int(o.served)} of ${int(decided)} counted readings served${refText ? `; ${refText}` : ""}. Endorsed shards served, over served plus not served. A shard counts neither way when one of its requests failed on Tensile’s side or could not be made in time, an answer was rows of the blob that are not the validator’s own, a request Tensile still owed is not on record, or its reading was not made or reached no server; before ${FULL_READ_SINCE_WORDS}, so did a shard not asked for, or one that failed on a blob that was available.`)
    + (data.rolled_up ? ` Before ${data.rolled_up.raw_from}, from the daily rollup.` : "");
  const visible = shown.slice(0, rowsShown);

  return (
    <>
      <div className="vd-head">
        <p className="crumb"><Link href={win === "24h" ? "/" : `/?window=${win}`}>Validators</Link> › {v.moniker || shortMid(v.operator_address || cons, 18, 4)}</p>
        <WindowSwitch value={win} onChange={setWin} />
      </div>

      {/* who it is and where it serves from, in one frame: the name and its state on the left, the endpoint, its
          hosting and the newest check on the right, the addresses folded away under them */}
      <section className="pan vd-pf" aria-label="Validator">
        <div className="vd-pf-main">
          <div className="vd-who">
            <Avatar v={v} />
            <div className="vd-who-t">
              <h1>{v.moniker || <span className="mono">{shortMid(v.operator_address || cons, 22, 6)}</span>}{self && <span className="ours" title="Huginn Tech runs both this validator and Tensile. It is measured by the same code as every other validator, never filtered or adjusted.">runs Tensile</span>}</h1>
              <div className="chips vd-chips">
                <span className="state" title={e.title}><i className={"dot " + e.dot} />{e.word}</span>
                {v.host && <span title={v.identity_reason || "The consensus-key check on the newest handshake."}>TLS identity <b className="word">{identityWord[v.identity_status] ?? v.identity_status}</b></span>}
              </div>
              {/* a state to act on carries the amber dot with its words; one that is not a fault (jailed, unbonded,
                  not checked yet) says its words on hover, without the dot */}
              {diag && diag.tone !== "ok" && !noEndpoint && (
                <p className={"vd-diag " + diag.tone} title={diag.tone === "none" ? diag.text : undefined}>
                  <span>{diag.title}</span>{diag.tone === "hold" && <Warn text={diag.text} />}
                  {diag.docs && <a href={diag.docs.href} rel="noopener noreferrer" target="_blank">{diag.docs.word} →</a>}
                </p>
              )}
              {(v.provider_since || site) && (
                <p className="vd-sub">
                  {v.provider_since && <span title={`When this validator first appeared as a Fibre provider, whatever endpoint it had then: ${utcWord(v.provider_since)}`}>Fibre provider since <b>{shortDate(v.provider_since)}</b></span>}
                  {site && <a href={site} title={site} rel="nofollow noopener noreferrer" target="_blank">{site.replace(/^https?:\/\//, "").replace(/\/$/, "")}</a>}
                </p>
              )}
            </div>
          </div>
          <dl className="vd-facts">
            {v.host
              ? <><dt>Endpoint</dt><dd><span className="vd-id"><Host s={v.host} /><CopyMark text={v.host} label="the endpoint" /></span>{endpointSince && <em title={`Registered ${utcWord(endpointSince)}`}>since {shortDate(endpointSince)}</em>}</dd></>
              : v.last_host
                ? <><dt>Last endpoint</dt><dd title="The registration stays on chain; the validator left the bonded provider list."><span className="vd-id"><Host s={v.last_host} /></span>{v.endpoint_closed_at && <em>left {dateUTC(v.endpoint_closed_at)}</em>}</dd></>
                : <><dt>Endpoint</dt><dd className="vd-none" title={noEndpoint && diag ? diag.text : undefined}><em>none registered</em>
                  {noEndpoint && diag?.docs && <a href={diag.docs.href} rel="noopener noreferrer" target="_blank">{diag.docs.word} →</a>}</dd></>}
            {v.host && v.hosting && <><dt>Hosting</dt><dd><HostingFact h={v.hosting} /></dd></>}
            {(v.host || c) && <>
              <dt>Last check</dt>
              <dd className="vd-chk">{c
                ? <>
                  <b title={utcWord(c.at)}>{whenUTC(c.at)}</b>
                  <span className="vd-stages" title={[c.host !== v.host && `checked ${c.host}`, c.raw_error].filter(Boolean).join(" · ") || "The newest handshake with the registered endpoint, stage by stage."}>
                    {stages.map(([name, ok, note], i) => {
                      const skip = firstFail >= 0 && i > firstFail;
                      return <span key={name} className={ok ? "ok" : skip ? "skip" : "bad"}>{name} <i aria-hidden="true">{ok ? "✓" : skip ? "–" : "✗"}</i>{note && <em>{note}</em>}</span>;
                    })}
                  </span>
                  {c.host !== v.host && <em title={c.host}>on the earlier endpoint</em>}
                </>
                : <em>no check yet</em>}</dd>
            </>}
          </dl>
        </div>
        <div className="vd-pf-foot">
          <details className="vd-addrs">
            <summary><Chevron />Addresses</summary>
            <dl>
              {v.operator_address && <><dt>Operator</dt><dd className="vd-addr"><span className="mono">{v.operator_address}</span><CopyMark text={v.operator_address} label="the operator address" /></dd></>}
              <dt>Consensus</dt><dd className="vd-addr" title={v.cons_address ? `hex ${v.address}` : undefined}><span className="mono">{cons}</span><CopyMark text={cons} label="the consensus address" /></dd>
            </dl>
          </details>
          <a className="vd-feed" href={`${API_BASE}/v1/validators/${own}/feed.atom`} type="application/atom+xml" title="Endpoint changes of this validator, as an Atom feed">Atom feed</a>
        </div>
      </section>

      <PreLive meta={meta} />
      <StatusLine meta={meta} metaError={metaErr} snap={{ record_through: data.record_through, window: data.window }} client={{ error: d.error, fetchedAt: d.fetchedAt, status: d.status }} measuring={measuring} />

      {/* what Tensile found when it read this validator's rows: the period's service rate and its fraction on the left,
          what was not served, what waits for its check, reachability and throughput on the same line to the right, and
          under them, across the frame, the latest checks whatever the period, one cell each */}
      <section className="pan vd-svc" id="observed" aria-labelledby="vd-observed">
        <div className="vp-h">
          <h2 className="vp-t" id="vd-observed" title="Each blob is read once, 10 minutes before its retention window ends, and every validator that endorsed it is asked for its own rows. A validator is not served when its own rows did not come back, at the reading and each time it was asked again."><Eye />Observed by Tensile</h2>
        </div>
        <div className="vd-svc-top">
          <div className={"vd-rate" + (rateCls ? " " + rateCls : "")} title={rateTitle}>
            <p className="vd-lbl">Service rate <span className="vd-per">· {per}</span></p>
            <p className="vd-rate-v">
              {notLive || !o || decided === 0 ? "—" : pctOf(o.served, decided)}
              {!notLive && held.length > 0 && <Warn text={`${plural(held.length, "reading")} in this period did not count: ${heldWhy}. ${heldText} The checks below show each one.`} />}
            </p>
            {!notLive && o && decided > 0 && <p className="vd-rate-s"><b>{int(o.served)}</b> / {int(decided)}</p>}
            {/* why there is no rate, under the dash only when no check shows below to say otherwise; the dash's title
                keeps the words */}
            {!notLive && (!o || decided === 0) && cells.length === 0 && <p className="vd-rate-s">{noRate}</p>}
          </div>
          <div className="vd-more">
            <span className={"vd-fi" + (!notLive && broken > 0 ? " bad" : "")} title={`Endorsed shards whose own rows did not come back, at the reading and each time they were asked again. Before ${FULL_READ_SINCE_WORDS}: rows that did not come back from a blob that could not be reconstructed.`}>
              {notLive
                ? <>Not served <b>—</b></>
                : broken > 0
                  ? <button type="button" className="vd-go" onClick={showNotServed} aria-label={`${int(broken)} not served: show them in the checks below`}>Not served <b>{int(broken)}</b></button>
                  : <>Not served <b>0</b></>}
              {!notLive && prov > 0 && <Warn text={`${int(prov)} of these ${prov === 1 ? "is" : "are"} younger than ${Math.round((v.provisional_faults?.settling_seconds ?? 1800) / 60)} minutes: counted, and final at ${whenUTC(v.provisional_faults!.until)} unless withdrawn.`} />}
            </span>
            <span className="vd-fi" title={`Endorsed shards whose retention window has not ended. Each is read 10 minutes before its window ends: the result shows in the checks below at once, and enters the counts when the window closes.${!notLive && notCountedText(o) ? ` Not counted in the period: ${notCountedText(o)}.` : ""}`}>
              Awaiting check <b>{notLive || data.in_retention_window == null ? "—" : int(data.in_retention_window)}</b>
            </span>
            <span className="vd-fi" title={!bonded ? "Out of the bonded list: not checked." : reach ? `${int(reach.num)} of ${int(reach.den)} handshakes completed with the registered endpoint over the period. Not signing uptime.${v.last_unreachable_at ? ` Last failed handshake ${utcWord(v.last_unreachable_at)}.` : ""}` : "No handshake yet."}>
              Reachability <b>{reach ? pctOf(reach.num, reach.den) : "—"}</b>
            </span>
            <span className="vd-fi" title={v.serve_bytes_per_second == null
              ? (v.serve_throughput_sample > 0 ? `${int(v.serve_throughput_sample)} of the 3 large-shard downloads it is shown from.` : "No large-shard download yet.")
              : `Median download speed over ${int(v.serve_throughput_sample)} shards of 2 MiB or more.`}>
              Throughput <b>{v.serve_bytes_per_second == null ? "—" : unit(`${bytes(v.serve_bytes_per_second)}/s`)}</b>
            </span>
          </div>
        </div>
        {/* the latest checks whatever the period, across the frame: what they are and the key over the cells */}
        <div className="vd-latest">
          {cells.length > 0
            ? <>
              <div className="vd-cap">
                <span className="vd-ttl">Latest {plural(cells.length, "check")} whatever the period, oldest to newest</span>
                <ul className="vd-key" aria-label="key">
                  {key.map((k) => <li key={k.word}>{k.sw.map((s) => <i key={s} className={"vd-c " + s} aria-hidden="true" />)}{k.word} <b>{int(k.n)}</b></li>)}
                </ul>
              </div>
              <Strip key={addr} cells={cells} />
            </>
            : <p className="vd-none-yet">No check of this validator on record yet.</p>}
        </div>
      </section>

      {/* what the chain records: one compact frame, calmer than Tensile's, so signing is never read as serving */}
      <section className="vd-oc" id="chain" aria-labelledby="vd-chain">
        <div className="vd-oc-h">
          <h2 id="vd-chain">On chain <span className="per">({per})</span></h2>
          <p>Read from the chain, nothing measured</p>
        </div>
        <dl className="vd-oc-l" style={{ "--n": timeouts > 0 ? 5 : 4 } as CSSProperties}>
          <div title={endorse ? endorse.title : `No settlement assigned it rows. ${ENDORSE_TITLE}`}>
            <dt>Endorsements</dt>
            <dd className={endorse ? undefined : "na"}><b>{endorse ? pctOf(endorse.n, endorse.of) : "—"}</b>{endorse && <em>{int(endorse.n)} / {int(endorse.of)}</em>}</dd>
          </div>
          <div title={`${load ? `${int(load.promises)} endorsed blobs in the period. ` : ""}Row data of the shards this validator stored and endorsed over the period's settled blobs: blob_size / 4096 per row, padding included. The row proofs stored beside them are not counted.`}>
            <dt>Shard data</dt>
            <dd className={load && !notLive ? undefined : "na"}><b>{load && !notLive ? unit(bytes(load.bytes)) : "—"}</b></dd>
          </div>
          <div title="Shard data this validator must hold at this moment: endorsed blobs whose retention window has not ended.">
            <dt>Held now</dt>
            <dd className={load ? undefined : "na"}><b>{load ? unit(bytes(load.stored_bytes)) : "—"}</b></dd>
          </div>
          <div title="Rows the assignment gives this validator on the newest settled blob, of every settled blob, by stake. Rows follow stake, not blob size.">
            <dt>Rows per blob</dt>
            <dd className={load ? undefined : "na"}><b>{load ? int(load.rows_per_blob) : "—"}</b></dd>
          </div>
          {timeouts > 0 && <div title="MsgPaymentPromiseTimeout submitted by this validator’s operator account in the period.">
            <dt>Timeouts reported</dt>
            <dd><b>{int(timeouts)}</b></dd>
          </div>}
        </dl>
      </section>

      {/* the latest checks in the Blobs list's own rows, newest first, whatever the period: the whole row opens the blob,
          and what Tensile found sits in its lane at the end */}
      <section id="evidence" className="listing lg-list vd-list">
        <div className="list-head">
          <div className="pb-lh">
            <h2 className="pb-th">Latest checks</h2>
            {probes.length > 0 && (
              <div className="seg pb-kinds" role="group" aria-label="filter">
                <button type="button" aria-pressed={!onlyNotServed} onClick={() => setFilter(false)}>All{" "}<span className="n">{int(probes.length)}</span></button>
                <button type="button" aria-pressed={onlyNotServed} onClick={() => setFilter(true)}>Not served{" "}<span className="n">{int(notServedRows.length)}</span></button>
              </div>
            )}
          </div>
        </div>
        <p className="vd-scope">The newest checks, whatever the period selected. The period sets the figures above.
          {onlyNotServed && broken > nsInPeriod && <> {int(broken)} not served in the period ({per}), {int(nsInPeriod)} of them among these checks. <a href={notServedHref}>Every one in the API →</a></>}</p>
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
              {probes.length === 0 && <tr className="lg-empty"><td colSpan={6}>No check of this validator on record yet.</td></tr>}
              {probes.length > 0 && shown.length === 0 && <tr className="lg-empty"><td colSpan={6}>
                No not-served check among the newest {int(probes.length)}.{broken > 0 && <> Older ones are <a href={notServedHref}>in the API →</a></>}</td></tr>}
              {visible.map(({ p, g, tries, made, full, at: readAt }) => {
                const open = isOpen(p);
                const r = resultOf(open ? { ...p, provisional: false } : p, g);
                const href = `/blob/?hash=${p.promise_hash}`;
                const t = Date.parse(readAt);
                const rows = p.rows_expected ? <><b>{int(p.rows_returned)}</b><span className="u"> / {int(p.rows_expected)}</span></> : "—";
                const ms = <>{int(p.total_duration_ms)}<span className="u"> ms</span></>;
                // a validator asked more than once says so after the word (on a line of its own where the lane is narrow)
                const at = made > 1 ? <span className="at">{askedTimes(made)}</span> : null;
                // at a full reading, rows of the blob that are not the validator's own, among its answers, leave it
                // counted neither way
                const foreign = full && !judged(tries) && tries.some(foreignRows);
                const notes = [
                  r.tone === "hold" && (full
                    ? (tries.some((x) => ownGap(x.classification))
                      ? "not counted: its own rows did not come back, but one of Tensile’s own requests at the reading failed or was not made in time"
                      : foreign ? "not counted: rows of the blob came back that are not its own, which show neither that it holds its rows nor that it does not"
                      : "not counted: its own rows did not come back, but one of Tensile’s own requests at the reading failed or was not made in time, no request of the reading reached any server, or a request Tensile still owed it is not on record")
                    : "not counted: the rows did not come back, and the blob was available from other validators"),
                  r.tone !== "hold" && foreign && "not counted: rows of the blob came back that are not its own, which show neither that it holds its rows nor that it does not",
                  tries.length > 1 && `requests in order: ${tries.map(answerWord).join(", ")}${judged(tries) ? "; the last answer carries the result" : ""}`,
                  open && "the retention window is still open: final when it closes",
                  !endOfWindow(p.schedule_label) && "read on the earlier schedule",
                  `outcome: ${p.outcome.toLowerCase().replace(/_/g, " ")}`,
                  (g === "not served" || r.tone === "hold") && p.raw_error && rawErrorWords(p),
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
        {/* how many show, a page more on each press, and every reading in the API */}
        <div className="pager vd-pager">
          <span className="count">{shown.length > 0 && <>Showing <b>{int(visible.length)}</b> of {int(shown.length)}</>}</span>
          <span className="ctl">
            {visible.length < shown.length && <button type="button" className="btn" onClick={showMore}>Show more</button>}
            <a className="btn" href={onlyNotServed ? notServedHref : `${API_BASE}/v1/probes?validator=${own}&limit=1000`}
              title={data.recent_probes_truncated ? "The newest readings are here; every one is in the API." : "Every reading, in the API."}>Full history →</a>
          </span>
        </div>
      </section>
    </>
  );
}

export default function ValidatorPage() {
  return <Suspense fallback={<p className="crumb" style={{ paddingTop: 22 }}>Loading…</p>}><Page /></Suspense>;
}

