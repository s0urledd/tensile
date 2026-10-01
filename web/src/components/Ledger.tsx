"use client";
import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { API_BASE, type Blob, int, bytes, tia, pctOf, nsDisplay, utc, utcWord } from "@/lib/api";
import { lane } from "@/lib/status";
import { unit } from "@/components/Unit";
import { Eye } from "@/components/Metrics";
import { Frac } from "@/components/CurrentProviders";
import Ident from "@/components/Ident";
import { reducedMotion } from "@/components/RollNumber";
import { age } from "@/components/BlobsDeck";

/**
 * The Blobs list as a ledger. A band opens each UTC day with the day's own
 * totals; under it the blobs, newest first. The blobs of one block sit
 * together: the block's height and time are on its first row only, the rows
 * after it closer and with no rule between them, and nothing else is drawn
 * to group them. Figures are right-aligned on their digits. A value the row
 * above already showed (the namespace, the publisher, the size, the fee)
 * steps back, so a burst of one publisher reads as one publisher. Endorsed
 * is the share of voting power with a short meter whose tick is the ⅔ a
 * settlement needs. Tensile's own reading has a lane of its own at the end,
 * empty until Tensile has read the blob.
 *
 * The first page is live. It is read when the chain moves (the header's
 * /v1/tip stream, shared): at most every 5 s while blobs arrive, every 15 s
 * once none has for two minutes, every 30 s if the chain stops moving, and
 * never while the tab is hidden. While the reader is at the top of the list
 * and neither pointing into it nor focused in it, new blobs come in on their
 * own, in one move with one glow (neither under reduced motion). While the
 * reader is in the list, nothing under them moves: the new blobs gather in a
 * pill at the table's edge, and a click on it brings them in. Older pages
 * hold still.
 */

const FAST_MS = 5000, SLOW_MS = 15000, FALLBACK_MS = 30000;
/** no new blob for this long reads at the slow pace */
const IDLE_AFTER_MS = 120000;
const SLIDE_MS = 420;

export type Feed = {
  /** the request this answer is for: a new page or filter starts a new feed */
  path: string;
  rows: Blob[];
  /** the settlements the filters select, all of them */
  total: number;
  loaded: boolean;
  error: string | null;
  /** when the newest row arrived, on the observer's clock (when it settled, on the first read) */
  lastNewAt: number;
};
const empty = (path: string): Feed => ({ path, rows: [], total: 0, loaded: false, error: null, lastNewAt: 0 });

async function readPage(path: string): Promise<{ blobs: Blob[]; total: number }> {
  const ctl = new AbortController();
  const t = window.setTimeout(() => ctl.abort(), 15000);
  try {
    const r = await fetch(API_BASE + path, { cache: "no-store", signal: ctl.signal });
    if (!r.ok) {
      let msg = String(r.status);
      try { const j = await r.json(); if (j?.error) msg = j.error; } catch { /* keep the status */ }
      throw new Error(msg);
    }
    const j = await r.json();
    return { blobs: Array.isArray(j.blobs) ? j.blobs : [], total: typeof j.total === "number" ? j.total : 0 };
  } finally {
    window.clearTimeout(t);
  }
}

/**
 * One page of /v1/blobs (path), read once, and again as the chain moves
 * while it is live (height: the tip's, from the shared stream), paced as the
 * comment at the top says. A page that is not live is read again only after
 * an error. skew: the observer's clock minus the reader's.
 */
export function useLedger(path: string, live: boolean, height: number | undefined, skew: number): Feed {
  const [feed, setFeed] = useState<Feed>(() => empty(path));
  const cur = useRef(feed);
  const busy = useRef(false);
  const last = useRef(0);
  const later = useRef<number | undefined>(undefined);
  const pathRef = useRef(path);
  pathRef.current = path;
  const liveRef = useRef(live);
  liveRef.current = live;
  const skewRef = useRef(skew);
  skewRef.current = skew;

  const apply = useCallback((f: Feed) => { cur.current = f; setFeed(f); }, []);
  const gap = () => {
    const f = cur.current;
    return f.error || (f.loaded && Date.now() + skewRef.current - f.lastNewAt > IDLE_AFTER_MS) ? SLOW_MS : FAST_MS;
  };

  const read = useCallback(async () => {
    if (busy.current || document.hidden) return;
    busy.current = true;
    last.current = Date.now();
    const p = pathRef.current;
    try {
      const page = await readPage(p);
      if (p !== pathRef.current) return;
      const before = cur.current.path === p && cur.current.loaded ? cur.current : null;
      const known = new Set(before?.rows.map((b) => b.promise_hash));
      const came = !!before && page.blobs.some((b) => !known.has(b.promise_hash));
      const lastNewAt = !before ? (page.blobs[0] ? Date.parse(page.blobs[0].settlement_time) : 0) : came ? Date.now() + skewRef.current : before.lastNewAt;
      apply({ path: p, rows: page.blobs, total: page.total, loaded: true, error: null, lastNewAt });
    } catch (e) {
      if (p !== pathRef.current) return;
      const f = cur.current.path === p ? cur.current : empty(p);
      apply({ ...f, error: e instanceof Error ? e.message : String(e) });
    } finally {
      busy.current = false;
      // the page or a filter changed while this read was out: read the new one now
      if (p !== pathRef.current) read();
    }
  }, [apply]);

  /** a read now, or at the end of the current gap if the last one was sooner */
  const request = useCallback(() => {
    window.clearTimeout(later.current);
    if (document.hidden) return;
    const wait = gap() - (Date.now() - last.current);
    if (wait <= 0) read();
    else later.current = window.setTimeout(read, wait);
  }, [read]); // eslint-disable-line react-hooks/exhaustive-deps

  // a new page or filter: nothing of the old one stands for it, and it is read at once
  useEffect(() => {
    window.clearTimeout(later.current);
    apply(empty(path));
    last.current = 0;
    read();
  }, [path, apply, read]);
  // live: a read each time the chain moves
  useEffect(() => { if (live) request(); }, [height, live, request]);
  // live: a slow read when the chain stops moving, and one when the page comes back into view; any page: another try after an error
  useEffect(() => {
    const t = window.setInterval(() => {
      if (!document.hidden && (liveRef.current || cur.current.error) && Date.now() - last.current >= FALLBACK_MS) read();
    }, 5000);
    const onVis = () => { if (document.hidden) window.clearTimeout(later.current); else if (liveRef.current) request(); };
    document.addEventListener("visibilitychange", onVis);
    return () => { window.clearInterval(t); window.clearTimeout(later.current); document.removeEventListener("visibilitychange", onVis); };
  }, [read, request]);

  return feed;
}

// ---- what the table shows ----

/** the rows on screen: the feed's, unless the reader holds them; move is the last arrival into them */
type Shown = { path: string; rows: Blob[]; total: number; loaded: boolean; move: { id: number; fresh: Set<string> } | null };

/** the feed onto the screen; the rows it did not show before are the move's */
function take(v: Shown, f: Feed): Shown {
  if (f.path !== v.path || !v.loaded) return { path: f.path, rows: f.rows, total: f.total, loaded: f.loaded, move: null };
  if (f.rows === v.rows && f.total === v.total) return v;
  const known = new Set(v.rows.map((b) => b.promise_hash));
  const fresh = new Set(f.rows.filter((b) => !known.has(b.promise_hash)).map((b) => b.promise_hash));
  return { path: f.path, rows: f.rows, total: f.total, loaded: true, move: fresh.size ? { id: (v.move?.id ?? 0) + 1, fresh } : v.move };
}

/** a day's settlements on record and their blob size, with when that was computed */
export type DayTotal = { settlements: number; bytes: number; at?: string };

const WD = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const dm = (d: Date) => d.toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
/** "Today · 1 Oct", "Yesterday · 30 Sep", "Mon 28 Sep 2026" */
function dayName(day: string, now: number): string {
  const d = new Date(day + "T00:00:00Z");
  const today = new Date(now).toISOString().slice(0, 10), yday = new Date(now - 86400_000).toISOString().slice(0, 10);
  if (day === today) return `Today · ${dm(d)}`;
  if (day === yday) return `Yesterday · ${dm(d)}`;
  return `${WD[d.getUTCDay()]} ${dm(d)} ${d.getUTCFullYear()}`;
}

type Line =
  | { kind: "day"; day: string; name: string; sum: DayTotal | null }
  | { kind: "blob"; b: Blob; cont: boolean; age: string | null; ns: boolean; pub: boolean; size: boolean; fee: boolean; lane: boolean };

/** the publisher of a row: who paid, else who submitted it */
const payer = (b: Blob) => b.publisher || b.signer;

/** a publisher as a chip: its mark, the address's prefix quietly, its last four characters */
function Who({ addr }: { addr: string }) {
  const i = addr.indexOf("1");
  return (
    <Link className="lg-who" href={`/publisher/?addr=${addr}`} title={addr}>
      <Ident addr={addr} />
      {i > 0 && <span className="hd">{addr.slice(0, i)} •••</span>}
      <span className="tl">{addr.slice(-4)}</span>
    </Link>
  );
}

type RowProps = {
  b: Blob; cont: boolean; age: string | null; ns: boolean; pub: boolean; size: boolean; fee: boolean; laneRep: boolean;
  fresh: boolean; onNs: (ns: string) => void; onOpen: (e: React.MouseEvent, href: string) => void;
};
/** one blob: the cells of the table, and the second line a phone shows under the first */
const Row = memo(function Row({ b, cont, age: ag, ns, pub, size, fee, laneRep, fresh, onNs, onOpen }: RowProps) {
  const href = `/blob/?hash=${b.promise_hash}`;
  const who = payer(b);
  const name = nsDisplay(b.namespace);
  const ln = lane(b);
  const share = b.attested_voting_power != null && b.total_voting_power ? b.attested_voting_power / b.total_voting_power : null;
  const rep = (on: boolean) => (on ? " rep" : "");
  return (
    // A click on a cell that sits above the cover link (a titled figure) opens the blob too; links and buttons keep their own.
    <tr className={`row${cont ? " cont" : ""}${fresh ? " fresh" : ""}`} data-h={b.promise_hash}
      onClick={(e) => { if (!(e.target as HTMLElement).closest("a, button") && !window.getSelection()?.toString()) onOpen(e, href); }}>
      <td className="c-h">{!cont && int(b.settlement_height)}<Link className="rc" href={href} tabIndex={-1} aria-hidden="true" /></td>
      <td className="c-t">{!cont && <span title={utcWord(b.settlement_time)}><span className="tm">{utc(b.settlement_time).slice(11, 19)}</span>{ag && <span className="ag">{ag}</span>}</span>}</td>
      <td className="c-b">
        <Link href={href} title={b.promise_hash} aria-label={`Blob ${b.promise_hash.slice(0, 10)}, height ${int(b.settlement_height)}`}>{b.promise_hash.slice(0, 6)}<span className="el">…</span>{b.promise_hash.slice(-4)}</Link>
        {!cont && <span className="ht">#{int(b.settlement_height)}</span>}
      </td>
      <td className={"c-ns" + rep(ns)}><button type="button" className="nsb" onClick={() => onNs(b.namespace)} title={`${b.namespace} · show only this namespace`}>{name}</button></td>
      <td className={"c-p" + rep(pub)}>{who ? <Who addr={who} /> : "—"}</td>
      <td className={"c-sz num" + rep(size)}>{unit(bytes(b.blob_size))}</td>
      <td className={"c-fee num" + rep(fee)}>{b.charge ? unit(tia(b.charge.fee_utia)) : "—"}</td>
      <td className="c-e num">
        {share == null ? "—" : (
          <span className="en" title={`${b.attested_with_rows != null ? `${int(b.attested_with_rows)} of ${int(b.validators_with_rows)} validators holding rows endorsed it. ` : ""}A settlement needs ⅔ of voting power.`}>
            <span className="pc">{pctOf(b.attested_voting_power!, b.total_voting_power!)}</span>
            <span className="em" aria-hidden="true"><i style={{ width: `${Math.min(100, share * 100).toFixed(2)}%` }} /></span>
          </span>
        )}
      </td>
      <td className="gap" aria-hidden="true" />
      <td className={"tn" + rep(laneRep)}>{ln && <span className={ln.tier === "hold" ? "hold" : undefined} title={ln.title}>{ln.word}</span>}</td>
      <td className={"c-m" + rep(ns && pub && size)}>
        <span className="nm">{name}</span><span className="sep">·</span>{bytes(b.blob_size)}{who && <><span className="sep">·</span><Who addr={who} /></>}
      </td>
    </tr>
  );
});

export default function Ledger({ feed, live, skew, days, onNs, children }: {
  feed: Feed;
  /** the first page: new blobs come in */
  live: boolean;
  /** the observer's clock minus the reader's, for the ages */
  skew: number;
  /** a day's totals, or null when they are not the list's (a filtered list) or not on record yet */
  days: (day: string) => DayTotal | null;
  onNs: (ns: string) => void;
  /** the pager, under the table; it counts what the table shows */
  children?: (total: number) => React.ReactNode;
}) {
  const [motion, setMotion] = useState(true);
  useEffect(() => { setMotion(!reducedMotion()); }, []);

  // ---- holding: while the reader is in the list, what is on screen stays ----
  const wrapRef = useRef<HTMLDivElement>(null);
  const bodyRef = useRef<HTMLTableSectionElement>(null);
  const [pointerIn, setPointerIn] = useState(false);
  const [focusIn, setFocusIn] = useState(false);
  // scrolled past the list's first line: the reader is reading it
  const [away, setAway] = useState(false);
  useEffect(() => {
    let raf = 0;
    const check = () => { raf = 0; const first = bodyRef.current?.firstElementChild; setAway(!!first && first.getBoundingClientRect().top < 0); };
    const on = () => { if (!raf) raf = requestAnimationFrame(check); };
    check();
    window.addEventListener("scroll", on, { passive: true });
    window.addEventListener("resize", on);
    return () => { cancelAnimationFrame(raf); window.removeEventListener("scroll", on); window.removeEventListener("resize", on); };
  }, []);
  const hold = live && (pointerIn || focusIn || away);

  const [shown, setShown] = useState<Shown>(() => ({ path: feed.path, rows: feed.rows, total: feed.total, loaded: feed.loaded, move: null }));
  useEffect(() => {
    // a new page or filter replaces the rows at once; new rows of the same list wait while the reader holds them
    setShown((v) => (feed.path !== v.path || !v.loaded || !hold ? take(v, feed) : v));
  }, [feed, hold]);
  const pending = live && feed.loaded && feed.path === shown.path ? Math.max(0, feed.total - shown.total) : 0;
  const bringIn = () => {
    setShown((v) => take(v, feed));
    const top = wrapRef.current?.getBoundingClientRect().top ?? 0;
    if (top < 0) window.scrollBy({ top: top - 12, behavior: motion ? "smooth" : "auto" });
  };

  // ---- the move: the rows that were there slide down by what came in, once, and what came in glows once ----
  const headDay = useRef<string | null>(null);
  useLayoutEffect(() => {
    const tb = bodyRef.current, mv = shown.move;
    if (!tb || !mv || !motion) return;
    const trs = [...tb.children] as HTMLElement[];
    // a day band that was already on top stays where it is; the rows slide out from under it
    const keep = trs[0]?.dataset.day && trs[0].dataset.day === headDay.current ? 1 : 0;
    const firstOld = trs.find((tr) => tr.dataset.h && !mv.fresh.has(tr.dataset.h));
    if (!firstOld || keep >= trs.length) return;
    const shift = firstOld.offsetTop - trs[keep].offsetTop;
    if (shift <= 0) return;
    const anims = trs.slice(keep).map((tr) => tr.animate([{ transform: `translateY(${-shift}px)` }, { transform: "none" }], { duration: SLIDE_MS, easing: "cubic-bezier(.2, .8, .2, 1)" }));
    return () => anims.forEach((a) => a.cancel());
  }, [shown.move?.id]); // eslint-disable-line react-hooks/exhaustive-deps
  useLayoutEffect(() => { headDay.current = (bodyRef.current?.firstElementChild as HTMLElement | null)?.dataset.day ?? null; });

  // ---- the ages, on the observer's clock: every second while the newest is under a minute old ----
  const [now, setNow] = useState(0);
  const skewRef = useRef(skew);
  skewRef.current = skew;
  useEffect(() => {
    let t: number | undefined;
    const tick = () => {
      const n = Date.now() + skewRef.current;
      setNow(n);
      const newest = shown.rows[0] ? Date.parse(shown.rows[0].settlement_time) : 0;
      t = window.setTimeout(tick, n - newest < 60000 ? 1000 - (n % 1000) : 15000);
    };
    tick();
    return () => window.clearTimeout(t);
  }, [shown.rows]);

  const lines = useMemo(() => {
    const out: Line[] = [];
    const clock = now || Date.now();
    let prev: Blob | null = null, lastAge: string | null = null;
    for (const b of shown.rows) {
      const day = b.settlement_time.slice(0, 10);
      if (!prev || prev.settlement_time.slice(0, 10) !== day) {
        out.push({ kind: "day", day, name: dayName(day, clock), sum: days(day) });
        prev = null;
        lastAge = null;
      }
      const cont = !!prev && prev.settlement_height === b.settlement_height;
      // an age on a block's first row, and only where it says something the one above did not
      const a: string | null = now && !cont ? age(now - Date.parse(b.settlement_time)) : null;
      const showAge: string | null = a && a !== lastAge ? a : null;
      if (showAge) lastAge = showAge;
      const lp = prev ? lane(prev) : null, lb = lane(b);
      out.push({
        kind: "blob", b, cont, age: showAge,
        ns: !!prev && prev.namespace === b.namespace,
        pub: !!prev && payer(prev) === payer(b),
        size: !!prev && prev.blob_size === b.blob_size,
        fee: !!prev && !!prev.charge && !!b.charge && prev.charge.fee_utia === b.charge.fee_utia,
        lane: !!lp && !!lb && lb.tier !== "hold" && lp.word === lb.word,
      });
      prev = b;
    }
    return out;
  }, [shown.rows, now, days]);

  const router = useRouter();
  const onOpen = useCallback((e: React.MouseEvent, href: string) => {
    // a plain click stays in the app; a modified or middle click is the browser's
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  }, [router]);

  const fresh = motion ? shown.move?.fresh : undefined;
  return (
    <>
      <div className="lg-wrap" ref={wrapRef}
        onPointerEnter={(e) => { if (e.pointerType === "mouse") setPointerIn(true); }}
        onPointerLeave={() => setPointerIn(false)}
        onFocus={() => setFocusIn(true)}
        onBlur={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node)) setFocusIn(false); }}>
        <div className="lg-newbar">
          {hold && pending > 0 && (
            <button type="button" className="lg-new" onClick={bringIn}>
              <i className="d" aria-hidden="true" /><b>{int(pending)}</b> new blob{pending === 1 ? "" : "s"}
              <svg width="12" height="12" viewBox="0 0 12 12" aria-hidden="true"><path d="M6 10V2.5M2.8 5.5 6 2.3l3.2 3.2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>
            </button>
          )}
        </div>
        <div className="lg-tw">
          <table className="lg-t">
            <thead>
              <tr>
                <th className="c-h">Height</th>
                <th className="c-t">Settled <span className="per">(UTC)</span></th>
                <th className="c-b">Blob</th>
                <th className="c-ns">Namespace</th>
                <th className="c-p">Publisher</th>
                <th className="c-sz num">Blob size</th>
                <th className="c-fee num">Fee paid</th>
                <th className="c-e num" title="Share of voting power whose signature on the settlement verified. A settlement needs ⅔.">Endorsed <Frac /></th>
                <th className="gap" aria-hidden="true" />
                <th className="tn" title="Tensile's own reading of each blob, once, near the end of its retention window."><span><Eye />Tensile</span></th>
              </tr>
            </thead>
            <tbody ref={bodyRef}>
              {!shown.loaded && <tr className="lg-empty"><td colSpan={10}>{feed.error ? `The observer API is not answering (${feed.error}).` : "Loading…"}</td></tr>}
              {shown.loaded && shown.rows.length === 0 && <tr className="lg-empty"><td colSpan={10}>No blob recorded{feed.path.includes("&namespace=") || feed.path.includes("&publisher=") ? " with this filter" : ""}.</td></tr>}
              {lines.map((l) => l.kind === "day"
                ? (
                  <tr key={`d${l.day}`} className="day" data-day={l.day}>
                    <td colSpan={8}>
                      {l.name}
                      {l.sum && <span className="sum" title={l.sum.at ? `On record that day, as of ${utc(l.sum.at).slice(11, 16)} UTC` : undefined}>{int(l.sum.settlements)} settlement{l.sum.settlements === 1 ? "" : "s"} · {bytes(l.sum.bytes)}</span>}
                    </td>
                    <td className="gap" aria-hidden="true" />
                    <td className="tn" aria-hidden="true" />
                  </tr>
                )
                : <Row key={l.b.promise_hash} b={l.b} cont={l.cont} age={l.age} ns={l.ns} pub={l.pub} size={l.size} fee={l.fee} laneRep={l.lane}
                  fresh={!!fresh?.has(l.b.promise_hash)} onNs={onNs} onOpen={onOpen} />)}
            </tbody>
          </table>
        </div>
      </div>
      {shown.loaded && children?.(shown.total)}
    </>
  );
}
