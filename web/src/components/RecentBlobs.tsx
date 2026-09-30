"use client";
import { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import { API_BASE, type Blob, type Rate, int, pctOf, bytes, ago, whenUTC, utcWord } from "@/lib/api";
import { recon } from "@/lib/status";
import { Mark, type Tier } from "@/components/Verdict";
import { Eye } from "@/components/Metrics";
import RollNumber, { reducedMotion } from "@/components/RollNumber";

/**
 * The overview's recent blobs: the newest settlements as a grid of squares,
 * one square per blob, and a readout of one blob's facts beside it.
 *
 * The grid is a ring. Each blob takes the next square in reading order as it
 * arrives, wrapping from the last square to the first, so a square keeps its
 * place for as long as its blob is among the newest 50 and nothing on the
 * grid ever moves; only the head advances. A new blob lights its square and
 * cools off; the older a blob, the fainter its square, so the trail shows
 * which way the grid is filling. A square's colour is the blob's status as the
 * Blobs list words it: read and available (solid), read and unavailable
 * (amber), still in its retention window and not read yet (hollow), or not
 * read (grey).
 *
 * The readout shows the newest blob by settlement, as the chain orders them,
 * or the blob under the pointer or keyboard focus.
 *
 * Reading the list: GET /v1/blobs, the page the Blobs list reads, every 5 s
 * while blobs arrive and every 15 s once none has for two minutes, only while
 * the page is visible and not paused. Each read asks for about as many rows as
 * the last one brought, and the API's total says whether more arrived than
 * the page returned, in which case one more read fills the gap. However many
 * arrive in one read, they light up in one sweep of at most 0.65 s, well
 * inside the interval, so arrivals never queue.
 */

/** the grid: 10 columns by 5 rows */
const CELLS = 50, COLS = 10;
const FAST_MS = 5000, SLOW_MS = 15000;
/** no new blob for this long reads at the slow pace */
const IDLE_AFTER_MS = 120000;
/** a sweep, however many squares it lights, takes at most this long to start them all */
const SWEEP_MS = 650;

type Cell = { b: Blob; seq: number };
type Arrival = { id: number; order: Map<string, number>; count: number };
type Feed = {
  /** newest arrival first, at most CELLS */
  cells: Cell[];
  /** the next arrival's place in the ring */
  next: number;
  total: number | null;
  /** the newest blob by settlement, as the API orders the list */
  latest: Blob | null;
  arrival: Arrival | null;
  loaded: boolean;
  error: string | null;
  /** when the newest blob arrived, on this reader's clock (or when it settled, on the first read) */
  lastNewAt: number;
};
const EMPTY: Feed = { cells: [], next: 0, total: null, latest: null, arrival: null, loaded: false, error: null, lastNewAt: 0 };

type Page = { blobs: Blob[]; total: number };

async function readPage(limit: number): Promise<Page> {
  const ctl = new AbortController();
  const t = window.setTimeout(() => ctl.abort(), 15000);
  try {
    const r = await fetch(`${API_BASE}/v1/blobs?limit=${limit}`, { cache: "no-store", signal: ctl.signal });
    if (!r.ok) throw new Error(String(r.status));
    const j = await r.json();
    return { blobs: Array.isArray(j.blobs) ? j.blobs : [], total: typeof j.total === "number" ? j.total : NaN };
  } finally {
    window.clearTimeout(t);
  }
}

/** a page of the newest blobs into the feed: new ones take the next squares, known ones take any newer reading */
function merge(s: Feed, p: Page): { feed: Feed; fresh: number } {
  const byHash = new Map(p.blobs.map((b) => [b.promise_hash, b]));
  const known = new Set(s.cells.map((c) => c.b.promise_hash));
  let cells = s.cells.map((c) => {
    const nb = byHash.get(c.b.promise_hash);
    return nb ? { ...c, b: nb } : c;
  });
  const fresh = p.blobs.filter((b) => !known.has(b.promise_hash)).slice(0, CELLS);
  const first = !s.loaded;
  const total = Number.isFinite(p.total) ? p.total : s.total;
  const latest = p.blobs[0] && (!s.latest || p.blobs[0].settlement_height >= s.latest.settlement_height) ? p.blobs[0] : s.latest;
  if (!fresh.length) return { feed: { ...s, cells, total, latest, loaded: true, error: null, lastNewAt: first && latest ? Date.parse(latest.settlement_time) : s.lastNewAt }, fresh: 0 };
  // the oldest of the new first, so the sweep runs forward and ends on the newest
  const order = new Map<string, number>();
  const add: Cell[] = [];
  let next = s.next;
  for (let i = fresh.length - 1; i >= 0; i--) {
    order.set(fresh[i].promise_hash, next - s.next);
    add.unshift({ b: fresh[i], seq: next++ });
  }
  cells = [...add, ...cells].slice(0, CELLS);
  const arrival: Arrival = { id: (s.arrival?.id ?? 0) + 1, order, count: fresh.length };
  const lastNewAt = first ? (latest ? Date.parse(latest.settlement_time) : 0) : Date.now();
  return { feed: { cells, next, total, latest, arrival, loaded: true, error: null, lastNewAt }, fresh: fresh.length };
}

function useBlobFeed(paused: boolean) {
  const [feed, setFeed] = useState<Feed>(EMPTY);
  const cur = useRef(feed);
  const limit = useRef(CELLS);
  const busy = useRef(false);
  const timer = useRef<number | undefined>(undefined);
  // the next read: when, and at what interval, for the countdown ring
  const [next, setNext] = useState<{ at: number; every: number } | null>(null);

  const apply = useCallback((f: Feed) => { cur.current = f; setFeed(f); }, []);

  const tick = useCallback(async () => {
    if (busy.current) return;
    busy.current = true;
    window.clearTimeout(timer.current);
    setNext(null);
    try {
      const before = cur.current;
      const page = await readPage(limit.current);
      let { feed: f, fresh } = merge(before, page);
      // More arrived than the page returned, all of it new: one more read fills the squares.
      const delta = before.total != null && f.total != null && Number.isFinite(f.total - before.total) ? f.total - before.total : 0;
      if (before.loaded && fresh === page.blobs.length && delta > page.blobs.length && page.blobs.length < CELLS) {
        const more = await readPage(Math.min(CELLS, delta));
        const m = merge(f, more);
        // one sweep for both reads
        if (m.fresh && f.arrival && fresh) {
          const order = new Map<string, number>();
          const all = [...m.feed.cells].filter((c) => c.seq >= before.next).sort((a, b) => a.seq - b.seq);
          all.forEach((c, i) => order.set(c.b.promise_hash, i));
          m.feed.arrival = { id: m.feed.arrival!.id, order, count: all.length };
        }
        f = m.feed;
        fresh += m.fresh;
      }
      apply(f);
      // ask next time for about as many as this time brought
      const came = Number.isFinite(delta) ? Math.max(fresh, delta) : fresh;
      limit.current = Math.min(CELLS, Math.max(2, Math.ceil(came * 1.5) + 2));
    } catch (e) {
      apply({ ...cur.current, error: e instanceof Error ? e.message : String(e) });
    } finally {
      busy.current = false;
    }
  }, [apply]);

  useEffect(() => {
    if (paused) { window.clearTimeout(timer.current); setNext(null); return; }
    let dead = false;
    const schedule = () => {
      if (dead || document.hidden) return;
      const idle = Date.now() - cur.current.lastNewAt > IDLE_AFTER_MS;
      const every = cur.current.error ? SLOW_MS : idle ? SLOW_MS : FAST_MS;
      setNext({ at: Date.now() + every, every });
      timer.current = window.setTimeout(async () => { await tick(); schedule(); }, every);
    };
    const run = async () => { await tick(); schedule(); };
    const onVis = () => {
      window.clearTimeout(timer.current);
      if (document.hidden) setNext(null);
      else run();
    };
    run();
    document.addEventListener("visibilitychange", onVis);
    return () => { dead = true; window.clearTimeout(timer.current); document.removeEventListener("visibilitychange", onVis); };
  }, [paused, tick]);

  return { feed, next };
}

// ---- words ----

type Status = { key: "kept" | "hold" | "window" | "gap"; word: string; tier: Tier; title: string };
function statusOf(b: Blob): Status {
  const s = recon(b);
  const key = s.tier === "kept" ? "kept" : s.tier === "hold" ? "hold" : s.word === "in retention window" ? "window" : "gap";
  return { key, word: s.word, tier: s.tier, title: s.title };
}
function nth(n: number): string {
  const r10 = n % 10, r100 = n % 100;
  return n + (r10 === 1 && r100 !== 11 ? "st" : r10 === 2 && r100 !== 12 ? "nd" : r10 === 3 && r100 !== 13 ? "rd" : "th");
}

export type Observed = { rec: Rate | null | undefined; examinedAll: boolean };

/**
 * Tensile's own figure beside the chain's: the Blob availability share (CIP-51's
 * term) over the blobs whose reading decides them, marked as observed and set
 * quieter than the chain's figures. It keeps its place while it loads.
 */
export function Availability({ observed }: { observed: Observed }) {
  const rec = observed.rec;
  return (
    <div className="ob">
      <aside className="rb-obs" aria-label="Observed by Tensile" aria-busy={!rec || undefined}
        title="Observed by Tensile: blobs whose rows were enough to reconstruct them, over the blobs read (available plus unavailable): the newest ones with a reading, up to the API's sample limit.">
        <span className="obs-tag"><Eye />Observed by Tensile</span>
        <p>
          <span className="rb-obs-l">Blob availability</span>
          {rec
            ? <><b className={`num${rec.den > 0 ? "" : " absent"}`}>{pctOf(rec.num, rec.den)}</b>
              <span className="rb-obs-h">{rec.den > 0 ? `${int(rec.num)} of ${observed.examinedAll ? "" : "the newest "}${int(rec.den)} blobs read` : "none read"}</span></>
            : <><b className="wait">000%</b><span className="rb-obs-h wait">0,000 of 0,000 blobs read</span></>}
        </p>
      </aside>
    </div>
  );
}

/**
 * The grid and the readout, as two siblings for the panel's grid to place:
 * the grid in one column, the readout in the next.
 */
export default function RecentBlobs() {
  const [paused, setPaused] = useState(false);
  const { feed, next } = useBlobFeed(paused);
  const [sel, setSel] = useState<string | null>(null);
  const [focusAt, setFocusAt] = useState<number | null>(null);
  const grid = useRef<HTMLOListElement>(null);
  // ages move on their own between reads
  const [, setNow] = useState(0);
  useEffect(() => { const t = window.setInterval(() => setNow((n) => n + 1), 15000); return () => window.clearInterval(t); }, []);
  const [motion, setMotion] = useState(true);
  useEffect(() => { setMotion(!reducedMotion()); }, []);

  const { cells, arrival } = feed;
  const slots: (Cell | null)[] = Array.from({ length: CELLS }, () => null);
  for (const c of cells) slots[c.seq % CELLS] = c;
  const head = feed.next - 1;
  const shown = (sel && cells.find((c) => c.b.promise_hash === sel)?.b) || feed.latest;
  const isLatest = !!shown && shown.promise_hash === feed.latest?.promise_hash;
  const shownCell = shown && !isLatest ? cells.find((c) => c.b.promise_hash === shown.promise_hash) : undefined;
  const shownRank = shownCell ? feed.next - 1 - shownCell.seq : null;
  const step = arrival ? Math.min(55, SWEEP_MS / Math.max(1, arrival.count)) : 0;
  const idle = feed.loaded && Date.now() - feed.lastNewAt > IDLE_AFTER_MS;
  const state = paused ? "paused" : feed.error ? "down" : !feed.loaded ? "wait" : "live";
  const liveWord = paused ? "Paused" : feed.error ? "Not answering" : feed.loaded ? "Live" : "Connecting";
  const liveTitle = paused
    ? "Paused: the grid is not reading new blobs."
    : feed.error
      ? `The observer API did not answer (${feed.error}); the grid shows the last read.`
      : `Reads the newest blobs every ${idle ? 15 : 5} s while this page is open${idle && feed.latest ? `; the newest settled ${ago(feed.latest.settlement_time)}` : ""}.`;

  // the grid is one stop in the tab order: arrows walk it as it is drawn, Home is the newest, End the oldest
  const filled = slots.map((c, i) => (c ? i : -1)).filter((i) => i >= 0);
  const headSlot = head >= 0 ? head % CELLS : -1;
  const tabSlot = focusAt != null && slots[focusAt] ? focusAt : headSlot;
  const walk = (e: React.KeyboardEvent, i: number) => {
    let j: number | null = null;
    const by = (d: number) => { for (let k = i + d; k >= 0 && k < CELLS; k += d) if (slots[k]) return k; return null; };
    if (e.key === "ArrowRight") j = by(1);
    else if (e.key === "ArrowLeft") j = by(-1);
    else if (e.key === "ArrowDown") j = by(COLS);
    else if (e.key === "ArrowUp") j = by(-COLS);
    else if (e.key === "Home") j = headSlot;
    else if (e.key === "End") j = cells.length ? cells[cells.length - 1].seq % CELLS : null;
    else return;
    e.preventDefault();
    if (j == null) return;
    setFocusAt(j);
    grid.current?.querySelector<HTMLElement>(`[data-slot="${j}"]`)?.focus();
  };

  const st = shown ? statusOf(shown) : null;
  return (
    <>
      <div className="rb" data-state={state}>
        <div className="rb-head">
          <h2 className="ov-eyebrow">Recent blobs</h2>
          <span className="rb-live" title={liveTitle}>
            <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true" className="rb-ring">
              <circle cx="8" cy="8" r="6" className="rb-ring-t" />
              {next && state === "live" && motion && (
                <circle key={next.at} cx="8" cy="8" r="6" className="rb-ring-f" pathLength={100} style={{ animationDuration: `${next.every}ms` }} />
              )}
              <circle cx="8" cy="8" r="2.2" className="rb-ring-d" />
            </svg>
            <span>{liveWord}</span>
          </span>
          <button type="button" className="rb-pause" aria-pressed={paused} onClick={() => setPaused((p) => !p)}
            title={paused ? "Resume reading new blobs" : "Pause the grid"}>
            {paused
              ? <svg viewBox="0 0 12 12" width="10" height="10" aria-hidden="true"><path d="M3 1.8v8.4L10 6z" fill="currentColor" /></svg>
              : <svg viewBox="0 0 12 12" width="10" height="10" aria-hidden="true"><path d="M3 2h2v8H3zM7 2h2v8H7z" fill="currentColor" /></svg>}
            {paused ? "Resume" : "Pause"}
          </button>
        </div>
        <p className="rb-count">
          {feed.total != null ? <RollNumber value={feed.total} format={int} className="rb-total" /> : <span className="rb-total wait">0,000</span>}
          <span>settlements on record</span>
        </p>
        <ol className="rb-grid" ref={grid} aria-label="The newest blobs, one square each"
          onMouseLeave={() => setSel(null)}
          onBlur={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node)) setSel(null); }}>
          {slots.map((c, i) => {
            if (!c) return <li key={`e${i}`} className="rb-slot"><span className="rb-c rb-empty" /></li>;
            const b = c.b;
            const rank = head - c.seq;
            const s = statusOf(b);
            const o = arrival?.order.get(b.promise_hash);
            const fresh = o != null && motion;
            const style = {
              "--age": (rank / (CELLS - 1)).toFixed(3),
              ...(fresh ? { "--d": `${Math.round(o * step)}ms` } : {}),
            } as React.CSSProperties;
            // The link keeps its square (and keyboard focus) whoever's blob is in it; the fill inside is
            // the blob's, and a new blob's fill is a new element, so its arrival plays once.
            return (
              <li key={`s${i}`} className="rb-slot">
                <Link href={`/blob/?hash=${b.promise_hash}`} prefetch={false} data-slot={i}
                  className={`rb-c${rank === 0 ? " head" : ""}${sel === b.promise_hash ? " on" : ""}`}
                  tabIndex={i === tabSlot ? 0 : -1}
                  aria-label={`${rank === 0 ? "Newest blob" : `${nth(rank + 1)} newest blob`}: height ${int(b.settlement_height)}, settled ${utcWord(b.settlement_time)}, ${bytes(b.blob_size)}, ${s.word}`}
                  onMouseEnter={() => setSel(b.promise_hash)} onFocus={() => { setSel(b.promise_hash); setFocusAt(i); }}
                  onKeyDown={(e) => walk(e, i)}>
                  <span key={b.promise_hash} className={`rb-f${fresh ? " fresh" : ""}`} data-s={s.key} style={style} />
                </Link>
              </li>
            );
          })}
        </ol>
        <p className="rb-key" aria-hidden="true">
          <span><i data-s="window" />in retention window</span>
          <span><i data-s="kept" />available</span>
          <span><i data-s="hold" />unavailable</span>
          {cells.some((c) => statusOf(c.b).key === "gap") && <span><i data-s="gap" />not read by Tensile</span>}
          <span className="rb-key-n"><i data-s="head" />newest</span>
        </p>
      </div>

      <div className="rb-read">
        <h3 className="rb-read-h">
          <span className="ov-eyebrow">{isLatest || !shown ? "Latest blob" : shownRank != null ? `Blob · ${shownRank === 0 ? "newest" : `${nth(shownRank + 1)} newest`}` : "Blob"}</span>
          {shown && <span className="ov-when" title={utcWord(shown.settlement_time)}>settled {ago(shown.settlement_time)}</span>}
        </h3>
        <dl className="rb-spec" aria-busy={!shown || undefined}>
          <div><dt>Height</dt><dd>{shown ? <RollNumber value={shown.settlement_height} format={int} /> : <span className="wait">0,000,000</span>}</dd></div>
          <div><dt>Time</dt><dd>{shown ? whenUTC(shown.settlement_time) : <span className="wait">Sep 00 00:00</span>}</dd></div>
          <div><dt>Blob size</dt><dd>{shown ? bytes(shown.blob_size) : <span className="wait">00.0 MiB</span>}</dd></div>
          <div><dt>Endorsements</dt><dd>{shown?.attested_with_rows != null ? <>{int(shown.attested_with_rows)} <span className="ov-of">of {int(shown.validators_with_rows)} validators</span></> : shown ? "—" : <span className="wait">00 of 00</span>}</dd></div>
          <div><dt>Endorsed voting power</dt><dd>{shown?.attested_voting_power != null && shown.total_voting_power ? pctOf(shown.attested_voting_power, shown.total_voting_power) : shown ? "—" : <span className="wait">00.00%</span>}</dd></div>
          <div className="rb-st">
            <dt title="Observed by Tensile"><Eye />Status</dt>
            <dd>{st ? <span className={`verdict verdict--${st.tier}`} title={st.title}><Mark tier={st.tier} /><span className="w">{st.word}</span></span> : <span className="wait">available</span>}</dd>
          </div>
        </dl>
        <p className="ov-links">
          {shown ? <Link href={`/blob/?hash=${shown.promise_hash}`}>Blob details <span aria-hidden="true">→</span></Link> : <span className="wait" aria-hidden="true">Blob details →</span>}
          <Link href="/blobs/">All blobs <span aria-hidden="true">→</span></Link>
        </p>
      </div>
    </>
  );
}
