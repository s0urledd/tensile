"use client";
import { memo, useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import Link from "next/link";
import { API_BASE, useApi, type Blob, type Rate, type Tip, int, pctOf, bytes, whenUTC, utcWord } from "@/lib/api";
import { recon } from "@/lib/status";
import { Mark, type Tier } from "@/components/Verdict";
import { Eye } from "@/components/Metrics";
import RollNumber, { reducedMotion } from "@/components/RollNumber";

/**
 * The overview's recent blobs: the newest settlements as a grid of squares,
 * one per blob, newest first, and a readout of one blob's facts beside it.
 *
 * The grid reads as text does, newest at the top left, so its reading order
 * is age order. New blobs enter at the top left and everything else moves on
 * by as many places, row by row: each row is a window on one tape, and all
 * five slide together in one 420 ms move per read, the places that leave a
 * row's right end entering the next row's left. The newest is ringed and
 * whatever arrived with it glows for 1.2 s; nothing else moves.
 *
 * Each square is filled with the blob's status as the Blobs list words it:
 * still in its retention window (the accent, lighter, edged), read and
 * available (the accent), read and unavailable (amber), or not read by
 * Tensile (grey). The inner square is the blob's size, known at settlement:
 * inside a 3 px rim of the status, its area is the blob's share of the
 * largest blob the protocol allows (128 MiB), and one too small to draw is a
 * 4 px dot.
 *
 * While the pointer is on the grid, a square has focus, or the reader pressed
 * Pause, the grid holds still: reads go on, "Paused · N new" counts what came
 * in, and letting go brings it all in one move.
 *
 * Reading the list: GET /v1/blobs when the chain moves (the header's /v1/tip
 * stream, shared, so no request of its own), at most every 5 s while blobs
 * arrive and every 15 s once none has for two minutes, every 30 s if the
 * chain stops moving, and never while the tab is hidden. Each read asks for
 * about as many rows as the last one brought, and the API's total says
 * whether more arrived than the page returned, in which case one more read
 * fills the gap.
 */

const COLS = 10, ROWS = 5, CELLS = COLS * ROWS;
const FAST_MS = 5000, SLOW_MS = 15000, FALLBACK_MS = 30000;
/** no new blob for this long reads at the slow pace */
const IDLE_AFTER_MS = 120000;
const SLIDE_MS = 420;
/** the protocol's largest blob (x/fibre MaxBlobSize at the pinned celestia-app, /v1/params protocol.max_blob_size_bytes): a full square */
const MAX_BLOB = 128 * 1024 * 1024;

type Cell = { b: Blob; seq: number };
type Feed = {
  /** newest arrival first, at most CELLS; seq counts arrivals on this page, the newest highest */
  cells: Cell[];
  /** the next arrival's seq */
  next: number;
  total: number | null;
  loaded: boolean;
  error: string | null;
  /** when the newest blob arrived, on the observer's clock (or when it settled, on the first read) */
  lastNewAt: number;
};
const EMPTY: Feed = { cells: [], next: 0, total: null, loaded: false, error: null, lastNewAt: 0 };

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

/**
 * a page of the newest blobs into the feed: new ones on top, in the API's order; known ones take any
 * newer reading. extra: arrivals the page had no room for (the API's total says how many), counted
 * so that "N new" and the move stay true, though only the newest CELLS can be shown.
 */
function merge(s: Feed, p: Page, now: number, extra = 0): { feed: Feed; fresh: number } {
  const byHash = new Map(p.blobs.map((b) => [b.promise_hash, b]));
  const known = new Set(s.cells.map((c) => c.b.promise_hash));
  const kept = s.cells.map((c) => {
    const nb = byHash.get(c.b.promise_hash);
    return nb ? { ...c, b: nb } : c;
  });
  const fresh = p.blobs.filter((b) => !known.has(b.promise_hash)).slice(0, CELLS);
  const total = Number.isFinite(p.total) ? p.total : s.total;
  const first = !s.loaded;
  // the oldest of the new takes the lowest seq, the newest the highest
  const n = fresh.length + (fresh.length ? Math.max(0, extra) : 0);
  const add: Cell[] = fresh.map((b, i) => ({ b, seq: s.next + n - 1 - i }));
  const lastNewAt = first ? (p.blobs[0] ? Date.parse(p.blobs[0].settlement_time) : 0) : fresh.length ? now : s.lastNewAt;
  return {
    feed: { cells: [...add, ...kept].slice(0, CELLS), next: s.next + n, total, loaded: true, error: null, lastNewAt },
    fresh: fresh.length,
  };
}

/**
 * The feed, read when the chain moves (height: the tip's, from the shared
 * stream), paced as the comment at the top says. skew: the observer's clock
 * minus the reader's.
 */
function useBlobFeed(height: number | undefined, skew: number) {
  const [feed, setFeed] = useState<Feed>(EMPTY);
  const cur = useRef(feed);
  const limit = useRef(CELLS);
  const busy = useRef(false);
  const last = useRef(0);
  const later = useRef<number | undefined>(undefined);
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
    try {
      const before = cur.current;
      const now = () => Date.now() + skewRef.current;
      // how many settled since the last read, by the API's total
      const known = new Set(before.cells.map((c) => c.b.promise_hash));
      const allNew = (pg: Page) => pg.blobs.length > 0 && pg.blobs.every((b) => !known.has(b.promise_hash));
      let page = await readPage(limit.current);
      const delta = before.loaded && before.total != null && Number.isFinite(page.total) ? page.total - before.total : 0;
      // More arrived than the page returned, all of it new: one more read fills the squares. The second
      // page, newest first, holds the first, so it replaces it.
      if (before.loaded && allNew(page) && delta > page.blobs.length && page.blobs.length < CELLS) page = await readPage(Math.min(CELLS, delta));
      // what even that page had no room for is counted, not shown
      const extra = before.loaded && allNew(page) ? Math.max(0, delta - page.blobs.length) : 0;
      const { feed: f, fresh } = merge(before, page, now(), extra);
      apply(f);
      // ask next time for about as many as this time brought
      const came = before.loaded ? Math.max(fresh, delta) : 0;
      limit.current = Math.min(CELLS, Math.max(2, Math.ceil(came * 1.5) + 2));
    } catch (e) {
      apply({ ...cur.current, error: e instanceof Error ? e.message : String(e) });
    } finally {
      busy.current = false;
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

  // the first read, and one each time the chain moves
  useEffect(() => { request(); }, [height, request]);
  // a read when the page comes back into view, and a slow one when the chain stops moving
  useEffect(() => {
    const t = window.setInterval(() => { if (!document.hidden && Date.now() - last.current >= FALLBACK_MS) read(); }, 5000);
    const onVis = () => { if (document.hidden) window.clearTimeout(later.current); else request(); };
    document.addEventListener("visibilitychange", onVis);
    return () => { window.clearInterval(t); window.clearTimeout(later.current); document.removeEventListener("visibilitychange", onVis); };
  }, [read, request]);

  return feed;
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
/** "8 s ago", "4 min ago", "3 h 5 min ago", "2 d ago", by the given clock */
function liveAgo(t: string, now: number): string {
  const s = Math.floor((now - Date.parse(t)) / 1000);
  if (!Number.isFinite(s)) return "";
  if (s < 1) return "just now";
  if (s < 60) return `${s} s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} min ago`;
  const h = Math.floor(m / 60), rm = m % 60;
  if (h < 24) return rm ? `${h} h ${rm} min ago` : `${h} h ago`;
  return `${Math.floor(h / 24)} d ago`;
}

/** a blob's age on the observer's clock, every second while it is under a minute old, then every 15 s */
function Age({ at, skew }: { at: string; skew: number }) {
  const [now, setNow] = useState(0);
  useEffect(() => {
    let t: number | undefined;
    const tick = () => {
      const n = Date.now() + skew;
      setNow(n);
      const age = n - Date.parse(at);
      t = window.setTimeout(tick, age < 60000 ? 1000 - (Math.max(0, age) % 1000) : 15000);
    };
    tick();
    return () => window.clearTimeout(t);
  }, [at, skew]);
  return now ? <span className="ov-when" title={utcWord(at)}>settled {liveAgo(at, now)}</span> : null;
}

/** the side of a blob's square, as a share of its place's: the square's area is the blob's share of the largest blob */
const sideOf = (b: Blob) => Math.sqrt(Math.max(0, Math.min(1, (b.blob_size || 0) / MAX_BLOB)));

export type Observed = { rec: Rate | null | undefined; examinedAll: boolean };

/**
 * Tensile's own figure beside the chain's: the Blob availability share (CIP-51's
 * term) over the blobs whose reading decides them. One quiet line under a
 * hairline, prefixed "Observed by Tensile", set smaller than the chain's
 * figures. It keeps its place while it loads.
 */
export function Availability({ observed }: { observed: Observed }) {
  const rec = observed.rec;
  return (
    <div className="ob">
      <p className="ob-row" aria-busy={!rec || undefined}
        title="Observed by Tensile: blobs whose rows were enough to reconstruct them, over the blobs read (available plus unavailable): the newest ones with a reading, up to the API's sample limit.">
        <span className="ob-pre"><Eye />Observed by Tensile</span>
        <span className="ob-fig">Blob availability {rec
          ? <b className={rec.den > 0 ? undefined : "absent"}>{pctOf(rec.num, rec.den)}</b>
          : <b className="wait">000%</b>}</span>
        {rec
          ? <span className="ob-h">{rec.den > 0 ? `${int(rec.num)} of ${observed.examinedAll ? "" : "the newest "}${int(rec.den)} blobs read` : "none read"}</span>
          : <span className="ob-h"><span className="wait">0,000 of 0,000 blobs read</span></span>}
      </p>
    </div>
  );
}

/** what the grid shows: a snapshot of the feed, and the last move into it */
type Shown = { cells: Cell[]; total: number | null; move: { id: number; n: number; fresh: Set<string> } | null };

type PlaceProps = { c: Cell; i: number; fresh: boolean; on: boolean; tab: boolean; ghost: boolean; onShow: (h: string) => void; onKey: (e: React.KeyboardEvent, i: number) => void; onFocusAt: (i: number) => void };
/** one place on the tape: a real one is the blob's link; a ghost is its copy past a row's end, there only to slide out of view */
const Place = memo(function Place({ c, i, fresh, on, tab, ghost, onShow, onKey, onFocusAt }: PlaceProps) {
  const b = c.b;
  const s = statusOf(b);
  const k = sideOf(b);
  const cls = `rb-c${i === 0 ? " rb-newest" : ""}${fresh ? " rb-new" : ""}${on ? " rb-on" : ""}${ghost ? " rb-ghost" : ""}`;
  const sq = <i className={`rb-sq${k * 20 < 4 ? " rb-dot" : ""}`} style={{ "--k": k.toFixed(4) } as React.CSSProperties} />;
  if (ghost) return <span className={cls} data-s={s.key} aria-hidden="true">{sq}</span>;
  return (
    <Link href={`/blob/?hash=${b.promise_hash}`} prefetch={false} data-i={i} className={cls} data-s={s.key} role="listitem"
      tabIndex={tab ? 0 : -1}
      aria-label={`${i === 0 ? "Newest blob" : `${nth(i + 1)} newest blob`}: height ${int(b.settlement_height)}, settled ${utcWord(b.settlement_time)}, ${bytes(b.blob_size)}, ${s.word}`}
      onMouseEnter={() => onShow(b.promise_hash)} onFocus={() => { onShow(b.promise_hash); onFocusAt(i); }}
      onKeyDown={(e) => onKey(e, i)}>
      {sq}
    </Link>
  );
});

/**
 * The grid and the readout, as two siblings for the panel's grid to place:
 * the grid in one column, the readout in the next.
 */
export default function RecentBlobs() {
  const tip = useApi<Tip>("/v1/tip", 4000); // the header's stream: no request of its own
  const skew = tip.data?.server_time && tip.fetchedAt ? Date.parse(tip.data.server_time) - Date.parse(tip.fetchedAt) : 0;
  const feed = useBlobFeed(tip.data?.height, skew);

  const [paused, setPaused] = useState(false);
  const [pointerIn, setPointerIn] = useState(false);
  const [focusIn, setFocusIn] = useState(false);
  const hold = paused || pointerIn || focusIn;
  const [motion, setMotion] = useState(true);
  useEffect(() => { setMotion(!reducedMotion()); }, []);

  // ---- the snapshot on screen: the feed, unless held ----
  const [shown, setShown] = useState<Shown>({ cells: [], total: null, move: null });
  useEffect(() => {
    if (hold || !feed.loaded) return;
    setShown((v) => {
      if (v.cells === feed.cells && v.total === feed.total) return v;
      const head = v.cells.length ? v.cells[0].seq : null;
      const came = head == null ? [] : feed.cells.filter((c) => c.seq > head);
      const n = head == null ? 0 : feed.next - 1 - head;
      return {
        cells: feed.cells,
        total: feed.total,
        move: n > 0 ? { id: (v.move?.id ?? 0) + 1, n, fresh: new Set(came.map((c) => c.b.promise_hash)) } : v.move,
      };
    });
  }, [feed, hold]);
  const waiting = shown.cells.length ? Math.max(0, feed.next - 1 - shown.cells[0].seq) : 0;

  // ---- the move: every row's tape slides by the places that came in, once, together ----
  const gridRef = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => {
    const g = gridRef.current, mv = shown.move;
    if (!g || !mv || !motion) return;
    const first = g.querySelectorAll<HTMLElement>(".rb-row:first-child .rb-c");
    const pitch = first.length > 1 ? first[1].offsetLeft - first[0].offsetLeft : 31;
    const d = Math.min(mv.n, COLS) * pitch;
    g.classList.add("rb-moving");
    const anims = [...g.querySelectorAll<HTMLElement>(".rb-tape")].map((t) =>
      t.animate([{ transform: `translateX(${-d}px)` }, { transform: "translateX(0)" }], { duration: SLIDE_MS, easing: "cubic-bezier(.2, .8, .2, 1)" }));
    const done = window.setTimeout(() => g.classList.remove("rb-moving"), SLIDE_MS + 20);
    return () => { window.clearTimeout(done); g.classList.remove("rb-moving"); anims.forEach((a) => a.cancel()); };
  }, [shown.move?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  // ---- pointer, focus and keys ----
  const [sel, setSel] = useState<string | null>(null);
  const [focusAt, setFocusAt] = useState(0);
  const cells = shown.cells;
  const walk = useCallback((e: React.KeyboardEvent, i: number) => {
    const n = gridRef.current?.querySelectorAll("[data-i]").length ?? 0;
    const j = e.key === "ArrowRight" ? i + 1 : e.key === "ArrowLeft" ? i - 1 : e.key === "ArrowDown" ? i + COLS : e.key === "ArrowUp" ? i - COLS
      : e.key === "Home" ? 0 : e.key === "End" ? n - 1 : null;
    if (j == null) return;
    e.preventDefault();
    if (j < 0 || j >= n) return;
    setFocusAt(j);
    gridRef.current?.querySelector<HTMLElement>(`[data-i="${j}"]`)?.focus();
  }, []);
  const onShow = useCallback((h: string) => setSel(h), []);

  // ---- the readout: the blob under the pointer or focus, else the newest by settlement ----
  const latest = cells.reduce<Blob | null>((m, c) => (!m || c.b.settlement_height > m.settlement_height || (c.b.settlement_height === m.settlement_height && c.b.settlement_tx_index > m.settlement_tx_index) ? c.b : m), null);
  const selAt = sel ? cells.findIndex((c) => c.b.promise_hash === sel) : -1;
  const blob = selAt >= 0 ? cells[selAt].b : latest;
  const isLatest = !!blob && blob.promise_hash === latest?.promise_hash;
  const st = blob ? statusOf(blob) : null;

  const state = paused ? "paused" : feed.error ? "down" : !feed.loaded ? "wait" : "live";
  const idle = feed.loaded && Date.now() + skew - feed.lastNewAt > IDLE_AFTER_MS;
  const liveWord = paused ? (waiting ? `Paused · ${int(waiting)} new` : "Paused") : feed.error ? "Not answering" : feed.loaded ? "Live" : "Connecting";
  const liveTitle = paused
    ? "Paused: the grid holds still while new blobs are counted."
    : feed.error
      ? `The observer API did not answer (${feed.error}); the grid shows the last read.`
      : `Reads the newest blobs as the chain moves, at most every ${idle ? 15 : 5} s, while this page is open.`;
  const statusKeys = new Set(cells.map((c) => statusOf(c.b).key));

  const rows = Array.from({ length: ROWS }, (_, r) => r);
  const mv = shown.move;
  return (
    <>
      <div className="rb" data-state={state}>
        <div className="rb-head">
          <h2 className="ov-eyebrow">Recent blobs</h2>
          <span className="rb-live" title={liveTitle}><i className="rb-live-d" aria-hidden="true" />{liveWord}</span>
          <button type="button" className="rb-pause" aria-pressed={paused} onClick={() => setPaused((p) => !p)}
            title={paused ? "Resume: bring in what arrived" : "Hold the grid still"}>
            {paused
              ? <svg viewBox="0 0 12 12" width="10" height="10" aria-hidden="true"><path d="M3 1.8v8.4L10 6z" fill="currentColor" /></svg>
              : <svg viewBox="0 0 12 12" width="10" height="10" aria-hidden="true"><path d="M3 2h2v8H3zM7 2h2v8H7z" fill="currentColor" /></svg>}
            {paused ? "Resume" : "Pause"}
          </button>
        </div>
        <p className="rb-count">
          {shown.total != null ? <RollNumber value={shown.total} format={int} className="rb-total" /> : <span className="rb-total wait">0,000</span>}
          <span>settlements on record</span>
        </p>
        <div className="rb-grid" ref={gridRef} role="list" aria-label="The newest blobs, newest first"
          onPointerEnter={() => setPointerIn(true)}
          onPointerLeave={() => { setPointerIn(false); setSel(null); }}
          onFocus={() => setFocusIn(true)}
          onBlur={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node)) { setFocusIn(false); setSel(null); } }}>
          {rows.map((r) => (
            <div key={r} className="rb-row" role="presentation">
              <div className="rb-tape" role="presentation">
                {/* the row's places, then the next row's first ten as ghosts, for a move to slide out of view */}
                {Array.from({ length: COLS * 2 }, (_, k) => {
                  const i = r * COLS + k, c = cells[i];
                  const ghost = k >= COLS;
                  if (!c) return ghost ? null : <span key={`e${i}`} className="rb-c rb-empty" aria-hidden="true" />;
                  return (
                    <Place key={c.b.promise_hash} c={c} i={i} ghost={ghost}
                      fresh={!!mv && motion && mv.fresh.has(c.b.promise_hash)} on={sel === c.b.promise_hash}
                      tab={!ghost && i === Math.min(focusAt, cells.length - 1)} onShow={onShow} onKey={walk} onFocusAt={setFocusAt} />
                  );
                })}
              </div>
            </div>
          ))}
        </div>
        <div className="rb-key" aria-hidden="true">
          <p>
            <span><i data-s="window" />in retention window</span>
            <span><i data-s="kept" />available</span>
            <span><i data-s="hold" />unavailable</span>
            {statusKeys.has("gap") && <span><i data-s="gap" />not read by Tensile</span>}
            <span><i data-s="head" />newest</span>
          </p>
          <p className="rb-key-size"><i className="rb-key-sq"><b /></i>Inner square area = blob size (full = 128 MiB)</p>
        </div>
      </div>

      <div className="rb-read">
        <h3 className="rb-read-h">
          <span className="ov-eyebrow">{isLatest || !blob ? "Latest blob" : `Blob · ${selAt === 0 ? "newest" : `${nth(selAt + 1)} newest`}`}</span>
          {blob && <Age at={blob.settlement_time} skew={skew} />}
        </h3>
        <dl className="rb-spec" aria-busy={!blob || undefined}>
          <div><dt>Height</dt><dd>{blob ? (isLatest ? <RollNumber value={blob.settlement_height} format={int} /> : int(blob.settlement_height)) : <span className="wait">0,000,000</span>}</dd></div>
          <div><dt>Time</dt><dd>{blob ? whenUTC(blob.settlement_time) : <span className="wait">Sep 00 00:00</span>}</dd></div>
          <div><dt>Blob size</dt><dd>{blob ? bytes(blob.blob_size) : <span className="wait">00.0 MiB</span>}</dd></div>
          <div><dt>Endorsements</dt><dd>{blob?.attested_with_rows != null ? <>{int(blob.attested_with_rows)} <span className="ov-of">of {int(blob.validators_with_rows)} validators</span></> : blob ? "—" : <span className="wait">00 of 00</span>}</dd></div>
          <div><dt>Endorsed voting power</dt><dd>{blob?.attested_voting_power != null && blob.total_voting_power ? pctOf(blob.attested_voting_power, blob.total_voting_power) : blob ? "—" : <span className="wait">00.00%</span>}</dd></div>
          <div className="rb-st">
            <dt title="Observed by Tensile"><Eye />Status</dt>
            <dd>{st ? <span className={`verdict verdict--${st.tier}`} title={st.title}><Mark tier={st.tier} /><span className="w">{st.word}</span></span> : <span className="wait">available</span>}</dd>
          </div>
        </dl>
        <p className="ov-links">
          {blob ? <Link href={`/blob/?hash=${blob.promise_hash}`}>Blob details <span aria-hidden="true">→</span></Link> : <span className="wait" aria-hidden="true">Blob details →</span>}
          <Link href="/blobs/">All blobs <span aria-hidden="true">→</span></Link>
        </p>
      </div>
    </>
  );
}
