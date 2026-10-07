"use client";
import Link from "next/link";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { useRouter } from "next/navigation";
import { API_BASE, useNewestBlob, type Blob, type Validator, type Publisher, int, utcWord } from "@/lib/api";
import { blobKey } from "@/lib/blobkey";
import { useFind } from "@/lib/blobfind";
import { siteTarget, type SiteTarget } from "@/lib/sitefind";
import Ident from "@/components/Ident";

/** the full placeholder needs about 300 px of field; a narrower one says only "Search" */
const WORDS = "Search tx hash, blob ID or address";
const WORDS_FIT = 300;

/** the most blobs one search lists in its panel, newest first; each opens its own page */
const SHOWN = 25;

/**
 * how long after a blob identifier is first looked up a search that found nothing asks again, each time Tensile
 * records a new blob while the panel is open: a reader who pasted a transaction hash a moment before its block was
 * read sees the blob appear
 */
const RETRY_FOR = 2 * 60 * 1000;

/** what the search takes, with the docs' example blob as the sample of each */
const KINDS: [string, string][] = [
  ["Transaction hash", "ADD4C519…5C16"],
  ["Blob ID, base64 or hex", "AJ+0/lcm…d8FQ"],
  ["Commitment or promise hash", "9fb4fe57…c150"],
  ["Valoper address", "celestiavaloper1…"],
  ["Publisher address", "celestia1…"],
];

const ICON = <svg width="15" height="15" viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.75" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="m10.5 10.5 3.5 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>;
/** a blob: its rows, stacked */
const BLOB = <svg className="hs-glyph" width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><rect x="2.5" y="2.5" width="11" height="11" rx="2.5" fill="none" stroke="currentColor" strokeWidth="1.3" /><path d="M5 6h6M5 8h6M5 10h4" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" /></svg>;

/** a key pressed in a field or an editor types there; "/" only opens the search from the page itself */
const typing = (t: EventTarget | null) =>
  t instanceof HTMLElement && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName));

/**
 * Whether what is in the field is an identifier whole, so the search can look it up as it stands. A paste is. Hex being
 * typed waits until it is whole or Enter: 64 characters starting 00 may be the first 64 of a blob ID in hex. Base64
 * and addresses are whole once they read as one, since a part of either reads as none.
 */
function whole(v: string, pasted: boolean): boolean {
  const t = v.trim();
  if (!siteTarget(t, blobKey)) return false;
  if (pasted) return true;
  const h = t.replace(/^0x/i, "");
  // typed hex is a hash once it reads as one (44 characters of hex also read as base64), a blob ID in hex at 66
  return !/^[0-9a-f]+$/i.test(h) || h.length >= 66 || (blobKey(t)?.kind === "hash" && !h.startsWith("00"));
}

/**
 * One record from the API, asked once for this path, and again each time `again` changes: null until its own answer
 * is in, so a record asked earlier never stands for the one asked now. missing: the API has no such record (404, 410);
 * invalid: it refused the address (400).
 */
type Got<T> = { data: T | null; missing: boolean; invalid: boolean; error: string | null };
function useRecord<T>(path: string | null, again: number): Got<T> | null {
  const [st, setSt] = useState<{ ask: string; got: Got<T> } | null>(null);
  const ask = path ? `${again}|${path}` : null;
  useEffect(() => {
    if (!path) return;
    let live = true;
    (async () => {
      let got: Got<T>;
      try {
        const r = await fetch(API_BASE + path);
        got = r.ok ? { data: (await r.json()) as T, missing: false, invalid: false, error: null }
          : { data: null, missing: r.status === 404 || r.status === 410, invalid: r.status === 400, error: `HTTP ${r.status}` };
      } catch (e) {
        got = { data: null, missing: false, invalid: false, error: e instanceof Error ? e.message : String(e) };
      }
      if (live) setSt({ ask: `${again}|${path}`, got });
    })();
    return () => { live = false; };
  }, [path, again]);
  return ask && st && st.ask === ask ? st.got : null;
}

/**
 * one record the search found, as a row of its panel that opens the record's page: what it is, in one word, and which;
 * a blob also says when it settled, so the several settlements of one blob ID tell apart
 */
type Item = { href: string; glyph: ReactNode; kind: string; title: ReactNode; label: string; at?: string; hash?: string };

const short = (s: string, head = 8, tail = 4) => (s.length > head + tail + 1 ? `${s.slice(0, head)}…${s.slice(-tail)}` : s);
/** an address as the lists print it: its prefix, then its last four */
const addrWords = (a: string) => { const i = a.lastIndexOf("1"); return i > 0 ? `${a.slice(0, i)} ••• ${a.slice(-4)}` : a; };
/** a plain left click: anything else (a new tab, a download, a modifier) is the browser's */
const plain = (e: React.MouseEvent) => !(e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0);

const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
/** "Oct 2, 18:23 UTC": the day always, since the settlements of one blob ID may fall on different days */
const at = (s: string) => { const d = new Date(s); return isNaN(d.getTime()) ? s : `${MON[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.toISOString().slice(11, 16)} UTC`; };

/** a blob as two lines: its block and settlement time over its promise hash */
const blobItem = (b: Blob): Item => ({
  href: `/blob/?hash=${b.promise_hash}`,
  label: `Blob ${b.promise_hash.slice(0, 10)}, block ${int(b.settlement_height)}`,
  glyph: BLOB, kind: "Blob", at: b.settlement_time, hash: b.promise_hash,
  title: <span className="hs-ht">#{int(b.settlement_height)}</span>,
});

/**
 * The site's search, in the header between the nav and the block. An identifier pasted, typed whole or entered is
 * looked up at once and what it names shows in a panel under the field, one row each, saying what it is: the blob or
 * blobs it matches, the validator, the publisher. Nothing opens until the reader picks a row, by click or by the arrow
 * keys and Enter (a row under a resting pointer is not picked). With nothing to look up, the panel lists what the
 * search takes. "/" anywhere on the page puts the cursor in it.
 */
export default function HeaderSearch() {
  const router = useRouter();
  const [q, setQ] = useState("");
  const [asked, setAsked] = useState<SiteTarget | null>(null);
  const [on, setOn] = useState(false);
  const [bad, setBad] = useState(false);
  const [pick, setPick] = useState(-1);
  const [fits, setFits] = useState(true);
  const input = useRef<HTMLInputElement>(null);
  const field = useRef<HTMLLabelElement>(null);
  const box = useRef<HTMLDivElement>(null);
  const pasted = useRef(false);

  useEffect(() => {
    const el = field.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(() => setFits(el.clientWidth >= WORDS_FIT));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  useEffect(() => {
    const key = (e: KeyboardEvent) => {
      if (e.key !== "/" || e.ctrlKey || e.metaKey || e.altKey || typing(e.target)) return;
      e.preventDefault();
      input.current?.focus();
    };
    document.addEventListener("keydown", key);
    return () => document.removeEventListener("keydown", key);
  }, []);

  // the lookups: each asks only for its own kind, once, and stands until the identifier changes; a blob identifier
  // that found nothing asks again as the tip names a newer blob, while the panel is open (RETRY_FOR)
  // again: Enter or a paste of the same identifier after a lookup that failed asks it once more ("Try again in a
  // moment" said so; the same identifier used to ask nothing until the field was cleared)
  const [again, setAgain] = useState(0);
  const newest = useNewestBlob(); // the header's tip stream: no request of its own
  const blob = asked?.kind === "blob" ? blobKey(asked.id) : null;
  const hit = useFind(blob, { limit: SHOWN, retryOn: on ? newest : null, retryForMs: RETRY_FOR, again });
  const val = useRecord<{ validator: Validator }>(asked?.kind === "validator" ? `/v1/validators/${asked.id}?window=24h` : null, again);
  const pub = useRecord<{ publisher: Publisher }>(asked?.kind === "publisher" ? `/v1/publishers/${asked.id}?window=all` : null, again);
  // the lookup shown ended with the observer not answering
  const failed = asked?.kind === "blob" ? !!hit?.error && hit.rows.length === 0
    : asked?.kind === "validator" ? !!val?.error && !val.missing && !val.invalid
    : asked?.kind === "publisher" ? !!pub?.error && !pub.missing && !pub.invalid
    : false;

  let items: Item[] = [];
  let note: ReactNode = null;
  if (asked?.kind === "blob") {
    if (!hit) note = <span className="hs-wait">Looking it up…</span>;
    else if (hit.error && hit.rows.length === 0) note = <>The observer did not answer ({hit.error}). Try again in a moment.</>;
    else if (hit.total === 0) note = blob?.kind === "id"
      ? <>Tensile has not indexed a blob with this blob ID yet. A blob appears once Tensile has read the block that settled it.</>
      : <>Tensile has not indexed a blob with this hash yet, or the transaction carries no Fibre blob.</>;
    else {
      items = hit.rows.slice(0, SHOWN).map(blobItem);
      if (hit.partial) note = <>The observer did not answer every lookup, so there may be more.</>;
      else if (hit.total > items.length) note = <>The newest {int(items.length)} of {int(hit.total)}.</>;
    }
  } else if (asked?.kind === "validator") {
    const v = val?.data?.validator;
    if (v) {
      items = [{
        href: asked.href, label: `Validator ${v.moniker || asked.id}`,
        glyph: <Ident addr={v.operator_address || asked.id} />, kind: "Validator",
        title: <span className="hs-nm" title={v.operator_address || asked.id}>{v.moniker || short(asked.id, 16, 4)}</span>,
      }];
    } else if (val?.missing) note = <>No validator with this address is on record.</>;
    else if (val?.invalid) note = <>Not a valid validator address. Check it and paste it again.</>;
    else if (val?.error) note = <>The observer did not answer ({val.error}). Try again in a moment.</>;
    else note = <span className="hs-wait">Looking it up…</span>;
  } else if (asked?.kind === "publisher") {
    const p = pub?.data?.publisher;
    if (p) {
      items = [{
        href: asked.href, label: `Publisher ${p.label || asked.id}`,
        glyph: <Ident addr={p.publisher} />, kind: "Publisher",
        title: <span className="hs-nm" title={p.publisher}>{p.label || addrWords(p.publisher)}</span>,
      }];
    } else if (pub?.missing) note = <>No publisher with this address is on record.</>;
    else if (pub?.invalid) note = <>Not a valid account address. Check it and paste it again.</>;
    else if (pub?.error) note = <>The observer did not answer ({pub.error}). Try again in a moment.</>;
    else note = <span className="hs-wait">Looking it up…</span>;
  }

  const ask = (t: SiteTarget | null) => { setAsked(t); setPick(-1); };
  // the row the arrow keys picked stays in sight in a long list
  useEffect(() => { if (pick >= 0) document.getElementById(`hs-i${pick}`)?.scrollIntoView({ block: "nearest" }); }, [pick]);
  const open = (href: string) => {
    setQ("");
    ask(null);
    setBad(false);
    // a row picked with Tab has the focus, not the field: whichever it is, it lets go, and the panel closes with it
    const el = document.activeElement;
    if (el instanceof HTMLElement && box.current?.contains(el)) el.blur();
    setOn(false);
    router.push(href);
  };
  const enter = () => {
    if (asked && pick >= 0 && pick < items.length) return open(items[pick].href);
    const t = siteTarget(q, blobKey);
    if (t) { if (t.id !== asked?.id) ask(t); else if (failed) setAgain((n) => n + 1); setBad(false); } else setBad(!!q.trim());
  };

  return (
    <div ref={box} className={`hs${on ? " on" : ""}${bad ? " bad" : ""}`} role="search"
      onFocus={() => setOn(true)}
      onBlur={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node | null)) setOn(false); }}>
      <div className="hs-box">
        <label className="hs-field" ref={field}>
          {ICON}
          <input ref={input} type="search" value={q} placeholder={fits ? WORDS : "Search"}
            aria-label="Search by transaction hash, blob ID, commitment, promise hash, valoper address or publisher address"
            aria-controls="hs-list" aria-expanded={on && items.length > 0} aria-activedescendant={on && pick >= 0 ? `hs-i${pick}` : undefined}
            role="combobox" aria-autocomplete="list"
            spellCheck={false} autoComplete="off" enterKeyHint="search"
            onPaste={(e) => {
              pasted.current = true;
              // the same identifier pasted over itself changes nothing in the field, so no change follows: a lookup
              // that failed is asked again here
              const t = failed ? siteTarget(e.clipboardData.getData("text"), blobKey) : null;
              if (t && t.id === asked?.id) setAgain((n) => n + 1);
            }}
            onChange={(e) => {
              const v = e.target.value;
              setQ(v);
              setBad(false);
              const t = whole(v, pasted.current) ? siteTarget(v, blobKey) : null;
              if (t?.id !== asked?.id) ask(t);
              pasted.current = false;
            }}
            onKeyDown={(e) => {
              pasted.current = false;
              if (e.key === "Enter") { e.preventDefault(); enter(); }
              else if (e.key === "Escape") input.current?.blur();
              else if ((e.key === "ArrowDown" || e.key === "ArrowUp") && items.length) {
                e.preventDefault();
                setPick((p) => (e.key === "ArrowDown" ? (p + 1) % items.length : p <= 0 ? items.length - 1 : p - 1));
              }
            }} />
          <kbd className="hs-key" aria-hidden="true">/</kbd>
        </label>
        {/* the panel keeps the field's focus when clicked, so it stays open; a row is a link and opens on click */}
        <div className={`hs-dd${asked ? " res" : ""}`} id="hs-panel" hidden={!on} onMouseDown={(e) => e.preventDefault()}>
          {asked ? (
            <>
              {items.length > 0 && (
                <ul className="hs-items" id="hs-list" role="listbox" aria-label="What the search found">
                  {items.map((it, i) => (
                    <li key={it.href} role="presentation">
                      <Link id={`hs-i${i}`} role="option" aria-selected={pick === i} aria-label={it.label}
                        className={`hs-item${pick === i ? " on" : ""}`} href={it.href}
                        onClick={(e) => { if (!plain(e)) return; e.preventDefault(); open(it.href); }}>
                        <span className="hs-ic">{it.glyph}</span>
                        <span className="hs-bd">
                          <span className="hs-top">
                            <span className="hs-k">{it.kind}</span>
                            {it.title}
                            {it.at && <span className="hs-at" title={`Settled ${utcWord(it.at)}`}>{at(it.at)}</span>}
                          </span>
                          {it.hash && <span className="hs-hash mono" title={it.hash}>{short(it.hash, 18, 10)}</span>}
                        </span>
                      </Link>
                    </li>
                  ))}
                </ul>
              )}
              {note && <p className="hs-note" role="status">{note}</p>}
            </>
          ) : (
            <>
              <p className="hs-h">Paste any of these</p>
              <ul className="hs-kinds">{KINDS.map(([k, v]) => <li key={k}><span>{k}</span><code>{v}</code></li>)}</ul>
              <p className="hs-ft" role="status">{bad ? "Not a hash, blob ID or address." : "Paste or press Enter to look it up"}</p>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
