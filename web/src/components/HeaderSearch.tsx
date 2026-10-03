"use client";
import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { blobKey } from "@/lib/blobkey";
import { siteTarget } from "@/lib/sitefind";

/** the full placeholder needs about 300 px of field; a narrower one says only "Search" */
const WORDS = "Search tx hash, blob ID or address";
const WORDS_FIT = 300;

/** what the search takes, with the docs' example blob as the sample of each */
const KINDS: [string, string][] = [
  ["Transaction hash", "ADD4C519…5C16"],
  ["Blob ID, base64 or hex", "AJ+0/lcm…d8FQ"],
  ["Commitment or promise hash", "9fb4fe57…c150"],
  ["Valoper address", "celestiavaloper1…"],
  ["Publisher address", "celestia1…"],
];

const ICON = <svg width="15" height="15" viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.75" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="m10.5 10.5 3.5 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>;

/** a key pressed in a field or an editor types there; "/" only opens the search from the page itself */
const typing = (t: EventTarget | null) =>
  t instanceof HTMLElement && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName));

/**
 * The site's search, in the header between the nav and the block: an identifier pasted or typed whole opens what it
 * names (sitefind.ts). A paste goes at once; typing goes on Enter. While the field has focus, a panel under it lists
 * what it takes. "/" anywhere on the page puts the cursor in it.
 */
export default function HeaderSearch() {
  const router = useRouter();
  const [q, setQ] = useState("");
  const [on, setOn] = useState(false);
  const [bad, setBad] = useState(false);
  const [fits, setFits] = useState(true);
  const input = useRef<HTMLInputElement>(null);
  const field = useRef<HTMLLabelElement>(null);
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

  const go = (s: string) => {
    const t = siteTarget(s, blobKey);
    if (!t) { setBad(!!s.trim()); return; }
    setQ("");
    setBad(false);
    input.current?.blur();
    router.push(t.href);
  };

  return (
    <div className={`hs${on ? " on" : ""}${bad ? " bad" : ""}`} role="search"
      onFocus={() => setOn(true)}
      onBlur={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node | null)) setOn(false); }}>
      <div className="hs-box">
        <label className="hs-field" ref={field}>
          {ICON}
          <input ref={input} type="search" value={q} placeholder={fits ? WORDS : "Search"}
            aria-label="Search by transaction hash, blob ID, commitment, promise hash, valoper address or publisher address"
            aria-describedby="hs-kinds" spellCheck={false} autoComplete="off" enterKeyHint="search"
            onPaste={() => { pasted.current = true; }}
            onChange={(e) => {
              const v = e.target.value;
              setQ(v);
              setBad(false);
              if (pasted.current && siteTarget(v, blobKey)) go(v);
              pasted.current = false;
            }}
            onKeyDown={(e) => {
              pasted.current = false;
              if (e.key === "Enter") go(q);
              else if (e.key === "Escape") input.current?.blur();
            }} />
          <kbd className="hs-key" aria-hidden="true">/</kbd>
        </label>
        {/* the panel keeps the field's focus when clicked, so it stays open to read */}
        <div className="hs-dd" id="hs-kinds" hidden={!on} onMouseDown={(e) => e.preventDefault()}>
          <p className="hs-h">Paste any of these</p>
          <ul>{KINDS.map(([k, v]) => <li key={k}><span>{k}</span><code>{v}</code></li>)}</ul>
          <p className="hs-ft" role="status">{bad ? "Not a hash, blob ID or address." : "Enter to search"}</p>
        </div>
      </div>
    </div>
  );
}
