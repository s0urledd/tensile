"use client";
import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { int } from "@/lib/api";

/**
 * A list filter as a chip: its name until something is picked, then the
 * pick, with a button that clears it. A click opens the choices under a
 * search field; typing narrows them by name or by the identifier itself,
 * and a whole identifier that is not among them (one past the list's end)
 * can be picked as typed, so every namespace and every publisher stays
 * reachable however long the lists grow.
 *
 * With find, the chip is a search whose pick opens a page rather than
 * filtering a list (Find a publisher): it only ever says its name, with no
 * chevron, and a whole identifier typed is opened.
 */
export type Choice = { value: string; label: ReactNode; count?: number; find: string };

export default function Picker({ name, icon, value, text, choices, accept, placeholder, onPick, onOpen, find = false }: {
  name: string;
  icon: ReactNode;
  /** the value picked, "" for none */
  value: string;
  /** the pick as the chip shows it */
  text: ReactNode;
  /** null while the list loads */
  choices: Choice[] | null;
  /** a typed identifier as a value to filter by, or null when it is not a whole one */
  accept: (typed: string) => string | null;
  placeholder: string;
  onPick: (v: string) => void;
  /** the first open: the page asks for the choices then */
  onOpen?: () => void;
  /** a search that opens what is picked: the chip is its name alone */
  find?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const [dx, setDx] = useState(0);
  const box = useRef<HTMLSpanElement>(null);
  const pop = useRef<HTMLDivElement>(null);
  const chip = useRef<HTMLButtonElement>(null);
  const input = useRef<HTMLInputElement>(null);

  // a press elsewhere or Escape closes it, as a menu is expected to
  useEffect(() => {
    if (!open) return;
    const down = (e: PointerEvent) => { if (!box.current?.contains(e.target as Node)) setOpen(false); };
    const key = (e: KeyboardEvent) => { if (e.key === "Escape") { setOpen(false); chip.current?.focus(); } };
    document.addEventListener("pointerdown", down);
    document.addEventListener("keydown", key);
    return () => { document.removeEventListener("pointerdown", down); document.removeEventListener("keydown", key); };
  }, [open]);
  // the list opens under the chip and stays inside the screen, 16px from its edges
  useLayoutEffect(() => {
    const p = pop.current;
    if (!open || !p) return;
    const r = p.getBoundingClientRect(), vw = document.documentElement.clientWidth;
    let s = 0;
    if (r.right > vw - 16) s = vw - 16 - r.right;
    if (r.left + s < 16) s = 16 - r.left;
    setDx(s);
    input.current?.focus({ preventScroll: true });
  }, [open]);

  const needle = q.trim().toLowerCase();
  const hit = (choices ?? []).filter((c) => !needle || c.find.includes(needle));
  const typed = needle ? accept(needle) : null;
  const extra = typed && !(choices ?? []).some((c) => c.value === typed) ? typed : null;
  const pick = (v: string) => { setOpen(false); setQ(""); onPick(v); chip.current?.focus(); };

  return (
    <span className={`lg-pick${value ? " on" : ""}`} ref={box}>
      <button ref={chip} type="button" className="lg-chip" aria-expanded={open} title={value || undefined}
        onClick={() => { if (!open) { setDx(0); onOpen?.(); } setOpen(!open); }}>
        {icon}
        {value
          ? <><span className="k">{name}</span><span className="v">{text}</span></>
          : find ? name : <>{name}<svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true"><path d="m2.2 3.8 2.8 2.8 2.8-2.8" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" /></svg></>}
      </button>
      {value && (
        <button type="button" className="lg-x" aria-label={`Show every ${name.toLowerCase()}`} title={`Show every ${name.toLowerCase()}`} onClick={() => pick("")}>
          <svg width="8" height="8" viewBox="0 0 8 8" aria-hidden="true"><path d="m1.5 1.5 5 5m0-5-5 5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" /></svg>
        </button>
      )}
      {open && (
        <div className="lg-pop" ref={pop} role="dialog" aria-label={name} style={dx ? { transform: `translateX(${dx}px)` } : undefined}>
          <label className="find">
            <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true"><circle cx="7" cy="7" r="4.75" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="m10.5 10.5 3.5 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
            <input ref={input} type="search" placeholder={placeholder} aria-label={find ? name : `Find a ${name.toLowerCase()}`} value={q} onChange={(e) => setQ(e.target.value)}
              onKeyDown={(e) => { if (e.key === "Enter") { const v = extra ?? (hit.length === 1 ? hit[0].value : null); if (v) pick(v); } }} />
          </label>
          <ul>
            {extra && <li><button type="button" onClick={() => pick(extra)}><span className="nm">{find ? "Open" : "Show"} <span className="mono">{extra.length > 20 ? `${extra.slice(0, 10)}…${extra.slice(-6)}` : extra}</span></span></button></li>}
            {choices === null && <li className="none">Loading…</li>}
            {choices !== null && hit.length === 0 && !extra && <li className="none">Nothing matches</li>}
            {hit.map((c) => (
              <li key={c.value}>
                <button type="button" aria-pressed={c.value === value} onClick={() => pick(c.value)}>
                  <span className="nm">{c.label}</span>
                  {c.count != null && <span className="ct">{int(c.count)}</span>}
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
    </span>
  );
}
