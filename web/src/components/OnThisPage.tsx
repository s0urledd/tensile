"use client";
import { useEffect, useRef, useState } from "react";

/**
 * The section menu of a long reference page: a rail that stays in view beside
 * the text on a wide screen, one compact "On this page" control at the top of
 * the screen on a narrow one. The section in view is marked in both.
 *
 * Plain links to the page's own ids, so the browser does the scrolling and a
 * copied address lands on the same heading. The section in view is the last
 * heading whose top has passed a line a little under the top of the screen.
 */
export type Section = { id: string; label: string; sub?: { id: string; label: string }[] };

export default function OnThisPage({ items, version }: { items: Section[]; version: string }) {
  const [at, setAt] = useState<string>(items[0]?.id ?? "");
  const [open, setOpen] = useState(false);
  const bar = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const ids = items.flatMap((s) => [s.id, ...(s.sub ?? []).map((x) => x.id)]);
    let raf = 0;
    const measure = () => {
      raf = 0;
      // the line sits under the mobile bar when it shows, and a little under the top otherwise
      const line = (bar.current?.offsetHeight ?? 0) + 96;
      let cur = ids[0];
      for (const id of ids) {
        const el = document.getElementById(id);
        if (el && el.getBoundingClientRect().top <= line) cur = id;
      }
      // at the very end of the page the last section is the one being read, however short it is
      if (window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 2) {
        const last = items[items.length - 1];
        cur = last.sub?.length ? last.sub[last.sub.length - 1].id : last.id;
      }
      setAt(cur);
    };
    const onScroll = () => { if (!raf) raf = requestAnimationFrame(measure); };
    measure();
    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onScroll);
    return () => {
      if (raf) cancelAnimationFrame(raf);
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
    };
  }, [items]);

  // the open list closes on Escape and on a tap outside it
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setOpen(false); };
    const onDown = (e: PointerEvent) => { if (!bar.current?.contains(e.target as Node)) setOpen(false); };
    window.addEventListener("keydown", onKey);
    window.addEventListener("pointerdown", onDown);
    return () => { window.removeEventListener("keydown", onKey); window.removeEventListener("pointerdown", onDown); };
  }, [open]);

  const top = items.find((s) => s.id === at || s.sub?.some((x) => x.id === at)) ?? items[0];
  const subAt = top?.sub?.find((x) => x.id === at);

  const list = (where: "rail" | "bar") => (
    <ul className="otp-list">
      {items.map((s) => {
        const on = s === top;
        return (
          <li key={s.id} className={on ? "on" : undefined}>
            <a href={`#${s.id}`} aria-current={at === s.id ? "location" : undefined} onClick={where === "bar" ? () => setOpen(false) : undefined}>{s.label}</a>
            {s.sub && s.sub.length > 0 && (
              <ul className="otp-sub">
                {s.sub.map((x) => (
                  <li key={x.id} className={at === x.id ? "on" : undefined}>
                    <a href={`#${x.id}`} aria-current={at === x.id ? "location" : undefined} onClick={where === "bar" ? () => setOpen(false) : undefined}>{x.label}</a>
                  </li>
                ))}
              </ul>
            )}
          </li>
        );
      })}
    </ul>
  );

  return (
    <aside className="otp" aria-label="On this page">
      <nav className="otp-rail" aria-label="Sections">
        <p className="otp-h">On this page</p>
        {list("rail")}
        <p className="otp-ver"><a href="#version">Version {version}</a></p>
      </nav>
      <div className={`otp-bar${open ? " open" : ""}`} ref={bar}>
        <button type="button" className="otp-btn" aria-expanded={open} aria-controls="otp-panel" onClick={() => setOpen((v) => !v)}>
          <span className="otp-btn-k">On this page</span>
          <span className="otp-btn-v">{subAt ? subAt.label : top?.label}</span>
          <svg className="otp-chev" viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M4 6l4 4 4-4" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /></svg>
        </button>
        <nav id="otp-panel" className="otp-panel" aria-label="Sections" hidden={!open}>
          {list("bar")}
        </nav>
      </div>
    </aside>
  );
}
