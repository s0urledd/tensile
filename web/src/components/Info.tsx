"use client";
import { useCallback, useEffect, useId, useRef, useState } from "react";
import { createPortal } from "react-dom";

/**
 * A definition, one tap away instead of three lines down the page.
 *
 * This site publishes accusations about named operators, so every figure has to
 * be defined somewhere a reader can find it. The first version put the
 * definition under the figure, in prose, and the arithmetic of that is brutal:
 * five layers times three lines each is fifteen lines of explanation wrapped
 * around five numbers, and the numbers are what the reader came for. The page
 * ended up reading as an essay with some data in it.
 *
 * So the definition moves behind a mark next to the label. Nothing is deleted
 * and nothing is made harder to find — the mark is visible, focusable and in
 * the tab order, and the text it holds is the same text — but the default view
 * is the measurement.
 *
 * Three things it has to get right:
 *
 * Reachable without a mouse. It is a real <button>, so Tab reaches it, Enter
 * and Space open it, Escape closes it and focus returns. A title= attribute
 * would have been a tenth of the code and unreachable by keyboard, invisible on
 * touch, and unstyleable.
 *
 * Not clipped by the table. Column headers live inside .tablewrap, which sets
 * overflow-x: auto, and an absolutely positioned panel inside that is cut off
 * at the edge. The panel is therefore portalled to <body> and positioned from
 * the trigger's rect, so it escapes every ancestor's overflow.
 *
 * Positioned inside the viewport. The rect is measured on open and the panel is
 * flipped above the trigger when there is no room below, and pulled back from
 * either edge, so a mark in the last column does not open a panel half off the
 * screen.
 */
export default function Info({ label, children, trigger, className, title }: {
  label: string; children: React.ReactNode;
  /** a word to open it by instead of the "i" mark, with its own class and hover title */
  trigger?: React.ReactNode; className?: string; title?: string;
}) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number; above: boolean } | null>(null);
  const btn = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLDivElement>(null);
  const id = useId();

  const place = useCallback(() => {
    const el = btn.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const W = panel.current?.offsetWidth ?? 320, GUTTER = 12;
    // Room below is measured against the panel's real height once it exists,
    // and against a conservative guess on the first frame.
    const h = panel.current?.offsetHeight ?? 180;
    const above = r.bottom + h + GUTTER > window.innerHeight && r.top > h + GUTTER;
    let left = r.left;
    if (left + W + GUTTER > window.innerWidth) left = window.innerWidth - W - GUTTER;
    if (left < GUTTER) left = GUTTER;
    setPos({ top: above ? r.top - 6 : r.bottom + 6, left, above });
  }, []);

  useEffect(() => {
    if (!open) return;
    place();
    // A second pass once the panel has been measured, so a tall one flips.
    const raf = requestAnimationFrame(place);
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") { setOpen(false); btn.current?.focus(); }
    };
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (!btn.current?.contains(t) && !panel.current?.contains(t)) setOpen(false);
    };
    // Closed rather than repositioned on scroll: a panel that follows the page
    // while the reader is scrolling past it is harder to dismiss than to reopen.
    const onScroll = () => setOpen(false);
    window.addEventListener("keydown", onKey);
    window.addEventListener("mousedown", onDown);
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", onScroll);
    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("mousedown", onDown);
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", onScroll);
    };
  }, [open, place]);

  return (
    <>
      <button ref={btn} type="button" className={trigger ? className : "info"} aria-expanded={open} aria-controls={id}
        aria-label={trigger ? undefined : `what ${label} means`} title={title} onClick={() => setOpen((v) => !v)}>
        {trigger ?? "i"}
      </button>
      {open && typeof document !== "undefined" && createPortal(
        // A click inside the panel stays in it: React carries a portal's events to the row that holds the trigger.
        <div ref={panel} id={id} role="dialog" aria-label={label} className="info-pop" onClick={(e) => e.stopPropagation()}
          style={{ top: pos?.top ?? -9999, left: pos?.left ?? -9999,
                   transform: pos?.above ? "translateY(-100%)" : undefined }}>
          <span className="label">{label}</span>
          <div>{children}</div>
        </div>,
        document.body)}
    </>
  );
}
