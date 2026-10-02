"use client";
import { useCallback, useEffect, useRef, useState } from "react";

/**
 * A dot beside a figure that needs attention, its reason in words on hover,
 * on focus and on a tap: amber for something to act on, red (fault) for
 * something that went wrong. The words are placed from the dot in a box of
 * their own on the screen, so a panel's frame, which clips, does not cut them.
 */
export default function Warn({ text, tone = "hold" }: { text: string; tone?: "hold" | "fault" }) {
  const dot = useRef<HTMLButtonElement>(null);
  const [at, setAt] = useState<{ top: number; left: number } | null>(null);
  const show = useCallback(() => {
    const r = dot.current?.getBoundingClientRect();
    if (r) setAt({ top: r.bottom + 8, left: Math.max(12, Math.min(r.left - 12, window.innerWidth - 12 - 340)) });
  }, []);
  const hide = useCallback(() => setAt(null), []);
  // closed rather than moved when the page scrolls or the window changes size
  useEffect(() => {
    if (!at) return;
    const key = (e: KeyboardEvent) => { if (e.key === "Escape") hide(); };
    window.addEventListener("scroll", hide, true);
    window.addEventListener("resize", hide);
    window.addEventListener("keydown", key);
    return () => { window.removeEventListener("scroll", hide, true); window.removeEventListener("resize", hide); window.removeEventListener("keydown", key); };
  }, [at, hide]);
  const cls = tone === "fault" ? " fault" : "";
  return (
    <>
      <button ref={dot} type="button" className={`warn${cls}`} aria-label={text}
        onPointerEnter={(e) => { if (e.pointerType === "mouse") show(); }} onPointerLeave={(e) => { if (e.pointerType === "mouse") hide(); }}
        onFocus={show} onBlur={hide} onClick={show} />
      {at && <span className={`warn-tip${cls}`} role="tooltip" style={{ top: at.top, left: at.left }}>{text}</span>}
    </>
  );
}
