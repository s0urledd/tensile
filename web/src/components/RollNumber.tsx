"use client";
import { useEffect, useRef, useState } from "react";

/**
 * A figure whose changed digits roll to their new value: the old digit
 * slides out and the new one in, upward when the figure grew and downward when
 * it shrank. Only the digits that changed move; separators and the digits that
 * stayed are still. The figure itself is never smoothed or interpolated: the
 * text is the value, the roll only changes how it is replaced.
 *
 * Two layers at most per digit, whatever the rate of change: a new value that
 * arrives mid-roll replaces the incoming digit and restarts its roll, so rolls
 * never queue. With reduced motion the text is simply replaced.
 */
export default function RollNumber({ value, format, className }: { value: number; format: (n: number) => string; className?: string }) {
  const text = format(value);
  const prev = useRef<{ text: string; value: number } | null>(null);
  // per position from the right: the digit rolling out, and a counter that re-keys the roll
  const [roll, setRoll] = useState<{ out: Map<number, string>; n: number; up: boolean }>({ out: new Map(), n: 0, up: true });
  const clear = useRef<number | undefined>(undefined);

  useEffect(() => {
    const p = prev.current;
    prev.current = { text, value };
    if (!p || p.text === text || reducedMotion()) return;
    const out = new Map<number, string>();
    const a = [...p.text].reverse(), b = [...text].reverse();
    for (let i = 0; i < b.length; i++) {
      if (/\d/.test(b[i]) && a[i] !== b[i]) out.set(i, a[i] ?? "");
    }
    if (!out.size) return;
    setRoll((r) => ({ out, n: r.n + 1, up: value >= p.value }));
    window.clearTimeout(clear.current);
    clear.current = window.setTimeout(() => setRoll((r) => ({ ...r, out: new Map() })), 720);
  }, [text, value]);
  useEffect(() => () => window.clearTimeout(clear.current), []);

  const chars = [...text];
  return (
    <span className={["rn", roll.up ? "" : "down", className ?? ""].filter(Boolean).join(" ")}>
      <span className="sr-only">{text}</span>
      <span aria-hidden="true" className="rn-v">
        {chars.map((c, j) => {
          const i = chars.length - 1 - j;
          const was = roll.out.get(i);
          if (was === undefined) return <span key={`s${i}`} className="rn-c">{c}</span>;
          return (
            <span key={`r${i}`} className="rn-c rn-roll">
              <span key={`o${roll.n}`} className="rn-out">{was || " "}</span>
              <span key={`i${roll.n}`} className="rn-in">{c}</span>
            </span>
          );
        })}
      </span>
    </span>
  );
}

export function reducedMotion(): boolean {
  try { return window.matchMedia("(prefers-reduced-motion: reduce)").matches; } catch { return false; }
}
