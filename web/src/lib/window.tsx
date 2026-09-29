"use client";
import { useCallback, useState } from "react";
import { useSearchParams } from "next/navigation";

export const WINDOWS = ["24h", "7d", "30d", "all"] as const;
export type WindowName = (typeof WINDOWS)[number];
export const windowLabel = (w: string) => (w === "all" ? "All" : w);
/** the period as a title names it, in parentheses after the name: "24h", "7d", "30d", "all" */
export const periodName = (w: string) => (w === "all" ? "all" : w);
/** a title with the period it counts: "Blob size per day (7d)", "Blobs (all)" */
export const withPeriod = (label: string, w: string) => `${label} (${periodName(w)})`;

/**
 * The selected period, carried in the URL (?window=7d) so a link to a page
 * opens on the period the sender was looking at. Changing it rewrites the
 * address without a navigation.
 */
export function useWindow(def: WindowName = "24h"): [WindowName, (w: WindowName) => void] {
  const params = useSearchParams();
  const fromUrl = params.get("window");
  const [win, setWin] = useState<WindowName>((WINDOWS as readonly string[]).includes(fromUrl ?? "") ? (fromUrl as WindowName) : def);
  const set = useCallback((w: WindowName) => {
    setWin(w);
    try {
      const u = new URL(window.location.href);
      if (w === def) u.searchParams.delete("window"); else u.searchParams.set("window", w);
      window.history.replaceState(null, "", u.pathname + u.search + u.hash);
    } catch { /* fine */ }
  }, [def]);
  return [win, set];
}

export function WindowSwitch({ value, onChange }: { value: WindowName; onChange: (w: WindowName) => void }) {
  return (
    <div className="seg" role="group" aria-label="period">
      {WINDOWS.map((w) => <button key={w} type="button" aria-pressed={value === w} onClick={() => onChange(w)}>{windowLabel(w)}</button>)}
    </div>
  );
}
