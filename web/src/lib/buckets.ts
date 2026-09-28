import type { Market } from "@/lib/api";
import type { WindowName } from "@/lib/window";

/**
 * The period's settlements in time buckets, from the market answer: one per
 * UTC hour for a day, one per UTC day otherwise (seven or thirty, or every
 * day on record for "all"). Every bucket the window touches is there, the
 * first and the last partial ones included, and an empty one is a zero, so
 * a quiet week is a quiet week and not a shorter chart.
 */
export type Bucket = { key: string; label: string; short?: string; title: string; bytes: number; settlements: number; partial: boolean };

const day = (d: string) => new Date(d + "T00:00:00Z").toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });

export function buckets(market: Market, win: WindowName): Bucket[] {
  const end = new Date(market.window.end).getTime();
  const start = new Date(market.window.start).getTime();
  // "all" has no start of its own: the first day on record
  const unbounded = win === "all" || market.window.start.startsWith("0001-");
  const out: Bucket[] = [];
  if (win === "24h") {
    const byHour = new Map((market.hourly ?? []).map((h) => [h.hour, h]));
    // 25 hours at most, whatever the start says
    const h1 = Math.floor(end / 3600_000);
    const h0 = unbounded ? h1 - 23 : Math.max(Math.floor(start / 3600_000), h1 - 24);
    for (let h = h0; h <= h1; h++) {
      const key = new Date(h * 3600_000).toISOString().slice(0, 13);
      const c = byHour.get(key);
      const partial = h === h1 || (h === h0 && start % 3600_000 !== 0);
      out.push({ key, label: `${key.slice(11)}:00`, title: `${day(key.slice(0, 10))} ${key.slice(11)}:00 UTC${partial ? " (partial hour)" : ""}`,
        bytes: c?.bytes ?? 0, settlements: c?.settlements ?? 0, partial });
    }
    return out;
  }
  const byDay = new Map(market.daily.map((d) => [d.day, d]));
  const d1 = Math.floor(end / 86400_000);
  let d0 = Math.floor(start / 86400_000);
  if (unbounded) {
    const first = market.daily.length > 0 ? Math.floor(new Date(market.daily[0].day + "T00:00:00Z").getTime() / 86400_000) : d1;
    d0 = Math.min(first, d1 - 6);
  }
  for (let d = d0; d <= d1; d++) {
    const key = new Date(d * 86400_000).toISOString().slice(0, 10);
    const c = byDay.get(key);
    const partial = d === d1 || (!unbounded && d === d0 && start % 86400_000 !== 0);
    out.push({ key, label: day(key), short: String(Number(key.slice(8))), title: `${day(key)} UTC${partial ? " (partial day)" : ""}`, bytes: c?.bytes ?? 0, settlements: c?.settlements ?? 0, partial });
  }
  return out;
}
