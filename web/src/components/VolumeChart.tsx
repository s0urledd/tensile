import { type Market, bytes, int } from "@/lib/api";
import type { WindowName } from "@/lib/window";

/**
 * Upload size (or settlements) over the selected period: the padded size the module
 * charges for, from the chain's own records. A day is one column per UTC
 * hour; longer periods are one column per UTC day (seven or thirty, or every
 * day on record for "all"). The newest column is partial and drawn so; a
 * column with nothing settled is a hairline, not an absence.
 */
type Col = { key: string; label: string; bytes: number; settlements: number; partial: boolean; tick: boolean };

function columns(market: Market, win: WindowName): Col[] {
  const end = new Date(market.window.end).getTime();
  const start = new Date(market.window.start).getTime();
  // "all" has no start of its own: the first day on record
  const unbounded = win === "all" || market.window.start.startsWith("0001-");
  const out: Col[] = [];
  if (win === "24h") {
    const byHour = new Map((market.hourly ?? []).map((h) => [h.hour, h]));
    // every hour the window touches, the first and the last partial ones
    // included: 25 at most, whatever the start says
    const h1 = Math.floor(end / 3600_000);
    const h0 = unbounded ? h1 - 23 : Math.max(Math.floor(start / 3600_000), h1 - 24);
    for (let h = h0; h <= h1; h++) {
      const key = new Date(h * 3600_000).toISOString().slice(0, 13);
      const c = byHour.get(key);
      out.push({ key, label: `${key.slice(11)}:00`, bytes: c?.bytes ?? 0, settlements: c?.settlements ?? 0,
        partial: h === h1 || (h === h0 && start % 3600_000 !== 0), tick: Number(key.slice(11)) % 4 === 0 });
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
  const n = d1 - d0 + 1;
  const every = n > 14 ? Math.ceil(n / 8) : 1;
  for (let d = d0; d <= d1; d++) {
    const key = new Date(d * 86400_000).toISOString().slice(0, 10);
    const c = byDay.get(key);
    const label = new Date(key + "T00:00:00Z").toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
    out.push({ key, label, bytes: c?.bytes ?? 0, settlements: c?.settlements ?? 0,
      partial: d === d1 || (!unbounded && d === d0 && start % 86400_000 !== 0), tick: (d1 - d) % every === 0 });
  }
  return out;
}

export default function VolumeChart({ market, win, metric = "bytes" }: { market: Market | null; win: WindowName; metric?: "bytes" | "settlements" }) {
  // the answer for another period, kept while this one loads, is not this chart
  if (!market || market.window.name !== win) return <p className="vchart-empty">Loading…</p>;
  const list = columns(market, win);
  const val = (c: Col) => (metric === "bytes" ? c.bytes : c.settlements);
  const max = Math.max(0, ...list.map(val));
  // the axis top: a round number of MiB (or GiB) above the largest column, or of settlements
  const mib = 1048576;
  const step = metric === "settlements" ? (max > 50 ? 20 : max > 10 ? 10 : 2)
    : max >= 1024 * mib ? 512 * mib : max >= 200 * mib ? 256 * mib : 20 * mib;
  const top = Math.max(step, Math.ceil(max / step) * step);
  const fmt = (v: number) => (metric === "bytes" ? bytes(v) : int(v));
  const unit = win === "24h" ? "hour" : "day";
  return (
    <div className="vchart" role="img" aria-label={`${metric === "bytes" ? "upload size" : "settlements"} per ${unit} over the selected period`}>
      {/* With nothing settled in the period an axis would only label an empty box: no tick labels, one sentence. */}
      <div className="y" aria-hidden={max === 0}>{max > 0 && <><span style={{ top: 0 }}>{fmt(top)}</span><span style={{ top: "50%" }}>{fmt(top / 2)}</span><span style={{ top: "100%" }}>0</span></>}</div>
      <div className="plot">
        {max > 0 && <><i className="gl" style={{ top: 0 }} /><i className="gl" style={{ top: "50%" }} /></>}
        {max === 0 && <span className="none">Nothing settled in this period</span>}
        {list.map((c) => {
          const v = val(c);
          const cls = "col" + (c.partial ? " partial" : "") + (v === 0 ? " zero" : "");
          const title = `${c.key.replace("T", " ")}${unit === "hour" ? ":00 UTC" : ""}${c.partial ? ` (partial ${unit})` : ""} · ${bytes(c.bytes)} · ${int(c.settlements)} settlement${c.settlements === 1 ? "" : "s"}`;
          return <div key={c.key} className={cls} title={title}><i style={{ height: `${Math.max(v / top * 100, v ? 1 : 0).toFixed(1)}%` }} /></div>;
        })}
      </div>
      <div className="x">{list.map((c) => <span key={c.key}>{c.tick ? c.label : ""}</span>)}</div>
    </div>
  );
}
