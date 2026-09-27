import { type Heatmap as HeatmapData, type HeatCell } from "@/lib/signing";
import { int, pctOf } from "@/lib/api";

/**
 * Day × schedule point, one cell per UTC day and in-window point: how many of
 * this validator's rated probes at that point on that day came back served.
 * The per-point rate spread over the calendar, so one bad day and a validator
 * that prunes early every day do not read the same.
 *
 * The encoding keeps the site's one rule about red: a cell is filled in the
 * served colour, and only the FAULT share of it is drawn in the fault colour,
 * as a band whose height is that share. A cell with rows but nothing rated is
 * flat grey ("no verdict"), a cell with no rows is hatched ("no data"), and
 * neither is ever red. Every cell carries its counts in its title, so the
 * colour is never the only way to read it.
 *
 * Laid out as a CSS grid over the container's width, so it fits a phone: the
 * columns shrink, and past a month they get a minimum width and the grid
 * scrolls inside its own frame rather than the page.
 */

// The schedule points by where they fall in the retention window, as the
// "Through the retention window" table on the same page names them (POINT in
// validator/page.tsx); the key stays in the tooltip.
const POINT_LABEL: Record<string, string> = { w1: "12%", w2: "45%", w3: "72%", w4: "last", end: "end", day: "day" };
const POINT_TITLE: Record<string, string> = {
  w1: "w1 · 12% of the retention window", w2: "w2 · 45% of the window", w3: "w3 · 72% of the window",
  w4: "w4 · within 2 min 30 s of the deadline", end: "end · the one reading, 10 min before the deadline", day: "every point of the day together",
};

function dayLabel(d: string): string {
  return new Date(d + "T00:00:00Z").toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
}

function cellTitle(day: string, point: string, c: HeatCell | undefined, beforeRaw: boolean): string {
  const where = `${dayLabel(day)} · ${POINT_TITLE[point] ?? point}`;
  if (!c) {
    if (beforeRaw && point !== "day") return `${where}: rolled up, per-point detail not kept`;
    return `${where}: no data, no rated or held-out probe`;
  }
  const rated = c.served + c.faults;
  const held = c.held_out > 0 ? ` · ${int(c.held_out)} held out (no verdict)` : "";
  if (rated === 0) return `${where}: no verdict · n = 0 rated · ${int(c.held_out)} probe${c.held_out === 1 ? "" : "s"} held out`;
  const broken = c.faults > 0 ? ` · ${int(c.faults)} not served` : "";
  return `${where}: n = ${int(rated)} rated · served ${int(c.served)} / ${int(rated)} (${pctOf(c.served, rated)})${broken}${held}${c.rolled ? " · from the daily rollup" : ""}`;
}

function Cell({ day, point, c, beforeRaw }: { day: string; point: string; c: HeatCell | undefined; beforeRaw: boolean }) {
  const t = point === "day" ? " total" : "";
  const title = cellTitle(day, point, c, beforeRaw);
  const rated = c ? c.served + c.faults : 0;
  if (!c) return <div className={"hm-c nodata" + t} title={title} aria-label={title} role="img" />;
  if (rated === 0) return <div className={"hm-c noverdict" + t} title={title} aria-label={title} role="img" />;
  const fault = c.faults / rated;
  return (
    <div className={"hm-c rated" + t} title={title} aria-label={title} role="img">
      {fault > 0 && <i style={{ height: `max(2px, ${(fault * 100).toFixed(1)}%)` }} />}
    </div>
  );
}

export default function Heatmap({ data }: { data: HeatmapData | undefined }) {
  if (!data) return null;
  const at = new Map(data.cells.map((c) => [`${c.day}|${c.point}`, c]));
  const rows = [...data.points, "day"];
  const n = data.days.length;
  const anyRow = data.cells.length > 0;
  // A label every few columns: first, last and evenly between, never crowded.
  const every = Math.max(1, Math.ceil(n / 6));
  const lastDrawn = Math.floor((n - 1) / every) * every;
  const showLabel = (i: number) => i % every === 0 || (i === n - 1 && i - lastDrawn >= every);
  const wide = n > 31;
  // An all-hatched grid is a page of nothing; say it in one line instead.
  if (!anyRow) return <div className="hm"><p className="errs">No rated or held-out probe of this validator in this period.</p></div>;
  return (
    <div className="hm">
      <div className="hm-scroll">
        <div className={"hm-grid" + (wide ? " wide" : "")} style={{ gridTemplateColumns: `3em repeat(${n}, minmax(${wide ? "6px" : "0"}, 1fr))` }}
          role="group" aria-label={`served over rated probes per UTC day and schedule point, ${n} day${n === 1 ? "" : "s"}`}>
          {rows.map((p) => (
            <div key={p} style={{ display: "contents" }}>
              <div className={"hm-y" + (p === "day" ? " total" : "")} title={POINT_TITLE[p] ?? p}>{POINT_LABEL[p] ?? p}</div>
              {data.days.map((d) => (
                <Cell key={d} day={d} point={p} c={at.get(`${d}|${p}`)} beforeRaw={!!data.raw_from && d < data.raw_from} />
              ))}
            </div>
          ))}
          <div className="hm-y" />
          {data.days.map((d, i) => (
            <div key={d} className={"hm-x" + (i > n / 2 ? " end" : "")}>{showLabel(i) ? dayLabel(d) : ""}</div>
          ))}
        </div>
      </div>
      <p className="mklegend hm-legend">
        <span><i className="hm-k rated" /> served</span>
        <span><i className="hm-k rated"><i /></i> not-served share</span>
        <span><i className="hm-k noverdict" /> no verdict</span>
        <span><i className="hm-k nodata" /> no data</span>
      </p>
      {data.raw_from && data.days.some((d) => d < data.raw_from!) && <p className="rolled">Days before {data.raw_from} come from the daily rollup, which keeps the whole day and not each point.</p>}
    </div>
  );
}
