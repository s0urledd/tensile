"use client";
import { type Validator, int } from "@/lib/api";
import Info from "@/components/Info";
import { readiness } from "@/components/Readiness";

/** ⅔ set from the text face's own numerals, on its baseline: the font's fraction glyph falls back heavier and off the line */
export function Frac() {
  return <b className="ov-frac"><span className="ov-frac-n">2</span><span className="ov-frac-s">⁄</span><span className="ov-frac-d">3</span></b>;
}

/**
 * Current Fibre providers: the share of voting power held by validators with
 * a Fibre provider registered, as the lead figure, and the same share on a
 * segmented meter with the ⅔ a blob needs to settle drawn through it. The
 * meter's segments are a scale (fiftieths), not a count: the fill is the exact
 * share, cut by the segment gaps. Both halves are the chain's own records
 * (x/staking, x/valaddr); see Readiness.tsx.
 *
 * Without rows yet it keeps its height with a quiet placeholder, so nothing
 * under it moves when the list arrives.
 */
export default function CurrentProviders({ rows }: { rows: Validator[] | null }) {
  const r = rows ? readiness(rows) : null;
  const ready = !!r && r.total > 0;
  const { pct, regPower = 0, quorum = 0, total = 0 } = r ?? {};
  const at = (n: number) => `${total > 0 ? Math.min(100, Math.max(0, (100 * n) / total)) : 0}%`;
  const vars = { "--v": at(regPower), "--q": at(quorum) } as React.CSSProperties;
  return (
    <div className="cp" aria-busy={!ready || undefined}>
      <h2 id="readiness-h" className="ov-eyebrow">
        Current Fibre providers
        <Info label="Current Fibre providers"><p>Share of the stake held by validators with a Fibre provider. A blob needs signatures from ⅔ of the stake to settle.</p></Info>
      </h2>
      <p className="cp-hero">
        <b className={`ov-fig${ready ? "" : " cp-wait"}`}>{ready ? pct!(regPower) : "00.0%"}</b>
        <span className="cp-help">{ready ? <>of voting power · <b>{int(r!.registered.length)}</b> of {int(r!.bonded.length)} validators</> : " "}</span>
      </p>
      <div className={`cp-meter${ready ? "" : " wait"}`} style={vars} role="img"
        aria-label={ready ? `${pct!(regPower)} of voting power with a Fibre provider; ${pct!(quorum)} needed` : "Loading"}>
        {ready && <span className="cp-q"><span><Frac /> needed</span></span>}
        <span className="cp-track"><i /></span>
      </div>
      <p className="cp-rest">
        {ready ? <><i className="cp-sw" aria-hidden="true" />no Fibre provider {pct!(total - regPower)} · {int(r!.bonded.length - r!.registered.length)} validators</> : " "}
      </p>
    </div>
  );
}
