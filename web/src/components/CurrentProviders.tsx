"use client";
import { useLayoutEffect, useRef, useState } from "react";
import { type Validator, int } from "@/lib/api";
import Info from "@/components/Info";
import { readiness } from "@/components/Readiness";

/** ⅔ set from the text face's own numerals, on its baseline: the font's fraction glyph falls back heavier and off the line */
export function Frac() {
  return <b className="ov-frac"><span className="ov-frac-n">2</span><span className="ov-frac-s">⁄</span><span className="ov-frac-d">3</span></b>;
}

const STRIP_H = 16;

/**
 * Current Fibre providers: the share of voting power held by validators with
 * a Fibre provider registered, as the lead figure, then the bonded set as one
 * strip: one segment per validator, as wide as its voting power, those with a
 * Fibre provider first and lit, largest first, then the others. The ⅔ a blob
 * needs to settle is a needle through it. Every segment is a validator and
 * every width a share of the chain's own records (x/staking, x/valaddr); see
 * Readiness.tsx. A segment three pixels wide or more gives its last pixel to
 * the gap before the next, so the widths stay exact to the pixel.
 *
 * The strip's box has its size in the stylesheet, and it is drawn once it
 * has been measured; without rows yet it keeps its height with a quiet
 * placeholder, so nothing under it moves when the list arrives.
 */
export default function CurrentProviders({ rows }: { rows: Validator[] | null }) {
  const r = rows ? readiness(rows) : null;
  const ready = !!r && r.total > 0;
  const { pct, regPower = 0, quorum = 0, total = 0 } = r ?? {};

  const box = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(0);
  useLayoutEffect(() => {
    const el = box.current;
    if (!el) return;
    const measure = () => setW(el.clientWidth);
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const byPower = (a: Validator, b: Validator) => (b.voting_power || 0) - (a.voting_power || 0);
  const on = ready ? [...r!.registered].sort(byPower) : [];
  const reg = new Set(on.map((v) => v.address));
  const off = ready ? r!.bonded.filter((v) => !reg.has(v.address)).sort(byPower) : [];
  let at = 0;
  const segs = ready && w > 0 ? [...on, ...off].map((v) => {
    const vp = v.voting_power || 0;
    const x = (at * w) / total, wd = (vp * w) / total;
    at += vp;
    return { v, x, wd: wd >= 3 ? wd - 1 : wd, lit: reg.has(v.address) };
  }) : [];
  const q = `${total > 0 ? Math.min(100, Math.max(0, (100 * quorum) / total)) : 0}%`;
  const offCount = ready ? r!.bonded.length - r!.registered.length : 0;

  return (
    <div className="cp" aria-busy={!ready || undefined}>
      <h2 id="readiness-h" className="ov-eyebrow">
        Current Fibre providers
        <Info label="Current Fibre providers"><p>Share of the stake held by validators with a Fibre provider. A blob needs signatures from ⅔ of the stake to settle.</p><p>Each segment of the bar is one validator, as wide as its voting power: those with a Fibre provider first, largest first.</p></Info>
      </h2>
      <p className="cp-hero">
        <b className={`ov-fig${ready ? "" : " cp-wait"}`}>{ready ? pct!(regPower) : "00.0%"}</b>
        <span className="cp-help">{ready ? <>of voting power · <b>{int(r!.registered.length)}</b> of {int(r!.bonded.length)} validators</> : " "}</span>
      </p>
      <div className={`cp-meter${ready ? "" : " wait"}`} style={{ "--q": q } as React.CSSProperties} role="img"
        aria-label={ready ? `${pct!(regPower)} of voting power with a Fibre provider, ${int(r!.registered.length)} of ${int(r!.bonded.length)} validators; ${pct!(quorum)} needed` : "Loading"}>
        {ready && <span className="cp-q"><span><Frac /> needed</span></span>}
        <div className="cp-strip" ref={box}>
          {segs.length > 0 && (
            <svg width={w} height={STRIP_H} aria-hidden="true" focusable="false">
              {segs.map((s) => (
                <rect key={s.v.address} className={s.lit ? "on" : undefined} x={s.x} width={Math.max(0.6, s.wd)} height={STRIP_H}>
                  <title>{`${s.v.moniker || s.v.operator_address || s.v.address} · ${pct!(s.v.voting_power || 0)}${s.lit ? "" : " · no Fibre provider"}`}</title>
                </rect>
              ))}
            </svg>
          )}
        </div>
      </div>
      <p className="cp-rest">
        {ready ? <><i className="cp-sw" aria-hidden="true" />no Fibre provider {pct!(total - regPower)} · {int(offCount)} validator{offCount === 1 ? "" : "s"}</> : " "}
      </p>
    </div>
  );
}
