"use client";
import { useLayoutEffect, useRef, useState } from "react";
import Link from "next/link";
import { type Validator, int, shortMid } from "@/lib/api";
import { validatorHref } from "@/lib/addr";
import Info from "@/components/Info";
import { endpointState, readiness } from "@/components/Readiness";

/** ⅔ set from the text face's own numerals, on its baseline: the font's fraction glyph falls back heavier and off the line */
export function Frac() {
  return <b className="ov-frac"><span className="ov-frac-n">2</span><span className="ov-frac-s">⁄</span><span className="ov-frac-d">3</span></b>;
}

const STRIP_H = 16;

/**
 * Current Fibre providers: the share of voting power held by validators with
 * a Fibre provider registered, as the lead figure, then the bonded set as one
 * strip: one segment per validator, as wide as its voting power, those with a
 * Fibre provider first and lit, largest first, the ones whose host did not
 * answer this observer's latest check after them in amber, then the others.
 * The ⅔ a blob needs to settle is a needle through it. Under it, one line per
 * part that is not a working Fibre provider, each only when there is one. Every segment is a validator and
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
  const reg = new Set(ready ? r!.registered.map((v) => v.address) : []);
  const up = ready ? r!.registered.filter((v) => endpointState(v) !== "unreachable").sort(byPower) : [];
  const down = ready ? r!.registered.filter((v) => endpointState(v) === "unreachable").sort(byPower) : [];
  const unchecked = up.filter((v) => endpointState(v) === "none");
  const off = ready ? r!.bonded.filter((v) => !reg.has(v.address)).sort(byPower) : [];
  let at = 0;
  const segs = ready && w > 0 ? [...up, ...down, ...off].map((v) => {
    const vp = v.voting_power || 0;
    const x = (at * w) / total, wd = (vp * w) / total;
    at += vp;
    const tone = !reg.has(v.address) ? undefined : endpointState(v) === "unreachable" ? "hold" : "on";
    return { v, x, wd: wd >= 3 ? wd - 1 : wd, tone };
  }) : [];
  const q = `${total > 0 ? Math.min(100, Math.max(0, (100 * quorum) / total)) : 0}%`;
  const power = (vs: Validator[]) => vs.reduce((n, v) => n + (v.voting_power || 0), 0);
  // a part of the stake in its small frame: its name and share in the text's colour, how many validators in the help grey
  const part = (sw: string, name: string, vs: Validator[]) => <>
    <i className={`cp-sw${sw ? ` ${sw}` : ""}`} aria-hidden="true" />
    <span className="cp-k">{name} {pct!(power(vs))}</span>
    <span className="cp-n">· {int(vs.length)} validator{vs.length === 1 ? "" : "s"}</span>
  </>;

  return (
    <div className="cp" aria-busy={!ready || undefined}>
      <h2 id="readiness-h" className="ov-eyebrow">
        Current Fibre providers
        <Info label="Current Fibre providers"><p>Share of the stake held by validators with a Fibre provider. A blob needs signatures from ⅔ of the stake to settle.</p><p>Each segment of the bar is one validator, as wide as its voting power: those with a Fibre provider first, largest first. Amber: its host did not answer Tensile&rsquo;s latest check.</p></Info>
      </h2>
      <p className="cp-hero">
        <b className={`ov-fig${ready ? "" : " cp-wait"}`}>{ready ? pct!(regPower) : "00.0%"}</b>
        <span className="cp-help">{ready ? <>of voting power · <b>{int(r!.registered.length)}</b> of {int(r!.bonded.length)} validators</> : " "}</span>
      </p>
      <div className={`cp-meter${ready ? "" : " wait"}`} style={{ "--q": q } as React.CSSProperties} role="img"
        aria-label={ready ? `${pct!(regPower)} of voting power with a Fibre provider, ${int(r!.registered.length)} of ${int(r!.bonded.length)} validators${down.length ? `, ${int(down.length)} of them unreachable` : ""}; ${pct!(quorum)} needed` : "Loading"}>
        {ready && <span className="cp-q"><span><Frac /> needed</span></span>}
        <div className="cp-strip" ref={box}>
          {segs.length > 0 && (
            <svg width={w} height={STRIP_H} aria-hidden="true" focusable="false">
              {segs.map((s) => (
                <rect key={s.v.address} className={s.tone} x={s.x} width={Math.max(0.6, s.wd)} height={STRIP_H}>
                  <title>{`${s.v.moniker || s.v.operator_address || s.v.address} · ${pct!(s.v.voting_power || 0)}${!s.tone ? " · no Fibre provider" : s.tone === "hold" ? " · unreachable" : ""}`}</title>
                </rect>
              ))}
            </svg>
          )}
        </div>
      </div>
      {/* the parts of the stake that are not a working provider, each in a small frame, the unreachable first: it opens
          the names of its validators, each a link to its page */}
      <ul className="cp-rest">
        {ready ? <>
          {down.length > 0 && <li>
            <Info label="Unreachable" className="cp-it" title="Show the unreachable validators" trigger={part("hold", "unreachable", down)}>
              <ul className="cp-names">
                {down.map((v) => (
                  <li key={v.address}>
                    <Link href={validatorHref(v.operator_address, v.address)}>{v.moniker || shortMid(v.operator_address || v.address, 18, 4)}</Link>
                    <span>{pct!(v.voting_power || 0)}</span>
                  </li>
                ))}
              </ul>
            </Info>
          </li>}
          <li><span className="cp-it">{part("", "no Fibre provider", off)}</span></li>
          {unchecked.length > 0 && <li><span className="cp-it">{part("none", "not checked yet", unchecked)}</span></li>}
        </> : <li>{" "}</li>}
      </ul>
    </div>
  );
}
