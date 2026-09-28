import type { ReactNode } from "react";
import type { RateTone } from "@/lib/api";

/**
 * One headline figure: the name, the value with its denominator beside it,
 * and at most one short helper line. Nothing about formulas or fields; the
 * definitions live on the methodology page.
 *
 * In a Metrics row each figure is a card of its own; in Figures (the chain
 * panel of the Blobs and Publishers pages) it is set straight on the panel,
 * and `size="hero"` makes it the page's lead figure.
 */
export function Metric({ label, value, den, help, tone, title, size }: {
  label: string;
  value: ReactNode;
  /** printed after the value in the quieter colour: "/ 60" */
  den?: ReactNode;
  help?: ReactNode;
  tone?: "fault" | "absent" | "words" | RateTone;
  title?: string;
  size?: "hero";
}) {
  return (
    <div className={"metric" + (size ? " " + size : "")} title={title}>
      <div className="label">{label}</div>
      <div className={"value num" + (tone ? " " + tone : "")}>{value}{den != null && <span className="den"> / {den}</span>}</div>
      <div className="help">{help ?? " "}</div>
    </div>
  );
}

export function Metrics({ children }: { children: ReactNode }) {
  return <section className="metrics">{children}</section>;
}

/** figures set on a panel rather than each in a card of its own */
export function Figures({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={"figs" + (className ? " " + className : "")}>{children}</div>;
}

/**
 * A figure Tensile measured itself, kept apart from the chain's: a quieter
 * strip, marked "Observed by Tensile", never the lead of the page. `frac`
 * draws the share as a thin meter beside the figure.
 */
export function Observed({ label, value, den, help, title, frac, absent }: {
  label: string;
  value: ReactNode;
  den?: ReactNode;
  help?: ReactNode;
  title?: string;
  frac?: number;
  absent?: boolean;
}) {
  return (
    <aside className="obs" title={title} aria-label="Observed by Tensile">
      <span className="obs-tag">
        <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M1.5 8s2.4-4.5 6.5-4.5S14.5 8 14.5 8 12.1 12.5 8 12.5 1.5 8 1.5 8Z" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinejoin="round" /><circle cx="8" cy="8" r="2" fill="currentColor" /></svg>
        Observed by Tensile
      </span>
      <span className="obs-fig">
        <span className="obs-label">{label}</span>
        <span className={"obs-value num" + (absent ? " absent" : "")}>{value}{den != null && <span className="den"> / {den}</span>}</span>
      </span>
      {frac != null && <span className="obs-meter" aria-hidden="true"><i style={{ width: `${Math.max(0, Math.min(1, frac)) * 100}%` }} /></span>}
      {help && <span className="obs-help">{help}</span>}
    </aside>
  );
}
