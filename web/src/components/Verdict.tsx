/**
 * Verdict marks.
 *
 * Three channels in priority order: the word, then the shape, then the colour.
 * The word is always present and never abbreviated to an icon. The shape is one
 * of four, because fifteen distinguishable shapes at ten pixels is not
 * achievable and pretending otherwise produces a legend nobody reads. The
 * colour is third and there are only two of them.
 *
 * What the tiers encode is the thing a reader actually needs, which is not the
 * class but what the class does to the rate:
 *
 *   kept    the rows came back                              HEALTHY
 *   fault   not found or bad rows — the only accusation     FAULT
 *   hold    no rows from a server that did not hand them over UNREACHABLE, IDENTITY_EXPIRED
 *   held    nothing was owed, not by this promise, or this site
 *           cannot say when the obligation ended    RETENTION_UNVERIFIED
 *   gap     not observed at all — a gap, never a verdict    NOT_PROBED, PROBE_ERROR
 *
 * Whether a reading counts against the validator is not its class alone: a
 * failure counts as not served only when the blob could not be
 * reconstructed (observer/verdict).
 *
 * FAULT owns the only pointed shape in the system and the only status colour
 * allowed to touch a word, so an accusation is pre-attentive and survives total
 * colour loss, and no later change can quietly promote another class into the
 * alarm channel without someone noticing the triangle.
 */

export type Tier = "kept" | "fault" | "hold" | "held" | "gap";

type Def = { label: string; tier: Tier; def: string };

const VERDICTS: Record<string, Def> = {
  HEALTHY: {
    label: "served", tier: "kept",
    def: "The endorsed rows came back and verified against the blob commitment.",
  },
  FAULT: {
    label: "not found or bad rows", tier: "fault",
    def: "Not found, or rows that do not verify against the blob commitment. Not served when the blob could not be reconstructed; otherwise counted neither way.",
  },
  UNREACHABLE: {
    label: "unreachable", tier: "hold",
    def: "No answer within 15 s, asked twice. Not served when the blob could not be reconstructed; otherwise counted neither way.",
  },
  IDENTITY_EXPIRED: {
    label: "certificate expired", tier: "hold",
    def: "The right key signed the certificate, but outside its validity window. Not served when the blob could not be reconstructed; otherwise counted neither way.",
  },
  IDENTITY_MISMATCH: {
    label: "wrong certificate", tier: "hold",
    def: "The certificate is not signed by this validator's consensus key. Not served when the blob could not be reconstructed; otherwise counted neither way.",
  },
  SERVER_ERROR: {
    label: "server error", tier: "hold",
    def: "An error, or an answer no client accepts, instead of the shard. Not served when the blob could not be reconstructed; otherwise counted neither way.",
  },
  THROTTLED: {
    label: "rate limited", tier: "hold",
    def: "Refused with a rate limit, which Tensile's own requests may have caused. Counted neither way.",
  },
  UNATTESTED: {
    label: "not endorsed", tier: "held",
    def: "No verified endorsement from this validator on the settled promise, so nothing proves it stored the shard. Not rated.",
  },
  NOT_REGISTERED: {
    label: "no endpoint", tier: "held",
    def: "No Fibre host in x/valaddr at the reading. Not served when the blob could not be reconstructed; otherwise counted neither way.",
  },
  SHADOWED_SHARD: {
    label: "shadowed", tier: "held",
    def: "Genuine rows of the blob, but another settled promise's set: the store answers by commitment. Served.",
  },
  UNMATCHED_GENUINE: {
    label: "unmatched genuine rows", tier: "held",
    def: "Genuine rows of the blob that match no settled promise's set. Served.",
  },
  TOLERATED: {
    label: "tolerated", tier: "held",
    def: "Earlier schedule: not found or unreachable just after must_serve_until, within the measured prune lag. Not counted.",
  },
  EXPECTED_GONE: {
    label: "expected gone", tier: "held",
    def: "Earlier schedule: not found after the window plus tolerance. Correct behaviour.",
  },
  SERVED_PAST_WINDOW: {
    label: "served after window", tier: "held",
    def: "Earlier schedule: still serving after the retention window ended. Not counted.",
  },
  UNREACHABLE_POST_WINDOW: {
    label: "unreachable after window", tier: "held",
    def: "Earlier schedule: unreachable after the retention window ended. Not counted.",
  },
  EXPECTED_UNASSIGNED: {
    label: "unassigned", tier: "held",
    def: "Validator was not assigned this shard.",
  },
  SERVING_UNASSIGNED: {
    label: "serving unassigned", tier: "held",
    def: "Validator returned a shard it was not assigned. Flagged for review.",
  },
  PROBE_ERROR: {
    label: "read failed", tier: "gap",
    def: "Tensile's own request failed, or the reading could not finish in time. A gap, not a verdict.",
  },
  NOT_PROBED: {
    label: "not read by Tensile", tier: "gap",
    def: "Tensile did not read this blob in time. A gap, not a verdict.",
  },
  RETENTION_UNVERIFIED: {
    label: "deadline unverified", tier: "held",
    def: "The deadline cannot be computed until an x/fibre parameter range is read. No verdict either way until then.",
  },
};

/** Every class, so no page can show a partial taxonomy. */
export const ALL_VERDICTS = Object.keys(VERDICTS);

export function verdictDef(cls: string): Def {
  return VERDICTS[cls] ?? { label: cls.toLowerCase().replace(/_/g, " "), tier: "gap", def: "" };
}

/** The colour a tier paints with, for callers that draw rather than compose. */
export function tierColor(tier: Tier): string {
  switch (tier) {
    case "fault": return "var(--fault)";
    case "hold": return "var(--hold)";
    case "kept": return "var(--text)";
    case "held": return "var(--text-2)";
    default: return "var(--text-3)";
  }
}

/**
 * The mark. Every shape is stroked in its own colour as well as filled, so it
 * keeps a boundary when the fill is the same value as the paper, and so the
 * shape rather than the fill is what survives a greyscale print.
 */
export function Mark({ tier, className }: { tier: Tier; className?: string }) {
  const cls = `mark mark--${tier}${className ? " " + className : ""}`;
  const common = { className: cls, viewBox: "0 0 10 10", "aria-hidden": true as const, focusable: "false" as const };
  switch (tier) {
    case "fault":   // the only pointed shape in the system
      return <svg {...common}><path d="M5 1 L9.2 8.6 H0.8 Z" fill="currentColor" /></svg>;
    case "kept":
      return <svg {...common}><circle cx="5" cy="5" r="3.6" fill="currentColor" /></svg>;
    case "hold":    // half disc: we saw half of what we needed to
      return <svg {...common}><circle cx="5" cy="5" r="3.6" fill="none" stroke="currentColor" strokeWidth="1.25" /><path d="M5 1.4 A3.6 3.6 0 0 1 5 8.6 Z" fill="currentColor" /></svg>;
    case "held":
      return <svg {...common}><circle cx="5" cy="5" r="3.4" fill="none" stroke="currentColor" strokeWidth="1.25" /></svg>;
    default:        // a gap is a dash, because nothing was observed to draw
      return <svg {...common}><path d="M1.4 5 H8.6" stroke="currentColor" strokeWidth="1.25" strokeLinecap="round" /></svg>;
  }
}

/** Mark plus word. The word is the primary channel and is never dropped. */
export default function Verdict({ cls, title }: { cls: string; title?: string }) {
  const d = verdictDef(cls);
  return (
    <span className={`verdict verdict--${d.tier}`} title={title ?? d.def}>
      <Mark tier={d.tier} />
      <span className="w">{d.label}</span>
    </span>
  );
}

/**
 * The whole taxonomy, in tier order. This is rendered on the methodology page
 * and nowhere else: repeating fifteen definitions below every table on the site
 * is how the current pages came to be four thousand pixels tall, and two of
 * those copies had disagreed about which classes exist.
 */
export function Legend() {
  const order: Tier[] = ["kept", "fault", "hold", "held", "gap"];
  const sorted = [...ALL_VERDICTS].sort(
    (a, b) => order.indexOf(verdictDef(a).tier) - order.indexOf(verdictDef(b).tier),
  );
  return (
    <div className="legend">
      {sorted.map((c) => (
        <div key={c} id={`verdict-${c.toLowerCase()}`}>
          <Verdict cls={c} title="" />
          <span className="def">{verdictDef(c).def}</span>
        </div>
      ))}
    </div>
  );
}
