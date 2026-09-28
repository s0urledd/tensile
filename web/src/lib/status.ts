import { type Blob, int } from "@/lib/api";
import type { Tier } from "@/components/Verdict";

/**
 * A blob's status as Tensile observed it, as a mark and a word, in the same
 * channel the verdicts use. Available: enough rows came back to reconstruct
 * the blob, in the words of celestia-app's own client ("some rows were
 * retrieved, but not enough to reconstruct" is its word for the other case).
 * One function, so the Blobs list and the overview's latest blob cannot
 * disagree.
 */
export function recon(b: Blob): { word: string; tier: Tier; title: string } {
  const r = b.reconstructable;
  const over = new Date(b.must_serve_until).getTime() <= Date.now();
  if (b.sampled_out && (!r || r.status === "unknown")) {
    return { word: "sampled out", tier: "gap", title: "The load policy of the time drew this blob out of its sample: not read." };
  }
  if (!r || r.status === "unknown" || r.status === "pending") {
    return !over
      ? { word: "in retention window", tier: "gap", title: "Read once, 10 minutes before the retention window ends." }
      : { word: "not read by Tensile", tier: "gap", title: "No reading of this blob was completed. Nothing is counted for or against a validator." };
  }
  const rows = `${int(r.served_distinct_rows)} of ${int(r.total_rows)} rows came back, ${int(r.needed_rows)} needed to reconstruct`;
  if (r.status === "yes" || r.status === "degraded") return { word: "available", tier: "kept", title: rows };
  return { word: "unavailable", tier: "hold", title: `${rows}: not enough to reconstruct.` };
}
