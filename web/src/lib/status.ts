import { type Blob, int } from "@/lib/api";
import type { Tier } from "@/components/Verdict";

/**
 * A blob's status as Tensile read it, as a mark and a word, in the same
 * channel the verdicts use. Available: enough rows came back to reconstruct
 * the blob. Unavailable: the validators, asked in the client's order and
 * again a minute later, could not give enough ("not enough shards to
 * reconstruct blob" in celestia-app's own client). One function, so the
 * Blobs list and the overview's latest blob cannot disagree.
 */
export function recon(b: Blob): { word: string; tier: Tier; title: string } {
  const r = b.reconstructable;
  const over = new Date(b.must_serve_until).getTime() <= Date.now();
  if (!r || (r.status !== "yes" && r.status !== "no")) {
    return !over
      ? { word: "in retention window", tier: "gap", title: "Read once, 10 minutes before the retention window ends." }
      : { word: "not read by Tensile", tier: "gap", title: "No reading of this blob decides it. Nothing is counted for or against a validator." };
  }
  const rows = `${int(r.served_distinct_rows)} distinct rows came back, ${int(r.needed_rows)} needed to reconstruct`;
  if (r.status === "yes") return { word: "available", tier: "kept", title: rows };
  return { word: "unavailable", tier: "hold", title: `${rows}: not enough to reconstruct.` };
}
