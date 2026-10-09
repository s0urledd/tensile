import { type Blob, int } from "@/lib/api";
import type { Tier } from "@/components/Verdict";

/** what recon() and lane() read of a blob: a Blobs list row, a blob page, or a transaction page's related blob */
export type Readable = Pick<Blob, "reconstructable" | "must_serve_until">;

/**
 * A blob's status as Tensile read it, as a mark and a word, in the same
 * channel the verdicts use. It is celestia-app's client's own result:
 * Available, enough rows came back to reconstruct the blob; Unavailable,
 * with the client's error ("no shards retrieved", "not enough shards to
 * reconstruct blob"). The Blobs list words each blob's status with it, in
 * its Tensile lane (lane() below); the overview's recent blobs carry no
 * status, since every blob there is still in its retention window.
 */
export function recon(b: Readable): { word: string; tier: Tier; title: string } {
  const r = b.reconstructable;
  const over = new Date(b.must_serve_until).getTime() <= Date.now();
  if (!r || (r.status !== "yes" && r.status !== "no")) {
    return !over
      ? { word: "in retention window", tier: "gap", title: "Read once, 10 minutes before the retention window ends." }
      : { word: "not read by Tensile", tier: "gap", title: "Tensile did not read this blob. Its own gaps count against no validator." };
  }
  const rows = `${int(r.served_distinct_rows)} distinct rows came back, ${int(r.needed_rows)} needed to reconstruct`;
  if (r.status === "yes") return { word: "available", tier: "kept", title: rows };
  return { word: "unavailable", tier: "hold", title: r.error ? `${rows}: ${r.error}.` : `${rows}.` };
}

const MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** "Oct 1, 21:08 UTC" */
function when(s: string): string {
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return `${MON[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.toISOString().slice(11, 16)} UTC`;
}

/**
 * The same status for the Blobs list's Tensile lane, whose head already
 * names Tensile: "retention window" (Celestia's term for the time a
 * validator must keep a blob's shards) until Tensile has read it, with the
 * time of the reading on hover; then available or unavailable, with when
 * Tensile checked it, or "not read" once the window has closed without a
 * reading.
 */
export function lane(b: Readable): { word: string; tier: Tier; title: string } {
  const r = b.reconstructable;
  const read = !!r && (r.status === "yes" || r.status === "no");
  const end = new Date(b.must_serve_until).getTime();
  if (!read && end > Date.now()) {
    const at = new Date(end - 10 * 60 * 1000).toISOString().slice(11, 16);
    return { word: "retention window", tier: "gap", title: `Tensile reads it once at ${at} UTC, 10 minutes before the retention window ends.` };
  }
  const rc = recon(b);
  if (!read) return { ...rc, word: "not read" };
  if (!r.point_at) return rc;
  return r.status === "yes"
    ? { ...rc, title: `Verified during retention · ${when(r.point_at)}` }
    : { ...rc, title: `Checked during retention · ${when(r.point_at)}. ${rc.title}` };
}
