"use client";
import { useState } from "react";
import { type Validator, API_BASE } from "@/lib/api";

/** avatar text: "node10" → N10, "Kiln" → KI, "P-OPS Team" → PT; the same
 *  rule on every page so one validator wears one badge. */
export function initialsOf(moniker: string | undefined, address: string): string {
  const m = (moniker || "").trim();
  if (!m) return address.slice(0, 2);
  const parts = m.split(/[\s._-]+/).filter(Boolean);
  const tail = m.match(/\d+$/);
  if (parts.length > 1) {
    // "node 10" keeps its number the way "node10" does
    if (parts.length === 2 && /^\d+$/.test(parts[1])) return (parts[0][0] + parts[1]).slice(0, 3);
    return parts[0][0] + parts[1][0];
  }
  return tail ? (m[0] + tail[0]).slice(0, 3) : m.slice(0, 2);
}

/** The validator's badge: the Keybase picture its operator set, served by
 *  our own API once the collector fetched it; initials until then, and
 *  again if the image fails to load. Asked as the API's 72 px square
 *  (?s=72), enough for the 36 px title on a 2x screen: the originals run to
 *  40 KB each, and a page of validators would fetch two megabytes of them. */
export default function Avatar({ v }: { v: Pick<Validator, "avatar_url" | "moniker" | "address"> }) {
  const [broken, setBroken] = useState(false);
  if (v.avatar_url && !broken) {
    return <img className="avatar" src={`${API_BASE}${v.avatar_url}?s=72`} alt="" width={24} height={24} loading="lazy" decoding="async" onError={() => setBroken(true)} />;
  }
  return <span className="avatar" aria-hidden="true">{initialsOf(v.moniker, v.address)}</span>;
}
