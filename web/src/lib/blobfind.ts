import { useEffect, useState } from "react";
import { API_BASE, type Blob } from "@/lib/api";
import type { BlobKey } from "@/lib/blobkey";

/** what an identifier matched: a blob's promise hash, its commitment (which a blob ID carries), or its settlement transaction */
export type MatchBy = "promise" | "commitment" | "tx";

/** the blobs an identifier names, as /v1/blobs answered for it */
export type Found = {
  /** the blobs it names, newest first, each once (up to the limit of each list) */
  rows: Blob[];
  /** how many blobs it names in all */
  total: number;
  /** what it matched, in the order of MatchBy */
  by: MatchBy[];
  /** the observer answered ?tx= without filtering on it: an API from before the filter, which cannot look a transaction up */
  noTx: boolean;
  /** every lookup failed, for this reason: nothing is known either way */
  error: string | null;
  /** when the answers came in */
  at: string;
};

const ALL: MatchBy[] = ["promise", "commitment", "tx"];
type List = { blobs?: Blob[]; total?: number; commitment?: string; tx?: string };

/** one lookup: its blobs, or "none" (a 404, or a 400 for a value the route does not take), or an error */
async function ask(path: string): Promise<{ data: unknown } | { none: true } | { error: string }> {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 20000);
  try {
    const r = await fetch(API_BASE + path, { cache: "no-store", signal: ctl.signal });
    if (r.status === 404 || r.status === 400) return { none: true };
    if (!r.ok) return { error: String(r.status) };
    return { data: await r.json() };
  } catch (e) {
    return { error: e instanceof DOMException && e.name === "AbortError" ? "no answer within 20 s" : e instanceof Error ? e.message : String(e) };
  } finally {
    clearTimeout(t);
  }
}

/**
 * Looks an identifier up as each thing it can be: 64 hex characters as a promise hash, a commitment and a transaction
 * hash (only a lookup tells them apart; as narrows it to some), a blob ID as its commitment. A list counts only when it
 * says it filtered on this identifier: an API from before ?tx= ignores the filter and answers the newest blobs.
 */
export async function findBlobs(key: BlobKey, as: MatchBy[] = ALL, limit = 25): Promise<Found> {
  const want = key.kind === "id" ? (["commitment"] as MatchBy[]) : ALL.filter((b) => as.includes(b));
  const hex = key.hex;
  const answers = await Promise.all(want.map((by) => ask(
    by === "promise" ? `/v1/blobs/${hex}` : by === "commitment" ? `/v1/blobs?commitment=${hex}&limit=${limit}` : `/v1/blobs?tx=${hex}&limit=${limit}`,
  )));
  const rows: Blob[] = [];
  const by: MatchBy[] = [];
  let more = 0, noTx = false;
  const errors: string[] = [];
  want.forEach((w, i) => {
    const a = answers[i];
    if ("error" in a) { errors.push(a.error); return; }
    if ("none" in a) return;
    let blobs: Blob[] = [];
    if (w === "promise") {
      const b = (a.data as { blob?: Blob }).blob;
      if (b?.promise_hash === hex) blobs = [b];
    } else {
      const l = a.data as List;
      const echo = w === "commitment" ? l.commitment : l.tx;
      if (w === "tx" && echo === undefined) noTx = true;
      if (echo !== hex) return;
      blobs = l.blobs ?? [];
      more += Math.max(0, (l.total ?? blobs.length) - blobs.length);
    }
    if (blobs.length) by.push(w);
    for (const b of blobs) if (!rows.some((x) => x.promise_hash === b.promise_hash)) rows.push(b);
  });
  rows.sort((a, b) => b.settlement_height - a.settlement_height || b.settlement_tx_index - a.settlement_tx_index);
  return { rows, total: rows.length + more, by, noTx, error: errors.length === want.length ? errors[0] : null, at: new Date().toISOString() };
}

/**
 * findBlobs as a hook: null until this identifier's answer is in (an earlier identifier's never stands for it).
 * refreshMs asks again while it names nothing or failed, for a blob the scanner has not indexed yet.
 */
export function useFind(key: BlobKey | null, opts: { as?: MatchBy[]; limit?: number; refreshMs?: number } = {}): Found | null {
  const as = opts.as ?? ALL;
  const sig = key ? `${key.kind}:${key.hex}:${as.join(",")}:${opts.limit ?? 25}` : "";
  const [st, setSt] = useState<{ sig: string; found: Found } | null>(null);
  useEffect(() => {
    if (!key) return;
    let live = true;
    let t: ReturnType<typeof setTimeout> | undefined;
    const run = async () => {
      const found = await findBlobs(key, as, opts.limit ?? 25);
      if (!live) return;
      setSt({ sig, found });
      if (opts.refreshMs && (found.error || found.rows.length === 0)) t = setTimeout(run, opts.refreshMs);
    };
    run();
    return () => { live = false; clearTimeout(t); };
  }, [sig]); // eslint-disable-line react-hooks/exhaustive-deps
  return st && st.sig === sig ? st.found : null;
}
