import type { Blob, FailedTx } from "@/lib/api";
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
  /** a lookup failed, for this reason: the rows are what the others found, and there may be more */
  partial: string | null;
  /** nothing was found and a lookup failed, for this reason: nothing is known either way */
  error: string | null;
  /** the transaction failed in a block: what the chain returned; set only with no rows */
  failedTx: FailedTx | null;
  /** when the answers came in */
  at: string;
};

export const ALL: MatchBy[] = ["promise", "commitment", "tx"];
type List = { blobs?: Blob[]; total?: number; commitment?: string; tx?: string; failed_tx?: FailedTx };
type Get = (url: string, init: RequestInit) => Promise<Pick<Response, "ok" | "status" | "json">>;

/** one lookup: its blobs, or "none" (a 404, or a 400 for a value the route does not take), or an error */
async function ask(get: Get, url: string): Promise<{ data: unknown } | { none: true } | { error: string }> {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 20000);
  try {
    const r = await get(url, { cache: "no-store", signal: ctl.signal });
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
 * says it filtered on this identifier: an API from before ?tx= ignores the filter and answers the newest blobs. One
 * lookup that fails leaves the answer incomplete (partial), and with nothing found unknown (error), never "none". A
 * transaction hash that settled no blob may have failed in a block: ?tx= then says so (failed_tx), and with nothing
 * found that is the answer (failedTx), not an unknown.
 * base is the API's address, get the fetch that asks it.
 */
export async function findBlobs(key: BlobKey, opts: { as?: MatchBy[]; limit?: number; base: string; get: Get }): Promise<Found> {
  const as = opts.as ?? ALL, limit = opts.limit ?? 25;
  const want = key.kind === "id" ? (["commitment"] as MatchBy[]) : ALL.filter((b) => as.includes(b));
  const hex = key.hex;
  const answers = await Promise.all(want.map((by) => ask(opts.get, opts.base + (
    by === "promise" ? `/v1/blobs/${hex}` : by === "commitment" ? `/v1/blobs?commitment=${hex}&limit=${limit}` : `/v1/blobs?tx=${hex}&limit=${limit}`
  ))));
  const rows: Blob[] = [];
  const by: MatchBy[] = [];
  let more = 0, noTx = false;
  let failedTx: FailedTx | null = null;
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
      // the transaction failed in a block: only beside no blob, and only on an answer that filtered on it (above)
      if (w === "tx" && l.failed_tx && (l.blobs ?? []).length === 0) failedTx = l.failed_tx;
      // a blob ID names its version too: the commitment's settlements under another version are not its own
      blobs = (l.blobs ?? []).filter((b) => key.kind !== "id" || (b.blob_version ?? 0) === key.version);
      more += Math.max(0, (l.total ?? (l.blobs ?? []).length) - (l.blobs ?? []).length);
    }
    if (blobs.length) by.push(w);
    for (const b of blobs) if (!rows.some((x) => x.promise_hash === b.promise_hash)) rows.push(b);
  });
  rows.sort((a, b) => b.settlement_height - a.settlement_height || b.settlement_tx_index - a.settlement_tx_index);
  const failed = errors[0] ?? null;
  return {
    rows, total: rows.length + more, by, noTx, partial: failed,
    // a failed transaction is known: a lookup that failed beside it leaves nothing open
    error: rows.length === 0 && !failedTx ? failed : null,
    failedTx: rows.length === 0 ? failedTx : null,
    at: new Date().toISOString(),
  };
}

/** an answer that still leaves the identifier open: nothing found, a lookup failed or incomplete; a transaction that failed in a block closes it */
export const isShort = (f: Found) => !(f.rows.length === 0 && !!f.failedTx) && !!(f.error || f.partial || f.rows.length === 0);
