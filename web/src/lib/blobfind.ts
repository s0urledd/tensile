import { useEffect, useRef, useState } from "react";
import { API_BASE } from "@/lib/api";
import type { BlobKey } from "@/lib/blobkey";
import { ALL, findBlobs, type Found, type MatchBy } from "@/lib/blobfind-core";

export type { Found, MatchBy };

/** an answer that leaves the blob unknown: nothing found, a lookup failed, or the answer is incomplete */
const short = (f: Found) => !!(f.error || f.partial || f.rows.length === 0);

/**
 * findBlobs as a hook: null until this identifier's answer is in (an earlier identifier's never stands for it).
 * refreshMs asks again while it names nothing, failed or is incomplete, for a blob the scanner has not indexed yet.
 * retryOn asks again, while the answer is still short of that, each time it changes: the tip's newest blob
 * (/v1/tip's latest_blob), so a blob is found as soon as Tensile records one rather than at the next refreshMs.
 * retryForMs stops that so long after the identifier was first asked (the timer of refreshMs goes on).
 * again, changed, asks the same identifier again from the start: a reader who was told the lookup failed and asks
 * once more.
 */
export function useFind(key: BlobKey | null, opts: { as?: MatchBy[]; limit?: number; refreshMs?: number; retryOn?: string | null; retryForMs?: number; again?: number } = {}): Found | null {
  const as = opts.as ?? ALL;
  const sig = key ? `${key.kind}:${key.hex}:${as.join(",")}:${opts.limit ?? 25}:${opts.again ?? 0}` : "";
  const [st, setSt] = useState<{ sig: string; found: Found } | null>(null);
  // the lookup of this identifier, for retryOn to reach: ask runs it again now, or once more after the one out
  const live = useRef<{ sig: string; since: number; ask: () => void } | null>(null);
  useEffect(() => {
    if (!key) return;
    let on = true, busy = false, again = false;
    let t: ReturnType<typeof setTimeout> | undefined;
    let last: Found | null = null;
    const run = async () => {
      clearTimeout(t);
      busy = true;
      const found = await findBlobs(key, { as, limit: opts.limit ?? 25, base: API_BASE, get: (url, init) => fetch(url, init) });
      busy = false;
      if (!on) return;
      last = found;
      setSt({ sig, found });
      if (again && short(found)) { again = false; run(); return; }
      again = false;
      if (opts.refreshMs && short(found)) t = setTimeout(run, opts.refreshMs);
    };
    live.current = { sig, since: Date.now(), ask: () => { if (busy) again = true; else if (last && short(last)) run(); } };
    run();
    return () => { on = false; clearTimeout(t); live.current = null; };
  }, [sig]); // eslint-disable-line react-hooks/exhaustive-deps
  // a new value of retryOn for the identifier asked: the first one seen for it is where it started, not a change
  const retryOn = opts.retryOn ?? null;
  const seen = useRef<{ sig: string; on: string | null } | null>(null);
  useEffect(() => {
    const prev = seen.current;
    seen.current = { sig, on: retryOn };
    const l = live.current;
    if (!retryOn || !prev || prev.sig !== sig || prev.on === retryOn || !l || l.sig !== sig) return;
    if (opts.retryForMs !== undefined && Date.now() - l.since > opts.retryForMs) return;
    l.ask();
  }, [sig, retryOn]); // eslint-disable-line react-hooks/exhaustive-deps
  return st && st.sig === sig ? st.found : null;
}
