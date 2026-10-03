import { useEffect, useState } from "react";
import { API_BASE } from "@/lib/api";
import type { BlobKey } from "@/lib/blobkey";
import { ALL, findBlobs, type Found, type MatchBy } from "@/lib/blobfind-core";

export type { Found, MatchBy };

/**
 * findBlobs as a hook: null until this identifier's answer is in (an earlier identifier's never stands for it).
 * refreshMs asks again while it names nothing, failed or is incomplete, for a blob the scanner has not indexed yet.
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
      const found = await findBlobs(key, { as, limit: opts.limit ?? 25, base: API_BASE, get: (url, init) => fetch(url, init) });
      if (!live) return;
      setSt({ sig, found });
      if (opts.refreshMs && (found.error || found.partial || found.rows.length === 0)) t = setTimeout(run, opts.refreshMs);
    };
    run();
    return () => { live = false; clearTimeout(t); };
  }, [sig]); // eslint-disable-line react-hooks/exhaustive-deps
  return st && st.sig === sig ? st.found : null;
}
