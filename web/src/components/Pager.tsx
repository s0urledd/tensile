"use client";
import { useCallback, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { int } from "@/lib/api";

/**
 * The page number, carried in the URL (?page=3) so a link opens on the page
 * the sender was looking at. Page 1 is the bare address.
 */
export function usePage(): [number, (p: number) => void] {
  const params = useSearchParams();
  const fromUrl = Number(params.get("page"));
  const [page, setPage] = useState(Number.isInteger(fromUrl) && fromUrl > 1 ? fromUrl : 1);
  const set = useCallback((p: number) => {
    const n = Math.max(1, Math.floor(p));
    setPage(n);
    try {
      const u = new URL(window.location.href);
      if (n === 1) u.searchParams.delete("page"); else u.searchParams.set("page", String(n));
      window.history.replaceState(null, "", u.pathname + u.search + u.hash);
    } catch { /* fine */ }
  }, []);
  return [page, set];
}

/** "Showing 26–50 of 153 settlements on record" and First ‹ Page 2 of 7 › Last */
export default function Pager({ total, page, size, maxPages, onPage, noun }: {
  total: number;
  page: number;
  size: number;
  /** the last page the API serves, when it stops before the list does */
  maxPages?: number;
  onPage: (p: number) => void;
  /** what the rows are, plural: "settlements" */
  noun: string;
}) {
  const pages = Math.min(maxPages ?? Infinity, Math.max(1, Math.ceil(total / size)));
  // a page past the end (an old link, a smaller period) goes to the last one
  useEffect(() => { if (page > pages) onPage(pages); }, [page, pages, onPage]);
  const at = Math.min(page, pages);
  const from = total === 0 ? 0 : (at - 1) * size + 1;
  const to = Math.min(total, at * size);
  return (
    <div className="pager">
      <span className="count">{total === 0 ? `No ${noun}` : `Showing ${int(from)}–${int(to)} of ${int(total)} ${noun}`}</span>
      {pages > 1 && (
        <span className="ctl" role="group" aria-label="pages">
          <button type="button" className="btn" disabled={at <= 1} onClick={() => onPage(1)}>First</button>
          <button type="button" className="btn" disabled={at <= 1} onClick={() => onPage(at - 1)} aria-label="previous page">‹</button>
          <span className="at">Page {int(at)} of {int(pages)}</span>
          <button type="button" className="btn" disabled={at >= pages} onClick={() => onPage(at + 1)} aria-label="next page">›</button>
          <button type="button" className="btn" disabled={at >= pages} onClick={() => onPage(pages)}>Last</button>
        </span>
      )}
    </div>
  );
}
