"use client";
import { type ReactNode, useCallback, useEffect, useState } from "react";
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
export default function Pager({ total, page, size, maxPages, onPage, noun, range }: {
  total: number;
  page: number;
  size: number;
  /** the last page the API serves, when it stops before the list does */
  maxPages?: number;
  onPage: (p: number) => void;
  /** what the rows are, plural: "settlements" */
  noun: ReactNode;
  /** the rows this page shows, counted from the list's first, where a page holds more than its size: a publisher's blobs, a page of them a page,
   *  with its escrow movements between them */
  range?: [number, number];
}) {
  const pages = Math.min(maxPages ?? Infinity, Math.max(1, Math.ceil(total / size)));
  // a page past the end (an old link, a smaller period) goes to the last one
  useEffect(() => { if (page > pages) onPage(pages); }, [page, pages, onPage]);
  const at = Math.min(page, pages);
  const [from, to] = range ?? [total === 0 ? 0 : (at - 1) * size + 1, Math.min(total, at * size)];
  return (
    <div className="pager">
      {/* one row is "Showing 1 of 1", not a range from it to itself */}
      <span className="count">{total === 0 ? <>No {noun}</> : <>Showing <b>{from === to ? int(from) : <>{int(from)}–{int(to)}</>}</b> of <b>{int(total)}</b> {noun}</>}</span>
      {pages > 1 && (
        <span className="ctl" role="group" aria-label="pages">
          <button type="button" className="btn" disabled={at <= 1} onClick={() => onPage(1)}>First</button>
          <button type="button" className="btn icon" disabled={at <= 1} onClick={() => onPage(at - 1)} aria-label="previous page"><svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M10 3.5 5.5 8l4.5 4.5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /></svg></button>
          <span className="at">Page <b>{int(at)}</b> of {int(pages)}</span>
          <button type="button" className="btn icon" disabled={at >= pages} onClick={() => onPage(at + 1)} aria-label="next page"><svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M6 3.5 10.5 8 6 12.5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /></svg></button>
          <button type="button" className="btn" disabled={at >= pages} onClick={() => onPage(pages)}>Last</button>
        </span>
      )}
    </div>
  );
}
