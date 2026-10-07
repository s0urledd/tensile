import type { Blob } from "./api";

/**
 * When a blob's status, as a page holds it, may have moved on without the page asking for it again: Tensile reads a
 * blob once, 10 minutes before its retention window ends (asking a validator that did not serve up to twice more,
 * about 90 s apart), and the API answers that reading's result as soon as it is in. Until then the status is pending;
 * once the window is over without a reading it is not_read. A page that read the blob before then holds pending, and
 * lane() (lib/status.ts) words a pending status whose window has ended as "not read", although Tensile may have read
 * it as available minutes before.
 *
 * So a status is asked for again at two moments: 2 minutes after the reading (the first answers are in), and 2 minutes
 * before the window ends (every answer is in, and the window is still open, so the word on screen has not yet turned).
 * READ_CHECKS are those moments, before the window's end.
 */
export const READ_CHECKS = [8 * 60_000, 2 * 60_000];

/** a status the API answers for good: the reading's result, the window closed without one, or no assignment to judge by */
function final(b: Pick<Blob, "reconstructable">): boolean {
  const s = b.reconstructable?.status;
  return s === "yes" || s === "no" || s === "not_read" || s === "unknown";
}

/**
 * Whether the status of a blob whose record was read at readAt (ms) may have changed by now (ms): it was not final, and
 * one of READ_CHECKS fell after that read and by now. Each check asks once: the read it brings is after it.
 */
export function recheck(b: Pick<Blob, "reconstructable" | "must_serve_until">, readAt: number, now: number): boolean {
  if (final(b)) return false;
  const end = Date.parse(b.must_serve_until);
  if (!Number.isFinite(end)) return false;
  return READ_CHECKS.some((before) => {
    const at = end - before;
    return readAt < at && at <= now;
  });
}
