import type { Blob } from "./api";

/**
 * When a blob's status, as a page holds it, may have moved on without the page asking for it again: Tensile reads a
 * blob once, 10 minutes before its retention window ends (asking a validator that did not serve up to twice more,
 * about 90 s apart), and the API answers that reading's result as soon as the collector has stored it. Until then the
 * status is pending while the window is open, and not_read once it is over. A page that read the blob before then
 * holds pending, and lane() (lib/status.ts) words a pending status whose window has ended as "not read", although
 * Tensile may have read it as available minutes before.
 *
 * So a status is asked for again 2 minutes after the reading (the first answers are in), and 2 minutes before the
 * window ends (every answer is in, and the window is still open, so the word on screen has not yet turned). A
 * collector that stores the reading late (it is behind, or was stopped) leaves it pending past both, and then not_read
 * once the window is over, until the reading is in: so it is asked for twice more, 3 and 15 minutes after the end,
 * while it is pending or not_read. READ_CHECKS are those moments, in ms before the window's end (after it, negative).
 */
export const READ_CHECKS = [8 * 60_000, 2 * 60_000, -3 * 60_000, -15 * 60_000];

/**
 * a status the API answers for good: the reading's result, or no assignment to judge by. not_read is not: a reading
 * the collector stores after the window's end turns it, so the checks after the end ask for it again.
 */
function final(b: Pick<Blob, "reconstructable">): boolean {
  const s = b.reconstructable?.status;
  return s === "yes" || s === "no" || s === "unknown";
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
