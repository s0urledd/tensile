/**
 * The API page's one word about the API (app/api/reference.tsx, Health): that it does not answer, or that the site
 * refuses this reader as too many requests. Never what /v1/health says of the observer's own checks: the site shows
 * nothing about the observer while its API answers.
 */

/** what /v1/health's answer says of the API itself: null when it answered, whatever its checks say */
export function apiDown(status: number, body: unknown): { word: string; why: string } | null {
  const b = body && typeof body === "object" ? body as { status?: unknown; error?: unknown } : {};
  // the site's proxy refusing a client over its rate limit: the API is up
  if (status === 429) return { word: "Busy", why: typeof b.error === "string" ? b.error : "too many requests" };
  // /v1/health's own answer, ok or not (it answers 503 while a check fails)
  if (typeof b.status === "string") return null;
  // the proxy's 502 while the API restarts, or any answer that is not the API's
  return { word: "Not answering", why: typeof b.error === "string" ? b.error : `HTTP ${status}` };
}
