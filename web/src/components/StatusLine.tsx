"use client";
import { type Meta, type RecordThrough, type Window, utcWord, apiFailing, throttled } from "@/lib/api";

/** what the page's own data stream says about the API right now. computing: a figure
 *  it asked for is being computed, which is neither an error nor an outage (lib/api.ts, Fetch) */
export type Client = { error: string | null; fetchedAt: string | null; status?: number; computing?: boolean };

/** the part of a snapshot the status line reads */
export type Snapshot = {
  computed_at?: string;
  record_through?: RecordThrough;
  window?: Window;
};

/**
 * One line, and only when the observer API does not answer: then the page
 * cannot show what it should, and says so. Nothing else about the observer
 * is shown on the site, not a failing check, a lag, a gap or a figure being
 * computed: the operator hears of those from the health watch
 * (deploy/healthwatch.sh), and /v1/health has them for anyone who asks.
 */
export default function StatusLine({ meta, metaError, snap, client }: {
  meta: Meta | null;
  metaError: string | null;
  snap: Snapshot | null;
  client: Client;
  /** kept for callers */
  measuring?: boolean;
}) {
  // An answer that is not an outage (a 404 for a blob not recorded yet) is
  // the page's to explain; only a network error or a 5xx is the API down.
  const apiDown = apiFailing(client) || (!!metaError && !meta);
  if (!apiDown) return null;
  const err = client.error ?? metaError ?? "no answer";
  return (
    <p className="notice" key="api">
      {throttled(client) ? "The observer API is busy" : "The observer API is not answering"} (<b>{err}</b>).{" "}
      {client.fetchedAt && snap
        ? <>Everything below is the last successful snapshot, from {utcWord(client.fetchedAt)}{snap.computed_at ? <>, computed {utcWord(snap.computed_at)}</> : ""}.</>
        : <>Nothing can be shown until it answers; the page retries every 30 seconds.</>}{" "}
      This is an observer outage, not a Fibre network outage.
    </p>
  );
}
