"use client";
import Link from "next/link";
import { type Meta, type RecordThrough, type Window, type ScanGap, int, utcWord, apiFailing, throttled } from "@/lib/api";

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
 * Exceptions only. In the ordinary case (the observer healthy, the chain
 * moving, nothing withheld) this renders nothing: the block ticker in the
 * header already shows the observer following the chain, and a paragraph
 * saying "all is well" on every page is noise a reader learns to skip, which
 * is exactly what they must not do on the day it says something else.
 *
 * What still earns a line, in order: the API not answering; the observer's
 * own checks failing, by name; the chain halted for the upgrade that brings
 * Fibre; figures withheld (a stale assignment pin, scan gaps). Each says what it means for the numbers below it.
 */
/**
 * A check's detail up to its first clause. Details carry the process's last
 * error (a raw RPC message, often a harmless retry from minutes ago); the
 * banner says what is wrong now, and the full text stays in the tooltip and
 * in /v1/health.
 */
function brief(detail: string): string {
  const cut = detail.search(/; last error|: error in json rpc/);
  return (cut > 0 ? detail.slice(0, cut) : detail).trim();
}

export default function StatusLine({ meta, metaError, snap, client }: {
  meta: Meta | null;
  metaError: string | null;
  snap: Snapshot | null;
  client: Client;
  /** kept for callers; the ordinary "measuring" state needs no line */
  measuring?: boolean;
}) {
  const failing = (meta?.checks ?? []).filter((c) => !c.ok);
  const notLive = !!meta?.app_version && !meta.fibre_active;
  // The chain is on the version that brings Fibre but the modules do not
  // answer yet: the upgrade height passed and the chain is halted for, or
  // just resuming from, the switch to the new binary.
  const activating = notLive && Number(meta!.app_version) >= Number(meta!.fibre_app_version || "10");
  // An answer that is not an outage (a 404 for a blob not recorded yet) is
  // the page's to explain; only a network error or a 5xx is the API down.
  const apiDown = apiFailing(client) || (!!metaError && !meta);
  const gaps: ScanGap[] = meta?.scan_gaps ?? [];
  const sig = meta?.upgrade_signal;

  // What each failing check means for a reader, in their words rather than
  // the process names. Scan gaps and the pin have lines of their own below.
  const IMPACT: Record<string, string> = {
    scanner: "new blobs are not being read from the chain",
    scanner_lag: "the observer is catching up with the chain",
    prober: "blobs are not being read",
    heartbeat: "endpoint checks are paused",
    collector: "figures are not being updated",
    disk: "the observer is short of disk",
    unassignable_publications: "some recent blobs could not be assigned to validators",
  };
  const chainStopped = failing.some((c) => c.name === "chain_liveness");
  const impacts = [...new Set(failing.flatMap((c) =>
    c.name === "chain_liveness" ? ["the chain has not produced a block recently"] : IMPACT[c.name] ? [IMPACT[c.name]] : c.name === "scan_gaps" || c.name === "pin" ? [] : [`${c.name}: ${brief(c.detail)}`]))];
  const lines: React.ReactNode[] = [];
  if (apiDown) {
    const err = client.error ?? metaError ?? "no answer";
    lines.push(
      <p className="notice" key="api">
        {throttled(client) ? "The observer API is busy" : "The observer API is not answering"} (<b>{err}</b>).{" "}
        {client.fetchedAt && snap
          ? <>Everything below is the last successful snapshot, from {utcWord(client.fetchedAt)}{snap.computed_at ? <>, computed {utcWord(snap.computed_at)}</> : ""}.</>
          : <>Nothing can be shown until it answers; the page retries every 30 seconds.</>}{" "}
        This is an observer outage, not a Fibre network outage.
      </p>,
    );
  } else if (impacts.length > 0) {
    lines.push(
      <p className="notice" key="checks" title={failing.map((c) => `${c.name}: ${c.detail}`).join(" · ")}>
        {chainStopped && impacts.length === 1
          ? <><b>The chain has stopped producing blocks.</b> Nothing new can be settled or measured until it resumes; this is the network, not the observer.</>
          : <><b>Observer partly down:</b> {impacts.join("; ")}. Figures may lag.</>}{" "}
        <Link href="/methodology/#gaps">Why →</Link>
      </p>,
    );
  }
  // After a restart or a change of rules a figure can take a minute or two to
  // compute: a cold window takes half a minute or more, and a cache computes
  // its windows one after another. The API says so rather than failing, and
  // the page asks again by itself: a quiet line, not an outage, and no promise
  // of how soon.
  if (!apiDown && client.computing) {
    lines.push(
      <p className="notice soft" key="computing">
        Some figures on this page are being computed. They fill in by themselves when ready.
      </p>,
    );
  }
  if (activating) {
    lines.push(
      <p className="notice" key="activating">
        <b>Fibre activating</b>{sig?.upgrade_height ? <> · upgrade height <span className="mono">#{int(sig.upgrade_height)}</span> reached</> : null}. The Fibre modules answer once validators restart on the new binary and blocks resume.
      </p>,
    );
  }
  if (meta?.pin_status === "chain_ahead") {
    lines.push(
      <p className="notice" key="pin">
        The chain runs an app version above this observer&rsquo;s assignment pin: <b>verdicts are withheld</b> until it is re-pinned.
      </p>,
    );
  }
  if (gaps.length > 0) {
    lines.push(
      <p className="notice soft" key="gaps">
        <b>{int(gaps.length)} scan gap{gaps.length === 1 ? "" : "s"}</b> ({gaps.map((g) => `#${int(g.from)}–#${int(g.to)}`).join(", ")}): a publication settled in an unread block is unknown here, never counted served or unserved.      </p>,
    );
  }
  return lines.length ? <>{lines}</> : null;
}
