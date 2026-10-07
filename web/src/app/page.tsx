"use client";
import { Suspense } from "react";
import { API_BASE, useApi, type Validator, type Meta } from "@/lib/api";
import { useWindowAfterMount, WindowSwitch } from "@/lib/window";
import StatusLine from "@/components/StatusLine";
import Validators from "@/components/Validators";
import PreLive from "@/components/PreLive";
import HostMap from "@/components/HostMap";
import CurrentProviders from "@/components/CurrentProviders";
import RecentBlobs from "@/components/RecentBlobs";

/** how often the overview reads the 24h validator list, which moves with every block */
const LIVE_POLL_MS = 15000;

/**
 * The overview, from the chain's own records: which validators run a Fibre
 * provider and how much stake that is (x/valaddr, x/staking), the newest
 * settlements, and each validator's endorsements over the selected period.
 * Whether an endpoint answers is this observer's check, and the map's key and
 * the table say so; serving is on the validator's page.
 *
 * The top is one panel: the map of Fibre providers with its counts under it,
 * then a row of instruments: the current Fibre providers' share of voting
 * power, the recent blobs, and the latest blob's readout. Every part of it
 * keeps its size while its data loads.
 */
function Overview() {
  // read after mount, so the page is prerendered whole (see useWindowAfterMount); nothing that
  // depends on the period is asked for until the URL has said which one it is
  const [win, setWin, winKnown] = useWindowAfterMount("24h");
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  // The 24h list is refreshed as often as every ten seconds on the observer
  // (endorsements move with every block; in September 2026 its 15-22 s
  // computation held it to about every 35-45 s), so it is read more often than
  // the longer periods, whose snapshots move every few minutes.
  const vals = useApi<{ validators: Validator[]; computed_at?: string }>(winKnown ? `/v1/validators?window=${win}` : null, win === "24h" ? LIVE_POLL_MS : undefined);

  const rows = vals.data?.validators ?? [];
  const notLive = !!meta?.app_version && !meta.fibre_active;

  return (
    <>
      {/* No visible headline for now; the page keeps one for screen readers. */}
      <h1 className="sr-only">Tensile · Celestia Fibre</h1>
      <PreLive meta={meta} />
      {/* The notice follows the stream the page is drawn from, the validators: the network summary it used to follow was
          shown nowhere, so its failure put an outage over a full table, and the validators' own failure went unsaid. */}
      <StatusLine meta={meta} metaError={metaErr} snap={vals.data} client={{ error: vals.error, fetchedAt: vals.fetchedAt, status: vals.status, computing: vals.computing }} />

      <div className="deck">
        <HostMap rows={vals.data ? rows : null} />
        <div className={`deck-row${notLive ? " no-cp" : ""}`}>
          {/* the stake question is the first one after activation; before it the row starts with the blobs */}
          {!notLive && <CurrentProviders rows={vals.data ? rows : null} />}
          <RecentBlobs />
        </div>
      </div>

      {/* the period drives the table's Shard data and Endorsements; the map, the stake and the blobs are now */}
      <Validators rows={rows} window={win} notLive={notLive} loading={vals.loading || (!vals.data && !vals.error)}
        failed={!vals.data && vals.error ? vals : null}
        periodSwitch={<WindowSwitch value={win} onChange={setWin} />} />
      <p className="tnote"><a href={`${API_BASE}/v1/feed.atom`} type="application/atom+xml">Network events (Atom)</a></p>
      {notLive && meta && <p className="tnote">Fibre is not live on {meta.chain_id} (app v{meta.app_version}{meta.fibre_app_version ? `, needs v${meta.fibre_app_version}` : ""}).</p>}
    </>
  );
}

export default function Page() {
  return <Suspense fallback={<h1 className="sr-only">Tensile · Celestia Fibre</h1>}><Overview /></Suspense>;
}
