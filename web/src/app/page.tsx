"use client";
import { Suspense } from "react";
import { API_BASE, useApi, type Network, type Validator, type Meta, MIN_RATED } from "@/lib/api";
import { useWindowAfterMount, WindowSwitch } from "@/lib/window";
import StatusLine from "@/components/StatusLine";
import Validators from "@/components/Validators";
import PreLive from "@/components/PreLive";
import HostMap from "@/components/HostMap";
import CurrentProviders from "@/components/CurrentProviders";
import RecentBlobs, { Availability } from "@/components/RecentBlobs";

/** how often the overview reads the 24h validator list, which moves with every block */
const LIVE_POLL_MS = 15000;

/**
 * The overview, from the chain's own records: which validators run a Fibre
 * provider and how much stake that is (x/valaddr, x/staking), the newest
 * settlements, and each validator's endorsements over the selected period.
 * Whether an endpoint answers is this observer's check, and the map's key and
 * the table say so; serving is on the validator's page.
 *
 * The top is one panel: the map of Fibre providers with its counts on a bar
 * under it, then a row of instruments: the current Fibre providers' share of
 * voting power with Tensile's Blob availability under it, the recent blobs, and
 * the latest blob's readout. Every part of it keeps its size while its data
 * loads.
 */
function Overview() {
  // read after mount, so the page is prerendered whole (see useWindowAfterMount)
  const [win, setWin] = useWindowAfterMount("24h");
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  const net = useApi<Network>(`/v1/network?window=${win}`);
  // Tensile's Blob availability figure is "now", not the period: every blob read so far
  const whole = useApi<Network>("/v1/network?window=all");
  // The 24h list is refreshed as often as every ten seconds on the observer
  // (endorsements move with every block; in September 2026 its 15-22 s
  // computation held it to about every 35-45 s), so it is read more often than
  // the longer periods, whose snapshots move every few minutes.
  const vals = useApi<{ validators: Validator[] }>(`/v1/validators?window=${win}`, win === "24h" ? LIVE_POLL_MS : undefined);

  const N = net.data;
  const rows = vals.data?.validators ?? [];
  const notLive = !!meta?.app_version && !meta.fibre_active;
  const o = N?.obligations;
  const decided = o ? o.served + o.broken : 0;
  const measuring = !!N && !notLive && !!o && o.total > 0 && decided < MIN_RATED && o.pending > 0;
  // The Blob availability share (CIP-51's term) over the blobs whose reading
  // decides them, drawn over the newest sample_limit blobs with a reading.
  const rc = whole.data?.reconstructable;
  const examined = rc?.publications_examined ?? 0;
  const observed = { rec: rc?.recoverable, examinedAll: examined < (rc?.sample_limit ?? Infinity) };

  return (
    <>
      {/* No visible headline for now; the page keeps one for screen readers. */}
      <h1 className="sr-only">Tensile · Celestia Fibre</h1>
      <PreLive meta={meta} />
      <StatusLine meta={meta} metaError={metaErr} snap={N} client={{ error: net.error, fetchedAt: net.fetchedAt, status: net.status, computing: net.computing || whole.computing || vals.computing }} measuring={measuring} />

      <div className="deck">
        <HostMap rows={vals.data ? rows : null} />
        <div className={`deck-row${notLive ? " no-cp" : ""}`}>
          {/* the stake question is the first one after activation; before it the row starts with the blobs */}
          {!notLive && <CurrentProviders rows={vals.data ? rows : null} />}
          <Availability observed={observed} />
          <RecentBlobs />
        </div>
      </div>

      {/* the period drives the table's Shard data and Endorsements; the map, the stake and the blobs are now */}
      <Validators rows={rows} window={win} notLive={notLive} loading={vals.loading}
        periodSwitch={<WindowSwitch value={win} onChange={setWin} />} />
      <p className="tnote"><a href={`${API_BASE}/v1/feed.atom`} type="application/atom+xml">Network events (Atom)</a></p>
      {notLive && meta && <p className="tnote">Fibre is not live on {meta.chain_id} (app v{meta.app_version}{meta.fibre_app_version ? `, needs v${meta.fibre_app_version}` : ""}).</p>}
    </>
  );
}

export default function Page() {
  return <Suspense fallback={<h1 className="sr-only">Tensile · Celestia Fibre</h1>}><Overview /></Suspense>;
}
