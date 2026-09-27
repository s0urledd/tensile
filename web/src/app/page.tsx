"use client";
import { Suspense } from "react";
import Link from "next/link";
import { API_BASE, useApi, type Network, type Validator, type Meta, type Market, type Blob, int, pctOf, bytes, tia, ago, whenUTC, MIN_RATED } from "@/lib/api";
import { useWindow, WindowSwitch } from "@/lib/window";
import StatusLine from "@/components/StatusLine";
import { Metric, Metrics } from "@/components/Metrics";
import Validators from "@/components/Validators";
import PreLive from "@/components/PreLive";
import HostMap from "@/components/HostMap";

/**
 * The overview, from the chain's own records: which validators run a Fibre
 * provider and how much stake that is (x/valaddr, x/staking), the newest
 * settlement, what x/fibre settled over the selected period, and each
 * validator's endorsements. Whether an endpoint answers is this observer's
 * check, and the table says so; serving is on the validator's page.
 */
function Overview() {
  const [win, setWin] = useWindow("24h");
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  const net = useApi<Network>(`/v1/network?window=${win}`);
  const vals = useApi<{ validators: Validator[] }>(`/v1/validators?window=${win}`);
  const market = useApi<Market>(`/v1/market?window=${win}`);
  const newest = useApi<{ blobs: Blob[] }>("/v1/blobs?limit=1");

  const N = net.data;
  const M = market.data;
  const rows = vals.data?.validators ?? [];
  const notLive = !!meta?.app_version && !meta.fibre_active;
  const o = N?.obligations;
  const decided = o ? o.served + o.broken : 0;
  const measuring = !!N && !notLive && !!o && o.total > 0 && decided < MIN_RATED && o.pending > 0;
  // The newest blob, whatever the period: the one whose MsgPayForFibre settled
  // last, as the chain recorded it. It opens the blob page.
  const last = newest.data?.blobs?.[0];
  const latest = last && (
    <div id="latest">
      <h2>Latest blob <span className="soft">· settled {ago(last.settlement_time)}</span></h2>
      <dl className="latest">
        <div><dt>Height</dt><dd>{int(last.settlement_height)}</dd></div>
        <div><dt>Time</dt><dd>{whenUTC(last.settlement_time)}</dd></div>
        <div><dt>Upload size</dt><dd>{bytes(last.blob_size)}</dd></div>
        {last.attested_with_rows != null && <div><dt>Endorsements</dt><dd>{int(last.attested_with_rows)} of {int(last.validators_with_rows)} validators</dd></div>}
        {last.attested_voting_power != null && !!last.total_voting_power && <div><dt>Endorsed voting power</dt><dd>{pctOf(last.attested_voting_power, last.total_voting_power)}</dd></div>}
      </dl>
      <p className="blobs">
        <Link href={`/blob/?hash=${last.promise_hash}`}>Blob details →</Link>
        <span className="sep">·</span>
        <Link href="/blobs/">All blobs →</Link>
      </p>
    </div>
  );
  const beside = !!vals.data && !!meta?.fibre_active;
  const none = !M || notLive;

  return (
    <>
      {/* No visible headline for now; the page keeps one for screen readers. */}
      <h1 className="sr-only">Tensile · Celestia Fibre</h1>
      <PreLive meta={meta} />
      <StatusLine meta={meta} metaError={metaErr} snap={N} client={{ error: net.error, fetchedAt: net.fetchedAt, status: net.status }} measuring={measuring} />

      {vals.data && <HostMap rows={rows} showReadiness={!!meta?.fibre_active} aside={latest || undefined} />}

      {/* the period drives every card below it; the map, the stake and the latest blob are now */}
      <div className="period-row"><WindowSwitch value={win} onChange={setWin} /></div>
      <Metrics>
        <Metric label="Blobs"
          value={none ? "—" : int(M.blobs)}
          tone={none || M.settlements === 0 ? "absent" : undefined}
          title="Blobs published through Fibre in this period. A blob paid for twice counts once."
          help={none ? " " : M.settlements === 0 ? "none in this period" : `${int(M.settlements)} settlement${M.settlements === 1 ? "" : "s"}`} />
        <Metric label="Upload size"
          value={none ? "—" : bytes(M.bytes)}
          tone={none || M.settlements === 0 ? "absent" : undefined}
          title="Total size of the blobs paid for in this period."
          help={none ? " " : "total blob size"} />
        <Metric label="Fees paid"
          value={none ? "—" : tia(M.fees_settled_utia)}
          tone={none || M.settlements === 0 ? "absent" : undefined}
          title="TIA paid for these blobs. It goes to validators and their delegators."
          help={none ? " " : M.paid_per_mib_utia == null ? "none in this period" : `${tia(M.paid_per_mib_utia)} per MiB`} />
        <Metric label="Publishers"
          value={none ? "—" : int(M.publishers_active)}
          tone={none || M.publishers_active === 0 ? "absent" : undefined}
          title="Accounts that published blobs in this period."
          help=" " />
        <Metric label="Payment promise timeouts"
          value={none ? "—" : int(M.timeouts)}
          tone={none ? "absent" : undefined}
          title="Payment promises not settled within an hour. The account is charged anyway."
          help={none ? " " : M.timeouts > 0 ? `${tia(M.timed_out_utia)} charged` : "none in this period"} />
      </Metrics>

      {/* Beside the map when it shows the stake panel; on its own otherwise. */}
      {latest && !beside && <section className="band" id="outcomes"><div>{latest}</div></section>}

      <Validators rows={rows} window={win} notLive={notLive} loading={vals.loading} />
      <p className="tnote"><a href={`${API_BASE}/v1/feed.atom`} type="application/atom+xml">Network events (Atom)</a></p>
      {notLive && meta && <p className="tnote">Fibre is not live on {meta.chain_id} (app v{meta.app_version}{meta.fibre_app_version ? `, needs v${meta.fibre_app_version}` : ""}).</p>}
    </>
  );
}

export default function Page() {
  return <Suspense fallback={<h1 className="sr-only">Tensile · Celestia Fibre</h1>}><Overview /></Suspense>;
}
