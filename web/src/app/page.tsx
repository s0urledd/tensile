"use client";
import { Suspense } from "react";
import Link from "next/link";
import { API_BASE, useApi, type Network, type Validator, type Meta, type Blob, int, pctOf, bytes, ago, whenUTC, MIN_RATED } from "@/lib/api";
import { useWindow, WindowSwitch } from "@/lib/window";
import StatusLine from "@/components/StatusLine";
import { Eye } from "@/components/Metrics";
import { Mark } from "@/components/Verdict";
import Validators from "@/components/Validators";
import PreLive from "@/components/PreLive";
import HostMap from "@/components/HostMap";
import { recon } from "@/lib/status";

/** how often the overview reads what moves with every block: the 24h
 *  validator list and the newest blob */
const LIVE_POLL_MS = 15000;

/**
 * The overview, from the chain's own records: which validators run a Fibre
 * provider and how much stake that is (x/valaddr, x/staking), the newest
 * settlement, and each validator's endorsements over the selected period.
 * Whether an endpoint answers is this observer's check, and the table says
 * so; serving is on the validator's page.
 */
function Overview() {
  const [win, setWin] = useWindow("24h");
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  const net = useApi<Network>(`/v1/network?window=${win}`);
  // Tensile's Blob availability figure is "now", not the period: every blob read so far
  const whole = useApi<Network>("/v1/network?window=all");
  // The 24h list is refreshed as often as every ten seconds on the observer
  // (endorsements move with every block; in September 2026 its 15-22 s
  // computation held it to about every 35-45 s), so it is read more often than
  // the longer periods, whose snapshots move every few minutes.
  const vals = useApi<{ validators: Validator[] }>(`/v1/validators?window=${win}`, win === "24h" ? LIVE_POLL_MS : undefined);
  const newest = useApi<{ blobs: Blob[] }>("/v1/blobs?limit=1", LIVE_POLL_MS);

  const N = net.data;
  const rows = vals.data?.validators ?? [];
  const notLive = !!meta?.app_version && !meta.fibre_active;
  const o = N?.obligations;
  const decided = o ? o.served + o.broken : 0;
  const measuring = !!N && !notLive && !!o && o.total > 0 && decided < MIN_RATED && o.pending > 0;
  // The newest blob, whatever the period: the one whose MsgPayForFibre settled
  // last, as the chain recorded it. It opens the blob page.
  const last = newest.data?.blobs?.[0];
  const latest = last && (
    <div id="latest" className="ov-latest">
      <h2 className="ov-latest-h"><span className="ov-eyebrow">Latest blob</span> <span className="ov-when"><span aria-hidden="true">· </span>settled {ago(last.settlement_time)}</span></h2>
      <dl className="ov-spec">
        <div><dt>Height</dt><dd>{int(last.settlement_height)}</dd></div>
        <div><dt>Time</dt><dd>{whenUTC(last.settlement_time)}</dd></div>
        <div><dt>Blob size</dt><dd>{bytes(last.blob_size)}</dd></div>
        {last.attested_with_rows != null && <div><dt>Endorsements</dt><dd>{int(last.attested_with_rows)} <span className="ov-of">of {int(last.validators_with_rows)} validators</span></dd></div>}
        {last.attested_voting_power != null && !!last.total_voting_power && <div className="ov-wide"><dt>Endorsed voting power</dt><dd>{pctOf(last.attested_voting_power, last.total_voting_power)}</dd></div>}
      </dl>
      <p className="ov-links">
        <Link href={`/blob/?hash=${last.promise_hash}`}>Blob details <span aria-hidden="true">→</span></Link>
        <Link href="/blobs/">All blobs <span aria-hidden="true">→</span></Link>
      </p>
    </div>
  );
  // Under the latest blob, Tensile's own readings, marked as such and set quieter than the
  // chain's: the Blob availability share (CIP-51's term) over the blobs whose reading decides them, and the
  // latest blob's own result, in the Blobs list's words.
  const rec = whole.data?.reconstructable?.recoverable;
  // The figure is drawn over the newest sample_limit blobs with a reading;
  // when there were more, say so.
  const examined = whole.data?.reconstructable?.publications_examined ?? 0;
  const examinedAll = examined < (whole.data?.reconstructable?.sample_limit ?? Infinity);
  const st = last ? recon(last) : null;
  const observed = (rec || st) && (
    <aside className="ov-obs" aria-label="Observed by Tensile">
      <span className="obs-tag"><Eye />Observed by Tensile</span>
      <dl className="ov-obs-grid">
        {rec && (
          <div title="Observed by Tensile: blobs whose rows were enough to reconstruct them, over the blobs read (available plus unavailable): the newest ones with a reading, up to the API's sample limit.">
            <dt>Blob availability</dt>
            <dd>
              <span className={`ov-obs-v num${rec.den > 0 ? "" : " absent"}`}>{pctOf(rec.num, rec.den)}</span>
              <span className="ov-obs-h">{rec.den > 0 ? `${int(rec.num)} of ${examinedAll ? "" : "the newest "}${int(rec.den)} blobs read` : "none read"}</span>
            </dd>
          </div>
        )}
        {st && (
          <div>
            <dt>Latest blob</dt>
            <dd><span className={`verdict verdict--${st.tier}`} title={st.title}><Mark tier={st.tier} /><span className="w">{st.word}</span></span></dd>
          </div>
        )}
      </dl>
    </aside>
  );
  const aside = latest || observed ? <>{latest}{observed}</> : null;
  const beside = !!vals.data && !!meta?.fibre_active;

  return (
    <>
      {/* No visible headline for now; the page keeps one for screen readers. */}
      <h1 className="sr-only">Tensile · Celestia Fibre</h1>
      <PreLive meta={meta} />
      <StatusLine meta={meta} metaError={metaErr} snap={N} client={{ error: net.error, fetchedAt: net.fetchedAt, status: net.status }} measuring={measuring} />

      {vals.data && <HostMap rows={rows} showReadiness={!!meta?.fibre_active} aside={aside || undefined} />}

      {/* Beside the map when it shows the stake panel; on its own otherwise. */}
      {aside && !beside && <section className="band" id="outcomes"><div>{aside}</div></section>}

      {/* the period drives the table's Shard data and Endorsements; the map, the stake and the latest blob are now */}
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
