"use client";
import { Suspense, useState } from "react";
import Link from "next/link";
import { API_BASE, useApi, type Network, type Validator, type Meta, type Market, int, pctOf, bytes, undecided, MIN_RATED, rateTone, leftOutText } from "@/lib/api";
import { useWindow, WindowSwitch, windowLabel } from "@/lib/window";
import StatusLine from "@/components/StatusLine";
import { Metric, Metrics } from "@/components/Metrics";
import OutcomeBar from "@/components/OutcomeBar";
import Validators from "@/components/Validators";
import PreLive from "@/components/PreLive";
import HostMap from "@/components/HostMap";

/**
 * The overview: the network's obligations over the selected period, the
 * endpoints answering now, what was settled, and the validators. Every
 * figure maps to one field of /v1/network, /v1/validators or /v1/market;
 * the definitions are on the methodology page, one disclosure away.
 */
function Overview() {
  const [win, setWin] = useWindow("24h");
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  const net = useApi<Network>(`/v1/network?window=${win}`);
  const vals = useApi<{ validators: Validator[] }>(`/v1/validators?window=${win}`);
  const market = useApi<Market>("/v1/market?window=7d");
  const [disc, setDisc] = useState<"" | "outcomes" | "blobs">("");

  const N = net.data;
  const rows = vals.data?.validators ?? [];
  const notLive = !!meta?.app_version && !meta.fibre_active;
  const o = N?.obligations;
  const decided = o ? o.served + o.broken : 0;
  const und = undecided(o);
  const brokenOn = rows.filter((v) => (v.obligations?.broken ?? 0) > 0).length;
  const reach = N?.reachability;
  const rc = N?.reconstructable;
  const judged = rc ? rc.yes + rc.degraded + rc.no : 0;
  const measuring = !!N && !notLive && !!o && o.total > 0 && decided < MIN_RATED && o.pending > 0;
  const period = windowLabel(win).toLowerCase() === "all" ? "all time" : `${win}`;
  // Fibre host registration among the bonded set, by count and by stake:
  // the first thing to watch after activation, and the ceiling on every
  // other figure, since a validator with no host cannot be probed at all.
  const reg = (() => {
    const bonded = rows.filter((v) => !v.jailed && (!v.bond_status || v.bond_status === "BOND_STATUS_BONDED"));
    const withHost = bonded.filter((v) => !!v.host);
    const power = bonded.reduce((s, v) => s + (v.voting_power || 0), 0);
    const hosted = withHost.reduce((s, v) => s + (v.voting_power || 0), 0);
    return { count: withHost.length, of: bonded.length, share: power > 0 ? pctOf(hosted, power) : "—" };
  })();

  const hasOutcomes = ((o?.total ?? 0) > 0 || (N?.publications ?? 0) > 0 || (market.data?.settlements ?? 0) > 0 || (market.data?.timeouts ?? 0) > 0);
  const outcomesBeside = !!vals.data && !!meta?.fibre_active;
  const outcomes = (
    <div id="outcomes">
        <h2>Obligation outcomes <span className="soft">· all validators · {period}</span></h2>
        <OutcomeBar o={o} absent={!N || notLive} />
        <p className="blobs">
          <button type="button" className="dis" aria-expanded={disc === "outcomes"} onClick={() => setDisc(disc === "outcomes" ? "" : "outcomes")}>About these outcomes</button>
          <span className="sep">·</span>
          <button type="button" className="dis" aria-expanded={disc === "blobs"} onClick={() => setDisc(disc === "blobs" ? "" : "blobs")}>Blob availability →</button>
          {rc && judged > 0 && <span className="soft">{int(rc.yes)} / {int(judged)} fully served</span>}
        </p>
        <p className="disc" hidden={disc !== "outcomes"}>
          {o && o.total > 0
            ? <>The bar shows all <b>{int(o.total)}</b> obligations settled in the period — one per validator and blob it signed for. The service rate counts only the <b>{int(decided)}</b> assessed ones (served + broken); undecided (no reading at the end of the window {int(o.end_unobserved)}, never observed serving {int(o.unobserved)}{(o.held_param_unverified ?? 0) > 0 && <>, deadline unverified {int(o.held_param_unverified)}</>}) and pending stay outside it. Unreachable from here is never counted as broken.</>
            : <>No obligation settled in this period. An obligation is one validator and one blob it signed for; it is decided by the last probe before the retention deadline.</>}
          {" "}<Link href="/methodology/#rates">Methodology →</Link>
        </p>
        <p className="disc" hidden={disc !== "blobs"}>
          {rc && judged > 0
            ? <>Fully served <b>{int(rc.yes)} / {int(judged)}</b> judged blobs: every validator proven to hold a shard served its rows at the latest complete probe point. Rebuildable <b>{int(rc.yes + rc.degraded)} / {int(judged)}</b>: enough distinct rows were observed to rebuild the blob — an observation of rows, not an actual rebuild.{rc.pending > 0 && <> {int(rc.pending)} still in window.</>}{rc.unknown > 0 && <> {int(rc.unknown)} not judged.</>}</>
            : rc && rc.pending > 0 ? <>No blob judged yet: <b>{int(rc.pending)}</b> still inside the retention window.</>
            : <>No blob judged in this period.</>}
          {" "}Full breakdown in <Link href="/blobs/">Blobs →</Link>
        </p>
    </div>
  );

  return (
    <>
      {/* No visible headline for now; the page keeps one for screen readers. */}
      <h1 className="sr-only">Tensile · Celestia Fibre</h1>
      <PreLive meta={meta} />
      <StatusLine meta={meta} metaError={metaErr} snap={N} client={{ error: net.error, fetchedAt: net.fetchedAt, status: net.status }} measuring={measuring} />

      {vals.data && <HostMap rows={rows} showReadiness={!!meta?.fibre_active} aside={hasOutcomes ? outcomes : undefined} />}

      {/* the period drives the figures below it and the outcomes beside the map, not the map or the quorum */}
      <div className="period-row"><WindowSwitch value={win} onChange={setWin} /></div>
      <Metrics>
        <Metric label="Service rate"
          value={!N || notLive || !o || o.total === 0 || decided === 0 ? "—" : pctOf(o.served, decided)}
          tone={!N || notLive || !o || decided === 0 ? "absent" : rateTone(o.served, decided)}
          help={!N || notLive ? " " : !o || o.total === 0 ? "no obligation in this period" : decided === 0 ? "awaiting results" : `${int(o.served)} / ${int(decided)} assessed`} />
        <Metric label="Broken obligations"
          value={!N || notLive ? "—" : int(o?.broken ?? 0)}
          tone={!N || notLive ? "absent" : (o?.broken ?? 0) > 0 ? "fault" : undefined}
          help={!N || notLive ? " " : (o?.broken ?? 0) > 0 ? `on ${int(brokenOn)} validator${brokenOn === 1 ? "" : "s"}` : "none in this period"} />
        <Metric label="Pending"
          value={!N || notLive ? "—" : int(o?.pending ?? 0)}
          tone={!N || notLive ? "absent" : undefined}
          help={!N || notLive ? " " : (leftOutText(o) || "window still open")}
          title="Obligations whose retention window has not ended: no verdict yet. Sampled out: blobs the probe budget drew out of its sample, committed in advance and never counted either way." />
        <Metric label="Validators registered"
          value={!vals.data ? "—" : int(reg.count)}
          den={vals.data && reg.of > 0 ? int(reg.of) : undefined}
          tone={!vals.data || reg.count === 0 ? "absent" : undefined}
          title="Bonded validators with a Fibre host in x/valaddr, and the share of bonded voting power they hold. A validator without one cannot serve Fibre data."
          help={!vals.data ? " " : reg.count === 0 ? (notLive ? " " : "none yet")
            : <>{reg.share} of stake{reach && reach.den > 0 ? <> · {int(reach.num)} reachable now</> : null}</>} />
        <Metric label="Settled data"
          value={!N || notLive ? "—" : bytes(N.publication_bytes)}
          tone={!N || notLive || N.publications === 0 ? "absent" : undefined}
          help={!N || notLive ? " " : `${int(N.publications)} publication${N.publications === 1 ? "" : "s"} · ${period}`} />
      </Metrics>

      {/* Beside the map when it shows the quorum panel; on its own otherwise. Until a blob settles there is nothing to show. */}
      {hasOutcomes && !outcomesBeside && <section className="band" id="outcomes"><div>{outcomes}</div></section>}

      <Validators rows={rows} window={win} notLive={notLive} loading={vals.loading} />
      <p className="tnote"><a href={`${API_BASE}/v1/feed.atom`} type="application/atom+xml">Network events (Atom)</a></p>
      {notLive && meta && <p className="tnote">Fibre is not live on {meta.chain_id} (app v{meta.app_version}{meta.fibre_app_version ? `, needs v${meta.fibre_app_version}` : ""}).</p>}
    </>
  );
}

export default function Page() {
  return <Suspense fallback={<h1 className="sr-only">Tensile · Celestia Fibre</h1>}><Overview /></Suspense>;
}
