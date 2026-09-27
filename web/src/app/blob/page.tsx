"use client";
import { Suspense } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useApi, type Blob, type Probe, type Meta, int, bytes, tia, utcWord, hhmm, hhmmss, dur, shortMid, nsDisplay, notFound, API_BASE } from "@/lib/api";
import StatusLine from "@/components/StatusLine";
import { Metric, Metrics } from "@/components/Metrics";
import Copy from "@/components/Copy";

type Assignment = { validator_address: string; moniker?: string; voting_power: number; row_count: number; attested: boolean | null; host_at_settlement: string | null };
type Detail = {
  blob: Blob;
  params: { shard_retention_s: number; payment_promise_timeout_s: number };
  assignments: Assignment[] | null;
  probes: Probe[] | null;
  /** schedule points, by scheduled_at, the observer does not trust itself at: nothing there counts */
  suspect_points?: { at: string; label: string; reason: string }[] | null;
};

/** one mark per classification; the word is in the title and the legend */
const MARK: Record<string, [string, string]> = {
  HEALTHY: ["ok", "served"], FAULT: ["fault", "not served"], UNATTESTED: ["unsigned", "not endorsed"], EXPECTED_GONE: ["gone", "expected gone after the window"],
  NOT_REGISTERED: ["none", "no endpoint"], NOT_PROBED: ["gone", "not probed"], SERVER_ERROR: ["other", "server error"], UNREACHABLE: ["other", "unreachable"],
  THROTTLED: ["other", "rate limited"], IDENTITY_EXPIRED: ["other", "certificate expired"], IDENTITY_MISMATCH: ["other", "wrong certificate"],
  TOLERATED: ["gone", "tolerated after the deadline"], UNREACHABLE_POST_WINDOW: ["gone", "unreachable after the window"], SERVED_PAST_WINDOW: ["gone", "served after the window"],
  RETENTION_UNVERIFIED: ["gone", "deadline unverified"], SHADOWED_SHARD: ["other", "shadowed by another promise"], UNMATCHED_GENUINE: ["other", "unmatched genuine rows"],
  PROBE_ERROR: ["gone", "probe error"], EXPECTED_UNASSIGNED: ["gone", "unassigned"], SERVING_UNASSIGNED: ["other", "serving unassigned"],
};
const markOf = (cls: string): [string, string] => MARK[cls] ?? ["other", cls.toLowerCase().replace(/_/g, " ")];
/**
 * The end-of-window reading (schedule label "end") counts as a reader of the
 * chain's own client meets the validator: no rows back is not served, genuine
 * rows of the blob are served (probe.EndReadClass; this is its wording).
 */
const END_NO_ROWS = new Set(["UNREACHABLE", "IDENTITY_MISMATCH", "IDENTITY_EXPIRED", "SERVER_ERROR", "THROTTLED", "NOT_REGISTERED"]);
const END_GENUINE = new Set(["SHADOWED_SHARD", "UNMATCHED_GENUINE"]);
/** the reading's name: the end-of-window reading, or the earlier schedule's point */
const pointName = (k: string) => (k === "end" ? "the end reading" : k);
/** the mark for one probe: an unsigned probe says what came back and is not rated either way */
const probeMark = (p: Probe): [string, string] => {
  if (p.schedule_label === "end" && END_NO_ROWS.has(p.classification)) return ["fault", `not served · ${markOf(p.classification)[1]}`];
  if (p.schedule_label === "end" && END_GENUINE.has(p.classification)) return ["ok", "served, genuine rows of the blob"];
  if (p.classification === "UNATTESTED") return ["unsigned", (p.outcome === "SERVED_OK" || p.outcome === "PARTIAL") ? "served, not endorsed" : `not endorsed · ${p.outcome.toLowerCase().replace(/_/g, " ")}`];
  return markOf(p.classification);
};

function Page() {
  const hash = useSearchParams().get("hash") ?? "";
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  const d = useApi<Detail>(hash ? `/v1/blobs/${hash}` : null);
  if (!hash) return <p className="notice">Open a blob from the <Link href="/blobs/">list</Link>, or add <code>?hash=&lt;promise hash&gt;</code> to the address.</p>;
  const data = d.data;
  if (!data) {
    return (
      <>
        <div className="head"><div><p className="crumb"><Link href="/blobs/">Blobs</Link> › {hash.slice(0, 10)}…</p><h1>{notFound(d) ? "Blob not recorded yet" : d.error ? "Blob" : "Loading…"}</h1></div></div>
        <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: d.error, fetchedAt: d.fetchedAt, status: d.status }} />
        {notFound(d) && <p className="notice">No publication with the promise hash <span className="mono">{shortMid(hash, 10, 6)}</span> is on record. A blob appears here once the scanner has read the block that settled it; this page checks again every 30 seconds.</p>}
      </>
    );
  }
  const b = data.blob;
  const probes = data.probes ?? [];
  const assignments = data.assignments ?? [];
  // signatures are a fact of the settled promise, not of any probe: read them from the assignments
  const signedKnown = assignments.some((a) => a.attested != null);
  const signedN = assignments.filter((a) => a.attested === true).length;
  const suspectAt = new Map((data.suspect_points ?? []).map((sp) => [sp.at, sp.reason.replace(",", " and ")]));
  const rc = b.reconstructable;
  // drawn out of the sample: one record, with the probability it was drawn at
  const so = b.sampled_out;
  const soText = so ? `p=${so.p.toFixed(2)}` : "";
  const judged = !!rc && (rc.status === "yes" || rc.status === "degraded" || rc.status === "no");
  const over = new Date(b.must_serve_until).getTime() <= Date.now();
  // Retrievable: enough rows came back to reconstruct the blob ("some rows
  // were retrieved, but not enough to reconstruct" is the client's own word
  // for the other case). Whether every endorsing validator served is said
  // beside it, not folded into it.
  const state: [string, string, string] =
    rc?.status === "yes" ? ["ok", "Retrievable", `Enough rows were retrieved at ${pointName(rc.point)} to reconstruct the blob, and every endorsing validator served its rows.`]
    : rc?.status === "degraded" ? ["ok", "Retrievable", `Enough rows were retrieved at ${pointName(rc.point)} to reconstruct the blob; ${int(rc.attested_validators - rc.served_by_attested)} of ${int(rc.attested_validators)} endorsing validators did not serve theirs.`]
    : rc?.status === "no" ? ["hold", "Not retrievable", `Rows were retrieved at ${pointName(rc.point)}, but fewer than the ${int(rc.needed_rows)} needed to reconstruct the blob.`]
    : rc?.status === "pending" && !over ? ["none", "In retention window", "Read once, 10 minutes before the retention window ends."]
    : rc?.status === "pending" ? ["none", "Not read by Tensile", "The observer was offline when this blob's reading was due, and the rows are pruned after the window. Nothing is counted for or against a validator."]
    : so ? ["none", "Sampled out", `Not read: the load policy of the time drew this blob out of its sample (${soText}).`]
    : !over ? ["none", "In retention window", "Read once, 10 minutes before the retention window ends."]
    : ["none", "Not read by Tensile", b.probe_count === 0 ? "No reading was taken for this blob." : "Row lists were not recorded for this publication, or its reading was not completed."];

  // the probe points, in the order they ran; a point's time is the earliest probe scheduled for it
  const byLabel = new Map<string, { at: string; phase: string; cls: Record<string, number> }>();
  for (const p of probes) {
    const cur = byLabel.get(p.schedule_label);
    if (!cur) byLabel.set(p.schedule_label, { at: p.scheduled_at, phase: p.phase, cls: {} });
    else if (p.scheduled_at < cur.at) cur.at = p.scheduled_at;
  }
  // the newest probe per (validator, point) is the cell; its classification counts once per point
  const cell = new Map<string, Probe>();
  for (const p of probes) {
    const k = p.validator_address + "|" + p.schedule_label;
    const cur = cell.get(k);
    if (!cur || p.started_at > cur.started_at) cell.set(k, p);
  }
  for (const p of cell.values()) { const e = byLabel.get(p.schedule_label)!; e.cls[p.classification] = (e.cls[p.classification] ?? 0) + 1; }
  const order = [...byLabel.entries()].sort((a, c) => a[1].at.localeCompare(c[1].at)).map(([k]) => k);
  const t0 = new Date(b.settlement_time).getTime(), tEnd = new Date(b.must_serve_until).getTime();
  const tMax = Math.max(tEnd, ...order.map((k) => new Date(byLabel.get(k)!.at).getTime())) + 2 * 60000;
  const hasPost = order.some((k) => new Date(byLabel.get(k)!.at).getTime() > tEnd);
  // the window takes 60% of the axis when points fall after the deadline (those minutes are stretched into the rest), else all of it
  const winShare = hasPost ? 60 : 100;
  const x = (t: number) => (t <= tEnd ? Math.max(0, (t - t0) / (tEnd - t0)) * winShare : winShare + (t - tEnd) / (tMax - tEnd) * (100 - winShare)).toFixed(2) + "%";
  const lastIn = [...order].reverse().find((k) => new Date(byLabel.get(k)!.at).getTime() <= tEnd);
  const countLine = (k: string) => {
    const c = byLabel.get(k)!.cls;
    const sus = suspectAt.get(byLabel.get(k)!.at);
    if (sus) {
      const n = Object.values(c).reduce((a, x) => a + x, 0);
      return (
        <div key={k} title={`At this point ${sus} of the validators probed failed at once. From one location that cannot be told from this observer's own network, so nothing at this point counts in any figure.`}>
          <b>{k} <span className="soft">· {hhmm(byLabel.get(k)!.at).replace(" UTC", "")}</span></b>
          <span>{int(n)} rows</span><span>not counted</span>
        </div>
      );
    }
    // at the end reading, no rows back is not served and genuine rows are served
    const endNo = k === "end" ? [...END_NO_ROWS].reduce((s, n) => s + (c[n] ?? 0), 0) : 0;
    const endOk = k === "end" ? [...END_GENUINE].reduce((s, n) => s + (c[n] ?? 0), 0) : 0;
    const served = (c.HEALTHY ?? 0) + endOk, gone = (c.EXPECTED_GONE ?? 0) + (c.TOLERATED ?? 0), unsigned = c.UNATTESTED ?? 0, notServed = (c.FAULT ?? 0) + endNo;
    const counted = new Set(["HEALTHY", "EXPECTED_GONE", "TOLERATED", "UNATTESTED", "FAULT", ...(k === "end" ? [...END_NO_ROWS, ...END_GENUINE] : [])]);
    const other = Object.entries(c).filter(([n]) => !counted.has(n)).reduce((s, [, n]) => s + n, 0);
    const post = byLabel.get(k)!.phase === "post";
    return (
      <div key={k}>
        <b>{k} <span className="soft">· {hhmm(byLabel.get(k)!.at).replace(" UTC", "")}</span></b>
        <span>{post ? `${int(gone)} expected gone` : `${int(served)} served`}</span>
        {notServed > 0 && <span className="word fault">{int(notServed)} not served</span>}
        {unsigned > 0 && <span>{int(unsigned)} not endorsed</span>}
        {other > 0 && <span>{int(other)} other</span>}
      </div>
    );
  };
  const rows = [...assignments].sort((a, c) => c.voting_power - a.voting_power || a.validator_address.localeCompare(c.validator_address));
  const winLen = dur(b.settlement_time, b.must_serve_until);
  const fill = rc && rc.total_rows > 0 ? Math.min(100, rc.served_distinct_rows / rc.total_rows * 100) : 0;
  const tick = rc && rc.total_rows > 0 ? Math.min(100, rc.needed_rows / rc.total_rows * 100) : 0;

  return (
    <>
      <div className="head">
        <div>
          <p className="crumb"><Link href="/blobs/">Blobs</Link> › {b.promise_hash.slice(0, 10)}…</p>
          <h1>Blob <span className="mono">{shortMid(b.promise_hash, 10, 6)}</span><Copy text={b.promise_hash} label="promise hash" /></h1>
          <div className="chips">
            <span className="state" title={state[2]}><i className={"dot " + state[0]} />{state[1]}</span>
            <span title={utcWord(b.must_serve_until)}>{over ? <>Window over since <b className="word">{hhmm(b.must_serve_until)}</b></> : <>In window until <b className="word">{hhmm(b.must_serve_until)}</b></>}</span>
            <span title="The padded upload size the module charges for, not the payload.">{bytes(b.blob_size)}</span>
          </div>
        </div>
      </div>
      <dl className="facts">
        <div><dt>Publisher</dt><dd title={b.signer}><Link className="mono" href={`/publisher/?addr=${b.signer}`}>{shortMid(b.signer, 14, 6)}</Link></dd></div>
        <div><dt>Namespace</dt><dd title={b.namespace}><span className="mono">{nsDisplay(b.namespace)}</span><Copy text={b.namespace} label="namespace" /></dd></div>
        <div><dt>Commitment</dt><dd title={b.commitment}><span className="mono">{shortMid(b.commitment, 8, 6)}</span><Copy text={b.commitment} label="commitment" /></dd></div>
        <div><dt>Settled</dt><dd title={utcWord(b.settlement_time)}><span className="mono">#{int(b.settlement_height)}</span><span className="soft"> · {hhmm(b.settlement_time)}</span></dd></div>
        <div><dt>Created</dt><dd>{hhmmss(b.creation_timestamp)}</dd></div>
        {b.assignment_error && <div><dt>Assignment</dt><dd className="word">{b.assignment_error}</dd></div>}
      </dl>
      <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: d.error, fetchedAt: d.fetchedAt }} />

      <Metrics>
        <Metric label="Rows retrieved" value={rc && (judged || rc.status === "pending") ? int(rc.served_distinct_rows) : "—"} tone={rc && (judged || rc.status === "pending") ? undefined : "absent"}
          help={rc && rc.total_rows > 0 ? `of ${int(rc.total_rows)} · ${int(rc.needed_rows)} needed` : "no reading completed"}
          title="Distinct rows retrieved at the reading, verified against the commitment; the tick is how many reconstruct the blob." />
        <Metric label="Validators served" value={rc && (judged || rc.status === "pending") ? int(rc.served_by_validators) : "—"} den={rc && (judged || rc.status === "pending") ? int(rc.assigned_validators) : undefined}
          tone={rc && (judged || rc.status === "pending") ? undefined : "absent"}
          help={rc && (judged || rc.status === "pending") ? `at ${pointName(rc.point)} · ${hhmm(rc.point_at)}${rc.status === "pending" ? ` · ${int(rc.probed_validators)} with a result` : ""}` : "no reading completed"} />
        <Metric label="Endorsed" value={signedKnown ? int(signedN) : "—"} den={signedKnown ? int(assignments.length) : undefined}
          tone={signedKnown ? undefined : "absent"}
          help={signedKnown ? "validators endorsed" : "signatures not recorded"}
          title="Assigned validators whose endorsement (signature) on the settled promise verified against their consensus key. The publisher stops collecting at two thirds of voting power, so about a third of the set is not endorsed on any blob." />
        <Metric label="Service window" value={winLen} help={`${hhmm(b.settlement_time).replace(" UTC", "")} → ${hhmm(b.must_serve_until)}${over ? " · over" : ""}`}
          title={`creation + max(payment_promise_timeout ${data.params.payment_promise_timeout_s} s, shard_retention ${data.params.shard_retention_s} s)`} />
        <Metric label="Fee" value={b.charge ? tia(b.charge.fee_utia) : "—"} tone={b.charge ? undefined : "absent"}
          help={b.charge ? `${int(b.charge.gas_units)} gas · ${b.charge.timed_out ? "timed out" : b.charge.settled ? "settled" : "pending"}` : "recorded before payments were kept"} />
      </Metrics>

      <section className="band">
        <div>
          <h2>Service window</h2>
          <p className="sub">Settled {hhmm(b.settlement_time)} → deadline {hhmm(b.must_serve_until)} ({winLen})</p>
          {order.length === 0 ? <p className="errs">{so ? <>Sampled out ({soText}): not probed at any point.</> : "No probe has run for this blob yet."}</p> : (
            <>
              <div className="tl" role="img" aria-label={`probe points: ${order.join(", ")}`}>
                <div className="axis" /><div className="win" style={{ left: 0, width: `${winShare}%` }} />
                <div className="cut" style={{ left: `${winShare}%` }} />
                <div className="edge l">settled {hhmm(b.settlement_time).replace(" UTC", "")}</div>
                {hasPost && <div className="edge r">after the deadline · stretched</div>}
                <div className={"cutlbl" + (winShare > 85 ? " end" : "")} style={{ left: `${winShare}%` }}>deadline {hhmm(b.must_serve_until).replace(" UTC", "")}</div>
                {order.map((k) => {
                  const at = byLabel.get(k)!.at, t = new Date(at).getTime(), ph = byLabel.get(k)!.phase;
                  return (
                    <span key={k}>
                      <div className={"lbl" + (k === lastIn && hasPost ? " up" : "")} style={{ left: x(t) }}>{k}</div>
                      <div className={"pt" + (ph === "grace" ? " grace" : ph === "post" ? " post" : "")} style={{ left: x(t) }} title={`${k} · ${utcWord(at)}`} />
                    </span>
                  );
                })}
              </div>
              <div className="ptcounts">{order.map(countLine)}</div>
            </>
          )}
        </div>
        <div>
          <h2>Rows retrieved</h2>
          {rc && rc.total_rows > 0 && (judged || rc.status === "pending") ? (
            <>
              <p className="sub">At {pointName(rc.point)} · {hhmm(rc.point_at)} · from {int(rc.served_by_validators)} validator{rc.served_by_validators === 1 ? "" : "s"}</p>
              <div className="meter" role="img" aria-label={`${int(rc.served_distinct_rows)} of ${int(rc.total_rows)} rows; ${int(rc.needed_rows)} needed`}>
                <i style={{ width: `${fill.toFixed(1)}%` }} /><div className="tick" style={{ left: `${tick.toFixed(1)}%` }} />
              </div>
              <div className="mnums"><span><b>{int(rc.served_distinct_rows)}</b> of {int(rc.total_rows)} rows</span><span>{int(rc.needed_rows)} needed to reconstruct</span></div>
              <p className="errs" title="The rows that came back at one reading, verified against the commitment; not an actual reconstruction.">
                {rc.status === "yes" && <><b>Retrievable.</b> Enough rows to reconstruct, and all {int(rc.attested_validators)} endorsing validators served.</>}
                {rc.status === "degraded" && <><b>Retrievable.</b> Enough rows to reconstruct; {int(rc.attested_validators - rc.served_by_attested)} of {int(rc.attested_validators)} endorsing validators did not serve.</>}
                {rc.status === "no" && <><b>Not retrievable</b>: not enough rows to reconstruct.</>}
                {rc.status === "pending" && <>{int(rc.probed_validators)} validators read so far; waiting for the rest.</>}
              </p>
            </>
          ) : <p className="errs">{state[2]}</p>}
        </div>
      </section>

      <section>
        <div className="vhead">
          <div><h2>Assigned validators</h2><p className="sub">{int(rows.length)} validators assigned rows of this blob</p></div>
          <div className="tools"><a className="dis" href={`${API_BASE}/v1/blobs/${b.promise_hash}`} title="the raw record, probe rows included">Probe rows →</a></div>
        </div>
        <div className="tablewrap">
          <table className={"marks" + (order.length ? "" : " nopts")}>
            <thead><tr>
              <th className="col-pin">Validator</th><th className="num">Voting power</th><th className="num">Rows</th><th>Endorsed</th><th>Host at settlement</th>
              {order.map((k) => <th key={k} className={"m" + (suspectAt.has(byLabel.get(k)!.at) ? " soft" : "")} title={`${k} · ${utcWord(byLabel.get(k)!.at)}${suspectAt.has(byLabel.get(k)!.at) ? " · not counted: the observer does not trust itself at this point" : ""}`}>{k}</th>)}
              <th className="go" />
            </tr></thead>
            <tbody>
              {rows.length === 0 && <tr className="empty"><td colSpan={6 + order.length}>No assignment recorded{b.assignment_error ? `: ${b.assignment_error}` : ""}.</td></tr>}
              {rows.map((a) => (
                <tr key={a.validator_address}>
                  <td className="id col-pin"><Link className="mon" href={`/validator/?addr=${a.validator_address}`}>{a.moniker || shortMid(a.validator_address, 12, 4)}</Link></td>
                  <td className="num">{int(a.voting_power)}</td>
                  <td className="num">{int(a.row_count)}</td>
                  <td title={a.attested === true ? "Signature verified against the consensus key: proof of storage." : a.attested === false ? "No verified signature on the settled promise: unproven, not absent. The publisher stops collecting at two thirds of voting power." : "Recorded before the observer verified signatures."}>{a.attested === true ? "yes" : a.attested === false ? <span className="soft">no</span> : "—"}</td>
                  <td className="mono soft">{a.host_at_settlement ? a.host_at_settlement : a.host_at_settlement === "" ? <span title="no endpoint registered when the promise settled">—</span> : <span className="sans" title="the registry could not be read at that height">not read</span>}</td>
                  {order.map((k) => {
                    const p = cell.get(a.validator_address + "|" + k);
                    const sus = suspectAt.has(byLabel.get(k)!.at);
                    const m = !p ? ["none", "no row"] : sus ? ["gone", `not counted, observer-side · filed as ${p.classification.toLowerCase().replace(/_/g, " ")}`] : probeMark(p);
                    return <td key={k} className="m"><span className={"mk " + m[0]} title={`${k} · ${m[1]}${p ? ` · ${utcWord(p.started_at)} · ${int(p.rows_returned)} / ${int(p.rows_expected)} rows · ${int(p.total_duration_ms)} ms${p.raw_error ? ` · ${p.raw_error}` : ""}` : ""}`} /></td>;
                  })}
                  <td className="go"><Link href={`/validator/?addr=${a.validator_address}`} aria-label={`open ${a.moniker || a.validator_address}`}>→</Link></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="mklegend">
          <span><span className="mk ok" /> served</span>
          <span title="The endorsed rows did not come back: not found, rows that do not verify, or, at the end reading, no answer, a rejected certificate, an error or no endpoint."><span className="mk fault" /> not served</span>
          <span title="No verified signature on the settled promise, so not rated either way: the publisher stops collecting at two thirds of stake."><span className="mk unsigned" /> not endorsed</span>
          <span title="At the earlier schedule's points: server error, unreachable, rate limited or a certificate problem, not counted either way."><span className="mk other" /> other</span>
          <span title="Expected gone after the window, not probed, or at a point where the observer does not trust itself. Never a fault."><span className="mk gone" /> not counted</span>
          <span><span className="mk none" /> no endpoint</span>
        </p>
      </section>
    </>
  );
}

export default function BlobPage() {
  return <Suspense fallback={<p className="crumb" style={{ paddingTop: 22 }}>Loading…</p>}><Page /></Suspense>;
}
