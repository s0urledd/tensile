"use client";
import { Suspense } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useApi, type Blob, type Probe, type Meta, int, bytes, tia, utcWord, hhmm, hhmmss, dur, shortMid, nsDisplay, notFound, pctOf, API_BASE } from "@/lib/api";
import StatusLine from "@/components/StatusLine";
import { Metric, Metrics } from "@/components/Metrics";
import Copy from "@/components/Copy";

type Assignment = {
  validator_address: string; moniker?: string; voting_power: number; row_count: number; attested: boolean | null; host_at_settlement: string | null;
  /** this validator's obligation on the blob, by the rule its counts use; absent when nothing is owed or no reading counts */
  service?: "served" | "not_served" | "in_retention_window" | "deadline_unverified" | "no_verdict" | "not_read";
  provisional?: boolean;
};
type Detail = {
  blob: Blob;
  params: { shard_retention_s: number; payment_promise_timeout_s: number };
  assignments: Assignment[] | null;
  probes: Probe[] | null;
  /** schedule points, by scheduled_at, the observer does not trust itself at: nothing there counts */
  suspect_points?: { at: string; label: string; reason: string }[] | null;
};

/** when the single end-of-window reading began (END_READ_SINCE on the observer) */
const END_READ_SINCE = "2026-09-27T16:20:28Z";

/** the service word and its mark */
const SERVICE: Record<string, [string, string, string]> = {
  served: ["ok", "Served", "The endorsed rows came back and verified against the commitment."],
  not_served: ["fault", "Not served", "The endorsed rows did not come back."],
  in_retention_window: ["none", "In retention window", "Read once, 10 minutes before the retention window ends."],
  deadline_unverified: ["gone", "Deadline unverified", "The retention deadline cannot be computed yet, so no verdict either way."],
  no_verdict: ["gone", "No verdict", "Read, but no reading near the end of the window counts either way."],
  not_read: ["gone", "Not read by Tensile", "Tensile did not read this shard. Nothing is counted either way."],
};

/** why a reading returned no rows, in a few words */
const REASON: Record<string, string> = {
  UNREACHABLE: "no answer", IDENTITY_MISMATCH: "wrong certificate", IDENTITY_EXPIRED: "certificate expired",
  SERVER_ERROR: "server error", THROTTLED: "rate limited", NOT_REGISTERED: "no endpoint",
};
const reasonOf = (p: Probe | undefined): string => {
  if (!p) return "";
  if (REASON[p.classification]) return REASON[p.classification];
  if (p.outcome === "NOT_FOUND") return "not found";
  if (p.outcome === "INVALID_ROWS") return "rows do not verify";
  return p.outcome.toLowerCase().replace(/_/g, " ");
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
  // the escrow owner, who paid; the transaction itself can be sent by anyone
  const pub = b.publisher || b.charge?.publisher || b.signer;
  const probes = data.probes ?? [];
  const assignments = data.assignments ?? [];
  // signatures are a fact of the settled promise, not of any probe: read them from the assignments
  const signedKnown = assignments.some((a) => a.attested != null);
  const signedN = assignments.filter((a) => a.attested === true).length;
  const stake = b.total_voting_power ? (b.attested_voting_power ?? 0) / b.total_voting_power : null;
  const suspectAt = new Set((data.suspect_points ?? []).map((sp) => sp.at));
  const rc = b.reconstructable;
  // drawn out of the sample: one record, with the probability it was drawn at
  const so = b.sampled_out;
  const judged = !!rc && (rc.status === "yes" || rc.status === "degraded" || rc.status === "no");
  const over = new Date(b.must_serve_until).getTime() <= Date.now();
  // Who served is read from the same service words the table shows, so the
  // sentence, the figures and the rows cannot disagree.
  const count = (s: string) => assignments.filter((a) => a.service === s).length;
  const served = count("served"), notServed = count("not_served"), noVerdict = count("no_verdict");
  const who = notServed > 0 ? `; ${int(notServed)} of ${int(signedN)} endorsing validators did not serve theirs.`
    : signedN > 0 && served === signedN ? ", and every endorsing validator served its rows."
    : noVerdict > 0 ? `; ${int(served)} of ${int(signedN)} endorsing validators served theirs, ${int(noVerdict)} without a verdict.`
    : ".";
  // Available: enough rows came back to reconstruct the blob ("some rows
  // were retrieved, but not enough to reconstruct" is the client's own word
  // for the other case). Whether every endorsing validator served is said
  // beside it, not folded into it.
  const state: [string, string, string] =
    rc?.status === "yes" || rc?.status === "degraded" ? ["ok", "Available", `Enough rows came back to reconstruct the blob${who}`]
    : rc?.status === "no" ? ["hold", "Unavailable", `Rows came back, but fewer than the ${int(rc.needed_rows)} needed to reconstruct the blob.`]
    : rc?.status === "pending" && !over ? ["none", "In retention window", "Read once, 10 minutes before the retention window ends."]
    : rc?.status === "pending" ? ["none", "Not read by Tensile", "Tensile was offline when this blob's reading was due, and the rows are pruned after the window. Nothing is counted for or against a validator."]
    : so ? ["none", "Sampled out", "Not read: the load policy of the time drew this blob out of its sample."]
    : !over ? ["none", "In retention window", "Read once, 10 minutes before the retention window ends."]
    : ["none", "Not read by Tensile", "No reading of this blob was completed."];

  // The reading behind each validator's word: the end reading, or on the
  // earlier schedule the newest reading inside the window, outside any point
  // the observer does not trust itself at.
  const reading = new Map<string, Probe>();
  for (const p of probes) {
    if (p.phase !== "in_window" || suspectAt.has(p.scheduled_at)) continue;
    const cur = reading.get(p.validator_address);
    const rank = (x: Probe) => (x.schedule_label === "end" ? "1" : "0") + x.started_at;
    if (!cur || rank(p) > rank(cur)) reading.set(p.validator_address, p);
  }
  const decided = served + notServed + noVerdict;
  // Blobs settled since the end reading began are read once, near the end;
  // earlier ones were read at several points of the window.
  const endRead = probes.some((p) => p.schedule_label === "end") || (probes.length === 0 && b.settlement_time >= END_READ_SINCE);
  const rows = [...assignments].sort((a, c) => c.voting_power - a.voting_power || a.validator_address.localeCompare(c.validator_address));
  const winLen = dur(b.settlement_time, b.must_serve_until);
  const fill = rc && rc.total_rows > 0 ? Math.min(100, rc.served_distinct_rows / rc.total_rows * 100) : 0;
  const tick = rc && rc.total_rows > 0 ? Math.min(100, rc.needed_rows / rc.total_rows * 100) : 0;
  // the rows of a reading still in progress, never of one the window closed on
  const shown = !!rc && rc.total_rows > 0 && (judged || (rc.status === "pending" && !over));

  return (
    <>
      <div className="head">
        <div>
          <p className="crumb"><Link href="/blobs/">Blobs</Link> › {b.promise_hash.slice(0, 10)}…</p>
          <h1>Blob <span className="mono">{shortMid(b.promise_hash, 10, 6)}</span><Copy text={b.promise_hash} label="promise hash" /></h1>
          <div className="chips">
            <span className="state" title={state[2]}><i className={"dot " + state[0]} />{state[1]}</span>
            <span title={utcWord(b.must_serve_until)}>{over ? <>Window over since <b className="word">{hhmm(b.must_serve_until)}</b></> : <>In window until <b className="word">{hhmm(b.must_serve_until)}</b></>}</span>
          </div>
        </div>
      </div>
      <dl className="facts">
        <div><dt>Publisher</dt><dd title={pub}><Link className="mono" href={`/publisher/?addr=${pub}`}>{shortMid(pub, 14, 6)}</Link></dd></div>
        <div><dt>Namespace</dt><dd title={b.namespace}><span className="mono">{nsDisplay(b.namespace)}</span><Copy text={b.namespace} label="namespace" /></dd></div>
        <div><dt>Commitment</dt><dd title={b.commitment}><span className="mono">{shortMid(b.commitment, 8, 6)}</span><Copy text={b.commitment} label="commitment" /></dd></div>
        <div><dt>Settled</dt><dd title={utcWord(b.settlement_time)}><span className="mono">#{int(b.settlement_height)}</span><span className="soft"> · {hhmm(b.settlement_time)}</span></dd></div>
        <div><dt>Created</dt><dd>{hhmmss(b.creation_timestamp)}</dd></div>
        {b.assignment_error && <div><dt>Assignment</dt><dd className="word">{b.assignment_error}</dd></div>}
      </dl>
      <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: d.error, fetchedAt: d.fetchedAt }} />

      <section className="group" id="chain">
        <div className="vhead"><div><h2>On chain</h2><p className="sub">Read from the chain, nothing measured.</p></div></div>
        <Metrics>
          <Metric label="Upload size" value={bytes(b.blob_size)} help="with padding, without parity" title="The size the blob paid for." />
          <Metric label="Fee paid" value={b.charge ? tia(b.charge.fee_utia) : "—"} tone={b.charge ? undefined : "absent"}
            help={b.charge ? `${int(b.charge.gas_units)} gas · ${b.charge.timed_out ? "timed out" : b.charge.settled ? "settled" : "not settled yet"}` : "recorded before payments were kept"}
            title="Charged to the publisher's escrow; not the settlement transaction's own fee." />
          <Metric label="Endorsements" value={stake != null ? pctOf(b.attested_voting_power ?? 0, b.total_voting_power ?? 0) : signedKnown ? int(signedN) : "—"}
            tone={stake != null || signedKnown ? undefined : "absent"}
            help={signedKnown ? `of voting power · ${int(signedN)} of ${int(assignments.length)} validators` : "signatures not recorded"}
            title="Voting power whose signature on the settlement verified. A settlement needs ⅔." />
          <Metric label="Retention window" value={winLen} help={`${hhmm(b.settlement_time).replace(" UTC", "")} → ${hhmm(b.must_serve_until)}${over ? " · over" : ""}`}
            title="How long the endorsing validators must serve the blob's rows." />
        </Metrics>
      </section>

      <section className="group" id="observed">
        <div className="vhead"><div><h2>Observed by Tensile</h2><p className="sub">{endRead ? "Each endorsed shard is read once, near the end of the retention window."
            : probes.length > 0 ? "Read on the earlier schedule, at several points in the retention window."
            : so ? "Not read: the load policy of the time drew this blob out of its sample."
            : "Not read by Tensile."}</p></div></div>
        <Metrics>
          <Metric label="Rows retrieved" value={shown ? int(rc!.served_distinct_rows) : "—"} tone={shown ? undefined : "absent"}
            help={shown ? `of ${int(rc!.total_rows)} · ${int(rc!.needed_rows)} needed${rc!.point_at ? ` · read ${hhmm(rc!.point_at)}` : ""}` : !over ? `read before ${hhmm(b.must_serve_until)}` : "no reading completed"}
            title="Distinct rows retrieved and verified against the commitment." />
          <Metric label="Served" value={decided > 0 ? int(served) : "—"} den={decided > 0 ? int(signedN) : undefined} tone={decided > 0 ? undefined : "absent"}
            help={decided > 0 ? (noVerdict > 0 ? `endorsing validators · ${int(noVerdict)} without a verdict` : "endorsing validators") : !over ? "read at the end of the window" : "no verdict"}
            title="Endorsing validators whose rows came back." />
          <Metric label="Not served" value={decided > 0 ? int(notServed) : "—"} tone={decided === 0 ? "absent" : notServed > 0 ? "fault" : undefined}
            help={decided > 0 ? (notServed > 0 ? "endorsed rows did not come back" : "none") : " "} title="Endorsing validators whose rows did not come back." />
        </Metrics>
        <div className="retrieved">
          {shown ? (
            <>
              <div className="meter" role="img" aria-label={`${int(rc!.served_distinct_rows)} of ${int(rc!.total_rows)} rows; ${int(rc!.needed_rows)} needed`}>
                <i style={{ width: `${fill.toFixed(1)}%` }} /><div className="tick" style={{ left: `${tick.toFixed(1)}%` }} />
              </div>
              <div className="mnums"><span><b>{int(rc!.served_distinct_rows)}</b> of {int(rc!.total_rows)} rows</span><span>{int(rc!.needed_rows)} needed to reconstruct</span></div>
            </>
          ) : null}
          <p className="errs"><b>{state[1]}.</b> {state[2]}</p>
        </div>
      </section>

      <section>
        <div className="vhead">
          <div><h2>Validators</h2><p className="sub">{int(rows.length)} validators assigned rows of this blob</p></div>
          <div className="tools"><a className="dis" href={`${API_BASE}/v1/blobs/${b.promise_hash}`} title="the raw record, every reading included">Readings in the API →</a></div>
        </div>
        <div className="tablewrap">
          <table className="marks">
            <thead><tr>
              <th className="col-pin">Validator</th><th className="num">Voting power</th><th className="num">Rows</th><th>Endorsed</th><th>Result</th><th>Host at settlement</th>
              <th className="go" />
            </tr></thead>
            <tbody>
              {rows.length === 0 && <tr className="empty"><td colSpan={7}>No assignment recorded{b.assignment_error ? `: ${b.assignment_error}` : ""}.</td></tr>}
              {rows.map((a) => {
                const p = reading.get(a.validator_address);
                // a blob the earlier load policy drew out of its sample was not read by design
                const sv = a.service === "not_read" && so ? ["gone", "Sampled out", "Not read: the load policy of the time drew this blob out of its sample."] : a.service ? SERVICE[a.service] : null;
                const reason = a.service === "not_served" ? reasonOf(p) : "";
                const word = sv ? `${sv[1]}${reason ? ` · ${reason}` : ""}${a.provisional ? " · provisional" : ""}` : "";
                const detail = p ? `${p.schedule_label === "end" ? "end reading" : `reading ${p.schedule_label}`} · ${utcWord(p.started_at)} · ${int(p.rows_returned)} / ${int(p.rows_expected)} rows · ${int(p.total_duration_ms)} ms${p.raw_error ? ` · ${p.raw_error}` : ""}` : "";
                return (
                  <tr key={a.validator_address} className={a.service === "not_served" ? "fault-row" : undefined}>
                    <td className="id col-pin"><Link className="mon" href={`/validator/?addr=${a.validator_address}`}>{a.moniker || shortMid(a.validator_address, 12, 4)}</Link></td>
                    <td className="num">{int(a.voting_power)}</td>
                    <td className="num">{int(a.row_count)}</td>
                    <td title={a.attested === true ? "Signature verified against the consensus key." : a.attested === false ? "No verified signature on the settlement: nothing owed. A settlement needs ⅔ of the voting power." : "Recorded before signatures were verified."}>{a.attested === true ? "yes" : a.attested === false ? <span className="soft">no</span> : "—"}</td>
                    <td title={[sv?.[2], detail].filter(Boolean).join(" · ") || (a.attested === false ? "Not endorsed: nothing owed." : "No reading that counts.")}>
                      {sv ? <><span className={"mk " + sv[0]} /> <span className={"word" + (sv[0] === "fault" ? " fault" : "")}>{word}</span></> : <span className="soft">—</span>}
                    </td>
                    <td className="mono soft">{a.host_at_settlement ? a.host_at_settlement : a.host_at_settlement === "" ? <span title="no endpoint registered when the promise settled">—</span> : <span className="sans" title="the registry could not be read at that height">not read</span>}</td>
                    <td className="go"><Link href={`/validator/?addr=${a.validator_address}`} aria-label={`open ${a.moniker || a.validator_address}`}>→</Link></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

export default function BlobPage() {
  return <Suspense fallback={<p className="crumb" style={{ paddingTop: 22 }}>Loading…</p>}><Page /></Suspense>;
}
