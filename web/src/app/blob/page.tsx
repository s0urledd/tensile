"use client";
import { Suspense, useState, type CSSProperties } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useApi, type Blob, type BlobReading, type Meta, int, bytes, tia, utcWord, hhmm, dur, shortMid, nsDisplay, notFound, pctOf, API_BASE } from "@/lib/api";
import StatusLine from "@/components/StatusLine";
import { PanelFig, Eye } from "@/components/Metrics";
import Copy from "@/components/Copy";
import Warn from "@/components/Warn";
import { unit } from "@/components/Unit";
import { Who } from "@/components/Ledger";
import { monthDayTime } from "@/components/BlobsDeck";
import { validatorHref } from "@/lib/addr";

type Assignment = {
  validator_address: string; moniker?: string; voting_power: number; row_count: number; attested: boolean | null; host_at_settlement: string | null;
  /** celestiavaloper1… from the staking set; absent when the chain names no validator at this consensus address */
  operator_address?: string;
  /** this validator's obligation on the blob, by the rule its counts use; absent when it counts neither way */
  service?: "served" | "not_served" | "in_retention_window" | "deadline_unverified";
  provisional?: boolean;
};
type Detail = {
  blob: Blob;
  params: { shard_retention_s: number; payment_promise_timeout_s: number };
  assignments: Assignment[] | null;
  probes: BlobReading[] | null;
};

/** when the single end-of-window reading began (END_READ_SINCE on the observer) */
const END_READ_SINCE = "2026-09-27T16:20:28Z";

/** the service word and what it means */
const SERVICE: Record<string, [string, string]> = {
  served: ["Served", "The endorsed rows came back and verified against the commitment."],
  not_served: ["Not served", "The endorsed rows did not come back, and the blob could not be reconstructed."],
  in_retention_window: ["In retention window", "Read once, 10 minutes before the retention window ends."],
  deadline_unverified: ["Deadline unverified", "The retention deadline cannot be computed yet, so no verdict either way."],
};

/** why a reading returned no rows, in a few words */
const REASON: Record<string, string> = {
  UNREACHABLE: "no answer", IDENTITY_MISMATCH: "wrong certificate", IDENTITY_EXPIRED: "certificate expired",
  SERVER_ERROR: "server error", THROTTLED: "rate limited", NOT_REGISTERED: "no endpoint",
};
const reasonOf = (p: BlobReading | undefined): string => {
  if (!p) return "";
  if (REASON[p.classification]) return REASON[p.classification];
  if (p.outcome === "NOT_FOUND") return "not found";
  if (p.outcome === "INVALID_ROWS") return "rows do not verify";
  return p.outcome.toLowerCase().replace(/_/g, " ");
};
/** a reading that failed on the validator's side: no rows, for a reason of its own (not Tensile's, not "not asked") */
const failed = (p: BlobReading | undefined): boolean =>
  !!p && p.outcome !== "SERVED_OK" && p.classification !== "NOT_PROBED" && p.classification !== "PROBE_ERROR" && p.classification !== "HEALTHY";

/** one validator's place on the blob: its mark, and the words a hover gives it */
type Mark = { tone: "ok" | "fault" | "hold" | ""; sign: string; word: string };
function markOf(a: Assignment, p: BlobReading | undefined, judged: boolean, available: boolean): Mark {
  if (a.service === "served") return { tone: "ok", sign: "✓", word: "Served: its rows came back and verified." };
  if (a.service === "not_served") return { tone: "fault", sign: "✗", word: `Not served${reasonOf(p) ? ` (${reasonOf(p)})` : ""}${a.provisional ? ", provisional" : ""}: its rows did not come back, and the blob could not be reconstructed.` };
  if (a.service === "in_retention_window") return { tone: "", sign: "", word: SERVICE.in_retention_window[1] };
  if (a.service === "deadline_unverified") return { tone: "", sign: "", word: SERVICE.deadline_unverified[1] };
  // counted neither way
  if (a.attested === false && p?.outcome === "SERVED_OK") return { tone: "ok", sign: "✓", word: "Not endorsed, so nothing owed; its rows came back and counted toward the blob." };
  if (failed(p)) return { tone: "hold", sign: "", word: `Its rows did not come back (${reasonOf(p)})${available ? "; not counted, the blob was available from the others" : "; counted neither way"}.` };
  if (a.attested === false) return { tone: "", sign: "", word: "Not endorsed: nothing owed." };
  if (!judged) return { tone: "", sign: "", word: "No reading that counts." };
  if (!p) return { tone: "", sign: "", word: "Not asked: the reading had enough rows before it reached this validator." };
  if (p.classification === "NOT_PROBED") return { tone: "", sign: "", word: "Tensile did not make this request in time: counted neither way." };
  if (p.classification === "PROBE_ERROR") return { tone: "", sign: "", word: "Tensile's request failed on its own side: counted neither way." };
  return { tone: "", sign: "", word: "Counted neither way." };
}

function Page() {
  const hash = useSearchParams().get("hash") ?? "";
  const [view, setView] = useState<"names" | "table">("names");
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
  const pub = b.publisher || b.signer;
  const probes = data.probes ?? [];
  const assignments = data.assignments ?? [];
  const rc = b.reconstructable;
  const judged = !!rc && (rc.status === "yes" || rc.status === "no");
  const available = rc?.status === "yes";
  const over = new Date(b.must_serve_until).getTime() <= Date.now();
  // Who served is read from the same service words the names carry, so the figures and the marks cannot disagree.
  const count = (s: string) => assignments.filter((a) => a.service === s).length;
  const served = count("served"), notServed = count("not_served");
  const asked = rc?.probed_validators ?? 0;
  // The client's result: Available, enough rows came back to reconstruct the blob; Unavailable, with its own error.
  const state: [string, string, string] =
    rc?.status === "yes" ? ["ok", "Available", `Enough rows came back to reconstruct the blob, from ${int(rc.served_by_validators)} of the ${int(asked)} validators asked.`]
    : rc?.status === "no" ? ["hold", "Unavailable", `${rc.error ? `${rc.error[0].toUpperCase()}${rc.error.slice(1)}: ` : ""}${rc.error === "no shards retrieved" ? "no rows came back" : `fewer than the ${int(rc.needed_rows)} rows needed came back`} from the ${int(asked)} validators asked.`]
    : !over ? ["none", "In retention window", "Read once, 10 minutes before the retention window ends."]
    : ["none", "Not read by Tensile", "Tensile did not read this blob: it missed the reading, its own network was down, or, before 27 September 2026, the load policy of the time did not draw it. Nothing is counted against a validator."];

  // The reading behind each validator's mark: the end reading, or on the earlier schedule the newest reading inside
  // the window.
  const reading = new Map<string, BlobReading>();
  for (const p of probes) {
    if (p.phase !== "in_window") continue;
    const cur = reading.get(p.validator_address);
    const rank = (x: BlobReading) => (x.schedule_label === "end" ? "1" : "0") + x.started_at;
    if (!cur || rank(p) > rank(cur)) reading.set(p.validator_address, p);
  }
  // Blobs settled since the end reading began are read once, near the end; earlier ones at several points of the window.
  const endRead = probes.some((p) => p.schedule_label === "end") || (probes.length === 0 && b.settlement_time >= END_READ_SINCE);
  const rows = [...assignments].sort((a, c) => c.voting_power - a.voting_power || a.validator_address.localeCompare(c.validator_address));
  const totalVp = rows.reduce((s, a) => s + a.voting_power, 0) || b.total_voting_power || 0;
  const share = (vp: number) => (totalVp > 0 ? vp / totalVp : 0);
  const marks = new Map(rows.map((a) => [a.validator_address, markOf(a, reading.get(a.validator_address), judged, available)]));
  // failures on the validators' side that the rule did not count, as the validator page names them
  const held = rows.filter((a) => marks.get(a.validator_address)!.tone === "hold");
  const heldWhy = [...held.reduce((m, a) => { const w = reasonOf(reading.get(a.validator_address)); return m.set(w, (m.get(w) ?? 0) + 1); }, new Map<string, number>())]
    .map(([w, n]) => `${w} (${int(n)})`).join(", ");
  const stake = b.total_voting_power ? (b.attested_voting_power ?? 0) / b.total_voting_power : null;
  const winLen = dur(b.settlement_time, b.must_serve_until);
  // the rows of a reading still in progress, never of one the window closed on
  const shown = !!rc && rc.total_rows > 0 && (judged || (rc.status === "pending" && !over));
  const fill = shown ? Math.min(100, rc!.served_distinct_rows / rc!.total_rows * 100) : 0;
  const tick = shown ? Math.min(100, rc!.needed_rows / rc!.total_rows * 100) : 0;

  return (
    <>
      <section className="pb-mast bd-mast">
        <p className="pb-kind">Blob</p>
        <div className="pb-id">
          <h1 className="pb-h1 bd-h1" title={b.promise_hash} aria-label={`Blob ${b.promise_hash}`}><span className="tl">{b.promise_hash.slice(0, 8)}</span><span className="dots" aria-hidden="true">•••</span><span className="tl">{b.promise_hash.slice(-6)}</span></h1>
        </div>
        <div className="pb-addr"><span className="mono">{b.promise_hash}</span><Copy text={b.promise_hash} label="the promise hash" /></div>
        <div className="chips bd-chips">
          <span className="state" title={state[2]}><i className={"dot " + state[0]} />{state[1]}</span>
          <span title={utcWord(b.settlement_time)}>Settled <b className="word">#{int(b.settlement_height)}</b></span>
          <span title={`${utcWord(b.settlement_time)} → ${utcWord(b.must_serve_until)}`}>{over ? <>Retention window over <b className="word">{hhmm(b.must_serve_until)}</b></> : <>In retention window until <b className="word">{hhmm(b.must_serve_until)}</b></>}</span>
        </div>

        {/* who paid, where it went and when: the publisher page's light frame of facts, one row each */}
        <dl className="pb-meta bd-meta">
          <dt>Publisher</dt>
          <dd>{pub ? <Who addr={pub} /> : "—"}{b.signer && pub && b.signer !== pub && <em title={`Sent by ${b.signer}; the escrow it settled from is the publisher's`}>sent by {b.signer.slice(0, b.signer.indexOf("1") + 1)}…{b.signer.slice(-4)}</em>}</dd>
          <dt>Namespace</dt>
          <dd><Link className="bd-ns" href={`/blobs/?namespace=${b.namespace}`} title={`${b.namespace} · every blob in it`}>{nsDisplay(b.namespace)}</Link><Copy text={b.namespace} label="namespace" /></dd>
          <dt>Commitment</dt>
          <dd title={b.commitment}><span className="mono">{shortMid(b.commitment, 10, 6)}</span><Copy text={b.commitment} label="commitment" /></dd>
          <dt>Settled</dt>
          <dd><b title={utcWord(b.settlement_time)}>{monthDayTime(b.settlement_time)}</b><em>UTC · height {int(b.settlement_height)}</em></dd>
          <dt>Created</dt>
          <dd><b title={utcWord(b.creation_timestamp)}>{monthDayTime(b.creation_timestamp)}</b><em>UTC</em></dd>
          <dt>Retention window</dt>
          <dd><b>{winLen}</b><em>until {hhmm(b.must_serve_until)}{over ? " · over" : ""}</em></dd>
          {b.assignment_error && <><dt>Assignment</dt><dd>{b.assignment_error}</dd></>}
        </dl>
      </section>
      <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: d.error, fetchedAt: d.fetchedAt }} />

      {/* the chain's figures beside Tensile's reading, in one frame: a label and a figure in each cell, the rest on hover */}
      <section className="pan bd-pan">
        <div className="bd-g" id="chain">
          <div className="vp-h"><h2 className="vp-t" title="Read from the chain, nothing measured.">On chain</h2></div>
          <dl className="vp-cells" style={{ "--n": 3 } as CSSProperties}>
            <PanelFig label="Blob size" value={unit(bytes(b.blob_size))} title="The size the blob paid for: Celestia's upload size, with header and padding, without parity." />
            <PanelFig label="Fee paid" value={b.charge ? unit(tia(b.charge.fee_utia)) : "—"} className={b.charge ? undefined : "na"}
              title={b.charge ? `${int(b.charge.gas_units)} gas · ${b.charge.timed_out ? "timed out" : b.charge.settled ? "settled" : "not settled yet"}. Charged to the publisher's escrow; not the settlement transaction's own fee.` : "Recorded before payments were kept."} />
            <PanelFig label="Endorsed" value={stake != null ? pctOf(b.attested_voting_power ?? 0, b.total_voting_power ?? 0) : "—"} className={stake != null ? undefined : "na"}
              title="Voting power whose signature on the settlement verified. A settlement needs ⅔." />
          </dl>
        </div>
        <div className="bd-g" id="observed">
          <div className="vp-h">
            <h2 className="vp-t" title={endRead ? "Read once, 10 minutes before the retention window ends, as celestia-app’s client downloads it: the validators in its order, until enough rows came back." : probes.length > 0 ? "Read on the earlier schedule, at several points in the retention window, and judged by the same rule." : "Not read by Tensile."}><Eye />Observed by Tensile</h2>
            {rc?.point_at ? <span className="vp-at" title={utcWord(rc.point_at)}>read {hhmm(rc.point_at)}</span> : !over && <span className="vp-at">reads before {hhmm(b.must_serve_until)}</span>}
          </div>
          <dl className="vp-cells" style={{ "--n": 3 } as CSSProperties}>
            <PanelFig label="Rows back" value={shown ? <>{int(rc!.served_distinct_rows)}<span className="u"> / {int(rc!.total_rows)}</span></> : "—"} className={shown ? undefined : "na"}
              title={shown ? `Distinct rows retrieved and verified against the commitment; ${int(rc!.needed_rows)} of the ${int(rc!.total_rows)} reconstruct the blob.` : "No reading completed."}>
              {shown && <dd className="bd-meter" aria-hidden="true"><i style={{ width: `${fill.toFixed(1)}%` }} /><span style={{ left: `${tick.toFixed(1)}%` }} /></dd>}
            </PanelFig>
            <PanelFig label="Served" value={judged ? int(served) : "—"} className={judged ? undefined : "na"}
              title={judged ? `Endorsing validators whose rows came back and verified; ${int(asked)} asked.` : "Read at the end of the retention window."} />
            <PanelFig label="Not served" className={!judged ? "na" : notServed > 0 ? "bad" : undefined}
              value={<>{judged ? int(notServed) : "—"}{judged && held.length > 0 && <Warn text={`${int(held.length)} validator${held.length === 1 ? "'s" : "s'"} rows did not come back: ${heldWhy}. ${available ? "Not counted: the blob was available from the others." : "Counted neither way."}`} />}</>}
              title="Endorsing validators whose rows did not come back from a blob that could not be reconstructed. On an available blob a validator that failed counts neither way." />
          </dl>
        </div>
      </section>

      {/* the validators assigned rows of the blob: by name, endorsed and not, Tensile's result marked on each, the
          voting power endorsed against the ⅔ a settlement needs; the table one switch away */}
      <section className="listing bd-vals" id="validators">
        <div className="list-head">
          <h2 className="bd-vh">Validators <span className="n">{int(rows.length)}</span></h2>
          <div className="lg-tools">
            {rows.length > 0 && (
              <div className="seg bd-seg" role="group" aria-label="show the validators as">
                <button type="button" aria-pressed={view === "names"} onClick={() => setView("names")}>Names</button>
                <button type="button" aria-pressed={view === "table"} onClick={() => setView("table")}>Table</button>
              </div>
            )}
            <a className="vd-api" href={`${API_BASE}/v1/blobs/${b.promise_hash}?rows=1`} title="This blob as JSON: every assignment, and every reading with the rows it returned">API →</a>
          </div>
        </div>
        {rows.length === 0
          ? <p className="bd-none">No assignment recorded{b.assignment_error ? `: ${b.assignment_error}` : ""}.</p>
          : view === "names"
            ? <Names rows={rows} marks={marks} reading={reading} share={share} stake={stake} />
            : <Table rows={rows} marks={marks} reading={reading} share={share} />}
      </section>
    </>
  );
}

type ListProps = { rows: Assignment[]; marks: Map<string, Mark>; reading: Map<string, BlobReading>; share: (vp: number) => number };
const nameOf = (a: Assignment) => a.moniker || (a.operator_address ? shortMid(a.operator_address, 18, 4) : shortMid(a.validator_address, 12, 4));
const pct = (f: number) => (f >= 0.0995 ? `${(f * 100).toFixed(1)}%` : f >= 0.001 ? `${(f * 100).toFixed(2)}%` : "<0.1%");

/** the validators by name in two groups, endorsed and not; the endorsed group carries the voting power bar and its ⅔ */
function Names({ rows, marks, reading, share, stake }: ListProps & { stake: number | null }) {
  const groups: [string, string, Assignment[]][] = [
    ["on", "Endorsed", rows.filter((a) => a.attested === true)],
    ["off", "Not endorsed", rows.filter((a) => a.attested === false)],
    ["un", "Signature not recorded", rows.filter((a) => a.attested == null)],
  ];
  return (
    <div className="bd-card">
      {groups.filter(([, , g]) => g.length > 0).map(([k, label, g]) => {
        const vp = g.reduce((s, a) => s + share(a.voting_power), 0);
        const f = k === "on" && stake != null ? stake : vp;
        return (
          <div key={k} className={`bd-grp ${k}`}>
            <div className="bd-gh">
              <h3><i className="pd" aria-hidden="true" />{label} <span className="n">{int(g.length)}</span></h3>
              <span className="bd-vp"><b>{(f * 100).toFixed(2)}%</b> of voting power</span>
            </div>
            {k === "on" && (
              <div className="bd-bar" role="img" aria-label={`${(f * 100).toFixed(2)}% of voting power endorsed; a settlement needs two thirds`}>
                <i style={{ width: `${Math.min(100, f * 100).toFixed(2)}%` }} /><span title="⅔ of the voting power, what a settlement needs" />
              </div>
            )}
            <ul className="bd-pills">
              {g.map((a) => {
                const m = marks.get(a.validator_address)!;
                const p = reading.get(a.validator_address);
                const tip = [`${nameOf(a)} · ${pct(share(a.voting_power))} of voting power · ${int(a.row_count)} rows`, m.word,
                  a.host_at_settlement ? `host ${a.host_at_settlement}` : "", p?.raw_error && m.tone !== "ok" ? p.raw_error : ""].filter(Boolean).join("\n");
                return (
                  <li key={a.validator_address}>
                    <Link className={`bd-pill${m.tone ? " " + m.tone : ""}`} href={validatorHref(a.operator_address, a.validator_address)} title={tip}>
                      <i className="pd" aria-hidden="true" /><span className="nm">{nameOf(a)}</span>{(m.sign || m.tone === "hold") && <span className="rk" aria-label={m.word}>{m.sign}</span>}
                    </Link>
                  </li>
                );
              })}
            </ul>
          </div>
        );
      })}
    </div>
  );
}

/** every assignment with its voting power, rows, endorsement, host and Tensile's result, for an operator's detail */
function Table({ rows, marks, reading, share }: ListProps) {
  return (
    <div className="lg-tw bd-tw">
      <table className="bd-t">
        <thead><tr><th>Validator</th><th>Voting power</th><th>Rows</th><th>Endorsed</th><th>Host at settlement</th><th className="tn"><span><Eye />Result</span></th></tr></thead>
        <tbody>
          {rows.map((a) => {
            const m = marks.get(a.validator_address)!;
            const p = reading.get(a.validator_address);
            const sv = a.service ? SERVICE[a.service] : null;
            const word = a.service === "not_served" ? `not served${reasonOf(p) ? ` · ${reasonOf(p)}` : ""}${a.provisional ? " · provisional" : ""}`
              : sv ? sv[0].toLowerCase() : m.tone === "ok" ? "served" : m.tone === "hold" ? reasonOf(p) : "—";
            const detail = p ? `${p.schedule_label === "end" ? "end reading" : `reading ${p.schedule_label}`} · ${utcWord(p.started_at)} · ${int(p.rows_returned)} / ${int(p.rows_expected)} rows · ${int(p.total_duration_ms)} ms${p.raw_error ? ` · ${p.raw_error}` : ""}` : "";
            return (
              <tr key={a.validator_address}>
                <td><Link className="bd-vn" href={validatorHref(a.operator_address, a.validator_address)}><i className={`pd ${a.attested === true ? "on" : a.attested === false ? "off" : "un"}`} aria-hidden="true" />{nameOf(a)}</Link></td>
                <td title={`${int(a.voting_power)} voting power`}>{pct(share(a.voting_power))}</td>
                <td>{int(a.row_count)}</td>
                <td title={a.attested === true ? "Signature verified against the consensus key." : a.attested === false ? "No verified signature on the settlement: nothing owed." : "Recorded before signatures were verified."}>{a.attested === true ? "yes" : a.attested === false ? <span className="q">no</span> : "—"}</td>
                <td className="mono">{a.host_at_settlement ? a.host_at_settlement : a.host_at_settlement === "" ? <span className="q" title="no endpoint registered when the promise settled">—</span> : <span className="q sans" title="the registry could not be read at that height">not read</span>}</td>
                <td className="tn"><span className={m.tone || "quiet"} title={[m.word, detail].filter(Boolean).join(" · ")}>{word}</span></td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default function BlobPage() {
  return <Suspense fallback={<p className="crumb" style={{ paddingTop: 22 }}>Loading…</p>}><Page /></Suspense>;
}
