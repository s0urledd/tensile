"use client";
import { Suspense, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useApi, type Blob, type BlobReading, type Meta, int, bytes, tia, utcWord, hhmm, dur, shortMid, nsDisplay, notFound, pctOf, API_BASE,
  endOfWindow, fullReading, ownGap, attemptsOf, judged as judgedBy, askedTimes, FULL_READ_SINCE, FULL_READ_SINCE_WORDS } from "@/lib/api";
import StatusLine from "@/components/StatusLine";
import { Eye } from "@/components/Metrics";
import Avatar from "@/components/Avatar";
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
  /**
   * this validator's obligation on the blob, by the rule its counts use; in_retention_window until the window closes,
   * even once the reading is in; absent when it counts neither way
   */
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
  not_served: ["Not served", "The validator's own rows did not come back, at the reading and each time it was asked again."],
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
  if (p.outcome === "PARTIAL") return "too few rows";
  return p.outcome.toLowerCase().replace(/_/g, " ");
};
/** one request's answer in a few words, for the list of a validator's requests */
const answerOf = (p: BlobReading): string =>
  p.service === "served" || p.outcome === "SERVED_OK" ? "served"
  : p.classification === "NOT_PROBED" ? "not made in time"
  : p.classification === "PROBE_ERROR" ? "Tensile's own error"
  : reasonOf(p);
/** a reading that failed on the validator's side: no rows, for a reason of its own (not Tensile's, not "not asked") */
const failed = (p: BlobReading | undefined): boolean =>
  !!p && p.outcome !== "SERVED_OK" && !ownGap(p.classification) && p.classification !== "HEALTHY";

/**
 * A validator's requests at the blob's reading: at a full reading the reading's own and any made again after an
 * answer that did not serve (the last carries the result and its reason); on the earlier schedule the newest reading
 * inside the window. full: the reading asked every endorser for its own rows (FULL_READ_SINCE).
 */
type Seat = { last: BlobReading; tries: BlobReading[]; full: boolean };
function seatsOf(probes: BlobReading[]): Map<string, Seat> {
  const by = new Map<string, BlobReading[]>();
  for (const p of probes) {
    if (p.phase !== "in_window") continue;
    by.set(p.validator_address, [...(by.get(p.validator_address) ?? []), p]);
  }
  const out = new Map<string, Seat>();
  for (const [v, rows] of by) {
    const ends = rows.filter((p) => endOfWindow(p.schedule_label));
    if (ends.length > 0) {
      const { last, tries } = attemptsOf(ends);
      out.set(v, { last, tries, full: fullReading(last.schedule_label, last.started_at) });
    } else {
      const last = rows.reduce((a, b) => (b.started_at > a.started_at ? b : a));
      out.set(v, { last, tries: [last], full: false });
    }
  }
  return out;
}

/**
 * What a validator's place on the blob counts as. Once the window has closed it is the record's word (the
 * assignment's service); while the window is open the record holds every endorser as in retention window, so a
 * validator the reading has answered for shows the reading's own result, from its requests' words, final when the
 * window closes.
 */
type Result = "served" | "not_served" | "in_retention_window" | "deadline_unverified" | "";
function resultOf(a: Assignment, s: Seat | undefined): { r: Result; early: boolean } {
  if (a.service === "in_retention_window" && s && endOfWindow(s.last.schedule_label)) return { r: judgedBy(s.tries), early: true };
  return { r: a.service ?? "", early: false };
}

/**
 * One validator's place on the blob: the words a hover gives it, the short word the table gives it, and a mark only
 * where something went wrong. Served carries no mark: it is what every endorser owes, so only what went wrong is
 * marked (and on the readings before a reading asked every endorser, most validators were never asked, and a mark on
 * the ones that were would have read as a mark against the rest).
 */
type Mark = { tone: "fault" | "hold" | ""; res: string; word: string; r: Result };
function markOf(a: Assignment, s: Seat | undefined, blob: { judged: boolean; available: boolean; full: boolean; closes: string }): Mark {
  const { r, early } = resultOf(a, s);
  const p = s?.last;
  const why = reasonOf(p);
  const n = s?.tries.length ?? 0;
  const times = n > 1 ? ` · ${askedTimes(n)}` : "";
  // every request of the validator, oldest first, when it was asked more than once
  const tries = n > 1 ? ` Asked ${int(n)} times: ${s!.tries.map(answerOf).join(", ")}.` : "";
  const final = early ? ` Final when the retention window closes at ${blob.closes}.` : "";
  const full = s?.full ?? blob.full;
  if (r === "served") return { r, tone: "", res: `served${times}`, word: `Served: its rows came back and verified.${tries}${final}` };
  if (r === "not_served") {
    return {
      r, tone: "fault", res: `not served${why ? ` · ${why}` : ""}${times}${a.provisional ? " · provisional" : ""}`,
      word: `Not served${why ? ` (${why})` : ""}${a.provisional ? ", provisional" : ""}: ${full ? "its own rows did not come back" : "its rows did not come back, and the blob could not be reconstructed"}.${tries}${final}`,
    };
  }
  if (r === "in_retention_window") return { r, tone: "", res: "in retention window", word: SERVICE.in_retention_window[1] };
  if (r === "deadline_unverified") return { r, tone: "", res: "deadline unverified", word: SERVICE.deadline_unverified[1] };
  // counted neither way
  if (a.attested === false && p?.outcome === "SERVED_OK") return { r, tone: "", res: "served", word: "Not endorsed, so nothing owed; its rows came back and counted toward the blob." };
  if (a.attested === false) return { r, tone: "", res: p || full ? "not endorsed" : "not asked", word: full ? "Not endorsed: nothing owed, so not asked." : "Not endorsed: nothing owed." };
  if (failed(p)) {
    const held = full
      ? (s!.tries.some((t) => ownGap(t.classification)) ? "; one of Tensile's own requests for it failed or was not made in time, so counted neither way"
        : !blob.judged ? "; no request of the reading reached a server, so counted neither way" : "; counted neither way")
      : blob.available ? "; not counted, the blob was available from the others" : "; counted neither way";
    return { r, tone: "hold", res: `${why}${times}`, word: `Asked, and its rows did not come back (${why})${held}.${tries}${final}` };
  }
  if (!blob.judged) return { r, tone: "", res: "—", word: "No reading that counts." };
  if (!p) return full
    ? { r, tone: "", res: "not read", word: "Tensile has no request to this validator on record at this reading: counted neither way." }
    : { r, tone: "", res: "not asked", word: "Not asked: the reading had enough rows before it reached this validator." };
  if (p.classification === "NOT_PROBED") return { r, tone: "", res: `not read${times}`, word: `Tensile could not make this request in time: counted neither way.${tries}${final}` };
  if (p.classification === "PROBE_ERROR") return { r, tone: "", res: `read failed${times}`, word: `Tensile's request failed on its own side: counted neither way.${tries}${final}` };
  return { r, tone: "", res: "—", word: "Counted neither way." };
}

function Page() {
  const hash = useSearchParams().get("hash") ?? "";
  const [table, setTable] = useState(false);
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  const d = useApi<Detail>(hash ? `/v1/blobs/${hash}` : null);
  // each validator's logo: the Keybase picture its operator set, which the validator list carries and a blob's assignments
  // do not; asked once, after the blob, so it never holds the page up (the pictures are the overview's, cached a day)
  const vl = useApi<{ validators: { address: string; avatar_url?: string }[] }>(hash && d.data ? "/v1/validators?window=24h" : null, 0);
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
  const asked = rc?.probed_validators ?? 0;
  // The client's result: Available, enough rows came back to reconstruct the blob; Unavailable, with its own error.
  const state: [string, string, string] =
    rc?.status === "yes" ? ["ok", "Available", `Enough rows came back to reconstruct the blob, from ${int(rc.served_by_validators)} of the ${int(asked)} validators asked.`]
    : rc?.status === "no" ? ["hold", "Unavailable", `${rc.error ? `${rc.error[0].toUpperCase()}${rc.error.slice(1)}: ` : ""}${rc.error === "no shards retrieved" ? "no rows came back" : `fewer than the ${int(rc.needed_rows)} rows needed came back`} from the ${int(asked)} validators asked.`]
    : !over ? ["none", "In retention window", "Read once, 10 minutes before the retention window ends."]
    : ["none", "Not read by Tensile", "Tensile did not read this blob: it missed the reading, its own network was down, or, before 27 September 2026, the load policy of the time did not draw it. Nothing is counted against a validator."];

  // Each validator's requests at the reading behind its mark: the end reading, its attempts with it, or on the earlier
  // schedule the newest reading inside the window.
  const seats = seatsOf(probes);
  // Blobs settled since the end reading began are read once, near the end; earlier ones at several points of the window.
  // A reading from FULL_READ_SINCE on asks every endorser for its own rows; one not made yet will, when it is due then.
  const endRead = probes.some((p) => endOfWindow(p.schedule_label)) || (probes.length === 0 && b.settlement_time >= END_READ_SINCE);
  const full = probes.length > 0 ? probes.some((p) => fullReading(p.schedule_label, p.started_at))
    : Date.parse(b.must_serve_until) - 10 * 60_000 >= Date.parse(FULL_READ_SINCE);
  const rows = [...assignments].sort((a, c) => c.voting_power - a.voting_power || a.validator_address.localeCompare(c.validator_address));
  const totalVp = rows.reduce((s, a) => s + a.voting_power, 0) || b.total_voting_power || 0;
  const share = (vp: number) => (totalVp > 0 ? vp / totalVp : 0);
  const closes = hhmm(b.must_serve_until);
  const marks = new Map(rows.map((a) => [a.validator_address, markOf(a, seats.get(a.validator_address), { judged, available, full, closes })]));
  // Who served is read from the same words the names carry, so the figures and the marks cannot disagree: the record's
  // once the window has closed, and while it is open the reading's own, as soon as it is in.
  const count = (s: Result) => rows.filter((a) => marks.get(a.validator_address)!.r === s).length;
  const served = count("served"), notServed = count("not_served");
  const early = judged && !over;
  const finalNote = `The reading is in; the record is final when the retention window closes at ${closes}${full ? ", and a validator that did not serve is asked again until then" : ""}.`;
  // failures on the validators' side that the rule did not count, as the validator page names them
  const held = rows.filter((a) => marks.get(a.validator_address)!.tone === "hold");
  const heldWhy = [...held.reduce((m, a) => { const w = reasonOf(seats.get(a.validator_address)?.last); return m.set(w, (m.get(w) ?? 0) + 1); }, new Map<string, number>())]
    .map(([w, n]) => `${w} (${int(n)})`).join(", ");
  const stake = b.total_voting_power ? (b.attested_voting_power ?? 0) / b.total_voting_power : null;
  const winLen = dur(b.settlement_time, b.must_serve_until);
  // the rows of a reading still in progress, never of one the window closed on
  const shown = !!rc && rc.total_rows > 0 && (judged || (rc.status === "pending" && !over));
  const avatars = new Map((vl.data?.validators ?? []).filter((v) => v.avatar_url).map((v) => [v.address, v.avatar_url!]));

  // who paid, where it went and when: the publisher page's light frame of facts, one row each
  const facts = (
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
  );
  const notServedDot = judged && held.length > 0 && <Warn text={`${int(held.length)} validator${held.length === 1 ? "'s" : "s'"} rows did not come back: ${heldWhy}. ${full ? `Counted neither way: one of Tensile's own requests for ${held.length === 1 ? "it" : "them"} failed or was not made in time.` : available ? "Not counted: the blob was available from the others." : "Counted neither way."}`} />;
  const readTitle = full ? "Read once, 10 minutes before the retention window ends: every validator that endorsed the blob is asked for its own rows, as celestia-app’s client asks for a shard, and one that did not serve is asked again, up to two more times, while the window is open."
    : endRead ? `Read once, 10 minutes before the retention window ends, as celestia-app’s client downloads it: the validators in its order, until enough rows came back. Readings before ${FULL_READ_SINCE_WORDS} were made this way, and keep the rule of their time.`
    : probes.length > 0 ? "Read on the earlier schedule, at several points in the retention window, and judged by the rule of its time." : "Not read by Tensile.";
  // the figures, in the facts' own rows: the chain's three, then Tensile's three, marked with its eye
  const figs = (
    <dl className="pb-meta bd-meta bd-figs">
      <dt>Blob size</dt><dd title="The size the blob paid for: Celestia's upload size, with header and padding, without parity."><b>{unit(bytes(b.blob_size))}</b></dd>
      <dt>Fee paid</dt><dd title="Charged to the publisher's escrow; not the settlement transaction's own fee.">{b.charge ? <><b>{unit(tia(b.charge.fee_utia))}</b><em>{b.charge.timed_out ? "timed out" : b.charge.settled ? "settled" : "not settled yet"}</em></> : <em>not recorded</em>}</dd>
      <dt>Endorsed</dt><dd title="Voting power whose signature on the settlement verified. A settlement needs ⅔.">{stake != null ? <><b>{pctOf(b.attested_voting_power ?? 0, b.total_voting_power ?? 0)}</b><em>of voting power</em></> : <em>not recorded</em>}</dd>
      <dt className="tz" title={readTitle}><Eye />Rows back</dt><dd title={shown ? `Distinct rows retrieved and verified against the commitment; ${int(rc!.needed_rows)} of the ${int(rc!.total_rows)} reconstruct the blob.` : undefined}>{shown ? <><b>{int(rc!.served_distinct_rows)}</b><em>of {int(rc!.total_rows)} · {int(rc!.needed_rows)} needed</em></> : <em>{!over ? `read before ${hhmm(b.must_serve_until)}` : "no reading"}</em>}</dd>
      <dt className="tz" title={readTitle}><Eye />Served</dt><dd title={judged ? `${full
        ? `Every validator that endorsed the blob is asked for its own rows: ${int(asked)} asked, and ${int(served)} served, their rows came back and verified.`
        : `This reading asked validators in the client's order until it had enough rows: it asked ${int(asked)}, and ${int(served)} endorsing validators' rows came back and verified. The rest were not asked.`}${early ? ` ${finalNote}` : ""}` : undefined}>{judged ? <><b>{int(served)}</b><em>of {int(asked)} asked</em></> : <em>—</em>}</dd>
      <dt className="tz" title={readTitle}><Eye />Not served</dt><dd title={`${full
        ? "Endorsing validators whose own rows did not come back, at the reading and each time they were asked again."
        : "Endorsing validators whose rows did not come back from a blob that could not be reconstructed. On an available blob a validator that failed counts neither way."}${early ? ` ${finalNote}` : ""}`}>{judged ? <><b className={notServed > 0 ? "bad" : undefined}>{int(notServed)}</b>{notServedDot}{rc?.point_at && <em title={early ? `${utcWord(rc.point_at)}. ${finalNote}` : utcWord(rc.point_at)}>read {hhmm(rc.point_at)}{early && <> · final {closes}</>}</em>}</> : <em>—</em>}</dd>
    </dl>
  );
  const sig = rows.length === 0
    ? <section className="bd-sig"><div className="bd-sh"><h2><Pen />Endorsements</h2></div><p className="bd-none">No assignment recorded{b.assignment_error ? `: ${b.assignment_error}` : ""}.</p></section>
    : <Signers rows={rows} marks={marks} share={share} stake={stake} avatars={avatars} table={table} onTable={() => setTable((t) => !t)} />;

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

      </section>
      <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: d.error, fetchedAt: d.fetchedAt }} />

      {/* who and where beside what it weighed and what Tensile found: two light frames of the same make and height */}
      <div className="bd-top">{facts}{figs}</div>
      {/* the endorsements under them at the page width, every validator shown, as a DA explorer sets a batch's signers */}
      <div className="bd-under" id="validators">{sig}</div>
      {table && rows.length > 0 && (
        <section className="bd-full" id="table">
          <div className="list-head">
            <h2 className="bd-vh">Validators <span className="n">{int(rows.length)}</span></h2>
            <a className="vd-api" href={`${API_BASE}/v1/blobs/${b.promise_hash}?rows=1`} title="This blob as JSON: every assignment, and every reading with the rows it returned">API →</a>
          </div>
          <Table rows={rows} marks={marks} seats={seats} share={share} />
        </section>
      )}
    </>
  );
}

type ListProps = { rows: Assignment[]; marks: Map<string, Mark>; share: (vp: number) => number };
type Logos = Map<string, string>;
const nameOf = (a: Assignment) => a.moniker || (a.operator_address ? shortMid(a.operator_address, 18, 4) : shortMid(a.validator_address, 12, 4));
const pct = (f: number) => (f >= 0.0995 ? `${(f * 100).toFixed(1)}%` : f >= 0.001 ? `${(f * 100).toFixed(2)}%` : "<0.1%");
/** the card's mark: a pen, as a signature */
const Pen = () => (
  <svg viewBox="0 0 16 16" width="15" height="15" aria-hidden="true"><path d="M10.6 2.6a1.6 1.6 0 0 1 2.3 2.3L6 11.8l-3 .8.8-3 6.8-7Z M9.4 3.8l2.3 2.3 M2.5 14h11" fill="none" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" /></svg>
);
/** a validator that did not endorse: a struck circle before its name */
const Ban = () => (
  <svg viewBox="0 0 12 12" width="11" height="11" aria-hidden="true"><circle cx="6" cy="6" r="4.6" fill="none" stroke="currentColor" strokeWidth="1.2" /><path d="M2.8 9.2l6.4-6.4" stroke="currentColor" strokeWidth="1.2" /></svg>
);

/**
 * The endorsements, set as a DA explorer sets a batch's signers: a band for the validators that endorsed and one for
 * those that did not, each a grid of small tiles (the logo, the name, its share of the voting power under it), every
 * validator shown. The voting power endorsed is in the figures above, so the card does not say it twice.
 */
function Signers({ rows, marks, share, stake, avatars, table, onTable }: ListProps & { stake: number | null; avatars: Logos; table: boolean; onTable: () => void }) {
  const on = rows.filter((a) => a.attested === true), off = rows.filter((a) => a.attested === false), un = rows.filter((a) => a.attested == null);
  const f = stake ?? on.reduce((s, a) => s + share(a.voting_power), 0);
  return (
    <section className="bd-sig">
      <div className="bd-sh">
        <h2><Pen />Endorsements</h2>
        <button type="button" className="bd-tbl" aria-pressed={table} onClick={onTable} aria-controls="table">Table</button>
      </div>
      <div className="bd-bands">
        <Band label="Endorsed" list={on} marks={marks} share={share} avatars={avatars} vp={f} />
        <Band label="Not endorsed" list={off} marks={marks} share={share} avatars={avatars} off vp={un.length === 0 && stake != null ? 1 - stake : undefined} />
        {un.length > 0 && <Band label="Signature not recorded" list={un} marks={marks} share={share} avatars={avatars} />}
      </div>
    </section>
  );
}

function Band({ label, list, marks, share, avatars, off, vp: given }: { label: string; list: Assignment[]; marks: Map<string, Mark>; share: (vp: number) => number; avatars: Logos; off?: boolean; vp?: number }) {
  if (list.length === 0) return null;
  const vp = given ?? list.reduce((s, a) => s + share(a.voting_power), 0);
  return (
    <div className={"bd-band" + (off ? " off" : "")}>
      <div className="bd-bh"><span>{label} · {int(list.length)}</span><span>{(vp * 100).toFixed(2)}%</span></div>
      <ul className="bd-tiles">
        {list.map((a) => {
          const m = marks.get(a.validator_address)!;
          return (
            <li key={a.validator_address}>
              <Link className={"bd-tile" + (m.tone ? " " + m.tone : "")} href={validatorHref(a.operator_address, a.validator_address)}
                title={[`${nameOf(a)} · ${pct(share(a.voting_power))} of voting power · ${int(a.row_count)} rows`, m.word, a.host_at_settlement ? `host ${a.host_at_settlement}` : ""].filter(Boolean).join("\n")}>
                {/* a logo for the validators that endorsed; the rest carry the struck circle alone, and their pictures are not fetched */}
                {!off && <Avatar v={{ avatar_url: avatars.get(a.validator_address), moniker: a.moniker, address: a.validator_address }} />}
                <span className="tx">
                  <span className="nm">{off && <Ban />}<span className="t">{nameOf(a)}</span>{m.tone && <i className="mk" aria-label={m.res} />}</span>
                  <span className="vp">{pct(share(a.voting_power))}</span>
                </span>
              </Link>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

/**
 * every assignment with its voting power, rows, endorsement, host and Tensile's result, for an operator's detail: one
 * row per validator, its result and reason from its last request, and every request in the hover
 */
function Table({ rows, marks, seats, share }: ListProps & { seats: Map<string, Seat> }) {
  return (
    <div className="lg-tw bd-tw">
      <table className="bd-t">
        <thead><tr><th>Validator</th><th>Voting power</th><th>Rows</th><th>Endorsed</th><th>Host at settlement</th><th className="tn"><span><Eye />Result</span></th></tr></thead>
        <tbody>
          {rows.map((a) => {
            const m = marks.get(a.validator_address)!;
            const s = seats.get(a.validator_address);
            const p = s?.last;
            const detail = p ? `${endOfWindow(p.schedule_label) ? "end reading" : `reading ${p.schedule_label}`}${s!.tries.length > 1 ? ` · last of ${int(s!.tries.length)} requests` : ""} · ${utcWord(p.started_at)} · ${int(p.rows_returned)} / ${int(p.rows_expected)} rows · ${int(p.total_duration_ms)} ms${p.raw_error ? ` · ${p.raw_error}` : ""}` : "";
            return (
              <tr key={a.validator_address}>
                <td><Link className="bd-vn" href={validatorHref(a.operator_address, a.validator_address)}>{nameOf(a)}</Link></td>
                <td title={`${int(a.voting_power)} voting power`}>{pct(share(a.voting_power))}</td>
                <td>{int(a.row_count)}</td>
                <td title={a.attested === true ? "Signature verified against the consensus key." : a.attested === false ? "No verified signature on the settlement: nothing owed." : "Recorded before signatures were verified."}>{a.attested === true ? "yes" : a.attested === false ? <span className="q">no</span> : "—"}</td>
                <td className="mono">{a.host_at_settlement ? a.host_at_settlement : a.host_at_settlement === "" ? <span className="q" title="no endpoint registered when the promise settled">—</span> : <span className="q sans" title="the registry could not be read at that height">not read</span>}</td>
                <td className="tn"><span className={m.tone || (m.res.startsWith("served") ? "sv" : "quiet")} title={[m.word, detail].filter(Boolean).join(" · ")}>{m.res}</span></td>
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
