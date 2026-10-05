"use client";
import { Suspense, useEffect, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useApi, askAgain, useNewestBlob, type Blob, type BlobReading, type Meta, int, bytes, tia, utcWord, hhmm, shortMid, nsDisplay, notFound, pctOf, API_BASE,
  endOfWindow, fullReading, ownGap, ownSide, sharedAnswer, rawErrorWords, foreignRows, asksAgain, attemptsOf, judged as judgedBy, askedTimes, FULL_READ_SINCE } from "@/lib/api";
import StatusLine from "@/components/StatusLine";
import { Eye } from "@/components/Metrics";
import Copy from "@/components/Copy";
import Warn from "@/components/Warn";
import { unit } from "@/components/Unit";
import { Who } from "@/components/Ledger";
import { monthDayTime } from "@/components/BlobsDeck";
import { validatorHref } from "@/lib/addr";
import { blobKey, blobIdOf, type BlobKey } from "@/lib/blobkey";
import { useFind } from "@/lib/blobfind";

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

/**
 * A blob opened by an identifier other than its promise hash: ?id= the client's blob ID, which is its commitment behind
 * a version byte, or ?tx= the hash of the transaction that settled it. One settlement opens here; several open the
 * Blobs list of them. bad is an identifier that is none.
 */
type Via = { kind: "id" | "tx"; text: string; key: BlobKey } | { kind: "bad"; of: "id" | "tx"; text: string };
function viaOf(id: string | null, tx: string | null): Via | null {
  if (id != null) {
    const k = blobKey(id);
    return k?.kind === "id" ? { kind: "id", text: k.id, key: k } : { kind: "bad", of: "id", text: id };
  }
  if (tx != null) {
    const k = blobKey(tx);
    return k?.kind === "hash" ? { kind: "tx", text: k.hex.toUpperCase(), key: k } : { kind: "bad", of: "tx", text: tx };
  }
  return null;
}

/** when the single end-of-window reading began (END_READ_SINCE on the observer) */
const END_READ_SINCE = "2026-09-27T16:20:28Z";

/** the service words a mark takes as they are, and what they mean (served and not served are worded by markOf) */
const SERVICE: Record<string, [string, string]> = {
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
  if (foreignRows(p)) return "other rows of the blob";
  if (p.outcome === "PARTIAL") return "too few rows";
  return p.outcome.toLowerCase().replace(/_/g, " ");
};
/**
 * one request's answer in a few words, for the list of a validator's requests; in full (the hover's list), a later
 * attempt answered by another request to the same endpoint, which failed before any blob was asked for, says so (the
 * table's cell keeps to the answer, which is the same)
 */
const answerOf = (p: BlobReading, full = false): string => {
  if (p.service === "served" || p.outcome === "SERVED_OK") return "served";
  if (p.classification === "NOT_PROBED") return "not made in time";
  if (p.classification === "PROBE_ERROR") return `Tensile's own ${ownSide(p.raw_error) || "error"}`;
  return full && sharedAnswer(p.raw_error) ? `same answer as its request for another blob at ${hhmm(p.started_at)}: ${reasonOf(p)}` : reasonOf(p);
};
/** answers in order, a run of the same one written once with its count: "not found ×2, no answer" */
const inOrder = (ws: string[]): string =>
  ws.reduce<[string, number][]>((a, w) => { const l = a[a.length - 1]; if (l && l[0] === w) l[1]++; else a.push([w, 1]); return a; }, [])
    .map(([w, k]) => (k > 1 ? `${w} ×${k}` : w)).join(", ");
/** a short answer, or other rows, of the blob that verified and are no settled promise's set (UNMATCHED_GENUINE) */
const unmatched = (p: BlobReading): boolean => p.classification === "UNMATCHED_GENUINE" && (p.outcome === "WRONG_ROWS" || p.outcome === "PARTIAL");
/** a reading that failed on the validator's side: no rows, for a reason of its own (not Tensile's, not "not asked") */
const failed = (p: BlobReading | undefined): boolean =>
  !!p && p.outcome !== "SERVED_OK" && !ownGap(p.classification) && p.classification !== "HEALTHY";

/**
 * A validator's requests at the blob's reading: at a full reading the reading's own and any made again after an
 * answer that did not serve; on the earlier schedule the newest reading inside the window. said is the last request
 * that reached the validator, which gives its reason (one Tensile could not make in time, or that failed on its own
 * side, never speaks for it), and made how many did. full: the reading asked every endorser for its own rows
 * (FULL_READ_SINCE).
 */
type Seat = { last: BlobReading; said: BlobReading; tries: BlobReading[]; made: number; full: boolean };
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
      const made = tries.filter((t) => !ownGap(t.classification));
      out.set(v, { last, said: made[made.length - 1] ?? last, tries, made: made.length, full: fullReading(last.schedule_label, last.started_at) });
    } else {
      const last = rows.reduce((a, b) => (b.started_at > a.started_at ? b : a));
      out.set(v, { last, said: last, tries: [last], made: ownGap(last.classification) ? 0 : 1, full: false });
    }
  }
  return out;
}

/**
 * What a validator's place on the blob counts as. Once the window has closed it is the record's word (the
 * assignment's service); while the window is open the record holds every endorser as in retention window, so a
 * validator the reading has answered for shows the reading's own result, from its requests' words, final when the
 * window closes. An answer with no word and no gap beside it (a deadline held, a reading that reached no server, an
 * attempt still to come) keeps the record's word until then; a gap is one of Tensile's own, or rows of the blob that
 * are not the validator's own.
 */
type Result = "served" | "not_served" | "in_retention_window" | "deadline_unverified" | "";
function resultOf(a: Assignment, s: Seat | undefined): { r: Result; early: boolean } {
  if (a.service === "in_retention_window" && s && endOfWindow(s.last.schedule_label)) {
    const j = judgedBy(s.tries);
    if (j || s.tries.some((t) => ownGap(t.classification) || (s.full && foreignRows(t)))) return { r: j, early: true };
  }
  return { r: a.service ?? "", early: false };
}

/**
 * Why a validator whose own rows did not come back at a full reading counts neither way, when the record does not say
 * it was Tensile's own gap: the reading reached no server; Tensile still owed it a later attempt that is not on record
 * (its last answer carries next_attempt_due); or rows of the blob came back that are not its own (a short answer that
 * was all its own rows would have counted as not served, so here it was not).
 */
type Cause = "own" | "none" | "owed" | "foreign" | "";
function causeOf(s: Seat, blob: BlobSide): Cause {
  if (s.tries.some((t) => ownGap(t.classification))) return "own";
  if (!blob.judged) return "none";
  if (s.tries.some(foreignRows)) return "foreign";
  if (s.last.next_attempt_due) return "owed";
  if (s.tries.some(unmatched)) return "foreign";
  return "";
}
/** a cause in words, after "counted neither way:" (one validator) */
const CAUSE: Record<Exclude<Cause, "">, string> = {
  own: "a request failed on Tensile's side or was not made in time",
  none: "no request of the reading reached a server",
  owed: "Tensile still owed it a request that is not on record",
  foreign: "rows of the blob came back that are not its own",
};
/** the same, short, for the figures' note, which counts them over every validator */
const CAUSE_SHORT: Record<Exclude<Cause, "">, string> = {
  own: "a request failed on Tensile's side or was not made in time",
  none: "no request of the reading reached a server",
  owed: "Tensile still owed a request that is not on record",
  foreign: "rows of the blob came back that are not the validator's own",
};

/**
 * One validator's place on the blob: the words a hover gives it, the short word a mark and the table give it, and a
 * mark only where something went wrong. Served carries no mark: it is what every endorser owes, so only what went
 * wrong is marked (and on the readings before a reading asked every endorser, most validators were never asked, and a
 * mark on the ones that were would have read as a mark against the rest). The table words a validator asked more than
 * once by each request's answer in turn, so the attempts read without a hover.
 */
type Mark = { tone: "fault" | "hold" | ""; res: string; tab: string; word: string; r: Result; cause?: Cause };
type BlobSide = { judged: boolean; available: boolean; full: boolean; retried: boolean; closes: string; until: number };
function markOf(a: Assignment, s: Seat | undefined, blob: BlobSide): Mark {
  const { r, early } = resultOf(a, s);
  // the validator's own last answer gives the reason, and only the requests that reached it count as asked
  const p = s?.said;
  const full = s?.full ?? blob.full;
  // at a full reading, why a validator whose own rows did not come back counts neither way
  const cause = full && s && r === "" && failed(p) ? causeOf(s, blob) : "";
  const why = cause === "foreign" ? "other rows of the blob" : reasonOf(p);
  const n = s?.made ?? 0;
  const times = n > 1 ? ` · ${askedTimes(n)}` : "";
  // every request of the validator, oldest first, when there were several, Tensile's own gaps among them; a run of
  // the same answer once, with its count, in the cell, and each in the hover
  const seq = s && s.tries.length > 1 ? inOrder(s.tries.map((t) => answerOf(t))) : "";
  const tries = seq ? ` Requests in order: ${s!.tries.map((t) => answerOf(t, true)).join(", ")}.` : "";
  // while the window is open, a validator whose last answer still owes it another request: its result can change
  const again = early && s && asksAgain(s.last, blob.until) ? "; it is asked again before then" : "";
  const final = early ? ` Final when the retention window closes at ${blob.closes}${again}.` : "";
  const prov = a.provisional ? " · provisional" : "";
  if (r === "served") return { r, tone: "", res: `served${times}`, tab: seq ? `served · ${seq}` : "served", word: `Served: its ${full ? "own " : ""}rows came back and verified.${tries}${final}` };
  if (r === "not_served") {
    return {
      r, tone: "fault", res: `not served${why ? ` · ${why}` : ""}${times}${prov}`, tab: seq ? `not served · ${seq}${prov}` : `not served${why ? ` · ${why}` : ""}${prov}`,
      word: `Not served${why ? ` (${why})` : ""}${a.provisional ? ", provisional" : ""}: ${full ? (p?.outcome === "PARTIAL" ? "fewer of its own rows came back than it holds" : "its own rows did not come back") : "its rows did not come back, and the blob could not be reconstructed"}.${tries}${final}`,
    };
  }
  const one = (res: string, word: string, tone: Mark["tone"] = ""): Mark => ({ r, tone, res, tab: res, word });
  if (r === "in_retention_window" && s && full && failed(p) && asksAgain(s.last, blob.until)) {
    return one("in retention window", `Its own rows have not come back yet (${why}); asked again before the retention window closes at ${blob.closes}.${tries}`);
  }
  if (r === "in_retention_window") return one("in retention window", SERVICE.in_retention_window[1]);
  if (r === "deadline_unverified") return one("deadline unverified", SERVICE.deadline_unverified[1]);
  // counted neither way
  // a validator that did not endorse owes nothing: whatever an earlier reading got from it, it is not served or not
  if (a.attested === false) return one("not endorsed", full ? "Not endorsed: nothing owed, so not asked." : "Not endorsed: nothing owed, so not judged.");
  if (failed(p)) {
    const held = full
      ? (cause ? `; counted neither way: ${CAUSE[cause]}` : "; counted neither way")
      : blob.available ? "; not counted, the blob was available from the others" : "; counted neither way";
    const word = cause === "foreign"
      ? "Rows of the blob came back that are not its own, so counted neither way."
      : `Its ${full ? "own " : ""}rows did not come back (${why})${held}.`;
    return { r, tone: "hold", cause, res: `${why}${times}`, tab: seq || why, word: `${word}${tries}${final}` };
  }
  if (!blob.judged) return one("—", "No reading that counts.");
  if (!p) return full
    ? one("not read", "No request to this validator on record at this reading: counted neither way.")
    : one("not asked", "Not asked: the reading had enough rows before it reached this validator.");
  const gap = (res: string, word: string): Mark => ({ r, tone: "", res, tab: seq ? `${res} · ${seq}` : res, word: `${word}${tries}${final}` });
  if (p.classification === "NOT_PROBED") return gap("not read", "Tensile could not make this request in time: counted neither way.");
  if (p.classification === "PROBE_ERROR") {
    const side = ownSide(p.raw_error);
    return gap("read failed", side === "network" ? "Tensile's own network was down for this request: counted neither way."
      : side === "resolver" ? "Tensile's own resolver was too slow for this request: counted neither way."
      : side === "clock" ? "Whether the certificate had lapsed rests on Tensile's own clock: counted neither way."
      : "Tensile's request failed on its own side: counted neither way.");
  }
  return one("—", "Counted neither way.");
}

function Page() {
  const sp = useSearchParams();
  const named = sp.get("hash") ?? "";
  // the address reads a raw + as a space, which base64 never holds: each goes back before blobKey trims a last one away
  const via = named ? null : viaOf(sp.get("id")?.replace(/ /g, "+") ?? null, sp.get("tx"));
  // the newest blob on record, from the header's tip stream (no request of its own): a blob not on record yet is
  // asked for again each time it changes, and every 30 s besides
  const newest = useNewestBlob();
  // its settlements, asked again while there are none (a blob the scanner has not read yet)
  const hit = useFind(via && via.kind !== "bad" ? via.key : null, { as: [via?.kind === "tx" ? "tx" : "commitment"], limit: 1, refreshMs: 30000, retryOn: newest });
  const picked = hit && !hit.error && !hit.partial && hit.total === 1 ? hit.rows[0].promise_hash : "";
  const many = !!hit && !hit.error && hit.total > 1;
  const router = useRouter();
  useEffect(() => {
    if (!via || via.kind === "bad") return;
    if (picked) {
      // the address names the blob as every other link to it does
      try {
        const u = new URL(window.location.href);
        u.search = `?hash=${picked}`;
        window.history.replaceState(null, "", u.pathname + u.search + u.hash);
      } catch { /* the address keeps the identifier; the page shows the blob all the same */ }
    } else if (many) {
      // several settlements: never the newest of them silently, but the list of them all
      router.replace(`/blobs/?blob=${encodeURIComponent(via.kind === "id" ? via.text : via.key.hex)}`);
    }
  }, [picked, many]); // eslint-disable-line react-hooks/exhaustive-deps
  const hash = named || picked;
  const [table, setTable] = useState(false);
  const { data: meta, error: metaErr } = useApi<Meta>("/v1/meta");
  const d = useApi<Detail>(hash ? `/v1/blobs/${hash}` : null);
  // every settlement of the same blob ID (its version, 0, and the commitment): the head says so when there is more than
  // this one
  const same = useApi<{ total: number }>(d.data ? `/v1/blobs?commitment=${d.data.blob.commitment}&limit=1` : null);
  // a promise hash not on record yet (404) is asked for again as soon as a newer blob is, not at the next 30 s
  const missing = !!hash && notFound(d);
  useEffect(() => {
    if (missing) askAgain(`/v1/blobs/${hash}`);
  }, [newest]); // eslint-disable-line react-hooks/exhaustive-deps
  if (!hash && !via) return <p className="notice">Open a blob from the <Link href="/blobs/">list</Link>, or add <code>?hash=&lt;promise hash&gt;</code>, <code>?id=&lt;blob ID&gt;</code> or <code>?tx=&lt;transaction hash&gt;</code> to the address.</p>;
  if (!hash && via) {
    const name = (via.kind === "bad" ? via.of : via.kind) === "id" ? "blob ID" : "transaction hash";
    // nothing on record: Tensile has not indexed it yet, or the transaction carries no Fibre blob
    const none = !!hit && !hit.error && hit.total === 0;
    // an observer that cannot look a transaction up has not said the blob is missing: its heading claims nothing
    const unasked = none && via.kind === "tx" && hit.noTx;
    return (
      <>
        <div className="head"><div><p className="crumb"><Link href="/blobs/">Blobs</Link> › {via.text.slice(0, 10)}…</p><h1>{none && !unasked ? "Not indexed yet" : unasked || via.kind === "bad" || hit?.error ? "Blob" : "Loading…"}</h1></div></div>
        <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: hit?.error ?? null, fetchedAt: hit && !hit.error ? hit.at : null }} />
        {via.kind === "bad" && <p className="notice"><span className="mono">{shortMid(via.text, 10, 6)}</span> is not a {name}: {via.of === "id" ? "the client's blob ID is a version byte, 0, then the 32-byte commitment, in base64, or 66 hex characters" : "one is 64 hex characters"}.</p>}
        {none && (via.kind === "tx"
          ? hit.noTx
            ? <p className="notice">This observer does not look blobs up by transaction hash yet. Open the blob from the <Link href="/blobs/">list</Link> or by its blob ID.</p>
            : <p className="notice">Tensile has not indexed <span className="mono" title={via.text}>{shortMid(via.text, 10, 6)}</span> yet, or the transaction carries no Fibre blob. A blob appears here once Tensile has read the block that settled it; this page checks again each time Tensile records a new blob, and every 30 seconds.</p>
          : <p className="notice">Tensile has not indexed a blob with the blob ID <span className="mono" title={via.text}>{shortMid(via.text, 10, 6)}</span> yet. A blob appears here once Tensile has read the block that settled it; this page checks again each time Tensile records a new blob, and every 30 seconds.</p>)}
      </>
    );
  }
  const data = d.data;
  if (!data) {
    return (
      <>
        <div className="head"><div><p className="crumb"><Link href="/blobs/">Blobs</Link> › {hash.slice(0, 10)}…</p><h1>{notFound(d) ? "Blob not recorded yet" : d.error ? "Blob" : "Loading…"}</h1></div></div>
        <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: d.error, fetchedAt: d.fetchedAt, status: d.status }} />
        {notFound(d) && <p className="notice">No publication with the promise hash <span className="mono">{shortMid(hash, 10, 6)}</span> is on record. A blob appears here once the scanner has read the block that settled it; this page checks again each time Tensile records a new blob, and every 30 seconds.</p>}
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
  // Each validator's requests at the reading behind its mark: the end reading, its attempts with it, or on the earlier
  // schedule the newest reading inside the window.
  const seats = seatsOf(probes);
  // the endorsers the reading asked: an earlier reading in the client's order also asked validators that had not
  // endorsed, which owe nothing and are not counted here
  const asked = assignments.filter((a) => a.attested === true && seats.has(a.validator_address)).length;
  // Blobs settled since the end reading began are read once, near the end; earlier ones at several points of the window.
  // A reading from FULL_READ_SINCE on asks every endorser for its own rows; one not made yet will, when it is due then.
  const endRead = probes.some((p) => endOfWindow(p.schedule_label)) || (probes.length === 0 && b.settlement_time >= END_READ_SINCE);
  const full = probes.length > 0 ? probes.some((p) => fullReading(p.schedule_label, p.started_at))
    : Date.parse(b.must_serve_until) - 10 * 60_000 >= Date.parse(FULL_READ_SINCE);
  // a validator that did not serve is asked again at readings labelled full; the first full readings, labelled end,
  // asked each validator once
  const retried = probes.length > 0 ? probes.some((p) => p.schedule_label === "full") : full;
  // a reading made since full readings that stopped at enough rows: judged by the earlier rule
  const enough = !full && probes.some((p) => p.schedule_label === "enough");
  const rows = [...assignments].sort((a, c) => c.voting_power - a.voting_power || a.validator_address.localeCompare(c.validator_address));
  // an earlier end reading that asked every endorser once, and no other validator (the first end readings, from
  // 27 September 2026); the others asked validators in the client's order until the rows were enough
  const everyEndorser = endRead && !full && !enough && rows.some((a) => a.attested === true)
    && rows.every((a) => a.attested !== true || seats.has(a.validator_address))
    && !rows.some((a) => a.attested === false && seats.has(a.validator_address));
  const totalVp = rows.reduce((s, a) => s + a.voting_power, 0) || b.total_voting_power || 0;
  const share = (vp: number) => (totalVp > 0 ? vp / totalVp : 0);
  const closes = hhmm(b.must_serve_until);
  // A full reading judges each endorser on its own answers, whatever the blob came to: one Tensile missed in part (not
  // read, or in its window) still counts the validators it asked.
  const counted = judged || (full && rows.some((a) => { const r = resultOf(a, seats.get(a.validator_address)).r; return r === "served" || r === "not_served"; }));
  const side: BlobSide = { judged: counted, available, full, retried, closes, until: Date.parse(b.must_serve_until) };
  const marks = new Map(rows.map((a) => [a.validator_address, markOf(a, seats.get(a.validator_address), side)]));
  // Who served is read from the same words the names carry, so the figures and the marks cannot disagree: the record's
  // once the window has closed, and while it is open the reading's own, as soon as it is in.
  const count = (s: Result) => rows.filter((a) => marks.get(a.validator_address)!.r === s).length;
  const served = count("served"), notServed = count("not_served");
  const early = counted && !over;
  const finalNote = `Final when the retention window closes at ${closes}${retried ? "; a validator that did not serve is asked again until then" : ""}.`;
  // The client's result: Available, enough rows came back to reconstruct the blob; Unavailable, with its own error.
  const state: [string, string, string] =
    rc?.status === "yes" ? ["ok", "Available", `Enough rows came back to reconstruct the blob; ${int(served)} of the ${int(asked)} endorsing validators asked served.`]
    : rc?.status === "no" ? ["hold", "Unavailable", `${rc.error ? `${rc.error[0].toUpperCase()}${rc.error.slice(1)}: ` : ""}${rc.error === "no shards retrieved" ? "no rows came back" : `fewer than the ${int(rc.needed_rows)} rows needed came back`} from the ${int(asked)} endorsing validators asked.`]
    : !over ? ["none", "In retention window", "Read once, 10 minutes before the retention window ends."]
    : counted ? ["none", "Not read by Tensile", "Tensile could not make every request in time, and the rows that came back fell short. Each validator it asked is judged on its own answers."]
    : ["none", "Not read by Tensile", "Tensile did not read this blob. Its own gaps count against no validator."];
  // failures on the validators' side that the rule did not count, as the validator page names them
  const held = rows.filter((a) => marks.get(a.validator_address)!.tone === "hold");
  const heldWhy = [...held.reduce((m, a) => {
    const w = marks.get(a.validator_address)!.cause === "foreign" ? "other rows of the blob" : reasonOf(seats.get(a.validator_address)?.said);
    return m.set(w, (m.get(w) ?? 0) + 1);
  }, new Map<string, number>())]
    .map(([w, n]) => `${w} (${int(n)})`).join(", ");
  const stake = b.total_voting_power ? (b.attested_voting_power ?? 0) / b.total_voting_power : null;
  // the rows of a reading still in progress, or of one that counts, never of one the window closed on uncounted, and
  // never before the reading has asked anyone: a blob not read yet has no rows back to count, not none
  const begun = !!rc && (!!rc.point_at || (rc.probed_validators ?? 0) > 0);
  const shown = !!rc && rc.total_rows > 0 && (counted || (rc.status === "pending" && !over && begun));
  // the endorsers an earlier reading that asked each of them once could not ask: Tensile's own gaps
  const unasked = everyEndorser ? Math.max(0, rows.filter((a) => a.attested === true).length - asked) : 0;

  // the identifiers a developer holds for the blob beside its promise hash: the client's blob ID (its version byte then
  // the commitment; shown for version 0, the only one Fibre has), and the settlement's transaction hash in upper case,
  // as the client and the chain's tools print it
  const blobId = (b.blob_version ?? 0) === 0 ? blobIdOf(b.commitment) : "";
  const txHash = (b.settlement_tx_hash ?? "").toUpperCase();
  // who paid, where it went and when: the publisher page's light frame of facts, one row each
  const facts = (
    <dl className="pb-meta bd-meta">
      <dt>Publisher</dt>
      <dd>{pub ? <Who addr={pub} /> : "—"}{b.signer && pub && b.signer !== pub && <em title={`Sent by ${b.signer}, paid from the publisher's escrow`}>sent by {b.signer.slice(0, b.signer.indexOf("1") + 1)}…{b.signer.slice(-4)}</em>}</dd>
      <dt>Namespace</dt>
      <dd><Link className="bd-ns" href={`/blobs/?namespace=${b.namespace}`} title={`${b.namespace} · every blob in it`}>{nsDisplay(b.namespace)}</Link><Copy text={b.namespace} label="namespace" /></dd>
      <dt>Commitment</dt>
      <dd title={b.commitment}><span className="mono">{shortMid(b.commitment, 10, 6)}</span><Copy text={b.commitment} label="commitment" /></dd>
      <dt title="This settlement's payment promise. A blob settled again has another promise hash.">Promise hash</dt>
      <dd title={b.promise_hash}><span className="mono">{shortMid(b.promise_hash, 10, 6)}</span><Copy text={b.promise_hash} label="the promise hash" /></dd>
      {/* the transaction that settled it; its height and time are in the chip under the title */}
      {txHash && <>
        <dt title="The transaction that settled this blob">Transaction</dt>
        <dd className="bd-tx"><span className="mono" title={txHash}>{shortMid(txHash, 10, 6)}</span><Copy text={txHash} label="transaction hash" /></dd>
      </>}
      {b.assignment_error && <><dt>Assignment</dt><dd>{b.assignment_error}</dd></>}
    </dl>
  );
  // at a full reading, why each of them counted neither way, with how many
  const causes = full ? [...held.reduce((m, a) => { const c = marks.get(a.validator_address)!.cause; return c ? m.set(c, (m.get(c) ?? 0) + 1) : m; }, new Map<Exclude<Cause, "">, number>())] : [];
  const causeWords = ([c, n]: [Exclude<Cause, "">, number]) => `${CAUSE_SHORT[c]}${causes.length > 1 || n < held.length ? ` (${int(n)})` : ""}`;
  const notServedDot = counted && held.length > 0 && <Warn text={`${int(held.length)} validator${held.length === 1 ? "'s" : "s'"} ${full ? "own " : ""}rows did not come back: ${heldWhy}. ${full ? `Counted neither way${causes.length > 0 ? `: ${causes.map(causeWords).join("; ")}` : ""}.` : available ? "Not counted: the blob was available from the others." : "Counted neither way."}`} />;
  const readTitle = full ? (retried
      ? "Read once, 10 minutes before the retention window ends. Each endorsing validator is asked for its own rows, up to twice more if it does not serve."
      : "Read once, 10 minutes before the retention window ends. Each endorsing validator was asked once for its own rows.")
    : endRead ? (everyEndorser
      ? "Read once, 10 minutes before the retention window ends; each endorsing validator was asked once. Not served counts only on an unavailable blob."
      : "Read once, 10 minutes before the retention window ends, in the client's order until enough rows came back. Not served counts only on an unavailable blob.")
    : probes.length > 0 ? "Read on the earlier schedule, at several points in the retention window, and judged by the rule of its time." : "Not read by Tensile.";
  // the figures, in the facts' own rows: the chain's three, then Tensile's three, marked with its eye
  const figs = (
    <dl className="pb-meta bd-meta bd-figs">
      <dt>Blob size</dt><dd title="The size the blob paid for: Celestia's upload size, with header and padding, without parity."><b>{unit(bytes(b.blob_size))}</b></dd>
      <dt>Fee paid</dt><dd title="Charged to the publisher's escrow; not the settlement transaction's own fee.">{b.charge ? <><b>{unit(tia(b.charge.fee_utia))}</b><em>{b.charge.timed_out ? "timed out" : b.charge.settled ? "settled" : "not settled yet"}</em></> : <em>not recorded</em>}</dd>
      <dt>Endorsed</dt><dd title="Voting power whose signature on the settlement verified. A settlement needs ⅔.">{stake != null ? <><b>{pctOf(b.attested_voting_power ?? 0, b.total_voting_power ?? 0)}</b><em>of voting power</em></> : <em>not recorded</em>}</dd>
      <dt className="tz" title={readTitle}><Eye />Rows back</dt><dd title={shown ? `Distinct rows that came back and verified against the commitment; ${int(rc!.needed_rows)} reconstruct the blob.` : undefined}>{shown ? <><b>{int(rc!.served_distinct_rows)}</b><em>of {int(rc!.total_rows)} · {int(rc!.needed_rows)} needed</em></> : <><span className="u">—</span><em>{!over ? `not read yet · read before ${hhmm(b.must_serve_until)}` : "no reading"}</em></>}</dd>
      {/* served and not served in one row: how many of those asked served, how many did not (red, with the dot for
          failures that counted neither way), then when the blob was read */}
      <dt className="tz" title={readTitle}><Eye />Served</dt><dd title={counted ? (full
        ? `Endorsing validators whose own rows came back and verified, of the ${int(asked)} asked${notServed > 0 ? `; ${int(notServed)} did not serve, ${retried ? "neither at the reading nor when asked again" : "at the reading"}` : ""}.`
        : everyEndorser
        ? `Endorsing validators whose rows came back and verified, of the ${int(asked)} asked once each${unasked > 0 ? `; Tensile could not ask ${int(unasked)} more, counted neither way` : ""}${notServed > 0 ? `; ${int(notServed)} did not serve, on a blob that could not be reconstructed` : ""}.`
        : `Endorsing validators whose rows came back and verified, of the ${int(asked)} asked in the client's order until enough rows came back; the rest were not asked.${notServed > 0 ? ` ${int(notServed)} did not serve, on a blob that could not be reconstructed.` : ""}`) : undefined}>
        {counted
          ? <>
            <b>{int(served)}</b><em>of {int(asked)} asked</em>
            {notServed > 0 && <span className="bd-nsv">· <b className="bad">{int(notServed)}</b> not served</span>}
            {notServedDot}
            {rc?.point_at && <em title={early ? `Read ${utcWord(rc.point_at)}. ${finalNote}` : utcWord(rc.point_at)}>{early ? <>· final at {closes}</> : <>· read {hhmm(rc.point_at)}</>}</em>}
          </>
          : <em>—</em>}
      </dd>
    </dl>
  );
  const sig = rows.length === 0
    ? <section className="bd-sig"><div className="bd-sh"><h2><Pen />Endorsements</h2></div><p className="bd-none">No assignment recorded{b.assignment_error ? `: ${b.assignment_error}` : ""}.</p></section>
    : <Signers rows={rows} marks={marks} share={share} stake={stake} table={table} onTable={() => setTable((t) => !t)} />;

  return (
    <>
      <section className="pb-mast bd-mast">
        <h1 className="bd-title">Blob</h1>
        {/* the blob as the client names it: its blob ID, as fibre.Submit returns it and Download takes it; the page itself
            is one settlement of it, by its promise hash, with its height and time in the chips under it */}
        <div className="pb-addr">
          <span className="bd-idl" title={`The ID the Fibre client returns for this blob: version ${b.blob_version ?? 0} and the commitment, in base64`}>Blob ID</span>
          <span className="mono">{blobId}</span><Copy text={blobId} label="the blob ID" />
          {(same.data?.total ?? 0) > 1 && <Link className="bd-many" href={`/blobs/?blob=${encodeURIComponent(blobId)}`} title="Every settlement of this blob ID">settled {int(same.data!.total)} times →</Link>}
        </div>
        <div className="chips bd-chips">
          {/* Tensile's reading, with its eye as its figures carry it: the chips after it are the chain's settlement */}
          <span className="state" title={`Tensile's reading: ${state[2]}`}><i className={"dot " + state[0]} /><Eye />{state[1]}</span>
          <span title={utcWord(b.settlement_time)}>Settled <b className="word">#{int(b.settlement_height)}</b> · {monthDayTime(b.settlement_time)} UTC</span>
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
 * those that did not, each a grid of small tiles (the name, its share of the voting power under it), every
 * validator shown. The voting power endorsed is in the figures above, so the card does not say it twice.
 */
function Signers({ rows, marks, share, stake, table, onTable }: ListProps & { stake: number | null; table: boolean; onTable: () => void }) {
  const on = rows.filter((a) => a.attested === true), off = rows.filter((a) => a.attested === false), un = rows.filter((a) => a.attested == null);
  const f = stake ?? on.reduce((s, a) => s + share(a.voting_power), 0);
  return (
    <section className="bd-sig">
      <div className="bd-sh">
        <h2><Pen />Endorsements</h2>
        <button type="button" className="bd-tbl" aria-pressed={table} onClick={onTable} aria-controls="table">Table</button>
      </div>
      <div className="bd-bands">
        <Band label="Endorsed" list={on} marks={marks} share={share} vp={f} />
        <Band label="Not endorsed" list={off} marks={marks} share={share} off vp={un.length === 0 && stake != null ? 1 - stake : undefined} />
        {un.length > 0 && <Band label="Signature not recorded" list={un} marks={marks} share={share} />}
      </div>
    </section>
  );
}

function Band({ label, list, marks, share, off, vp: given }: { label: string; list: Assignment[]; marks: Map<string, Mark>; share: (vp: number) => number; off?: boolean; vp?: number }) {
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
 * row per validator, its result and reason from the last request that reached it, and each request's answer in turn
 * when there were several
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
            const p = s?.said;
            const detail = p ? `${endOfWindow(p.schedule_label) ? "end reading" : `reading ${p.schedule_label}`}${s!.made > 1 ? ` · the last of ${int(s!.made)} answers` : ""} · ${utcWord(p.started_at)} · ${int(p.rows_returned)} / ${int(p.rows_expected)} rows · ${int(p.total_duration_ms)} ms${p.raw_error ? ` · ${rawErrorWords(p)}` : ""}` : "";
            return (
              <tr key={a.validator_address}>
                <td><Link className="bd-vn" href={validatorHref(a.operator_address, a.validator_address)}>{nameOf(a)}</Link></td>
                <td title={`${int(a.voting_power)} voting power`}>{pct(share(a.voting_power))}</td>
                <td>{int(a.row_count)}</td>
                <td title={a.attested === true ? "Signature verified against the consensus key." : a.attested === false ? "No verified signature on the settlement: nothing owed." : "Recorded before signatures were verified."}>{a.attested === true ? "yes" : a.attested === false ? <span className="q">no</span> : "—"}</td>
                <td className="mono">{a.host_at_settlement ? a.host_at_settlement : a.host_at_settlement === "" ? <span className="q" title="no endpoint registered when the promise settled">—</span> : <span className="q sans" title="the registry could not be read at that height">not read</span>}</td>
                <td className="tn"><span className={m.tone || (m.res.startsWith("served") ? "sv" : "quiet")} title={[m.word, detail].filter(Boolean).join(" · ")}>{m.tab}</span></td>
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
