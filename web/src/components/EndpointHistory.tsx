"use client";
import { useCallback } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { type EndpointEvent, int, utcWord, failedTitle } from "@/lib/api";
import { CopyMark } from "@/components/Ledger";
import Warn from "@/components/Warn";
import { age, monthDayTime } from "@/components/BlobsDeck";
import { openRow } from "@/lib/row";

/**
 * the transactions among a validator's endpoint rows: every registration that took effect or failed, with or without
 * its hash (one recorded before cost lines has none, and is a transaction all the same); the endpoint before Tensile's
 * record and a change in a record gap are none
 */
export const endpointTxCount = (rows: EndpointEvent[]) =>
  rows.filter((r) => r.outcome === "registered" || r.outcome === "changed" || r.outcome === "same" || r.outcome === "failed").length;

/**
 * an endpoint that wraps only after a dot or a hyphen of its host name, never inside a label or its port
 * ("celestia-testnet-fibre.itrocket.net:7980" breaks as "celestia-testnet-" "fibre.itrocket." "net:7980")
 */
function Host({ host }: { host: string }) {
  const i = host.lastIndexOf(":");
  const name = i > 0 ? host.slice(0, i) : host;
  const parts = name.match(/[^.-]*[.-]|[^.-]+/g) ?? [name];
  const last = parts.pop() ?? "";
  return <>{parts.map((p, k) => <span key={k}>{p}<wbr /></span>)}<span className="hp">{last}{i > 0 ? host.slice(i) : ""}</span></>;
}

/**
 * A validator's Fibre endpoint registrations on chain, newest first, as a transaction table in six equal columns: the
 * block height, left-aligned as every table's first column is, then, each centred under its head, the time (how long
 * ago under it), its transaction (the link to its page, and the copy mark), what it did, whether it took effect, and the
 * endpoint (the one registered now marked "current" under it). The first registration reads Registered and every later
 * successful one Changed; a failure reads what it asked ("Change requested"). Status holds the chain's outcome only:
 * Success with the green dot, Failed with the red one, hung alike to the word's left. A failure's address is quiet,
 * never bold and never marked current: it changed nothing. Why it failed, and that the endpoint stayed, are on its page.
 * The row opens its transaction's page. Below 1080px each row is three lines: what it did and whether it took effect,
 * the endpoint and when, the block and the transaction.
 */
export default function EndpointHistory({ rows, current, truncated }: {
  rows: EndpointEvent[];
  /** the endpoint registered now: the newest successful row with this address is marked current */
  current?: string;
  /** the answer holds the newest 50 registrations, and the validator has older ones */
  truncated?: boolean;
}) {
  const router = useRouter();
  const onOpen = useCallback((e: React.MouseEvent, href: string) => {
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  }, [router]);
  const now = Date.now();
  // the newest successful row whose address is the one registered now
  const cur = current ? rows.findIndex((r) => r.outcome !== "failed" && r.host === current) : -1;
  const txN = endpointTxCount(rows);
  return (
    <>
      <div className="lg-tw">
        <table className="lg-t eh-t">
          <thead>
            <tr>
              <th className="c-bh">Height</th>
              <th className="c-tm">Time <span className="per">(UTC)</span></th>
              <th className="c-x" title="The registration's transaction. Its page has the gas, the fee and, if it failed, why.">TX hash</th>
              <th className="c-ac">Action</th>
              <th className="c-st" title="Whether the registration took effect. A failed one changed nothing.">Status</th>
              <th className="c-ep">Endpoint</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => {
              const failed = r.outcome === "failed";
              // a registration that took effect in a block on record, with or without its transaction's hash
              const took = r.outcome === "registered" || r.outcome === "changed" || r.outcome === "same";
              const hash = r.tx_hash?.toLowerCase() ?? "";
              const H = hash.toUpperCase();
              const href = hash ? `/tx/?hash=${hash}` : "";
              // a failure asked to change an endpoint, or to register one where there was none
              const attempted = r.attempted ?? (rows.slice(i + 1).some((o) => o.outcome !== "failed") ? "change" : "registration");
              // the first registration reads Registered, the endpoint before the record and one made in a record gap
              // where there was none included; every later successful one Changed
              const first = r.outcome === "registered" || r.outcome === "before_record" || (r.outcome === "after_gap" && !!r.first);
              const action = failed ? (attempted === "change" ? "Change requested" : "Registration requested")
                : first ? "Registered" : "Changed";
              // the block it took place in; the seed and a record gap's row have none on record
              const onRecord = r.outcome !== "before_record" && !!r.time && r.height != null;
              const gapWords = `${first ? "Registered" : "Changed"} while Tensile's record had a gap${r.height ? `, before block #${int(r.height)}` : ""}. Its transaction is not on record.`;
              return (
                <tr key={`${r.outcome}|${r.height ?? ""}|${r.tx_index ?? ""}|${r.host}`} className={`row${failed ? " xf" : ""}${href ? "" : " np"}`}
                  onClick={href ? (e) => openRow(e, href, onOpen) : undefined} onAuxClick={href ? (e) => openRow(e, href, onOpen) : undefined}>
                  <td className="c-bh">{onRecord ? <><span className="ph">Block </span>{int(r.height)}</> : <span className="na">—</span>}</td>
                  <td className="c-tm">
                    {r.outcome === "before_record"
                      ? <span className="eh-pre" title="Registered before the first block Tensile read; its transaction is not on record."><em>Before Tensile’s record</em></span>
                      : r.time
                        ? <span className="tw" title={utcWord(r.time)}><span className="tm">{monthDayTime(r.time)}</span><span className="ag">{age(now - Date.parse(r.time))}</span></span>
                        : <span className="eh-pre"><em>In a record gap</em><Warn text={gapWords} /></span>}
                  </td>
                  <td className="c-x">{hash
                    ? <><Link href={href} aria-label={`Transaction ${H.slice(0, 6)}…${H.slice(-4)}, ${failed ? "failed " : ""}${action.toLowerCase()}${r.height ? `, block ${int(r.height)}` : ""}`}>{H.slice(0, 6)}<span className="el">…</span>{H.slice(-4)}</Link>
                      <CopyMark text={H} label="the transaction hash" /></>
                    : <span className="na">—</span>}</td>
                  <td className="c-ac">{action}</td>
                  <td className="c-st">{failed
                    ? <span className="st f" title={failedTitle(r.reason)}><i className="dot fault" />Failed</span>
                    : took ? <span className="st" title="It took effect in this block."><i className="dot ok" />Success</span>
                    : <span className="na">—</span>}</td>
                  <td className="c-ep">{failed
                    ? <span className="mono rq" title="Requested; the endpoint did not change."><Host host={r.host} /></span>
                    : i === cur
                      ? <span className="ew"><span className="mono eh-n"><Host host={r.host} /></span><span className="cur" title="The endpoint registered now.">current</span></span>
                      : <span className="mono eh-n"><Host host={r.host} /></span>}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="pager vd-pager eh-pager">
        <span className="count">{int(txN)} {txN === 1 ? "transaction" : "transactions"}</span>
        {truncated && <span className="ctl">The newest 50 registrations.</span>}
      </div>
    </>
  );
}
