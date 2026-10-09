"use client";
import { useCallback } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { type EndpointEvent, int, utcWord } from "@/lib/api";
import { CopyMark } from "@/components/Ledger";
import Warn from "@/components/Warn";
import { age, monthDayTime } from "@/components/BlobsDeck";
import { openRow } from "@/lib/row";

/**
 * A validator's Fibre endpoint registrations on chain, newest first, as a transaction table: when and in which block,
 * its transaction (the link to its page, and the copy mark), what it did, whether it took effect, and the endpoint, each
 * in a column of its own. The first registration reads Registered and every later one Changed; a failure reads what it
 * asked ("Change requested") with Failed beside it, and its address quiet, never bold and never marked current: it
 * changed nothing. Why it failed, and that the endpoint stayed, are on its page. The row opens its transaction's page.
 */
export default function EndpointHistory({ rows, current, truncated, apiHref }: {
  rows: EndpointEvent[];
  /** the endpoint registered now: the newest successful row with this address is marked current */
  current?: string;
  truncated?: boolean;
  /** the validator's answer in the API, where every registration is */
  apiHref: string;
}) {
  const router = useRouter();
  const onOpen = useCallback((e: React.MouseEvent, href: string) => {
    if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    router.push(href);
  }, [router]);
  const now = Date.now();
  // the newest successful row whose address is the one registered now
  const cur = current ? rows.findIndex((r) => r.outcome !== "failed" && r.host === current) : -1;
  const txN = rows.filter((r) => !!r.tx_hash).length;
  return (
    <>
      <div className="lg-tw">
        <table className="lg-t eh-t">
          <thead>
            <tr>
              <th className="c-tb">Time <span className="per">(UTC)</span> · Block</th>
              <th className="c-x" title="The registration's transaction. Its page has the gas, the fee and, if it failed, why.">TX hash</th>
              <th className="c-ac">Action</th>
              <th className="c-st" title="Whether the registration took effect. A failed one changed nothing.">Status</th>
              <th className="c-ep">Endpoint</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => {
              const failed = r.outcome === "failed";
              const hash = r.tx_hash?.toLowerCase() ?? "";
              const H = hash.toUpperCase();
              const href = hash ? `/tx/?hash=${hash}` : "";
              // a failure asked to change an endpoint, or to register one where there was none
              const attempted = r.attempted ?? (rows.slice(i + 1).some((o) => o.outcome !== "failed") ? "change" : "registration");
              const action = failed ? (attempted === "change" ? "Change requested" : "Registration requested")
                : r.outcome === "registered" || r.outcome === "before_record" ? "Registered" : "Changed";
              const why = r.reason ? (r.other_message_failed ? `Another message in this transaction failed: ${r.reason}.` : `Failed: ${r.reason}.`)
                : r.other_message_failed ? "Another message in this transaction failed." : "Failed.";
              return (
                <tr key={`${r.outcome}|${r.height ?? ""}|${r.tx_index ?? ""}|${r.host}`} className={`row${failed ? " xf" : ""}${href ? "" : " np"}`}
                  onClick={href ? (e) => openRow(e, href, onOpen) : undefined} onAuxClick={href ? (e) => openRow(e, href, onOpen) : undefined}>
                  <td className="c-tb">
                    {r.outcome === "before_record"
                      ? <span className="eh-pre" title="Registered before the first block Tensile read; its transaction is not on record."><em>Before Tensile’s record</em></span>
                      : r.time
                        ? <span className="tb" title={`${utcWord(r.time)} · block #${int(r.height)}`}><span className="tm">{monthDayTime(r.time)}</span><span className="ag">{age(now - Date.parse(r.time))}</span><span className="bk">#{int(r.height)}</span></span>
                        : <span className="eh-pre"><em>In a record gap</em><Warn text={`Changed while Tensile's record had a gap${r.height ? `, before block #${int(r.height)}` : ""}. Its transaction is not on record.`} /></span>}
                  </td>
                  <td className="c-x">{hash
                    ? <><Link href={href} aria-label={`Transaction ${H.slice(0, 6)}…${H.slice(-4)}, ${failed ? "failed " : ""}${action.toLowerCase()}${r.height ? `, block ${int(r.height)}` : ""}`}>{H.slice(0, 6)}<span className="el">…</span>{H.slice(-4)}</Link>
                      <CopyMark text={H} label="the transaction hash" /></>
                    : <span className="na">—</span>}</td>
                  <td className="c-ac">{action}</td>
                  <td className="c-st">{failed
                    ? <span className="st f" title={`${why} The endpoint did not change.`}><i className="dot fault" />Failed</span>
                    : hash ? <span className="st" title="It took effect in this block."><i className="dot ok" />Success</span>
                    : <span className="na">—</span>}</td>
                  <td className="c-ep">{failed
                    ? <span className="mono rq" title="Requested; the endpoint did not change.">{r.host}</span>
                    : <><span className="mono eh-n">{r.host}</span>{i === cur && <span className="cur" title="The endpoint registered now.">current</span>}</>}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="pager vd-pager">
        <span className="count">{int(txN)} {txN === 1 ? "transaction" : "transactions"}</span>
        {truncated && <span className="ctl"><a className="btn" href={apiHref}>Older registrations are in the API →</a></span>}
      </div>
    </>
  );
}
