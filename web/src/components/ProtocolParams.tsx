"use client";
import { useEffect, useRef } from "react";
import { bytes, shortHex, span, useApi, utc } from "@/lib/api";
import type { ParamEntry, Params } from "@/lib/withdrawals";
import { H3 } from "@/components/RefHeading";
import { fragmentId } from "@/lib/fragment";

/**
 * The x/fibre parameters this site computes every deadline from, and the
 * protocol constants it pins, from /v1/params. Lives on the methodology page
 * because that is where every rule that uses them is defined:
 * must_serve_until is creation + max(payment_promise_timeout,
 * shard_retention), and the assignment floor and row counts decide who owes
 * which rows. Nothing here is typed into the page; the chain values come
 * from the observer's store, the constants from the pinned code.
 *
 * The three headings render before the answer lands, so a link to any of
 * them finds its place on arrival.
 */

const LABEL: Record<string, string> = {
  withdrawal_delay: "Withdrawal delay",
  payment_promise_timeout: "Payment promise timeout",
  payment_promise_height_window: "Promise height window",
  shard_retention: "Shard retention",
  full_stake_storage_budget: "Full-stake storage budget",
};

function values(e: ParamEntry) {
  return [
    ["shard_retention", span(e.shard_retention_s)],
    ["payment_promise_timeout", span(e.payment_promise_timeout_s)],
    ["withdrawal_delay", span(e.withdrawal_delay_s)],
    ["payment_promise_height_window", `${e.payment_promise_height_window.toLocaleString("en-US")} blocks`],
    ["full_stake_storage_budget", bytes(e.full_stake_storage_budget_bytes)],
  ] as const;
}

function since(e: ParamEntry): string {
  const at = e.effective_from_time ? ` · ${utc(e.effective_from_time)}` : " · time not read";
  const where = `height ${e.effective_from_height.toLocaleString("en-US")}${e.effective_from_tx_index >= 0 ? `, after tx ${e.effective_from_tx_index - 1}` : ""}`;
  return e.source === "seed" ? `In force at ${where}, where the record begins${at}` : `In force from ${where}${at}`;
}

/** A row of a parameter frame: the reader's name, the chain's name under it, the value. */
function Row({ name, code, children }: { name: string; code?: string; children: React.ReactNode }) {
  return (
    <div>
      <dt>{name}{code && <code>{code}</code>}</dt>
      <dd>{children}</dd>
    </div>
  );
}

export default function ProtocolParams() {
  const { data: p, error } = useApi<Params>("/v1/params", 300000);
  // The answer lands after the browser has scrolled to a linked heading, and the frames it fills push every heading
  // from here on down the page. Once, when they first fill, a linked heading at or after them is scrolled to again.
  const settled = useRef(false);
  useEffect(() => {
    if (!p || settled.current) return;
    settled.current = true;
    const id = fragmentId(window.location.hash);
    const here = document.getElementById("params-chain");
    const target = id ? document.getElementById(id) : null;
    if (!here || !target) return;
    if (target === here || here.compareDocumentPosition(target) & Node.DOCUMENT_POSITION_FOLLOWING) target.scrollIntoView({ block: "start" });
  }, [p]);
  const wait = <p className="muted">{error ? `The observer API is not answering (${error}); the values are at /api/v1/params.` : "Loading…"}</p>;
  const c = p?.current ?? null;
  const k = p?.protocol;
  return (
    <>
      <H3 id="params-chain">On chain now</H3>
      {!p ? wait : c ? (
        <>
          <p className="ref-cap">{since(c)}</p>
          <dl className="ref-kv">
            {values(c).map(([key, v]) => (
              <Row key={key} name={LABEL[key]} code={key}>
                <span className="mono">{v}</span>{c.changed.includes(key) && <span className="chip">changed here</span>}
              </Row>
            ))}
          </dl>
          {/* computed here from two of the values above, so it stands apart from what the chain holds */}
          {p.derived && (
            <div className="ref-derived" id="serving-window">
              <p className="ref-derived-k">Derived <span>computed from the parameters above, not read from the chain</span></p>
              <dl className="ref-kv">
                <Row name="Serving window">
                  <span className="mono">{span(p.derived.must_serve_window_s)}</span><span className="ref-kv-n">from each promise&rsquo;s creation</span>
                  <span className="ref-derived-f"><code>= max(payment_promise_timeout, shard_retention)</code></span>
                </Row>
              </dl>
            </div>
          )}
        </>
      ) : (
        <p className="muted">No x/fibre parameters on record yet: the scanner records them the first time it reads a block where Fibre exists.</p>
      )}

      <H3 id="params-history">Parameter history</H3>
      {!p ? null : p.history.length > 1 ? (
        <div className="tablewrap framed ref-table">
          <table>
            <thead><tr><th>From height</th><th>Block time (UTC)</th><th>Source</th><th>Changed</th></tr></thead>
            <tbody>
              {[...p.history].reverse().map((e) => (
                <tr key={`${e.effective_from_height}-${e.effective_from_tx_index}`}>
                  <td className="mono">{e.effective_from_height.toLocaleString("en-US")}</td>
                  <td className="mono">{e.effective_from_time ? utc(e.effective_from_time) : <span className="faint">not read</span>}</td>
                  <td>{e.source === "seed" ? "start of record" : e.source === "finalize" ? "end of block (governance)" : "transaction"}</td>
                  <td className="wrap">{e.changed.length ? e.changed.map((f) => {
                    const v = values(e).find(([key]) => key === f);
                    return `${LABEL[f] ?? f} → ${v ? v[1] : "?"}`;
                  }).join(", ") : <span className="faint">—</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="ref-cap">No change recorded since the record began. A change appears here with the height and block time it took effect from.</p>
      )}

      <H3 id="params-pinned">Pinned in code</H3>
      {!p || !k ? (p ? null : wait) : (
        <>
          <p className="ref-cap">celestia-app <span className="mono">{shortHex(k.pinned_celestia_app_commit, 6)}</span> · exposed by no RPC, so read from the build the observer runs</p>
          <dl className="ref-kv">
            <Row name="Rows"><span className="mono">{k.original_rows.toLocaleString("en-US")} original + {k.parity_rows.toLocaleString("en-US")} parity = {k.total_rows.toLocaleString("en-US")}</span></Row>
            <Row name="Max blob size"><span className="mono">{bytes(k.max_blob_size_bytes)}</span><span className="ref-kv-n">rows are {bytes(k.min_row_size_bytes)} or wider, up to {bytes(k.max_row_size_bytes)}</span></Row>
            <Row name="Rows per validator"><span className="mono">{k.min_rows_per_validator} to {k.max_rows_per_validator.toLocaleString("en-US")}</span></Row>
            <Row name="Thresholds"><span className="mono">liveness {k.liveness_threshold} · safety {k.safety_threshold}</span></Row>
            <Row name="Parameter bounds">
              {Object.entries(k.param_bounds).map(([name, b]) => (
                <span key={name} className="ref-kv-line"><span className="mono">{b.min_s != null ? span(b.min_s) : "0"} – {span(b.max_s)}</span><span className="ref-kv-n">{(LABEL[name] ?? name).toLowerCase()}</span></span>
              ))}
            </Row>
            <Row name="Assignment pin">
              <span className="mono">{shortHex(k.assignment_fingerprint, 6)}</span>
              {k.fingerprint_matches == null ? <span className="ref-kv-n">the scanner has not reported one</span> : k.fingerprint_matches ? <span className="ref-kv-n">the scanner runs the same pin</span> : <span className="ref-kv-n err">the scanner reports a different pin</span>}
            </Row>
          </dl>
        </>
      )}
    </>
  );
}
