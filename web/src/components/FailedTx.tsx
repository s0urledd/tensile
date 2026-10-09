import type { ReactNode } from "react";
import { type FailedTx as Failed, type FailedTxMsg, type Meta, int, utcWord } from "@/lib/api";
import StatusLine from "@/components/StatusLine";
import Copy from "@/components/Copy";
import Warn from "@/components/Warn";
import { monthDayTime } from "@/components/BlobsDeck";

/**
 * past this many messages, only the one that failed and the first this many that carry Fibre are listed; past this
 * many inside a MsgExec, its name gives the first this many Fibre ones and counts the rest
 */
const LISTED = 12;

/** a message's name: its type URL after the last dot */
const nameOf = (url: string) => url.slice(url.lastIndexOf(".") + 1);

/** the fee as the chain printed it ("800utia", coins joined by commas), with each amount's digits grouped and a space before its denomination */
const coins = (fee: string) =>
  fee.split(",").map((c) => c.replace(/^(\d+)(\D.*)$/, (_, n: string, d: string) => `${n.length <= 15 ? int(Number(n)) : n} ${d}`)).join(", ");

/**
 * the fee in TIA, as the site writes amounts, but always down to the utia ("2500utia" is 0.0025 TIA, where tia()
 * would round it to 0.003): a transaction fee is small and paid exactly; another denomination as coins() prints it
 */
const feeTia = (fee: string) =>
  fee.split(",").map((c) => {
    const m = /^(\d{1,15})utia$/.exec(c.trim());
    return m ? `${(Number(m[1]) / 1e6).toFixed(6).replace(/\.?0+$/, "")} TIA` : coins(c);
  }).join(", ");

/** a message, or a MsgExec with a message inside it, that carries Fibre */
const carries = (m: FailedTxMsg) => m.fibre || !!m.inner?.some((i) => i.fibre);

/** a MsgExec's messages inside the brackets of its name, each as `as` gives it, and how many more there are */
function within(inner: FailedTxMsg[], as: (url: string) => string) {
  const shown = inner.length > LISTED ? inner.filter((i) => i.fibre).slice(0, LISTED) : inner;
  const rest = inner.length - shown.length;
  const names = shown.map((i) => as(i.type_url));
  if (rest > 0) names.push(shown.length > 0 ? `+${int(rest)} more` : `${int(rest)} messages`);
  return names.join(", ");
}

/**
 * One message of the transaction, by its name: a MsgExec with the names of the messages inside it, in order. The one
 * the error names is marked failed; one that carries no Fibre is quieter. The hover gives the full type URLs.
 */
function Msg({ m, failed }: { m: FailedTxMsg; failed: boolean }) {
  const inner = m.inner && m.inner.length > 0 ? m.inner : null;
  const label = inner ? `${nameOf(m.type_url)} (${within(inner, nameOf)})` : nameOf(m.type_url);
  const title = inner ? `${m.type_url} (${within(inner, (url) => url)})` : m.type_url;
  if (failed) return <span title={title}><b className="bad">{label}</b> <em>failed</em></span>;
  return <span className={carries(m) ? undefined : "u"} title={title}>{label}</span>;
}

/**
 * A Fibre transaction that failed in a block, opened by its hash (/blob/?tx=): the blob page's mast, its light frame of
 * facts (why, the code, the messages, gas and fee) and, under them at the page width, the error as the node returned
 * it. None of its messages took effect, so it settled no blob and is in no figure.
 */
export default function FailedTx({ hex, f, meta, metaErr, at }: { hex: string; f: Failed; meta: Meta | null; metaErr: string | null; at: string }) {
  // the hash in upper case, as the client and the chain's tools print it
  const HEX = hex.toUpperCase();
  const failedAt = f.failed_msg_index;
  // past LISTED messages: the one that failed and the first LISTED that carry Fibre, in order
  const carrying = new Set(f.messages.filter(carries).slice(0, LISTED).map((m) => m.index));
  const listed = f.messages.length > LISTED ? f.messages.filter((m) => m.index === failedAt || carrying.has(m.index)) : f.messages;
  const msgs: ReactNode[] = [];
  listed.forEach((m, i) => {
    if (i > 0) msgs.push(" · ");
    msgs.push(<Msg key={m.index} m={m} failed={failedAt != null && m.index === failedAt} />);
  });
  return (
    <>
      <section className="pb-mast bd-mast">
        <h1 className="bd-title">Transaction failed</h1>
        <div className="pb-addr"><span className="bd-idl">Transaction</span><span className="mono">{HEX}</span><Copy text={HEX} label="transaction hash" /></div>
        <div className="chips bd-chips">
          <span className="state" title="This transaction failed in this block: none of its messages took effect."><i className="dot fault" />Failed</span>
          <span title={utcWord(f.time)}>Block <b className="word">#{int(f.height)}</b> · {monthDayTime(f.time)} UTC</span>
        </div>
      </section>
      <StatusLine meta={meta} metaError={metaErr} snap={null} client={{ error: null, fetchedAt: at }} />

      {/* why it failed and what it carried, in the blob page's light frame of facts, at the page width as the error is */}
      <div className="bd-top bd-one">
        <dl className="pb-meta bd-meta">
          {f.reason && <><dt>Reason</dt><dd>{f.reason}</dd></>}
          <dt>Code</dt><dd><span className="mono">{f.codespace ? `${f.codespace} ${f.code}` : f.code}</span></dd>
          <dt>Messages</dt><dd>{msgs}{listed.length < f.messages.length && <em>of {int(f.messages.length)} messages</em>}</dd>
          <dt>Gas</dt><dd><b>{int(f.gas_used)}</b><em>used of {int(f.gas_wanted)}</em></dd>
          <dt>Transaction fee</dt>
          <dd>{f.ante_passed
            ? <><b title={f.fee ? coins(f.fee) : undefined}>{f.fee ? `Paid ${feeTia(f.fee)}` : "None"}</b><em>this transaction cannot run again</em></>
            : <><b>Not taken</b><em>the chain stopped it before running it, so the same transaction could still be included in a later block</em></>}</dd>
        </dl>
      </div>
      {/* the error as the node returned it, at the page width in the endorsements' frame */}
      <div className="bd-under">
        <section className="bd-sig bd-err">
          <div className="bd-sh">
            <h2>Error</h2>
            <span className="bd-err-a">
              {f.log_cut && <Warn text="Shortened by Tensile: the node's stack trace after a panic, and anything past 8 KiB, are not kept." />}
              <Copy text={f.log} label="the error" title="Copy the error" />
            </span>
          </div>
          <pre className="bd-log">{f.log}</pre>
        </section>
      </div>
    </>
  );
}
