import type { Metadata } from "next";
import type { ReactNode } from "react";
import Link from "next/link";
import Copy from "@/components/Copy";
import { API_URL, SOURCE_URL } from "@/lib/site";
import * as ex from "./examples";

export const metadata: Metadata = {
  title: "API · Tensile · Celestia Fibre",
  description: "Tensile's figures as JSON: validator service, blobs, publishers and the network. Read-only, no key.",
};

/**
 * The API page. It is organised by what a reader wants to know rather than
 * by route: three kinds of reader, a handful of questions each, and for
 * every question the one call that answers it, the few fields that carry the
 * answer and a real answer cut down to them (examples.ts). What every route
 * shares (periods, addresses, units, pages, errors, limits) is said once, in
 * the reference at the bottom.
 *
 * Every field and parameter named here exists in the API with the meaning
 * given; documented ones are the ones the stability promise covers.
 */

type Field = [name: string, what: ReactNode];

type Question = {
  id: string;
  q: string;
  lede?: ReactNode;
  /** the route and its query, after the base URL */
  call: string;
  fields: Field[];
  note?: ReactNode;
  example: string;
  xml?: boolean;
};

type Use = {
  id: string;
  title: string;
  who: string;
  intro: ReactNode;
  icon: ReactNode;
  questions: Question[];
};

const svg = { width: 20, height: 20, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.7, strokeLinecap: "round" as const, strokeLinejoin: "round" as const, "aria-hidden": true };

const HUGINN = "e4401aea8b1f8359fe58216d70d78a402689a2a4";
const PUBLISHER = "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr";

const USES: Use[] = [
  {
    id: "operators",
    title: "Validator operators",
    who: "Watch your own validator: its endpoint, the shards it serves, its endorsements.",
    intro: <>Every validator route takes the address you have: the consensus address (40 hex characters or <code>celestiavalcons1…</code>), the operator address (<code>celestiavaloper1…</code>) or the operator&rsquo;s account (<code>celestia1…</code>). Answers name the validator by its consensus address in hex.</>,
    icon: <svg {...svg}><rect x="3.5" y="4" width="17" height="6.5" rx="1.8" /><rect x="3.5" y="13.5" width="17" height="6.5" rx="1.8" /><path d="M7.5 7.25h.01M7.5 16.75h.01M11 7.25h5M11 16.75h5" /></svg>,
    questions: [
      {
        id: "status",
        q: "How is my validator doing right now?",
        lede: "A small answer for monitoring and alerts, from the same figures as the validator page.",
        call: "/validators/celestiavaloper1d2ktc37cme7ydk30ylzhamutcynhdvyet7nt3x/status",
        fields: [
          ["endpoint_state", <>reachable or unreachable: one failed check after a success still counts as reachable, two in a row do not.</>],
          ["identity_status", <>verified when the endpoint&rsquo;s certificate is endorsed by your consensus key; expired, mismatch, no_tls or unreachable say why not. unverified is a handshake with no certificate check recorded yet, and unknown an endpoint not checked yet.</>],
          ["obligations.served", <>Endorsed shards whose rows came back verified when Tensile read the blob.</>],
          ["obligations.broken", <>Endorsed shards whose rows did not come back on a blob that was Unavailable, &ldquo;not served&rdquo; on the site.</>],
          ["signing", <>assigned is the settled promises that gave you rows while your host was registered; signed is how many carry your endorsement.</>],
          ["last_endpoint_check", <>When your endpoint was last checked, and the outcome.</>],
        ],
        note: <>The period is 24h unless you pass <code>window=7d</code>, <code>30d</code> or <code>all</code>. While a not-served count is younger than 30 minutes it also appears as <code>provisional_faults</code>: counted, but still open to withdrawal.</>,
        example: ex.status,
      },
      {
        id: "serving",
        q: "Am I serving the shards I endorsed?",
        lede: "The validator page's own answer: the period you ask for, and all four periods side by side.",
        call: `/validators/${HUGINN}`,
        fields: [
          ["validator.obligations", <>served, broken and their rate for the period.</>],
          ["validator.obligations.not_counted", <>Endorsed shards that count neither way, such as a blob that was Available without your rows, or one not read by Tensile.</>],
          ["windows[]", <>The same counts for 24h, 7d, 30d and all, in one answer.</>],
          ["network_reference.median_rate", <>The median service rate across validators with at least 20 decided shards, to hold yours against.</>],
        ],
        note: <>The answer also carries your 50 newest readings as <code>recent_probes</code>; the next question has every one.</>,
        example: ex.serving,
      },
      {
        id: "readings",
        q: "Which of my readings failed, and why?",
        lede: <>Every reading Tensile took, newest first. Narrow it with <code>validator</code>, <code>class</code> and <code>since</code>, or <code>served=no</code> for only the ones counted against you.</>,
        call: "/probes?validator=0db46fed54d395de21761113588645bfcc28aee9&class=UNREACHABLE&limit=1",
        fields: [
          ["probes[].outcome", <>What happened on the wire, such as SERVED_OK or TLS_HANDSHAKE_FAIL.</>],
          ["probes[].classification", <>How Tensile judged the reading, such as HEALTHY (rows came back verified) or UNREACHABLE.</>],
          ["probes[].raw_error", <>The error the request ended with, word for word.</>],
          ["probes[].rows_returned", <>Rows that came back from the download; 0 when it never started.</>],
          ["next_before", <>Pass it back as <code>before</code>, with the same filters, for the next page.</>],
        ],
        note: <>Up to 1,000 readings a page (<code>limit</code>, 100 by default). <code>rows=1</code> adds <code>row_indices</code>, the index of every row that came back, in the order returned, and <code>rows_sha256</code>, a digest of those rows; with it a page holds at most 200 (<code>limit</code> up to 200).</>,
        example: ex.failed,
      },
      {
        id: "endorsing",
        q: "Am I endorsing settlements?",
        lede: "Endorsing is not a duty: the Fibre client stops collecting endorsements at two thirds of the stake, so a missing one is not a fault.",
        call: `/validators/${HUGINN}`,
        fields: [
          ["validator.signing.signed", <>Settled promises carrying your endorsement, out of assigned: those that gave you rows while your host was registered.</>],
          ["validator.signing.no_host", <>Promises that settled while you had no Fibre host registered; they count on neither side.</>],
          ["validator.signing.last_endorsed_at", <>When the newest promise carrying your endorsement settled, in any period.</>],
          ["validator.signing.recent", <>Your 20 newest assigned promises and how many you endorsed: a run of zeros is an endorsement that stopped.</>],
        ],
        example: ex.endorsing,
      },
      {
        id: "holding",
        q: "What do I hold, and how fast do I serve it?",
        call: "/validators/f345f91cd3c36238f550a024800c0a2cd0d7d49c",
        fields: [
          ["validator.load.stored_bytes", <>Shard data you must hold right now: the rows of every endorsed blob whose retention window is still open.</>],
          ["validator.load.bytes", <>The shard data you endorsed in the period, over <code>promises</code> blobs.</>],
          ["validator.load.rows_per_blob", <>Your rows on the newest blob; rows follow stake, not blob size.</>],
          ["validator.serve_bytes_per_second", <>Median download speed over your served shards of 2 MiB or more, from <code>serve_throughput_sample</code> readings; null under three.</>],
          ["in_retention_window", <>Endorsed shards whose retention window has not ended, mostly not read yet.</>],
        ],
        note: <>The protocol&rsquo;s row limits are in <a href="#rules">/params</a>: <code>protocol.min_rows_per_validator</code> and <code>max_rows_per_validator</code>.</>,
        example: ex.holding,
      },
      {
        id: "feed",
        q: "Tell me when something changes.",
        lede: "An Atom feed per validator, for any feed reader or bot that follows Atom.",
        call: "/validators/96a4f561df2c45a2fe0607f7a8e1c493251aedec/feed.atom",
        fields: [
          ["category", <>What happened: registered, host-changed, bonded-joined, bonded-left, first-reachable, unreachable, recovered, identity, identity-restored or first-fault.</>],
          ["updated", <>When it happened.</>],
          ["id", <>Fixed for each event, so a reader never sees the same change twice.</>],
        ],
        note: <>A reachability or certificate change is published once three checks in a row agree, about a quarter of an hour. The feed holds the newest 50 entries of the last 30 days. <code>/feed.atom</code> is the network&rsquo;s: registrations, host changes, joins and departures, and each validator&rsquo;s first not-served reading.</>,
        example: ex.feed,
        xml: true,
      },
    ],
  },
  {
    id: "rollups",
    title: "DA and rollup teams",
    who: "Follow your blobs: whether they were available, what they cost, where your escrow stands.",
    intro: <>Find blobs by promise hash, commitment, namespace or the account that paid. Hashes and commitments are 64 hex characters, namespaces 58.</>,
    icon: <svg {...svg}><path d="M12 3.5 20.5 8 12 12.5 3.5 8Z" /><path d="m3.5 12 8.5 4.5 8.5-4.5" /><path d="m3.5 16 8.5 4.5 8.5-4.5" /></svg>,
    questions: [
      {
        id: "blob",
        q: "Was my blob available?",
        lede: "Tensile reads every blob once, 10 minutes before its retention window ends, the way the Fibre client downloads it.",
        call: "/blobs/36f68ba9a781754e80037357ebf485d25e471332f436596904467099cfda2417",
        fields: [
          ["blob.reconstructable.status", <>yes (Available) or no (Unavailable) once read; pending before the reading, not_read when the window closed without one, and unknown when its rows could not be assigned to validators or their count is not on record.</>],
          ["blob.reconstructable.served_distinct_rows", <>Distinct verified rows that came back, against the needed_rows that rebuild the blob.</>],
          ["blob.must_serve_until", <>The end of the retention window: endorsing validators owe their rows until then.</>],
          ["assignments[].service", <>For each validator: served or not_served once decided; in_retention_window while the window is still open, and deadline_unverified while the retention deadline cannot be computed. Absent when there is no result either way, such as a validator the reading never needed to ask.</>],
          ["blob.charge.fee_utia", <>What the promise was charged.</>],
        ],
        note: <>On an Unavailable blob, <code>reconstructable.error</code> carries the Fibre client&rsquo;s error. <code>rows=1</code> adds each reading&rsquo;s <code>row_indices</code>, the index of every row that came back, in the order returned, and <code>rows_sha256</code>, a digest of those rows.</>,
        example: ex.blob,
      },
      {
        id: "commitment",
        q: "I have the commitment, not the promise hash.",
        lede: "One blob can be paid for and settled more than once, so the answer is a list, newest first.",
        call: "/blobs?commitment=a6488bbe560dd24aab75d902fc2975b51066ae3fae74f689218523e2bd49797f",
        fields: [
          ["blobs[].promise_hash", <>Each settlement&rsquo;s promise: open it with <code>{"/blobs/{promise_hash}"}</code>.</>],
          ["blobs[].settlement_height", <>The block it settled in; settlement_time is that block&rsquo;s time.</>],
          ["blobs[].reconstructable.status", <>Each one&rsquo;s result, as on the blob itself.</>],
          ["total", <>How many settlements the commitment has.</>],
        ],
        example: ex.commitment,
      },
      {
        id: "namespace",
        q: "List my namespace's blobs.",
        lede: <>Newest first, 50 a page (<code>limit</code> up to 500). <code>namespace</code>, <code>commitment</code> and <code>publisher</code> combine.</>,
        call: "/blobs?namespace=00000000000000000000000000000000000000736f762d6e696b6f2d61&limit=2",
        fields: [
          ["total", <>Every blob the filters select.</>],
          ["truncated", <>true when there is another page.</>],
          ["next_before_height, next_before_tx_index", <>Pass them as before_height and before_tx_index for the next page; the cursor does not shift as new blobs settle.</>],
          ["offset", <>Numbered pages instead, up to 100,000 blobs deep.</>],
        ],
        example: ex.namespace,
      },
      {
        id: "paid",
        q: "Which blobs did my account pay for?",
        lede: "By the account whose escrow paid for each blob. The account that submitted the settlement does not count: anyone can submit one.",
        call: `/blobs?publisher=${PUBLISHER}&limit=1`,
        fields: [
          ["blobs[].publisher", <>The paying account, the same on every row here.</>],
          ["blobs[].namespace", <>The namespace each blob was published to.</>],
          ["blobs[].charge.fee_utia", <>What each promise cost.</>],
        ],
        note: <>Pages like the <a href="#namespace">namespace list</a>.</>,
        example: ex.paidBy,
      },
      {
        id: "costs",
        q: "What did I pay, and what is left in escrow?",
        call: `/publishers/${PUBLISHER}?window=7d`,
        fields: [
          ["publisher.fees_utia", <>Fees in the period; paid_per_mib_utia is the same per MiB of blob size.</>],
          ["publisher.escrow.balance_utia", <>Your escrow balance on chain; available_utia is what is left of it after queued withdrawals.</>],
          ["publisher.timeouts", <>Promises reported timed out: a floor, since only timeouts someone submitted are on chain.</>],
          ["publisher.pending_withdrawals", <>Queued withdrawals: how many, how much, and when the next one becomes available.</>],
          ["recent_payments[]", <>Your 100 newest escrow movements: settlements, timeouts, deposits and withdrawals.</>],
        ],
        note: <>The answer also lists your 50 newest blobs (<code>recent_blobs</code>) and the four periods side by side (<code>windows</code>).</>,
        example: ex.publisher,
      },
      {
        id: "rules",
        q: "What are the retention and fee rules right now?",
        call: "/params",
        fields: [
          ["current.shard_retention_s", <>How long validators keep a shard, in seconds; payment_promise_timeout_s beside it is the promise&rsquo;s.</>],
          ["derived.must_serve_window_s", <>How long endorsing validators owe a blob&rsquo;s rows: the longer of the two.</>],
          ["protocol.max_blob_size_bytes", <>The largest blob, from the pinned celestia-app build.</>],
          ["price_formula", <>The fee formula and its constants: the same charge whether the promise settles or times out.</>],
        ],
        note: <><code>history</code> lists every set of parameters on record, oldest first, with the height it took effect from.</>,
        example: ex.params,
      },
    ],
  },
  {
    id: "ecosystem",
    title: "Team and ecosystem",
    who: "The network as a whole: providers and stake, hosting, the blob market, the quorum, service.",
    intro: <>Network-wide figures. Every route here takes <code>window=24h</code>, <code>7d</code>, <code>30d</code> or <code>all</code>, except <code>/hosting</code>.</>,
    icon: <svg {...svg}><circle cx="12" cy="12" r="8.5" /><path d="M3.5 12h17M12 3.5c2.4 2.3 3.6 5.1 3.6 8.5s-1.2 6.2-3.6 8.5c-2.4-2.3-3.6-5.1-3.6-8.5s1.2-6.2 3.6-8.5Z" /></svg>,
    questions: [
      {
        id: "providers",
        q: "Who runs Fibre providers, and with how much stake?",
        call: "/validators",
        fields: [
          ["validators[]", <>Every bonded validator, and any other Tensile has a record of, in one list.</>],
          ["validators[].voting_power", <>Its voting power in the chain&rsquo;s validator set.</>],
          ["validators[].endpoint_state", <>reachable or unreachable, as on the <a href="#status">status</a>; absent until first checked.</>],
          ["validators[].hosting.provider", <>Who hosts its endpoint, and hosting.country the ISO country code; absent until its open endpoint has been looked up.</>],
          ["validators[].signing.signed", <>Promises it endorsed, out of signing.assigned.</>],
        ],
        example: ex.validators,
      },
      {
        id: "hosting",
        q: "How concentrated is hosting?",
        call: "/hosting",
        fields: [
          ["summary.by_provider[]", <>Hosts and share of the registered hosts&rsquo; stake per provider; networks not mapped to a provider are grouped as Other. by_country and by_asn split the same way.</>],
          ["summary.nakamoto_third.provider", <>The fewest providers that together carry more than a third of that stake, and who they are; Other is never one of them.</>],
          ["sources", <>The IP databases behind the lookup, with their licences; DB-IP&rsquo;s asks for the attribution it names.</>],
        ],
        example: ex.hosting,
      },
      {
        id: "market",
        q: "How is the blob market doing?",
        call: "/market?window=7d",
        fields: [
          ["settlements", <>Settled payments; blobs is the distinct blobs they paid for, so one blob paid twice is one blob and two settlements.</>],
          ["fees_settled_utia", <>Fees in the period; paid_per_mib_utia is the same per MiB of blob size.</>],
          ["escrow_total_utia", <>Every escrow on the chain, as the x/fibre module account holds it.</>],
          ["daily[]", <>The same figures per UTC day.</>],
          ["top_publishers[]", <>The largest publishers by fees, with their shares of fees and bytes.</>],
        ],
        note: <><code>/publishers</code> lists every publisher in the period; <code>/namespaces</code> every namespace, newest settlement first.</>,
        example: ex.market,
      },
      {
        id: "quorum",
        q: "How close do settlements run to two thirds?",
        call: "/signing?window=7d",
        fields: [
          ["meets_threshold", <>Settled promises whose verified endorsements reach the ⅔ quorum, over all of them.</>],
          ["buckets[]", <>Promises by the share of stake that endorsed them, from below ⅔ up to every validator.</>],
          ["signers_median", <>How many validators&rsquo; endorsements a promise carries, at the median.</>],
        ],
        example: ex.signing,
      },
      {
        id: "network",
        q: "How are service and availability across the network?",
        call: "/network?window=7d",
        fields: [
          ["obligations.rate", <>Served over served plus not served, across every validator&rsquo;s endorsed shards.</>],
          ["reconstructable.recoverable", <>Blobs Available, over the blobs Tensile&rsquo;s reading decided.</>],
          ["reconstructable.publications_examined", <>The newest publications the figure looked at, up to sample_limit; not_read is those not read by Tensile.</>],
          ["reachability", <>Registered endpoints reachable at their latest check; reachability_window pools every check in the period.</>],
        ],
        example: ex.network,
      },
      {
        id: "verify",
        q: "Can I check these figures myself?",
        lede: "Every record behind the figures is published as one signed archive per UTC day.",
        call: "/exports",
        fields: [
          ["exports[].name", <>Download it from <code>{"/exports/{name}"}</code> and check it against sha256.</>],
          ["exports[].signature", <>An ed25519 signature over the archive&rsquo;s manifest; <code>/exports/pubkey</code> serves the key.</>],
          ["exports[].methodology_version", <>The rules that day&rsquo;s records were judged by.</>],
        ],
        note: <><code>sentinel-verify-export</code> checks an archive and <code>sentinel-recompute</code> re-derives every verdict and figure from it; both are in the <a href={SOURCE_URL} rel="noopener noreferrer" target="_blank">source repository</a>.</>,
        example: ex.exports,
      },
    ],
  },
];

/** One line of JSON with its keys set apart from its values. A string value
 *  and the comma after it are one box, so a narrow screen moves a time or a
 *  hash to the next line whole instead of breaking it at a hyphen. */
function jsonLine(line: string): ReactNode[] {
  const out: ReactNode[] = [];
  let last = 0;
  for (const m of line.matchAll(/"(?:[^"\\]|\\.)*"(,?)/g)) {
    if (m.index > last) out.push(line.slice(last, m.index));
    const key = !m[1] && line[m.index + m[0].length] === ":";
    out.push(<span key={m.index} className={key ? "k" : "s"}>{m[0]}</span>);
    last = m.index + m[0].length;
  }
  out.push(line.slice(last));
  return out;
}

/** An answer line by line, each wrapping under its own indent plus two, so a
 *  long line that wraps still reads as part of its level. The text itself is
 *  unchanged: copied, it is the answer as the API sent it. */
function lines(text: string, xml?: boolean): ReactNode[] {
  return text.split("\n").map((line, i) => {
    const hang = line.length - line.trimStart().length + 4;
    return (
      <span key={i} className="ln" style={{ paddingLeft: `${hang}ch`, textIndent: `-${hang}ch` }}>
        {xml ? line : jsonLine(line)}
        {"\n"}
      </span>
    );
  });
}

/** One call as a curl command, with a button that copies it. */
function Call({ path }: { path: string }) {
  const cmd = `curl -s '${API_URL}${path}'`;
  return (
    <div className="api-call">
      <code>{cmd}</code>
      <Copy text={cmd} label="the curl command" />
    </div>
  );
}

function Example({ text, xml }: { text: string; xml?: boolean }) {
  return (
    <figure className="api-ex">
      <figcaption>{xml ? "Example entry" : "Example answer"}<span>excerpt</span></figcaption>
      <pre><code>{lines(text, xml)}</code></pre>
    </figure>
  );
}

function Recipe({ q }: { q: Question }) {
  return (
    <article className="api-q" id={q.id} aria-labelledby={`${q.id}-h`}>
      <div className="api-q-text">
        <h3 id={`${q.id}-h`}><a href={`#${q.id}`}>{q.q}</a></h3>
        {q.lede && <p className="api-lede">{q.lede}</p>}
        <Call path={q.call} />
        <dl className="api-fields">
          {q.fields.map(([name, what]) => (
            <div key={name}><dt><code>{name}</code></dt><dd>{what}</dd></div>
          ))}
        </dl>
        {q.note && <p className="api-note">{q.note}</p>}
      </div>
      <Example text={q.example} xml={q.xml} />
    </article>
  );
}

/** The reference: one topic, a title and a few lines. */
function Topic({ id, title, children }: { id: string; title: string; children: ReactNode }) {
  return (
    <div className="api-topic" id={id}>
      <h3>{title}</h3>
      <ul>{children}</ul>
    </div>
  );
}

export default function Developers() {
  return (
    <div className="api">
      <div className="page-head">
        <div>
          <h1>API</h1>
          <p className="lede">Every figure on Tensile as JSON: validator service, blobs, publishers and the network. The pages of this site read the same routes.</p>
        </div>
      </div>

      <section className="api-q api-intro" aria-label="Base URL">
        <div className="api-q-text">
          <p className="api-eyebrow">Base URL</p>
          <div className="api-call api-base">
            <code>{API_URL}</code>
            <Copy text={API_URL} label="the base URL" />
          </div>
          <div className="chips">
            <span>Celestia <b>Mocha</b> testnet, <code>mocha-5</code></span>
            <span>Read-only: GET and HEAD</span>
            <span>No key, no sign-up</span>
            <span>JSON, times in UTC</span>
          </div>
          <p className="api-lede">Try it: the newest block Tensile has read.</p>
          <Call path="/tip" />
        </div>
        <Example text={ex.tip} />
      </section>

      <nav className="api-uses" aria-labelledby="use-h">
        <h2 id="use-h">Pick your use</h2>
        <div className="api-use-grid">
          {USES.map((u) => (
            <div className="api-use" key={u.id}>
              <span className="api-icon">{u.icon}</span>
              <h3><a href={`#${u.id}`}>{u.title}</a></h3>
              <p>{u.who}</p>
              <ul>
                {u.questions.map((q) => <li key={q.id}><a href={`#${q.id}`}>{q.q}</a></li>)}
              </ul>
            </div>
          ))}
        </div>
        <p className="api-more">Periods, addresses, units, pages, errors and limits are in the <a href="#reference">reference</a> below.</p>
      </nav>

      {USES.map((u) => (
        <section className="api-group" id={u.id} key={u.id} aria-labelledby={`${u.id}-title`}>
          <div className="api-group-head">
            <span className="api-icon">{u.icon}</span>
            <div>
              <h2 id={`${u.id}-title`}>{u.title}</h2>
              <p>{u.intro}</p>
            </div>
          </div>
          {u.questions.map((q) => <Recipe key={q.id} q={q} />)}
        </section>
      ))}

      <section className="api-ref" id="reference" aria-labelledby="reference-title">
        <div className="api-group-head">
          <div>
            <h2 id="reference-title">Reference</h2>
            <p>What every route has in common.</p>
          </div>
        </div>
        <div className="api-ref-grid">
          <Topic id="periods" title="Periods">
            <li><code>window</code> is <code>24h</code> (the default), <code>7d</code>, <code>30d</code> or <code>all</code>, on <code>/validators</code>, a validator and its status, <code>/network</code>, <code>/signing</code>, <code>/market</code>, <code>/publishers</code> and a publisher.</li>
            <li><code>as_of</code>, a time in RFC 3339 such as <code>2026-09-28T00:00:00Z</code>, ends the period then instead of now. The answer&rsquo;s <code>as_of_note</code> names what stays as of now: on the validator routes, <code>/network</code> and <code>/signing</code>, a validator&rsquo;s jailed flag, bond status and current host; on <code>/market</code> and <code>/publishers</code>, escrow balances and queued withdrawals. A publisher&rsquo;s answer pins only <code>publisher</code>; the periods, lists and withdrawals beside it stay as of now. The status route does not take it.</li>
          </Topic>
          <Topic id="addresses" title="Addresses">
            <li>A validator: its consensus address as 40 hex characters or <code>celestiavalcons1…</code>, its operator address <code>celestiavaloper1…</code>, or the operator&rsquo;s account <code>celestia1…</code>. Answers use the consensus address in hex.</li>
            <li>A publisher: its <code>celestia1…</code> account.</li>
            <li><code>promise_hash</code> and <code>commitment</code> are 64 hex characters, <code>namespace</code> 58; answers write hex in lower case.</li>
          </Topic>
          <Topic id="units" title="Units">
            <li>Sizes are bytes. <code>blob_size</code> is the blob&rsquo;s upload size, the size its fee is charged on; shard data is the row data a validator holds.</li>
            <li>Amounts are utia: 1 TIA is 1,000,000 utia.</li>
            <li>Times are RFC 3339 in UTC. A name ending in <code>_s</code> is seconds, <code>_ms</code> milliseconds.</li>
            <li>A rate is <code>{"{num, den, value}"}</code>: <code>value</code> is num / den, and null when den is 0. A share is a fraction from 0 to 1.</li>
          </Topic>
          <Topic id="pages" title="Pages">
            <li><code>/blobs</code>: <code>limit</code> 1 to 500, 50 by default. Page on with <code>before_height</code> and <code>before_tx_index</code> from the answer&rsquo;s <code>next_before_height</code> and <code>next_before_tx_index</code>, or with <code>offset</code> up to 100,000.</li>
            <li><code>/probes</code>: <code>limit</code> up to 1,000, 100 by default; with <code>rows=1</code> a page holds at most 200 (<code>limit</code> up to 200). Page on with <code>before</code> from <code>next_before</code>.</li>
            <li><code>/namespaces</code>: <code>limit</code> up to 500, 100 by default; <code>truncated</code> says there are more.</li>
            <li><code>/validators</code> and <code>/publishers</code> answer the whole list. A validator carries its newest 50 readings, a publisher its newest 100 payments and 50 blobs; <code>recent_probes_truncated</code> and <code>recent_blobs_truncated</code> say when there are more.</li>
          </Topic>
          <Topic id="errors" title="Errors">
            <li>An error is JSON, <code>{"{\"error\": \"…\"}"}</code>, saying what was wrong.</li>
            <li><code>400</code> a parameter or address that cannot be read; <code>404</code> nothing on record, or no such route; <code>405</code> a method other than GET or HEAD; <code>429</code> over a limit below; <code>500</code> an internal error, its details kept back; <code>502</code> the API behind the site did not answer.</li>
            <li><code>503</code> with <code>&quot;computing&quot;: true</code> and <code>Retry-After: 5</code>: the figure is being computed, ask again in a few seconds. <code>/health</code> also answers 503 when one of its checks fails.</li>
          </Topic>
          <Topic id="limits" title="Limits">
            <li>Per client address: bursts of up to 120 requests, then 10 a second, and 16 at once. Beyond that the answer is <code>429</code> with <code>Retry-After: 5</code>.</li>
            <li><code>as_of</code> answers are computed on request and rationed across everyone: 4 in a burst, then one every 2 seconds, 2 at a time.</li>
            <li>A request that runs past 60 seconds ends in <code>502</code>.</li>
          </Topic>
          <Topic id="freshness" title="Caching and freshness">
            <li>Answers may be cached for 15 seconds, <code>/hosting</code> for 60 and the feeds for 5 minutes. <code>/tip</code>, <code>/health</code>, <code>as_of</code> answers and errors are never cached; export archives are cached for a day.</li>
            <li>Figures are computed in the background: <code>computed_at</code> says when, <code>record_through</code> the last block they include.</li>
            <li><code>/tip</code> is the newest block Tensile has read. <code>/health</code> answers 200 when every check passes and 503 with the failing ones otherwise.</li>
          </Topic>
          <Topic id="stability" title="Stability">
            <li>A field or route on this page keeps its name, type and meaning within <code>/v1</code>. If one ever has to go, this page will say so, with a date, first.</li>
            <li>New fields can appear at any time: ignore the ones you do not know. Fields not on this page can change without notice.</li>
            <li>What each figure means is on the <Link href="/methodology/">methodology</Link> page.</li>
          </Topic>
        </div>
      </section>
    </div>
  );
}
