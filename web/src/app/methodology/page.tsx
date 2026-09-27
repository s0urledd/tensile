import { Legend } from "@/components/Verdict";
import { DISPUTE_URL } from "@/lib/site";
import ProtocolParams from "@/components/ProtocolParams";

// The rules version, as verdict.MethodologyVersion in the Go code and
// methodology_version in /v1/meta and every export manifest. Bumped in the
// same change as any rule that can move a figure.
const METHODOLOGY_VERSION = "2026-09-27.1";

export const metadata = { title: "Methodology · Tensile · Celestia Fibre" };

export default function Methodology() {
  return (
    <div className="prose">
      <h1>Methodology</h1>
      <p className="muted">What Tensile measures and what each word on the site means. Version <b>{METHODOLOGY_VERSION}</b>, published with every figure as <code>methodology_version</code>.</p>

      <h2 id="why">What Tensile adds to the chain</h2>
      <p>The chain records who registered a Fibre host (<code>x/valaddr</code>), every <code>MsgPayForFibre</code> with its signatures, and the <code>x/fibre</code> parameters. It records nothing about service afterwards: whether an endpoint answers, whether a validator still serves a shard it signed for, or whether it pruned early. There is no serving proof and no slashing for not serving. Tensile checks that from outside, as an ordinary client, and publishes every row it bases a figure on.</p>

      <h2 id="promise">The obligation</h2>
      <p>A validator that signs a settled promise owes its assigned rows to anyone who asks until <code>must_serve_until = creation_timestamp + max(payment_promise_timeout, shard_retention)</code>, using the parameters in force when the blob settled.</p>

      <h2 id="params">Protocol parameters</h2>
      <p>Read from chain state and every <code>EventUpdateFibreParams</code>; protocol constants come from the pinned celestia-app build. Served at <code>/api/v1/params</code>.</p>
      <ProtocolParams />

      <h2 id="probe">One probe</h2>
      <p>Resolve the registered host, open TCP, complete TLS 1.3, check the certificate is endorsed by the validator&rsquo;s consensus key, call <code>DownloadShard</code>, verify every row against the blob commitment, and check the rows are exactly the assigned ones (<code>fibre-assign</code>, bit-identical to celestia-app). Each step is timed and recorded.</p>

      <h2 id="schedule">Schedule</h2>
      <p>Since 27 September 2026, 16:20 UTC, each endorsed shard is read once, 10 minutes before its retention window ends, with the 15 s timeout of celestia-app&rsquo;s own client. Validators that did not endorse a blob are not read. Earlier blobs were read at 12%, 45%, 72% and 92% of the window and twice after it.</p>

      <h2 id="verdicts">Served and not served</h2>
      <p>Only a validator whose endorsement is on the settled promise has to serve it: a Fibre server stores the shard before it signs. A missing endorsement proves nothing either way, so those shards are <strong>not endorsed</strong> and not rated.</p>
      <p>At the end reading a shard is <strong>served</strong> if its rows come back and verify against the blob commitment, and <strong>not served</strong> otherwise: not found, bad rows, no answer, a rejected certificate, an error, a rate limit or no registered host. That is what a reader using celestia-app&rsquo;s client gets. Before 27 September only not found and bad rows counted. If a not-served reading was a power loss, the <a href={DISPUTE_URL} rel="noopener noreferrer" target="_blank">dispute route</a> puts it on the record.</p>
      <Legend />

      <h2 id="signing">Endorsements</h2>
      <p>A validator <em>endorses</em> a payment promise by signing it after storing its shard. <strong>Endorsed ⅔</strong> is the settled promises carrying a validator&rsquo;s verified endorsement over the promises that assigned it rows while it had a Fibre host registered (a promise from before its host existed is one it could not endorse, and is left out): how often it made the two-thirds quorum. The Blobs page shows each promise&rsquo;s endorsed share of stake against the quorum of <code>floor(total &times; 2 / 3)</code>. Neither is a duty: a missing endorsement is unproven, not a fault.</p>

      <h2 id="rates">Rates</h2>
      <ul>
        <li><strong>Service rate</strong>: served over served plus not served, per endorsed shard. A rate over fewer than twenty shards sorts after the others.</li>
        <li><strong>In retention window</strong>: not read yet. <strong>Not read by Tensile</strong> and <strong>sampled out</strong>: no verdict either way.</li>
        <li><strong>Provisional</strong>: a not-served reading younger than 30 minutes, counted but still open to withdrawal.</li>
        <li><strong>Retrievable</strong>: enough rows retrieved to reconstruct the blob, 4096 of 16384 for version 0.</li>
        <li><strong>Reachability</strong>: completed handshakes over attempts, every five minutes per endpoint. One failed check after a success is <strong>flaky</strong>; two in a row is unreachable.</li>
        <li><strong>Throughput</strong>: median download speed over served shards of 2 MiB or more, from three readings up.</li>
        <li>Every rate is a ratio of sums, with a 95% upper bound on the not-served share.</li>
      </ul>

      <h2 id="gaps">Gaps</h2>
      <p>When Tensile could not read (its own downtime or errors, or earlier sampling), the row is <strong>NOT_PROBED</strong> or <strong>PROBE_ERROR</strong>: shown, never a zero, never in a rate. A reading where at least half the validators failed at once is treated as Tensile&rsquo;s own failure and left out. Health is at <code>/api/v1/health</code>.</p>

      <h2 id="sampling">Load on validators</h2>
      <p>Each endorsed shard is read once, so a validator is asked for about what publishers sent it, over one connection with 2 s between requests. Before 27 September, blobs above a per-validator budget were sampled; the daily sampling secrets are at <code>/api/v1/sampling</code>.</p>

      <h2 id="load">Load</h2>
      <p><strong>Load</strong> is the row data a validator committed to store: its rows, each <code>blob_size / original_rows</code> bytes, of every settled blob it endorsed, kept for the retention window. Rows it was assigned and did not endorse are no duty, the same shards the service rate counts. <strong>Rows per blob</strong> is its share of every blob, by stake. All of it is read from the chain, nothing measured.</p>

      <h2 id="publishers">Publishers and fees</h2>
      <ul>
        <li>Every figure on the Publishers page is read from the chain: settlements, reported timeouts, escrow deposits, withdrawals and balances.</li>
        <li><strong>Fees</strong> are recomputed with the module&rsquo;s formula, <code>(650,000 + 45,000 × ⌈size / 256 KiB⌉)</code> gas at one utia per gas, since no event carries the amount.</li>
        <li><strong>Timeouts</strong> are a floor: only timeouts someone submitted are on chain.</li>
        <li id="withdrawals"><strong>Withdrawals</strong> are read from chain state with each escrow balance, because settlements can draw on queued withdrawals without emitting an event.</li>
      </ul>

      <h2 id="evidence">Evidence behind each figure</h2>
      <p>Every headline figure is tagged <strong>chain</strong> (recorded on chain), <strong>verified</strong> (bytes or certificates Tensile checked) or <strong>observed</strong> (what Tensile&rsquo;s network saw). Every snapshot names the block it was computed through (<code>record_through</code>). Daily exports are signed, <code>/api/v1/exports</code>, and <code>sentinel-recompute</code> re-derives every verdict and figure from them.</p>

      <h2 id="vantage">Two locations</h2>
      <p>Endpoints are checked every five minutes from two locations; a host is unreachable only when both fail. A not-found or bad-rows reading is re-checked from the second location within twenty minutes and withdrawn if the rows verify there. Rates use this location&rsquo;s readings. Locations are in <code>/api/v1/meta</code>.</p>

      <h2 id="not">What Tensile does not do</h2>
      <ul>
        <li>It is not an availability proof and produces no slashing evidence.</li>
        <li>It does not extrapolate: shards not read are not counted as served.</li>
        <li>It ranks validators only by stored readings anyone can download from the API.</li>
      </ul>
    </div>
  );
}
