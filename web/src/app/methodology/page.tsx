import { Legend } from "@/components/Verdict";
import { DISPUTE_URL } from "@/lib/site";
import ProtocolParams from "@/components/ProtocolParams";

// The rules version, as verdict.MethodologyVersion in the Go code and
// methodology_version in /v1/meta and every export manifest. Bumped in the
// same change as any rule that can move a figure.
const METHODOLOGY_VERSION = "2026-09-29.1";

export const metadata = { title: "Methodology · Tensile · Celestia Fibre" };

export default function Methodology() {
  return (
    <div className="prose">
      <h1>Methodology</h1>
      <p className="muted">What Tensile measures and what each word on the site means. Version <b>{METHODOLOGY_VERSION}</b>, published with every figure as <code>methodology_version</code>.</p>

      <h2 id="why">What Tensile adds to the chain</h2>
      <p>The chain records who registered a Fibre host (<code>x/valaddr</code>), every <code>MsgPayForFibre</code> with its signatures, and the <code>x/fibre</code> parameters. It records nothing about service afterwards: whether a blob can still be downloaded, or which validators still serve the rows they signed for. There is no serving proof and no slashing for not serving. Tensile checks that from outside, as an ordinary client, and publishes every row it bases a figure on.</p>

      <h2 id="promise">The obligation</h2>
      <p>A validator that signs a settled promise owes its assigned rows to anyone who asks until <code>must_serve_until = creation_timestamp + max(payment_promise_timeout, shard_retention)</code>, using the parameters in force when the blob settled.</p>

      <h2 id="params">Protocol parameters</h2>
      <p>Read from chain state and every <code>EventUpdateFibreParams</code>; protocol constants come from the pinned celestia-app build. Served at <code>/api/v1/params</code>.</p>
      <ProtocolParams />

      <h2 id="reading">Reading a blob</h2>
      <p>Tensile reads each blob once, 10 minutes before its retention window ends, the way celestia-app&rsquo;s Fibre client downloads it:</p>
      <ul>
        <li>It asks every validator the assignment gives rows, endorsing or not, in the order the client uses (<code>validator.Set.Select</code>), and asks the next one whenever the rows still needed outnumber the rows already on their way.</li>
        <li>Each request (connect, TLS with the consensus-key check, <code>DownloadShard</code>) gets 15 s. After a failed connection or a timeout it is made once more at once.</li>
        <li>Every row is verified against the blob commitment. The reading stops at 4096 distinct verified rows, enough to reconstruct the blob.</li>
        <li>If every validator has been asked and the rows are still short, those that did not serve are asked again one minute later.</li>
      </ul>
      <p>Blobs settled before 27 September 2026, 16:20 UTC were read on an earlier schedule, at several points of the window; their stored readings are judged by the rules below.</p>

      <h2 id="verdicts">Available, served and not served</h2>
      <p>A blob is <strong>available</strong> when at least 4096 distinct rows came back and verified (of 16384 for blob version 0). It is <strong>unavailable</strong> when, after both passes, fewer did: in celestia-app&rsquo;s words, &ldquo;not enough shards to reconstruct blob&rdquo;.</p>
      <p>Only validators whose endorsement is on the settled promise owe the blob; the others are asked too, but never counted. A validator is <strong>served</strong> on a blob when its rows came back and verified. It is <strong>not served</strong> only when the blob was unavailable and its rows did not come back, whatever the reason, a rate limit included. A validator the reading did not need to ask, or one that failed while the blob was available all the same, is counted neither way. If a not-served reading was a power loss, the <a href={DISPUTE_URL} rel="noopener noreferrer" target="_blank">dispute route</a> puts it on the record.</p>
      <Legend />

      <h2 id="signing">Endorsements</h2>
      <p>A validator <em>endorses</em> a payment promise by signing it after storing its shard. <strong>Endorsed ⅔</strong> is the settled promises carrying a validator&rsquo;s verified endorsement over the promises that assigned it rows while it had a Fibre host registered: how often it made the two-thirds quorum. Neither is a duty: a missing endorsement is unproven, not a fault.</p>

      <h2 id="rates">Rates</h2>
      <ul>
        <li><strong>Available</strong>: available blobs over available plus unavailable ones.</li>
        <li><strong>Service rate</strong>: served over served plus not served, per endorsed shard. A rate over fewer than twenty shards sorts after the others.</li>
        <li><strong>In retention window</strong>: the window has not ended. <strong>Not read by Tensile</strong>: no reading decides the blob; nothing is counted either way.</li>
        <li><strong>Provisional</strong>: a not-served reading younger than 30 minutes, counted but still open to withdrawal.</li>
        <li><strong>Reachability</strong>: completed handshakes over attempts, every five minutes per endpoint. One failed check after a success still counts as reachable; two in a row is unreachable.</li>
        <li><strong>Throughput</strong>: median download speed over served shards of 2 MiB or more, from three readings up.</li>
      </ul>

      <h2 id="gaps">Gaps</h2>
      <p>When Tensile could not read a blob in time, or its own request failed, nothing is counted for or against a validator.</p>
      <p>When at least half of the validators asked failed at once and the blob could not be reconstructed, Tensile cannot tell its own failure from theirs, so that reading counts neither way. This stays until Tensile reads a blob of its own beside each reading. It counts validators, not rows: a blob that becomes unavailable with fewer than half of them failing is counted. Health is at <code>/api/v1/health</code>.</p>

      <h2 id="load-on-validators">Load on validators</h2>
      <p>A reading stops once the rows are enough, so a validator is asked for some blobs, not all of them, and never has more than one request from Tensile at a time.</p>

      <h2 id="load">Load</h2>
      <p><strong>Load</strong> is the row data a validator committed to store: its rows, each <code>blob_size / original_rows</code> bytes, of every settled blob it endorsed, kept for the retention window. <strong>Rows per blob</strong> is its share of every blob, by stake. All of it is read from the chain, nothing measured.</p>

      <h2 id="publishers">Publishers and fees</h2>
      <ul>
        <li>Every figure on the Publishers page is read from the chain: settlements, reported timeouts, escrow deposits, withdrawals and balances.</li>
        <li><strong>Fees</strong> are recomputed with the module&rsquo;s formula, <code>(650,000 + 45,000 × ⌈size / 256 KiB⌉)</code> gas at one utia per gas, since no event carries the amount.</li>
        <li><strong>Timeouts</strong> are a floor: only timeouts someone submitted are on chain.</li>
        <li id="withdrawals"><strong>Withdrawals</strong> are read from chain state with each escrow balance, because settlements can draw on queued withdrawals without emitting an event.</li>
      </ul>

      <h2 id="evidence">Evidence behind each figure</h2>
      <p>The validator, blob and Blobs pages show what the chain records under <strong>On chain</strong>, and Tensile&rsquo;s own readings under <strong>Observed by Tensile</strong>. Every snapshot names the block it was computed through (<code>record_through</code>). Daily exports are signed, <code>/api/v1/exports</code>, and <code>sentinel-recompute</code> re-derives every verdict and figure from them.</p>

      <h2 id="vantage">Two locations</h2>
      <p>Endpoints are checked every five minutes from two locations; a host is unreachable only when both fail. A not-served reading is re-checked from the second location within twenty minutes and withdrawn if the rows verify there. Locations are in <code>/api/v1/meta</code>.</p>

      <h2 id="not">What Tensile does not do</h2>
      <ul>
        <li>It is not an availability proof and produces no slashing evidence.</li>
        <li>It does not extrapolate: shards not read are not counted as served.</li>
        <li>It ranks validators only by stored readings anyone can download from the API.</li>
      </ul>
    </div>
  );
}
