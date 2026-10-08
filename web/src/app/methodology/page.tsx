import { Legend } from "@/components/Verdict";
import { API_URL, DISPUTE_URL, SOURCE_URL } from "@/lib/site";
import ProtocolParams from "@/components/ProtocolParams";
import OnThisPage, { type Section } from "@/components/OnThisPage";
import { H2, H3 } from "@/components/RefHeading";

// The rules version, as verdict.MethodologyVersion in the Go code and
// methodology_version in /v1/meta and every export manifest. Bumped in the
// same change as any rule that can move a figure.
const METHODOLOGY_VERSION = "2026-10-08";

export const metadata = { title: "Methodology · Tensile · Celestia Fibre" };

/*
 * A reference page: what a figure means, how it is computed, and where its
 * record is. Every section and every figure has an id of its own, so a mark
 * beside a figure elsewhere on the site can link straight to its definition.
 * Ids a section answered to before it moved or was renamed stay on it as
 * aliases (RefHeading), so an older link still lands.
 */
const SECTIONS: Section[] = [
  { id: "overview", label: "Overview" },
  { id: "blobs", label: "Blobs and settlements" },
  {
    id: "results", label: "Results and metrics", sub: [
      { id: "available", label: "Available" },
      { id: "served", label: "Served" },
      { id: "not-served", label: "Not served" },
      { id: "service-rate", label: "Service rate" },
      { id: "reachability", label: "Reachability" },
      { id: "throughput", label: "Throughput" },
      { id: "endorsements", label: "Endorsements" },
      { id: "shard-data", label: "Shard data" },
    ],
  },
  { id: "checks", label: "How checks work" },
  { id: "publishers", label: "Publishers and escrow" },
  { id: "params", label: "Protocol parameters" },
  { id: "evidence", label: "Evidence and corrections" },
];

const API_ORIGIN = API_URL.replace(/\/v1$/, "");

/** A figure's definition: its name, what it means in one line, then whatever it needs. */
function Def({ id, aliases, name, say, children }: { id: string; aliases?: string[]; name: string; say: React.ReactNode; children?: React.ReactNode }) {
  return (
    <div className="ref-def">
      <H3 id={id} aliases={aliases}>{name}</H3>
      <p className="ref-say">{say}</p>
      {children}
    </div>
  );
}

/** How a figure is computed, in the mono face, with what it is taken over. */
function Formula({ children, note }: { children: React.ReactNode; note?: React.ReactNode }) {
  return (
    <div className="ref-f">
      <code>{children}</code>
      {note && <span className="ref-f-n">{note}</span>}
    </div>
  );
}

/** What enters a figure and what stays out of it. */
function Rules({ rows }: { rows: [string, React.ReactNode, string?][] }) {
  return (
    <dl className="ref-rules">
      {rows.map(([k, v, id]) => <div key={k}><dt id={id}>{k}</dt><dd>{v}</dd></div>)}
    </dl>
  );
}

/** A list of terms, each linkable on its own. */
function Term({ id, aliases, term, children }: { id?: string; aliases?: string[]; term: React.ReactNode; children: React.ReactNode }) {
  return (
    <div>
      <dt id={id}>
        {aliases?.map((a) => <span key={a} id={a} className="ref-alias" aria-hidden="true" />)}
        {term}
        {id && <a className="ref-hash" href={`#${id}`} aria-label="Link to this term">#</a>}
      </dt>
      <dd>{children}</dd>
    </div>
  );
}

/** Technical cases a reader can open; never the definition itself. */
function More({ summary, children }: { summary: string; children: React.ReactNode }) {
  return (
    <details className="ref-more">
      <summary>{summary}</summary>
      <div>{children}</div>
    </details>
  );
}

export default function Methodology() {
  return (
    <div className="meth">
      <header className="meth-head">
        <h1>Methodology</h1>
        <p className="meth-lede">What each figure on Tensile means, how it is computed, and where to find the record behind it.</p>
        <p className="meth-ver">Version <a href="#version">{METHODOLOGY_VERSION}</a>, published as <code>methodology_version</code> in <code>/v1/meta</code></p>
      </header>

      <OnThisPage items={SECTIONS} version={METHODOLOGY_VERSION} />

      <article className="meth-doc">
        {/* ------------------------------------------------------------ overview */}
        <section className="ref-sec">
          <H2 id="overview" aliases={["why", "not"]}>Overview</H2>
          <p className="ref-lead">The chain records which rows each validator promised to store. Tensile checks, from outside and as an ordinary client, whether it serves them, and publishes every record a figure rests on.</p>
          <p>The chain records who registered a Fibre host (<code>x/valaddr</code>), every <code>MsgPayForFibre</code> with the validator signatures on it, and the <code>x/fibre</code> parameters. It records nothing about serving afterwards: there is no serving proof and no slashing for not serving.</p>
          <div className="ref-scope" role="list">
            <div role="listitem">
              <p className="ref-scope-k"><b>Blob readings</b><span>once per blob · one location</span></p>
              <div className="ref-scope-v">
                <p>Each settled blob is read 10 minutes before its retention window ends. Every validator that endorsed it is asked for its own rows, and every row is verified against the blob commitment.</p>
                <p className="ref-scope-g"><a href="#available">Available</a><a href="#served">Served</a><a href="#not-served">Not served</a><a href="#service-rate">Service rate</a><a href="#throughput">Throughput</a></p>
              </div>
            </div>
            <div role="listitem">
              <p className="ref-scope-k"><b>Endpoint checks</b><span>every five minutes · two locations</span></p>
              <div className="ref-scope-v">
                <p>Every registered Fibre endpoint is checked for DNS, TCP, a TLS 1.3 handshake and a certificate signed by the validator&rsquo;s consensus key. No blob data is asked for.</p>
                <p className="ref-scope-g"><a href="#reachability">Reachability</a><a href="#endpoint-checks">TLS identity</a></p>
              </div>
            </div>
            <div role="listitem">
              <p className="ref-scope-k"><b>Chain record</b><span>as each block arrives</span></p>
              <div className="ref-scope-v">
                <p>Settlements, endorsements and parameter changes, read from each block as it arrives, and escrow balances from chain state. Nothing here is measured.</p>
                <p className="ref-scope-g"><a href="#endorsements">Endorsements</a><a href="#shard-data">Shard data</a><a href="#fees">Fees</a><a href="#escrow">Escrow</a><a href="#params">Parameters</a></p>
              </div>
            </div>
          </div>
          <p>A reading is an observation, not an availability proof, and produces no slashing evidence. Validators are ranked only by stored readings anyone can download from the API.</p>
        </section>

        {/* ------------------------------------------------------------- blobs */}
        <section className="ref-sec">
          <H2 id="blobs" aliases={["names"]}>Blobs and settlements</H2>
          <p className="ref-lead">A blob is the data; a settlement is one payment for it on chain. Most blobs settle once.</p>
          <dl className="ref-terms">
            <Term id="blob-id" term="Blob ID">The version byte and commitment the Fibre client returns, in base64. Search also takes it in hex.</Term>
            <Term id="hashes" term="Transaction and promise hash">Each settlement is a <code>MsgPayForFibre</code> transaction carrying one payment promise, which validators endorse. A blob&rsquo;s page is addressed by its promise hash; search takes the transaction hash too.</Term>
            <Term id="settled-twice" term="Settled more than once">A blob paid for twice is one blob and two settlements, each with its own promise, its own endorsements and its own reading. Blob counts count it once; settlement counts count both.</Term>
            <Term id="blob-size" term="Blob size">The size the blob paid for: Celestia&rsquo;s upload size, with header and padding, without parity.</Term>
            <Term id="namespaces" term="Namespace">Shown as text when its bytes are text, as a name and its trailing tag in hex when a client numbers its namespaces (<code>tensile·04</code>), and in hex otherwise.</Term>
            <Term id="retention-window" aliases={["promise"]} term="Retention window">
              A validator that endorsed a settled promise owes its assigned rows to anyone who asks until the window ends:
              <Formula note="with the parameters in force when the blob settled">must_serve_until = creation_timestamp + max(payment_promise_timeout, shard_retention)</Formula>
            </Term>
          </dl>
        </section>

        {/* ----------------------------------------------------------- results */}
        <section className="ref-sec">
          <H2 id="results" aliases={["verdicts", "rates"]}>Results and metrics</H2>
          <p className="ref-lead">Each blob read gets one result, Available or Unavailable. Each validator that endorsed it is judged served or not served on it, and its service rate counts those results.</p>
          <p>Reachability, endorsements and served are three separate facts: an endpoint that completes a handshake, a signature on chain, and rows that came back. None is inferred from another.</p>
          <div className="ref-box" id="rate-rules">
            <p className="ref-box-k">Shared rules</p>
            <ul>
              <li id="in-retention-window">A reading counts once its blob&rsquo;s retention window has closed. Until then the blob is <strong>in retention window</strong>, shown as <strong>Awaiting check</strong> on a validator&rsquo;s page; the reading shows on the blob&rsquo;s page as soon as it is in.</li>
              <li>Every rate is printed beside its numerator and denominator.</li>
              <li>A service rate, reachability or throughput from fewer than 20 observations is shown without a gauge and sorts after the others.</li>
            </ul>
          </div>

          <Def id="available" name="Available" say={<>Enough of the blob&rsquo;s rows came back at its reading to reconstruct it.</>}>
            <p>The reading is an observation inside the retention window, made 10 minutes before the window ends. Its result is the Fibre client&rsquo;s own, one of three:</p>
            <dl className="ref-terms ref-terms--tight">
              <Term term="Available">At least 4,096 distinct rows came back and verified.</Term>
              <Term term={<>Unavailable <span className="ref-q">no shards retrieved</span></>}>No rows came back.</Term>
              <Term term={<>Unavailable <span className="ref-q">not enough shards to reconstruct blob</span></>}>Some rows came back, fewer than 4,096.</Term>
            </dl>
            <Formula note="over the blobs read whose retention window has ended">available / (available + unavailable)</Formula>
            <Rules rows={[["Not counted", "A blob still in its retention window, and a blob with no reading."]]} />
          </Def>

          <Def id="served" name="Served" say="The validator’s own endorsed rows came back and verified against the blob commitment, at the reading or when it was asked again.">
            <p>Rows of another settled promise over the same blob count too (<em>shadowed</em>; see <a href="#verification">Verifying the validator’s own rows</a>). Only rows that came back count; nothing is extrapolated.</p>
          </Def>

          <Def id="not-served" name="Not served" say="None of the validator’s answers served its rows. Its last answer gives the reason, in one of the words below.">
            <div className="ref-box ref-box--anchor" id="not-counted">
              <span id="gaps" className="ref-alias" aria-hidden="true" />
              <p className="ref-box-k">Counted neither way</p>
              <p>A validator whose rows did not come back is not counted as not served when any of its answers at the reading was:</p>
              <ul>
                <li>a request that failed on Tensile&rsquo;s side: before it reached the validator, on Tensile&rsquo;s own network, resolver or clock, or cut off by a restart;</li>
                <li>a request, or a re-ask it was owed, that was not made;</li>
                <li>rows of the blob that are not its own and that no settled promise explains;</li>
                <li>an answer that cannot be judged: an unrecognised outcome, or a chain version whose row assignment the pinned code cannot compute.</li>
              </ul>
              <p>Nor is anyone not served on a reading in which no request reached any server. In each case the validator counts neither way on that blob.</p>
              <p id="own-side">Two failures look the same whichever side caused them, so they count as the validator&rsquo;s only when Tensile&rsquo;s own side is shown working in the same minutes:</p>
              <ul>
                <li>a lookup of the validator&rsquo;s host name that fails other than with &ldquo;no such host&rdquo;: Tensile&rsquo;s resolver must have answered other validators&rsquo; names and a fresh test name, and its connections must have reached other servers;</li>
                <li>a connection turned away as unreachable: Tensile&rsquo;s connections over the same IP version must have reached other servers.</li>
              </ul>
              <p>Otherwise the failure is Tensile&rsquo;s gap.</p>
            </div>
            <Rules rows={[
              ["Provisional", "A not-served reading younger than 30 minutes: counted, and still open to withdrawal until then.", "provisional"],
            ]} />
            <p>A not-served reading can be challenged; see <a href="#corrections">Disputes and corrections</a>.</p>
            <h4 id="words" className="ref-h4">Words on a reading</h4>
            <p>Each answer is recorded with one of these words. A failure among them is not served only as the validator&rsquo;s last answer, and never in the cases above.</p>
            <Legend />
          </Def>

          <Def id="service-rate" aliases={["what-this-excludes"]} name="Service rate" say="The share of a validator’s endorsed shards that it served, over the period.">
            <Formula note="per endorsed shard">served / (served + not served)</Formula>
            <Rules rows={[
              ["Counts", "Every endorsed shard whose retention window has closed."],
              ["Not counted", "Shards still in their window (Awaiting check), shards on which it counts neither way, and blobs it did not endorse, which it does not owe."],
            ]} />
            <p>Beside it, the network median: the median of the validators&rsquo; own rates, over those with at least 20 decided shards.</p>
          </Def>

          <Def id="reachability" name="Reachability" say="The share of endpoint checks in the period that completed a TLS handshake with the validator’s registered Fibre endpoint.">
            <Formula note="a check every five minutes">completed handshakes / checks</Formula>
            <Rules rows={[
              ["Counts", "Every check from the main location, failed ones included."],
              ["Not counted", "Checks from the second location, and a check that failed before it left Tensile."],
            ]} />
            <p>A check asks for no blob data, so reachability says nothing about serving rows, and it is not signing uptime.</p>
            <p id="reachable-now">The status shown now, reachable or unreachable, is the newest check, with two allowances. One failed check after a success still shows reachable; two in a row show unreachable. An endpoint the main location cannot reach shows reachable when the second location completed a handshake with it in the last 15 minutes. Neither allowance changes the percentage.</p>
          </Def>

          <Def id="throughput" name="Throughput" say="The median download speed of the validator’s served shards of 2 MiB or more.">
            <Formula note="shown from three shards up">median(shard bytes / download time)</Formula>
            <p>Timed over the download alone: the dial, handshake and identity check cost the same for a small shard as for a large one.</p>
          </Def>

          <Def id="endorsements" aliases={["signing", "quorum"]} name="Endorsements" say="How often the validator’s verified signature is on the settlement, of the settled blobs that assigned it rows while it had a Fibre host registered.">
            <Formula>endorsed settlements / settlements that assigned it rows</Formula>
            <p>A validator endorses a payment promise by signing it after storing its shard. A settlement needs signatures from ⅔ of the voting power, and the first validators to respond fill it. Endorsing is not a duty: a missing endorsement is unproven, not a fault. An endorsement counts when the blob settles, not when it is read, and only endorsed shards are owed and read.</p>
            <p>On a blob&rsquo;s page, <strong>Endorsed</strong> is the voting power whose signature on that settlement verified.</p>
          </Def>

          <Def id="shard-data" aliases={["load"]} name="Shard data" say="The row data of the shards a validator stored and endorsed, read from the chain.">
            <p>Its rows of every settled blob it endorsed, kept for the retention window; each row is <code>blob_size / original_rows</code> bytes with padding. The row proofs stored beside them, about a fifth more on a 16 MiB blob, are not counted.</p>
            <Rules rows={[
              ["Rows per blob", "Its share of every blob, by stake."],
              ["Held now", "Shard data of the blobs it endorsed that are still in their retention window."],
            ]} />
          </Def>
        </section>

        {/* ------------------------------------------------------------ checks */}
        <section className="ref-sec">
          <H2 id="checks">How checks work</H2>
          <p className="ref-lead">Blob readings ask validators for rows; endpoint checks only open a connection. The two run independently.</p>

          <Def id="reading" aliases={["probe", "schedule"]} name="When a blob is read" say="Once, 10 minutes before its retention window ends.">
            <p>Every validator that endorsed the blob is asked for its own rows the way celestia-app&rsquo;s Fibre client asks for a shard: in the client&rsquo;s order, 15 s per request with one re-dial. Every row that comes back is verified against the blob commitment. Validators that did not endorse owe nothing and are not asked. Every request stays on record.</p>
            <p>A host name with several addresses is connected to as the client connects: the next address is tried 250 ms after the last, or at once when it fails, and the first to connect is used. One dead address does not fail the request.</p>
            <More summary="Timing limits">
              <p>A blob whose window, counted from its settlement, is 10 minutes or shorter is read half way through it. No request starts later than a minute before <code>must_serve_until</code>.</p>
            </More>
          </Def>

          <Def id="re-asks" aliases={["load-on-validators"]} name="Asked again" say={<>A validator whose answer did not serve is asked again, up to two more times, 90 s after its last answer, while more than a minute remains before <code>must_serve_until</code>.</>}>
            <p>Each answer is a row of its own; the last carries the result and its reason. An attempt the validator&rsquo;s own time leaves no room for is not owed. A validator is asked only about the blobs it endorsed: once per blob, and at most twice more.</p>
            <More summary="One failure, several waiting attempts">
              <p>A later attempt that fails before the validator&rsquo;s identity is verified (no such host, a connection refused, timed out or unroutable, a failed handshake or certificate) also answers that validator&rsquo;s other attempts waiting at the same host. Each gets a row of its own that names the request it repeats.</p>
            </More>
          </Def>

          <Def id="verification" name="Verifying the validator’s own rows" say="Every row is checked against the blob commitment, and the rows a validator returns against its own assignment.">
            <dl className="ref-terms ref-terms--map">
              <div className="ref-terms-h" aria-hidden="true"><dt>What came back</dt><dd>Result</dd></div>
              <Term term="Exactly its assigned rows, verified">served</Term>
              <Term term="Exactly another settled promise’s rows of the same blob">served (shadowed)</Term>
              <Term term="Fewer of its own rows than it holds">not served, as its last answer</Term>
              <Term term="Rows that fail the commitment, or no such shard">not served, as its last answer</Term>
              <Term term="Genuine rows of the blob, not its own, that match no settled promise">counted neither way</Term>
            </dl>
            <p>A request is addressed by commitment alone, and the store answers with the first shard in promise-hash order, so with several promises over one blob no client can ask for a particular promise&rsquo;s rows.</p>
            <More summary="When a verdict waits">
              <p>Rows that verify but match no promise on record yet may belong to one that settles later. Their verdict is drawn once every promise that could own them is on record, <code>payment_promise_timeout</code> after the request.</p>
            </More>
          </Def>

          <Def id="endpoint-checks" name="Endpoint checks" say="Every five minutes, the registered Fibre endpoint of each bonded validator gets DNS, TCP, a TLS 1.3 handshake and a check that its certificate is signed by the validator’s consensus key.">
            <p>No blob data is asked for. The checks give <a href="#reachability">Reachability</a> and the validator&rsquo;s TLS identity:</p>
            <Rules rows={[
              ["verified", "The certificate is signed by this validator’s consensus key."],
              ["expired", "The right key signed it, outside its validity window."],
              ["not this validator’s key", "Signed by another key: no client downloads from this endpoint."],
              ["no TLS", "The TLS handshake did not complete."],
            ]} />
          </Def>

          <Def id="locations" aliases={["vantage"]} name="Observation locations" say="Endpoints are checked from two locations; blobs are read from one, as one client’s download is.">
            <p>The second location&rsquo;s checks confirm the status shown now (see <a href="#reachable-now">Reachability</a>) and enter no percentage. A blob reading follows one network path, and asking again, twice, 90 s apart, keeps a passing failure on that path from counting.</p>
          </Def>
        </section>

        {/* -------------------------------------------------------- publishers */}
        <section className="ref-sec">
          <H2 id="publishers">Publishers and escrow</H2>
          <p className="ref-lead">A publisher is the account whose escrow pays for a blob. Every figure on the Publishers pages is read from the chain, except Tensile&rsquo;s readings of the blobs.</p>
          <p>Those carry Tensile&rsquo;s eye: <strong>Available</strong> on a publisher&rsquo;s page, and the <strong>Tensile</strong> column beside its blobs, as defined under <a href="#available">Available</a>.</p>
          <dl className="ref-terms">
            <Term id="fees" term="Fees">
              The module&rsquo;s charge for a blob, taken from the publisher&rsquo;s escrow; not the settlement transaction&rsquo;s own fee. No event carries the amount, so it is recomputed with the module&rsquo;s formula, at one utia per gas:
              <Formula note="the same charge when a promise times out">650,000 + 45,000 × ceil(blob size / 256 KiB) gas</Formula>
            </Term>
            <Term id="escrow" term="Escrow available">What the account can spend from its escrow now: its balance less what is queued to withdraw. An amber dot marks an account that cannot pay for one more blob of its usual size, or one whose queued withdrawal was partly spent by settlements.</Term>
            <Term id="timeouts" term="Timeouts">A payment promise not settled in time, charged as a blob. A floor: only the timeouts someone submitted are on chain.</Term>
            <Term id="withdrawals" term="Withdrawals">Read from chain state with each escrow balance, because settlements can draw on a queued withdrawal without emitting an event; that part is not paid out. A withdrawal is payable once the withdrawal delay has passed.</Term>
          </dl>
        </section>

        {/* ------------------------------------------------------------ params */}
        <section className="ref-sec">
          <H2 id="params">Protocol parameters</H2>
          <p className="ref-lead">Every deadline on the site is computed from these values, served at <code>/api/v1/params</code>.</p>
          <p>Chain parameters are read from chain state and every <code>EventUpdateFibreParams</code>. Protocol constants are exposed by no RPC, so they are read from the celestia-app build the observer runs.</p>
          <ProtocolParams />
        </section>

        {/* ---------------------------------------------------------- evidence */}
        <section className="ref-sec">
          <H2 id="evidence">Evidence and corrections</H2>
          <p className="ref-lead">Past observations are kept, and every result can be recomputed from them.</p>

          <Def id="record" name="The record behind a figure" say={<>Every figure is computed from the chain record and Tensile&rsquo;s stored readings, through a block each snapshot names (<code>record_through</code>).</>}>
            <p>Pages keep the two apart: <strong>On chain</strong> for what the chain records; <strong>Observed by Tensile</strong>, and Tensile&rsquo;s eye on a blob&rsquo;s page and in the Blobs list, for its readings. The 7d, 30d and all figures are summed from per-day records sealed once a day is final, each re-checked against the raw readings in turn; they equal a count over every row.</p>
          </Def>

          <Def id="exports" name="Signed daily exports" say="Each UTC day of the record is published as a tarball, with a manifest of SHA-256 digests signed with ed25519.">
            <p>The list is at <a href={`${API_URL}/exports`} rel="noopener noreferrer" target="_blank"><code>/api/v1/exports</code></a> and the signing key at <a href={`${API_URL}/exports/pubkey`} rel="noopener noreferrer" target="_blank"><code>/api/v1/exports/pubkey</code></a>; <code>sentinel-verify-export</code> checks the digests and the signature offline. Every reading row carries what produced it: the promise hash, the time, the wire outcome, the row indices returned, a digest of the returned rows, the gRPC status, the observer build and the chain&rsquo;s app version.</p>
          </Def>

          <Def id="recompute" aliases={["sampling"]} name="Recomputing a figure" say={<><code>sentinel-recompute</code> re-derives every verdict and obligation figure from an export or the live record, and compares them with the API at the same moment.</>}>
            <pre className="ref-code"><code>{`cd fibre-sentinel && go run ./cmd/sentinel-recompute -data-dir <dir> \\\n  -window 7d -as-of <RFC3339> -api ${API_ORIGIN}`}</code></pre>
            <p><code>?as_of=</code> on <code>/v1/network</code> and <code>/v1/validators</code> answers what was published at any past moment. The tool is in the <a href={SOURCE_URL} rel="noopener noreferrer" target="_blank">repository</a>.</p>
          </Def>

          <Def id="version" name="Methodology version" say={<>{METHODOLOGY_VERSION}.</>}>
            <p>Published as <code>methodology_version</code> in <code>/v1/meta</code> and in every export manifest. It changes with any rule that can move a figure.</p>
          </Def>

          <Def id="corrections" name="Disputes and corrections" say={<>To challenge a reading, open an issue with its promise hash and time, as the <a href={DISPUTE_URL} rel="noopener noreferrer" target="_blank">dispute route</a> describes.</>}>
            <p>A correction is recorded as an amendment: the reading keeps the verdict it was given beside the new one, with when and why it changed (<code>classification_at_probe</code>, <code>amended_at</code>). A deadline corrected after a parameter change only moves earlier, so that correction can withdraw a not-served reading, never add one.</p>
          </Def>
        </section>
      </article>
    </div>
  );
}
