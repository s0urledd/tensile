"use client";
import type { ReactNode } from "react";
import { type ValidatorDetail, type EndpointCheck, type Meta, int, ago, whenUTC, dateUTC } from "@/lib/api";

/**
 * The first thing on a validator's page: what state its Fibre endpoint is in,
 * in plain words, and what the operator can do about it.
 *
 * The page below is evidence — stage marks, rates, probe rows — and an
 * operator who arrives from a link in a chat wants the conclusion first. That
 * conclusion is derived here from fields the API already publishes and from
 * nothing else: the chain's own words (jailed, bond status, x/valaddr host)
 * first, then the newest handshake stage by stage, then the certificate check.
 * It never re-classifies a probe, and it does not repeat the not-served count:
 * the figures below carry it.
 *
 * Two rules the copy keeps:
 *
 * - It never states a fault. Unreachable, a lapsed certificate or a missing
 *   registration are states, and the text says so, because from one location
 *   an unreachable endpoint can be this observer's own path.
 * - One short paragraph: what was observed, whether it counts, then the
 *   general shape of the fix, pointing at the Celestia docs for the steps. A
 *   remote observer cannot see the operator's machine, and a confident
 *   specific instruction built on a guess would be worse than none.
 */

/** Celestia's own operator guide for the Fibre server; the anchors are its section ids. */
export const FIBRE_DOCS = "https://docs.celestia.org/operate/consensus-validators/fibre";
const REGISTER_DOCS = `${FIBRE_DOCS}#register-the-public-address`;
const TLS_DOCS = `${FIBRE_DOCS}#transport-security-tls`;
const TROUBLESHOOT_DOCS = `${FIBRE_DOCS}#troubleshoot-startup`;

/** identity reasons that mean "the right key, a lapsed window", as the API's identityStatus groups them */
const CLOCK_REASONS = new Set(["cert_not_yet_valid"]);

type Tone = "ok" | "hold" | "none";
type State = { tone: Tone; title: string; body: ReactNode };

/** host:port → [host, port]; a bare host keeps Fibre's default port */
function split(host: string): [string, string] {
  const m = /^\[?(.*?)\]?:(\d+)$/.exec(host);
  return m ? [m[1], m[2]] : [host, "7980"];
}

function Docs({ href, children }: { href: string; children: ReactNode }) {
  return <a href={href} rel="noopener noreferrer" target="_blank">{children}</a>;
}

function state(v: ValidatorDetail, c: EndpointCheck | undefined, decided: number): State {
  if (v.jailed) {
    return {
      tone: "none", title: "Jailed: out of the bonded set",
      body: <>Publishers skip it and this observer does not dial it{v.last_host && <> (last host <span className="mono">{v.last_host}</span>)</>}. Shards it signed for before are still owed; checks resume once it is back in the bonded set.</>,
    };
  }
  if (v.bond_status && v.bond_status !== "BOND_STATUS_BONDED") {
    const word = v.bond_status.replace("BOND_STATUS_", "").toLowerCase();
    return {
      tone: "none", title: `${word[0].toUpperCase()}${word.slice(1)}: out of the bonded set`,
      body: <>Only bonded validators are in the Fibre provider list, so nothing is checked. Shards it signed for while bonded are still owed.</>,
    };
  }
  if (!v.host) {
    return {
      tone: "none", title: "No Fibre endpoint registered",
      body: <>Nothing to check and nothing counted against it{v.last_host && <>; last host <span className="mono">{v.last_host}</span>{v.endpoint_closed_at && <> until {dateUTC(v.endpoint_closed_at)}</>}</>}. To register: <code>celestia-appd tx valaddr set-host &lt;host&gt;:7980 --from &lt;key&gt;</code> (<Docs href={REGISTER_DOCS}>guide</Docs>).</>,
    };
  }
  const [name, port] = split(v.host);
  const host = <span className="mono">{v.host}</span>;
  if (v.reachable === null) {
    return { tone: "none", title: "Registered, not checked yet", body: <>{host} is registered; the first check runs within five minutes.</> };
  }
  if (v.reachable === false) {
    // The newest heartbeat, when it is about the host registered now. A check
    // against an older host says nothing about this one.
    const chk = c && c.host === v.host ? c : undefined;
    const at = chk ? <>At {whenUTC(chk.at)}</> : <>At the newest check</>;
    const err = chk?.raw_error ? <> (<code>{chk.raw_error}</code>)</> : null;
    const notFault = <>{v.also_failed_from && <> Also failed from a second location.</>}{v.last_reachable_at ? <> Last handshake {ago(v.last_reachable_at)}.</> : null}</>;
    const refused = chk && (chk.outcome === "TCP_REFUSED" || /refused/i.test(chk.raw_error ?? ""));
    if (chk && !chk.dns_ok) {
      return {
        tone: "hold", title: "The host name does not resolve",
        body: <>{at}, <span className="mono">{name}</span> returned no address{err}.{notFault} Publish the DNS record on public DNS, or register the IP instead (<code>set-host &lt;ip&gt;:{port}</code>).</>,
      };
    }
    if (chk && !chk.tcp_ok && refused) {
      return {
        tone: "hold", title: `Connection refused on port ${port}`,
        body: <>{at}, {host} refused the connection.{notFault} Check that the Fibre server is running on a public interface and that port {port} is open to any address (<Docs href={TROUBLESHOOT_DOCS}>troubleshooting</Docs>).</>,
      };
    }
    if (chk && !chk.tcp_ok && (chk.outcome === "TCP_TIMEOUT" || /time(d)? ?out/i.test(chk.raw_error ?? ""))) {
      return {
        tone: "hold", title: `No answer on port ${port}`,
        body: <>{at}, {host} did not answer.{notFault} Usually a firewall or security group blocking port {port}, or the host is down; the port must be open to any address.</>,
      };
    }
    if (chk && !chk.tcp_ok) {
      // no route, network unreachable and the like: neither refused nor timed out
      return {
        tone: "hold", title: `Could not connect on port ${port}`,
        body: <>{at}, the connection to {host} failed{err}.{notFault} Check that the host is up and port {port} is reachable from the public internet.</>,
      };
    }
    if (chk && !chk.tls_ok) {
      return {
        tone: "hold", title: "TCP connects, TLS does not",
        body: <>{at}, port {port} accepted the connection but TLS did not complete{err}.{notFault} Another service may hold the port, or a proxy is not passing TLS through; the Fibre server terminates TLS itself (<Docs href={TLS_DOCS}>transport security</Docs>).</>,
      };
    }
    return {
      tone: "hold", title: "Unreachable at the newest check",
      body: <>{host} did not complete a TLS handshake{v.last_unreachable_at && <> ({ago(v.last_unreachable_at)})</>}{err}.{notFault}</>,
    };
  }
  const reason = v.identity_reason ? <> (<code>{v.identity_reason}</code>)</> : null;
  if (v.identity_status === "expired") {
    const clock = !!v.identity_reason && CLOCK_REASONS.has(v.identity_reason);
    return {
      tone: "hold", title: "The certificate endorsement has lapsed",
      body: <>{host} completes TLS, but the consensus-key endorsement is outside its validity window{reason}, so clients reject it and publishers will not upload here. {clock ? "Fix the server clock, then restart" : "Check the signer connection and the clock, then restart"} the Fibre server for a fresh endorsement (<Docs href={TLS_DOCS}>transport security</Docs>).</>,
    };
  }
  if (v.identity_status === "mismatch") {
    return {
      tone: "hold", title: "The certificate is not this validator’s",
      body: <>{host} completes TLS, but its certificate is not endorsed by this validator’s consensus key{reason}, so publishers will not upload here. Point the Fibre server’s signer at this validator’s own key and chain ID, and check the registered host is this validator’s server (<Docs href={TLS_DOCS}>transport security</Docs>).</>,
    };
  }
  if (v.identity_status !== "verified") {
    return { tone: "none", title: "Reachable; certificate not checked yet", body: <>{host} completed TLS {(v.last_reachable_at || v.last_seen_at) && ago(v.last_reachable_at || v.last_seen_at)}; the consensus-key check has not run on it yet.</> };
  }
  if (v.confirmed_from) {
    return {
      tone: "hold", title: "Reachable from a second location only",
      body: <>The main check could not complete TLS with {host}, but a second location did {v.last_seen_at ? ago(v.last_seen_at) : "within the last 15 minutes"}, with this validator’s certificate. It counts as reachable. If some publishers fail too, look for firewall rules, geo-blocking or routing that treat source addresses differently.</>,
    };
  }
  const o = v.obligations;
  return {
    tone: "ok", title: "Reachable, certificate verified",
    body: <>{host} completed TLS with a certificate endorsed by this validator’s consensus key {(v.last_reachable_at || v.last_seen_at) ? ago(v.last_reachable_at || v.last_seen_at) : "at the newest check"}.{o && decided > 0 && o.broken === 0 && <> {int(o.served)} of {int(decided)} endorsed shard{decided === 1 ? "" : "s"} read in this period {o.served === 1 ? "was" : "were"} served.</>}</>,
  };
}

export default function Diagnosis({ v, check, decided }: {
  v: ValidatorDetail;
  check?: EndpointCheck;
  meta?: Meta | null;
  /** served + broken obligations in the period */
  decided: number;
}) {
  const s = state(v, check, decided);
  const broken = v.obligations?.broken ?? 0;
  // A healthy endpoint with nothing broken needs no box: the state pills above already say so.
  // With shards not served, the box stays and says the endpoint answers, so the count below
  // is not read as an outage.
  if (s.tone === "ok" && broken === 0) return null;
  return (
    <section className={"diag " + s.tone} aria-label="Endpoint status">
      <p className="diag-h"><i className={"dot " + (s.tone === "ok" ? "ok" : s.tone === "hold" ? "hold" : "none")} />{s.title}</p>
      <p>{s.body}</p>
    </section>
  );
}
