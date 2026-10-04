import { type ValidatorDetail, type EndpointCheck, int, ago, whenUTC, dateUTC } from "@/lib/api";

/**
 * What state a validator's Fibre endpoint is in, in plain words, and what the
 * operator can do about it. The validator page shows the title in its profile
 * panel, beside the name: a state to act on with an amber dot whose words are
 * the text, a neutral one with the words on hover, and a link to the Celestia
 * docs where they have a section for it.
 *
 * The conclusion is derived here from fields the API already publishes and
 * from nothing else: the chain's own words (jailed, bond status, x/valaddr
 * host) first, then the newest handshake stage by stage, then the certificate
 * check. It never re-classifies a probe, and it does not repeat the
 * not-served count: the figures carry it.
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

export type DiagnosisTone = "ok" | "hold" | "none";
/** the state's title, its paragraph as plain words, and the docs section for it with the link's word */
export type DiagnosisState = { tone: DiagnosisTone; title: string; text: string; docs?: { href: string; word: string } };

/** host:port → [host, port]; a bare host keeps Fibre's default port */
function split(host: string): [string, string] {
  const m = /^\[?(.*?)\]?:(\d+)$/.exec(host);
  return m ? [m[1], m[2]] : [host, "7980"];
}

export function diagnose(v: ValidatorDetail, c: EndpointCheck | undefined, decided: number): DiagnosisState {
  if (v.jailed) {
    return {
      tone: "none", title: "Jailed: out of the bonded set",
      text: `Publishers skip it and its endpoint is not checked${v.last_host ? ` (last host ${v.last_host})` : ""}. Shards it endorsed are still owed.`,
    };
  }
  if (v.bond_status && v.bond_status !== "BOND_STATUS_BONDED") {
    const word = v.bond_status.replace("BOND_STATUS_", "").toLowerCase();
    return {
      tone: "none", title: `${word[0].toUpperCase()}${word.slice(1)}: out of the bonded set`,
      text: "Only bonded validators are in the Fibre provider list, so nothing is checked. Shards it endorsed while bonded are still owed.",
    };
  }
  if (!v.host) {
    return {
      tone: "none", title: "No Fibre endpoint registered",
      text: `Nothing to check${v.last_host ? `; last host ${v.last_host}${v.endpoint_closed_at ? ` until ${dateUTC(v.endpoint_closed_at)}` : ""}` : ""}. Shards it endorsed before are still owed. To register: celestia-appd tx valaddr set-host <host>:7980 --from <key>.`,
      docs: { href: REGISTER_DOCS, word: "Guide" },
    };
  }
  const [name, port] = split(v.host);
  const host = v.host;
  if (v.reachable === null) {
    return { tone: "none", title: "Registered, not checked yet", text: `${host} is registered; the first check runs within five minutes.` };
  }
  if (v.reachable === false) {
    // The newest heartbeat, when it is about the host registered now. A check
    // against an older host says nothing about this one.
    const chk = c && c.host === v.host ? c : undefined;
    const at = chk ? `At ${whenUTC(chk.at)}` : "At the newest check";
    const err = chk?.raw_error ? ` (${chk.raw_error})` : "";
    const notFault = `${v.also_failed_from ? " Also failed from a second location." : ""}${v.last_reachable_at ? ` Last handshake ${ago(v.last_reachable_at)}.` : ""}`;
    const refused = chk && (chk.outcome === "TCP_REFUSED" || /refused/i.test(chk.raw_error ?? ""));
    if (chk && !chk.dns_ok) {
      return {
        tone: "hold", title: "The host name does not resolve",
        text: `${at}, ${name} returned no address${err}.${notFault} Publish the DNS record on public DNS, or register the IP instead (set-host <ip>:${port}).`,
      };
    }
    if (chk && !chk.tcp_ok && refused) {
      return {
        tone: "hold", title: `Connection refused on port ${port}`,
        text: `${at}, ${host} refused the connection.${notFault} Check that the Fibre server is running on a public interface and that port ${port} is open to any address.`,
        docs: { href: TROUBLESHOOT_DOCS, word: "Troubleshooting" },
      };
    }
    if (chk && !chk.tcp_ok && (chk.outcome === "TCP_TIMEOUT" || /time(d)? ?out/i.test(chk.raw_error ?? ""))) {
      return {
        tone: "hold", title: `No answer on port ${port}`,
        text: `${at}, ${host} did not answer.${notFault} Usually a firewall or security group blocking port ${port}, or the host is down; the port must be open to any address.`,
      };
    }
    if (chk && !chk.tcp_ok) {
      // no route, network unreachable and the like: neither refused nor timed out
      return {
        tone: "hold", title: `Could not connect on port ${port}`,
        text: `${at}, the connection to ${host} failed${err}.${notFault} Check that the host is up and port ${port} is reachable from the public internet.`,
      };
    }
    if (chk && !chk.tls_ok) {
      return {
        tone: "hold", title: "TCP connects, TLS does not",
        text: `${at}, port ${port} accepted the connection but TLS did not complete${err}.${notFault} Another service may hold the port, or a proxy is not passing TLS through; the Fibre server terminates TLS itself.`,
        docs: { href: TLS_DOCS, word: "Transport security" },
      };
    }
    return {
      tone: "hold", title: "Unreachable at the newest check",
      text: `${host} did not complete a TLS handshake${v.last_unreachable_at ? ` (${ago(v.last_unreachable_at)})` : ""}${err}.${notFault}`,
    };
  }
  const reason = v.identity_reason ? ` (${v.identity_reason})` : "";
  if (v.identity_status === "expired") {
    const clock = !!v.identity_reason && CLOCK_REASONS.has(v.identity_reason);
    return {
      tone: "hold", title: "The certificate endorsement has lapsed",
      text: `${host} completes TLS, but its certificate endorsement is outside its validity window${reason}, so publishers will not upload here. ${clock ? "Fix the server clock, then restart" : "Check the signer connection and the clock, then restart"} the Fibre server for a fresh one.`,
      docs: { href: TLS_DOCS, word: "Transport security" },
    };
  }
  if (v.identity_status === "mismatch") {
    return {
      tone: "hold", title: "The certificate is not this validator’s",
      text: `${host} completes TLS, but its certificate is not endorsed by this validator’s consensus key${reason}, so publishers will not upload here. Point the Fibre server’s signer at this validator’s key and chain ID, and check the registered host is its server.`,
      docs: { href: TLS_DOCS, word: "Transport security" },
    };
  }
  const lastOk = v.last_reachable_at || v.last_seen_at;
  if (v.identity_status !== "verified") {
    return { tone: "none", title: "Reachable; certificate not checked yet", text: `${host} completed TLS${lastOk ? ` ${ago(lastOk)}` : ""}; the consensus-key check has not run on it yet.` };
  }
  if (v.confirmed_from) {
    return {
      tone: "hold", title: "Reachable from a second location only",
      text: `The main check could not complete TLS with ${host}; a second location did ${v.last_seen_at ? ago(v.last_seen_at) : "within the last 15 minutes"}, with this validator’s certificate, so it counts as reachable. If publishers fail too, check firewall, geo-blocking or routing rules.`,
    };
  }
  const o = v.obligations;
  return {
    tone: "ok", title: "Reachable, certificate verified",
    text: `${host} completed TLS with a certificate endorsed by this validator’s consensus key ${lastOk ? ago(lastOk) : "at the newest check"}.${o && decided > 0 && o.broken === 0 ? ` ${int(o.served)} of ${int(decided)} endorsed shard${decided === 1 ? "" : "s"} read in this period ${o.served === 1 ? "was" : "were"} served.` : ""}`,
  };
}
