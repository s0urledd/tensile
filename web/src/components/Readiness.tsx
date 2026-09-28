import type { Validator } from "@/lib/api";

/**
 * How much of the bonded stake has a Fibre provider registered. A
 * MsgPayForFibre settles only with signatures from validators holding at
 * least floor(2/3 of total voting power) (x/fibre, keeper/msg_server.go), and
 * a validator signs only through a Fibre server it has registered in
 * x/valaddr. Both halves are the chain's own records: the bonded set from
 * x/staking, the providers from x/valaddr AllBondedFibreProviders. Whether a
 * host answers is this observer's check, and stays in the table.
 */

/** the endpoint's standing, from the API's endpoint_state when it sends one */
export type EndpointState = "reachable" | "unreachable" | "none";
type WithState = Validator & { endpoint_state?: EndpointState };

export function endpointState(v: Validator): EndpointState {
  const s = (v as WithState).endpoint_state;
  if (s === "reachable" || s === "unreachable" || s === "none") return s;
  // a registered host this observer has not checked yet is no one's failure
  if (!v.host || v.reachable == null) return "none";
  return v.reachable ? "reachable" : "unreachable";
}

export function bondedOf(rows: Validator[]): Validator[] {
  return rows.filter((v) => !v.jailed && (!v.bond_status || v.bond_status === "BOND_STATUS_BONDED"));
}

export function readiness(rows: Validator[]) {
  const bonded = bondedOf(rows);
  const total = bonded.reduce((s, v) => s + (v.voting_power || 0), 0);
  const registered = bonded.filter((v) => !!v.host);
  const regPower = registered.reduce((s, v) => s + (v.voting_power || 0), 0);
  const quorum = Math.floor((total * 2) / 3);
  const pct = (n: number) => (total > 0 ? `${((100 * n) / total).toFixed(1)}%` : "—");
  return { bonded, total, registered, regPower, quorum, pct };
}
