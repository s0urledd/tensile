/**
 * Who this deployment says it is.
 *
 * Read at build time so an operator running this code for their own network
 * puts their own repository and validator here without editing a component —
 * and so that moving this repository, which is a decision still open, is one
 * environment variable rather than a search across the site.
 */
export const SOURCE_URL = (process.env.NEXT_PUBLIC_SOURCE_URL ?? "https://github.com/s0urledd/tensile").replace(/\/$/, "");

/**
 * The API's public /v1 address, which the API page prints without the /v1 as
 * its base URL. The site's own pages call the same routes on their own origin
 * (/api), so this is only what a reader copies. Unset, the page prints its
 * own site's /api once it runs, since one export serves every network's site,
 * and the public deployment's address until then.
 */
export const API_URL_FIXED = !!process.env.NEXT_PUBLIC_API_URL;
export const API_URL = (process.env.NEXT_PUBLIC_API_URL || "https://tensile.huginn.tech/api/v1").replace(/\/$/, "");

/**
 * Where a reader sees a settlement transaction in the chain's own record: a block explorer for the network the API
 * names (/v1/meta's chain_id), since one export serves every network's site. Celenium, which Celestia's docs list for
 * Mocha, shows the transaction's MsgPayForFibre and its blob, and takes the hash in either case. An operator sets
 * NEXT_PUBLIC_TX_EXPLORER, a URL with {hash} in it, for a network not here; the networks here keep their own.
 */
const TX_EXPLORERS: [RegExp, string][] = [
  [/^mocha-\d+$/, "https://mocha.celenium.io/tx/{hash}"],
  [/^celestia$/, "https://celenium.io/tx/{hash}"],
];
const TX_EXPLORER = (process.env.NEXT_PUBLIC_TX_EXPLORER ?? "").trim();

/** the explorer's page for a transaction on the network chainId, or "" where none is known (or the network is not yet) */
export function txExplorerUrl(chainId: string | null | undefined, hash: string): string {
  const url = chainId ? TX_EXPLORERS.find(([re]) => re.test(chainId))?.[1] || TX_EXPLORER : "";
  return url && hash ? url.replace("{hash}", hash.toUpperCase()) : "";
}

/** The dispute route: what to do about a verdict you think is wrong. */
export const DISPUTE_URL = `${SOURCE_URL}/blob/main/docs/verdicts.md#disputing-a-verdict`;

/**
 * The validator this observer's own operator runs on the measured network,
 * as the API reports its address. Empty disables the marking entirely.
 *
 * It is used for one thing: marking that row in the table, because a site
 * that grades operators and is run by one of them should say which row is
 * its own. Nothing filters, excludes, ranks or adjusts on it — the figures
 * about that validator come from the same code and the same rows as
 * everyone else's, which is the point of saying which row it is.
 */
export const SELF_VALIDATOR = (process.env.NEXT_PUBLIC_SELF_VALIDATOR ?? "").trim().toLowerCase();
