import type { BlobKey } from "@/lib/blobkey";

/**
 * What the header's search reads in what a reader pasted, and where its records live: a blob identifier (a transaction
 * hash, a blob ID in base64 or hex, a commitment or a promise hash, read by blobKey) and the Blobs list of what it
 * matches, a validator's operator or consensus address and its page, an account address and its publisher's page. id
 * is the identifier as the site keeps it: a hash in lower-case hex, a blob ID in base64, an address in lower case. Only
 * identifiers: a name or any other words are none. The parser is passed in, so this module has no imports to load.
 */
export type SiteTarget = { kind: "blob" | "validator" | "publisher"; id: string; href: string };

/** a bech32 address with this prefix and a 20-byte payload: 32 characters of data and 6 of checksum */
const bech = (prefix: string) => new RegExp(`^${prefix}1[02-9ac-hj-np-z]{38}$`);
const VALOPER = bech("celestiavaloper");
const VALCONS = bech("celestiavalcons");
const ACCOUNT = bech("celestia");
/** a consensus address in hex, the key the API keeps a validator's rows under */
const CONS_HEX = /^[0-9a-f]{40}$/;
/** an account address of any length: a publisher can be an account of 32 bytes as well as 20 */
const PUBLISHER = /^celestia1[02-9ac-hj-np-z]{38,}$/;

/**
 * The address a validator's page asks the API for, in lower case: its consensus address (40 hex characters or
 * celestiavalcons1…), its operator address or the operator's account address. Null for anything else, which the page
 * says is no validator's address and never puts in the API's path: a value from a link could otherwise carry its own
 * query or a dot segment into it.
 */
export function validatorAddr(s: string | null | undefined): string | null {
  const a = (s ?? "").trim().toLowerCase();
  return CONS_HEX.test(a) || VALOPER.test(a) || VALCONS.test(a) || ACCOUNT.test(a) ? a : null;
}

/** The account address a publisher's page asks the API for, in lower case; null for anything else, as validatorAddr. */
export function publisherAddr(s: string | null | undefined): string | null {
  const a = (s ?? "").trim().toLowerCase();
  return PUBLISHER.test(a) ? a : null;
}

export function siteTarget(s: string | null | undefined, blobKey: (s: string) => BlobKey | null): SiteTarget | null {
  const t = (s ?? "").trim();
  if (!t) return null;
  const k = blobKey(t);
  // the Blobs page keeps a hash in lower-case hex and a blob ID in base64 in its address
  if (k) { const id = k.kind === "id" ? k.id : k.hex; return { kind: "blob", id, href: `/blobs/?blob=${encodeURIComponent(id)}` }; }
  const a = t.toLowerCase();
  if (VALOPER.test(a) || VALCONS.test(a)) return { kind: "validator", id: a, href: `/validator/?addr=${a}` };
  if (ACCOUNT.test(a)) return { kind: "publisher", id: a, href: `/publisher/?addr=${a}` };
  return null;
}
