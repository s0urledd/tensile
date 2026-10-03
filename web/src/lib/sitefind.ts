import type { BlobKey } from "@/lib/blobkey";

/**
 * Where the header's search takes what a reader pasted: a blob identifier to the Blobs list of what it matches (a
 * transaction hash, a blob ID in base64 or hex, a commitment or a promise hash, read by blobKey), a validator's
 * operator or consensus address to that validator's page, and an account address to that publisher's page. Only
 * identifiers: a name or any other words go nowhere. The parser is passed in, so this module has no imports to load.
 */
export type SiteTarget = { kind: "blob" | "validator" | "publisher"; href: string };

/** a bech32 address with this prefix and a 20-byte payload: 32 characters of data and 6 of checksum */
const bech = (prefix: string) => new RegExp(`^${prefix}1[02-9ac-hj-np-z]{38}$`);
const VALOPER = bech("celestiavaloper");
const VALCONS = bech("celestiavalcons");
const ACCOUNT = bech("celestia");

export function siteTarget(s: string | null | undefined, blobKey: (s: string) => BlobKey | null): SiteTarget | null {
  const t = (s ?? "").trim();
  if (!t) return null;
  const k = blobKey(t);
  // the Blobs page keeps a hash in lower-case hex and a blob ID in base64 in its address
  if (k) return { kind: "blob", href: `/blobs/?blob=${encodeURIComponent(k.kind === "id" ? k.id : k.hex)}` };
  const a = t.toLowerCase();
  if (VALOPER.test(a) || VALCONS.test(a)) return { kind: "validator", href: `/validator/?addr=${a}` };
  if (ACCOUNT.test(a)) return { kind: "publisher", href: `/publisher/?addr=${a}` };
  return null;
}
