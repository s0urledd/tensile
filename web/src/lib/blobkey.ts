/**
 * The identifiers a developer holds for a blob, as the site reads them: a promise hash, a commitment or the hash of the
 * transaction that settled it (each 64 hex characters, in either case, with or without 0x), or the blob ID the Fibre
 * client returns. A blob ID is one version byte, 0, then the 32-byte commitment (celestia-app's fibre.BlobID): the
 * client's JSON prints it in base64, standard or URL-safe, with or without padding, and BlobID.String in hex (66
 * characters). Anything else is no blob's identifier.
 */
export type BlobKey =
  /** 64 hex characters, in lower case: a promise hash, a commitment or a transaction hash, which only a lookup tells apart */
  | { kind: "hash"; hex: string }
  /** a blob ID: its commitment in lower-case hex, and the ID as the client prints it (standard base64) */
  | { kind: "id"; hex: string; id: string };

const B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/** the bytes of base64, standard or URL-safe, padded or not; null for anything that is not base64 */
function unbase64(s: string): number[] | null {
  const t = s.replace(/-/g, "+").replace(/_/g, "/").replace(/={1,2}$/, "");
  if (!/^[A-Za-z0-9+/]*$/.test(t) || t.length % 4 === 1) return null;
  const out: number[] = [];
  let acc = 0, bits = 0;
  for (const ch of t) {
    acc = ((acc << 6) | B64.indexOf(ch)) & 0xffff;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out.push((acc >> bits) & 0xff);
    }
  }
  return out;
}

function base64(bytes: number[]): string {
  let out = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const n = (bytes[i] << 16) | ((bytes[i + 1] ?? 0) << 8) | (bytes[i + 2] ?? 0);
    out += B64[(n >> 18) & 63] + B64[(n >> 12) & 63] + (i + 1 < bytes.length ? B64[(n >> 6) & 63] : "=") + (i + 2 < bytes.length ? B64[n & 63] : "=");
  }
  return out;
}

const hexOf = (bytes: number[]) => bytes.map((b) => b.toString(16).padStart(2, "0")).join("");

/** the blob ID of a version-0 blob (the only version Fibre has), from its commitment, as the client prints it */
export function blobIdOf(commitment: string): string {
  const c = commitment.toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(c)) return "";
  const bytes = [0];
  for (let i = 0; i < 64; i += 2) bytes.push(parseInt(c.slice(i, i + 2), 16));
  return base64(bytes);
}

/**
 * Reads what a reader typed, pasted or put in an address. A blob ID in an address may come back with its + read as a
 * space, which base64 never holds, so a space inside is a +.
 */
export function blobKey(s: string | null | undefined): BlobKey | null {
  const t = (s ?? "").trim();
  if (!t) return null;
  const h = t.toLowerCase().replace(/^0x/, "");
  if (/^[0-9a-f]{64}$/.test(h)) return { kind: "hash", hex: h };
  // BlobID.String: the version byte and the commitment in hex
  if (/^00[0-9a-f]{64}$/.test(h)) return { kind: "id", hex: h.slice(2), id: blobIdOf(h.slice(2)) };
  const bytes = unbase64(t.replace(/ /g, "+"));
  if (!bytes || bytes.length !== 33 || bytes[0] !== 0) return null;
  const hex = hexOf(bytes.slice(1));
  return { kind: "id", hex, id: blobIdOf(hex) };
}

/** the form a found blob keeps in the address and the search's chip: a hash in lower-case hex, a blob ID in base64 */
export const keyText = (k: BlobKey): string => (k.kind === "id" ? k.id : k.hex);
