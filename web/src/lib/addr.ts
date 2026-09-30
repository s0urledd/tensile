/**
 * Address spellings an operator might paste, matched without a round trip.
 *
 * A validator's operator address (celestiavaloper1…) and the account it signs
 * with (celestia1…) are the same twenty bytes under two prefixes, so an
 * operator who pastes their account address into the search box is asking for
 * their validator. Substring matching on operator_address cannot see that; the
 * bytes can. The consensus address is a different key altogether and is
 * never derived here: the API resolves operator and account addresses to it
 * through the staking set (observer/api/validator_addr.go).
 */

const B32 = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";

/**
 * The payload bytes of a bech32 string as lower-case hex, or null when it is
 * not bech32. The checksum is not verified: this only ever compares two
 * strings for the same bytes, and a mistyped checksum on the pasted side
 * cannot make two different payloads equal.
 */
export function bech32Hex(s: string): string | null {
  const str = s.trim().toLowerCase();
  const sep = str.lastIndexOf("1");
  if (sep < 1 || str.length - sep < 8) return null;
  const words: number[] = [];
  for (const ch of str.slice(sep + 1, -6)) {
    const v = B32.indexOf(ch);
    if (v < 0) return null;
    words.push(v);
  }
  // five-bit groups back to bytes, dropping the zero padding at the end
  let acc = 0, bits = 0, out = "";
  for (const w of words) {
    acc = (acc << 5) | w;
    bits += 5;
    if (bits >= 8) {
      bits -= 8;
      out += ((acc >> bits) & 0xff).toString(16).padStart(2, "0");
    }
  }
  return out;
}

/**
 * Whether needle is the account address behind operator: a celestia1…
 * string (any account prefix, no "val" in it) with the same bytes as the
 * operator address.
 */
export function isOperatorAccount(needle: string, operator: string | undefined): boolean {
  if (!operator || !/^[a-z]+1[02-9ac-hj-np-z]{38,}$/.test(needle)) return false;
  const hrp = needle.slice(0, needle.lastIndexOf("1"));
  if (hrp.includes("val")) return false;
  const a = bech32Hex(needle);
  return a !== null && a.length === 40 && a === bech32Hex(operator);
}

/**
 * The address a link to a validator's page carries: its operator address
 * (celestiavaloper1…), the one explorers list and operators know, whenever
 * the staking set names one, else the address the row already carries. The
 * page and the API take every spelling (observer/api/validator_addr.go), so
 * a link made with a consensus address, hex or celestiavalcons1…, still opens
 * the same page.
 */
export function pageAddr(operator: string | undefined, fallback: string): string {
  return operator || fallback;
}

/** The validator page of one validator, by pageAddr; query is appended as given ("&window=7d"). */
export function validatorHref(operator: string | undefined, fallback: string, query = ""): string {
  return `/validator/?addr=${encodeURIComponent(pageAddr(operator, fallback))}${query}`;
}
