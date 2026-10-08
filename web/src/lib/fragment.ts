/**
 * The id a page's address points to after its #, decoded: the heading or row a link to it opens. A fragment that is no
 * valid escape ("#100%", a link typed or cut short by hand) is taken as it stands, since decoding it throws, and a throw
 * in the effect that reads it would take the whole page down.
 */
export function fragmentId(hash: string): string {
  const raw = hash.replace(/^#/, "");
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}
