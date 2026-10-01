/**
 * A publisher's mark: a 5×5 pattern mirrored about its middle column, drawn
 * from the address, in the accent only. It lets the eye tell two publishers
 * apart in a list without a colour that needs a key; it sits beside the
 * address and never stands in for it.
 */
export default function Ident({ addr }: { addr: string }) {
  // FNV-1a over the address: fifteen bits, one per cell of the left half and the middle column
  let h = 2166136261;
  for (let i = 0; i < addr.length; i++) { h ^= addr.charCodeAt(i); h = Math.imul(h, 16777619) >>> 0; }
  let bits = h & 0x7fff;
  let on = 0;
  for (let i = 0; i < 15; i++) on += (bits >> i) & 1;
  // a pattern nearly empty or nearly full reads as no pattern: flip half its cells
  if (on < 5 || on > 11) bits ^= 0x5a5a;
  let d = "";
  for (let y = 0; y < 5; y++) {
    for (let x = 0; x < 3; x++) {
      if (!((bits >> (y * 3 + x)) & 1)) continue;
      d += `M${x} ${y}h1v1h-1z`;
      if (x < 2) d += `M${4 - x} ${y}h1v1h-1z`;
    }
  }
  return (
    <svg className="ident" viewBox="-1 -1 7 7" width="16" height="16" aria-hidden="true" focusable="false">
      <rect className="ident-bg" x="-1" y="-1" width="7" height="7" rx="1.7" />
      <path d={d} />
    </svg>
  );
}
