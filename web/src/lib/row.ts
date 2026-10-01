import type React from "react";

/**
 * A table row that opens a page, with no link stretched over it: a phone's browser that does not make a
 * table row the box of an absolutely placed link spreads every row's cover over the page, the last row's
 * on top, and a tap anywhere opened the last row. A click on a link or a button in the row is that one's
 * own; a plain click goes through go (the app's router); a modified or middle click opens a new tab, as a
 * link would.
 */
export function openRow(e: React.MouseEvent, href: string, go: (e: React.MouseEvent, href: string) => void) {
  if ((e.target as HTMLElement).closest("a, button") || window.getSelection()?.toString()) return;
  if (e.button === 1 || e.metaKey || e.ctrlKey || e.shiftKey) { window.open(href, "_blank", "noopener"); return; }
  if (e.button === 0) go(e, href);
}
