/**
 * Headings of a reference page that can be linked to: each carries its id, a
 * "#" link to itself that shows on hover and focus, and any older ids the
 * section answered to, so a link made before a section moved or was renamed
 * still lands on it.
 */
function Aliases({ ids }: { ids?: string[] }) {
  return <>{ids?.map((a) => <span key={a} id={a} className="ref-alias" aria-hidden="true" />)}</>;
}

function Hash({ id, label }: { id: string; label: string }) {
  return <a className="ref-hash" href={`#${id}`} aria-label={`Link to ${label}`}>#</a>;
}

export function H2({ id, aliases, children }: { id: string; aliases?: string[]; children: string }) {
  return <h2 id={id} className="ref-h2"><Aliases ids={aliases} />{children}<Hash id={id} label={children} /></h2>;
}

export function H3({ id, aliases, children, className }: { id: string; aliases?: string[]; children: string; className?: string }) {
  return <h3 id={id} className={`ref-h3${className ? " " + className : ""}`}><Aliases ids={aliases} />{children}<Hash id={id} label={children} /></h3>;
}
