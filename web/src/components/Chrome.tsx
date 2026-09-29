"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useRef, useState } from "react";
import { useApi, type Meta, type Tip, int, ago, since, span, utcWord } from "@/lib/api";
import { SOURCE_URL, DISPUTE_URL } from "@/lib/site";

/**
 * Sibling deployments of this observer on other networks, from
 * NEXT_PUBLIC_NETWORKS ("mainnet=https://observer.example.org,mocha=https://mocha.observer.example.org"),
 * baked in at build time. The one whose origin we are on is marked; the
 * others are links. One build serves every network, since each site talks
 * to its own same-origin /api.
 */
const NETWORKS: [string, string][] = (process.env.NEXT_PUBLIC_NETWORKS ?? "")
  .split(",").map((s) => s.trim()).filter(Boolean)
  .map((s) => { const i = s.indexOf("="); return i > 0 ? [s.slice(0, i).trim(), s.slice(i + 1).trim()] as [string, string] : null; })
  .filter((x): x is [string, string] => !!x);

const NAV: [string, string][] = [
  ["/", "Overview"],
  ["/blobs/", "Blobs"],
  ["/publishers/", "Publishers"],
  ["/developers/", "API"],
];

/**
 * The mark: a tensile specimen between the grips of a testing machine, its
 * gauge section marked. Tensile holds a validator to what it signed for
 * across the whole retention window and records whether it held.
 */
function Mark() {
  return (
    <svg className="mark-logo" width="22" height="22" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
      <rect x="3.5" y="1.5" width="17" height="4.5" rx="1.3" />
      <rect x="3.5" y="18" width="17" height="4.5" rx="1.3" />
      <path opacity=".5" d="M7.4 6H16.6C16.6 8.3 13.5 8.4 13.5 10.1V13.9C13.5 15.6 16.6 15.7 16.6 18H7.4C7.4 15.7 10.5 15.6 10.5 13.9V10.1C10.5 8.4 7.4 8.3 7.4 6Z" />
      <rect x="8.6" y="11.3" width="6.8" height="1.4" rx=".45" />
    </svg>
  );
}

type Theme = "light" | "dark";

/** Two states, sun and moon. The first visit starts from the system
 *  preference; from then on the choice is the reader's and is remembered. */
function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>("light");
  useEffect(() => {
    let t: string | null = null;
    try { t = localStorage.getItem("theme"); } catch { /* storage unavailable */ }
    if (t !== "light" && t !== "dark") {
      t = window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    }
    setTheme(t as Theme);
    document.documentElement.dataset.theme = t;
  }, []);
  const apply = (t: Theme) => {
    setTheme(t);
    document.documentElement.dataset.theme = t;
    try { localStorage.setItem("theme", t); } catch { /* fine */ }
  };
  const next: Theme = theme === "light" ? "dark" : "light";
  const title = theme === "light" ? "Theme: light. Switch to dark." : "Theme: dark. Switch to light.";
  return (
    <button type="button" className="theme" onClick={() => apply(next)} title={title} aria-label={title}>
      {theme === "dark" ? (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5z" /></svg>
      ) : (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" aria-hidden="true"><circle cx="12" cy="12" r="4" /><path d="M12 2.5v2.5M12 19v2.5M2.5 12H5M19 12h2.5M4.9 4.9l1.8 1.8M17.3 17.3l1.8 1.8M4.9 19.1l1.8-1.8M17.3 6.7l1.8-1.8" /></svg>
      )}
    </button>
  );
}

/** "mocha-5" as the chip prints it; the mainnet chain id is "celestia" */
export function netName(id: string | undefined): string {
  if (!id) return "network";
  if (id === "celestia" || /^mainnet$/i.test(id)) return "Mainnet";
  return id.charAt(0).toUpperCase() + id.slice(1);
}

/**
 * The network chip: which chain this observer watches, with the observer's
 * own state as its dot. It opens as a menu: this site's network, any sibling
 * deployment configured in NEXT_PUBLIC_NETWORKS as a link, and Mainnet as a
 * placeholder until Fibre is there and an observer follows it.
 */
function NetworkChip({ meta, error }: { meta: Meta | null; error: string | null }) {
  const [origin, setOrigin] = useState("");
  const box = useRef<HTMLDetailsElement>(null);
  useEffect(() => { setOrigin(window.location.origin); }, []);
  // a click elsewhere or Escape closes the menu, as a menu is expected to
  useEffect(() => {
    const close = (e: Event) => { const el = box.current; if (el?.open && !(e instanceof KeyboardEvent ? false : el.contains(e.target as Node))) el.open = false; };
    const key = (e: KeyboardEvent) => { if (e.key === "Escape" && box.current?.open) box.current.open = false; };
    document.addEventListener("click", close);
    document.addEventListener("keydown", key);
    return () => { document.removeEventListener("click", close); document.removeEventListener("keydown", key); };
  }, []);
  const health = meta?.health;
  // unknown while the API does not answer, whatever the last reading said
  const dot = error || !meta ? "none" : health === "ok" ? "ok" : "hold";
  const title = !meta
    ? (error ? `Observer API unreachable: ${error}` : "Connecting to the observer API…")
    : [
      `network ${meta.chain_id}`,
      error ? `observer state unknown: API unreachable (${error})` : `observer ${health === "ok" ? "healthy" : health}`,
      meta.chain_height ? `chain tip #${int(Number(meta.chain_height))}` : "",
      meta.app_version ? (meta.fibre_active ? `Fibre live on app v${meta.app_version}` : `Fibre not live: app v${meta.app_version}`) : "",
    ].filter(Boolean).join(" · ");
  const label = meta ? netName(meta.chain_id) : "connecting…";
  const others = NETWORKS.filter(([, url]) => origin === "" || url.replace(/\/$/, "") !== origin);
  const mainnetHere = meta?.chain_id === "celestia";
  const mainnetLinked = others.some(([name]) => netName(name) === "Mainnet");
  return (
    <details className="net menu" title={title} ref={box}>
      <summary><i className={"dot " + dot} />{label}</summary>
      <div className="list" role="menu">
        <span aria-current="true">{label}</span>
        {others.map(([name, url]) => <a key={name} href={url} role="menuitem">{netName(name)}</a>)}
        {!mainnetHere && !mainnetLinked && (
          <span className="soon" aria-disabled="true" title="Fibre is not on mainnet yet. This observer will follow it there.">Mainnet<i className="tagx">soon</i></span>
        )}
      </div>
    </details>
  );
}

/**
 * The newest block this observer has read, with a dot that beats on each new
 * one: the one thing on the page that moves on its own, so a reader can see
 * at a glance that the observer is following the chain. No age is printed:
 * a block's header time is set when it is proposed, so even the newest block
 * is already about one block time old when it commits and a counter would
 * never start at zero. The age is in the tooltip. The dot turns amber when
 * the block is over thirty seconds old (a halted chain, or an observer that
 * stopped reading) and grey when the API does not answer. Until Fibre is
 * live it also counts down to the upgrade that brings it.
 */
function BlockTicker({ meta }: { meta: Meta | null }) {
  const { data: tip, error, fetchedAt } = useApi<Tip>("/v1/tip", 4000);
  const [now, setNow] = useState(0);
  useEffect(() => {
    setNow(Date.now());
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  if (!tip || !tip.height) {
    return error ? <span className="blocktick" title={`Observer API unreachable: ${error}`}><i className="dot none" />—</span> : null;
  }
  // The server's clock, not the reader's: a laptop a minute fast would
  // otherwise call every block a minute old.
  const skew = fetchedAt ? Date.parse(tip.server_time) - Date.parse(fetchedAt) : 0;
  const age = tip.block_time && now ? Math.max(0, (now + skew - Date.parse(tip.block_time)) / 1000) : null;
  const dot = error ? "none" : age === null || age > 30 ? "hold" : "ok";
  const sig = meta?.upgrade_signal;
  const countdown = !tip.fibre_active && sig?.eta_seconds ? span(sig.eta_seconds) : "";
  const title = [
    `Block #${int(tip.height)}${tip.block_time ? ` · made ${utcWord(tip.block_time)}${age !== null ? `, ${age < 90 ? `${Math.round(age)}s` : since(tip.block_time)} ago` : ""}` : ""}`,
    error ? `API unreachable (${error}); showing the last reading` : "",
    countdown && sig?.upgrade_height ? `Fibre activates at #${int(sig.upgrade_height)}, about ${countdown} at the chain's recent pace` : "",
  ].filter(Boolean).join(" · ");
  return (
    <span className="blocktick" title={title}>
      <i key={tip.height} className={"dot " + dot + (dot === "ok" ? " beat" : "")} />
      <span className="num">#{int(tip.height)}</span>
      {countdown && <span className="soon">Fibre in {countdown}</span>}
    </span>
  );
}

export function Header() {
  const { data: meta, error } = useApi<Meta>("/v1/meta", 30000);
  const path = usePathname();
  return (
    <header className="top">
      <div className="wrap">
        <Link className="brand" href="/" aria-label="Tensile, the independent observer for Celestia Fibre"><Mark />Tensile</Link>
        <nav aria-label="site">
          {NAV.map(([href, name]) => (
            <Link key={href} href={href} className={(href === "/" ? path === "/" : path.startsWith(href)) ? "on" : ""}>{name}</Link>
          ))}
        </nav>
        <div className="right">
          <BlockTicker meta={meta} />
          <NetworkChip meta={meta} error={error} />
          <span className="divider" aria-hidden="true" />
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}

/**
 * The footer: the three kinds of evidence every figure on the site rests on,
 * and who runs the observer. Nothing that changes with the data.
 */
export function Footer() {
  const { data: meta } = useApi<Meta>("/v1/meta", 30000);
  return (
    <footer className="foot">
      <div title={meta?.server_time ? `Observer time ${utcWord(meta.server_time)}${meta.last_probe_at ? ` · newest probe ${ago(meta.last_probe_at)}` : ""}` : undefined}>
        Tensile by <a href="https://huginn.tech" rel="noopener noreferrer" target="_blank">Huginn Tech</a>
        <span>·</span><Link href="/methodology/">methodology</Link>
        <span>·</span><a href={SOURCE_URL} rel="noopener noreferrer" target="_blank">GitHub</a>
        <span>·</span><a href={DISPUTE_URL} rel="noopener noreferrer" target="_blank">dispute a verdict</a>
        <span>·</span><a href="https://db-ip.com" rel="noopener noreferrer" target="_blank">IP geolocation by DB-IP</a>
      </div>
    </footer>
  );
}
