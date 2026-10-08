"use client";
import { Fragment, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import Copy from "@/components/Copy";
import { API_BASE, useApi, type Meta } from "@/lib/api";
import { apiDown } from "@/lib/apidown";
import { fragmentId } from "@/lib/fragment";
import { API_URL, API_URL_FIXED } from "@/lib/site";
import { GROUPS, type Endpoint, type Param } from "./endpoints";

/**
 * The parts of the API page that run in the browser: the base URL and the
 * notice that the API does not answer, which depend on the site serving the
 * page, and the reference, which opens, filters and sends. One static export
 * serves every network's site, each with its own API on its own /api, so the
 * base URL is read from the page's origin unless NEXT_PUBLIC_API_URL fixed
 * one at build time.
 */

const DEFAULT_BASE = API_URL.replace(/\/v1$/, "");

function useBase(): string {
  const [base, setBase] = useState(DEFAULT_BASE);
  useEffect(() => {
    if (API_URL_FIXED) return;
    try { setBase(new URL(API_BASE || "/", window.location.origin).href.replace(/\/$/, "")); } catch { /* keep the default */ }
  }, []);
  return base;
}

export function BaseUrl() {
  const base = useBase();
  return (
    <div className="api-base">
      <span className="api-base-label">Base URL</span>
      <code>{base}</code>
      <Copy text={base} label="the base URL" />
    </div>
  );
}

/**
 * A pill in the title row, only while the API does not answer (or the site refuses this reader as too many requests,
 * which reads busy). The observer's own checks are not shown: the site says nothing about the observer while the API
 * answers, and /v1/health is listed below for anyone who asks. Asked every minute, not while the tab is hidden.
 */
export function Health() {
  const [down, setDown] = useState<{ word: string; why: string } | null>(null);
  useEffect(() => {
    let live = true, last = 0;
    const check = async () => {
      if (document.hidden) return;
      last = Date.now();
      const ctl = new AbortController();
      const t = setTimeout(() => ctl.abort(), 20000);
      let next: { word: string; why: string } | null;
      try {
        const r = await fetch(`${API_BASE}/v1/health`, { cache: "no-store", signal: ctl.signal });
        let body: unknown = null;
        try { body = await r.json(); } catch { /* not JSON: not the API's answer */ }
        next = apiDown(r.status, body);
      } catch (e) {
        next = { word: "Not answering", why: e instanceof DOMException && e.name === "AbortError" ? "no answer within 20 s" : e instanceof Error ? e.message : String(e) };
      } finally {
        clearTimeout(t);
      }
      if (live) setDown(next);
    };
    check();
    const t = setInterval(check, 60_000);
    const onVis = () => { if (!document.hidden && Date.now() - last >= 60_000) check(); };
    document.addEventListener("visibilitychange", onVis);
    return () => { live = false; clearInterval(t); document.removeEventListener("visibilitychange", onVis); };
  }, []);
  if (!down) return null;
  return (
    <span className="api-health" role="status" title={`GET /v1/health: ${down.why}`}>
      <i className="dot hold" aria-hidden="true" />
      {down.word}
    </span>
  );
}

/** Text with `backticked` names set as code. */
function ticks(s: string): ReactNode[] {
  return s.split("`").map((part, i) => (i % 2 ? <code key={i}>{part}</code> : <Fragment key={i}>{part}</Fragment>));
}

/** A path with its {placeholders} set apart. */
function PathText({ path }: { path: string }) {
  return <>{path.split(/(\{[^}]+\})/).map((part, i) => (part.startsWith("{") ? <i key={i}>{part}</i> : part))}</>;
}

/** One line of JSON with its keys and numbers set apart. A string value and
 *  the comma after it are one box, so a narrow screen moves a time or a hash
 *  to the next line whole instead of breaking it at a hyphen. */
function jsonLine(line: string): ReactNode[] {
  const out: ReactNode[] = [];
  let last = 0;
  for (const m of line.matchAll(/"(?:[^"\\]|\\.)*"(?:(\s*:)|,?)|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|\b(?:true|false|null)\b/g)) {
    const i = m.index ?? 0;
    if (i > last) out.push(line.slice(last, i));
    const cls = m[0].startsWith('"') ? (m[1] !== undefined ? "k" : "s") : "n";
    out.push(<span key={i} className={cls}>{m[0]}</span>);
    last = i + m[0].length;
  }
  out.push(line.slice(last));
  return out;
}

/** Text line by line, each wrapping under its own indent plus two, so a
 *  wrapped line still reads as part of its level. Copied, the text is the
 *  answer as it came. */
function lines(text: string, json: boolean): ReactNode {
  if (text.length > 400_000) return text;
  return text.split("\n").map((line, i) => {
    const hang = line.length - line.trimStart().length + 2;
    return (
      <span key={i} className="ln" style={{ paddingLeft: `${hang}ch`, textIndent: `-${hang}ch` }}>
        {json ? jsonLine(line) : line}{"\n"}
      </span>
    );
  });
}

function Code({ text, format, label }: { text: string; format?: Endpoint["format"]; label: string }) {
  return <pre className="api-code" tabIndex={0} aria-label={label}><code>{lines(text, !format)}</code></pre>;
}

function TypeCell({ p }: { p: Param }) {
  const bits: ReactNode[] = [];
  if (p.values?.length === 1) bits.push(<code key="v">{p.values[0]}</code>);
  else if (p.values) bits.push(<span key="v">one of {p.values.map((v, i) => <Fragment key={v}>{i > 0 && " "}<code>{v}</code></Fragment>)}</span>);
  else bits.push(<span key="t">{p.type}</span>);
  if (p.range) bits.push(<span key="r">{p.range}</span>);
  if (p.default !== undefined) bits.push(<span key="d">default <code>{p.default}</code></span>);
  return <>{bits.map((b, i) => <Fragment key={i}>{i > 0 && ", "}{b}</Fragment>)}</>;
}

function Params({ ep }: { ep: Endpoint }) {
  return (
    <table className="api-params">
      <thead><tr><th scope="col">Name</th><th scope="col">Type</th><th scope="col">Description</th></tr></thead>
      <tbody>
        {ep.params.map((p) => (
          <tr key={p.name}>
            <td><code>{p.name}</code>{p.required && <span className="api-req">required</span>}</td>
            <td><TypeCell p={p} /></td>
            <td>{ticks(p.desc)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

const REASON: Record<number, string> = {
  200: "OK", 206: "Partial Content", 304: "Not Modified", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed",
  416: "Range Not Satisfiable", 429: "Too Many Requests", 500: "Internal Server Error", 502: "Bad Gateway", 503: "Service Unavailable",
};

type Answer =
  | { url: string; status: number; ms: number; bytes: number; body: string; kind: "json" | "text" | "binary"; type: string }
  | { url: string; error: string };

/** Parameters that take an address, a hash or a namespace get a row of their own. */
const WIDE = new Set(["validator", "blob", "namespace", "commitment", "tx", "publisher", "exclude"]);

/**
 * The chain endpoints.ts's examples were taken on. Its Try it values name records of that chain: a validator, a blob,
 * its namespace and publisher, an export. On another network's site (one export serves every network) they would ask
 * for records its API does not hold, so there the newest the API itself names stand in for them.
 */
const EXAMPLE_CHAIN = "mocha-5";

type Named = "validator" | "blob" | "namespace" | "publisher" | "export";
type Live = Partial<Record<Named, string>>;
const NONE_YET: Live = {};

/** which record a parameter's Try it value names, if any */
function recordOf(ep: Endpoint, p: Param): Named | null {
  if (!p.example) return null;
  if (p.name === "validator" || (p.name === "addr" && p.example.startsWith("celestiavaloper1"))) return "validator";
  if (p.name === "addr" && p.example.startsWith("celestia1")) return "publisher";
  if (p.name === "hash" && ep.path.startsWith("/v1/blobs/")) return "blob";
  if (p.name === "namespace") return "namespace";
  if (p.name === "name" && ep.path.startsWith("/v1/exports/")) return "export";
  return null;
}

/**
 * The records this site's API names, for the parameters of ep that name one: null on the examples' own chain (and
 * until the site has said which chain it is on), empty while they are read. The newest blob gives a blob, its
 * namespace and its publisher; the newest reading a validator (before the first one, the validator with the most
 * voting power); the newest export its digest's name. Asked only when the endpoint is opened.
 */
function useLive(ep: Endpoint): Live | null {
  const { data: meta } = useApi<Meta>("/v1/meta"); // the header's stream: no request of its own
  const other = !!meta?.chain_id && meta.chain_id !== EXAMPLE_CHAIN;
  const wanted = useMemo(() => new Set(ep.params.map((p) => recordOf(ep, p)).filter((r): r is Named => !!r)), [ep]);
  const [live, setLive] = useState<Live | null>(null);
  useEffect(() => {
    if (!other || wanted.size === 0) return;
    let on = true;
    const get = async (path: string) => {
      try {
        const r = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
        return r.ok ? await r.json() : null;
      } catch { return null; }
    };
    (async () => {
      const out: Live = {};
      if (wanted.has("blob") || wanted.has("namespace") || wanted.has("publisher")) {
        const b = (await get("/v1/blobs?limit=1"))?.blobs?.[0];
        if (b) { out.blob = b.promise_hash; out.namespace = b.namespace; out.publisher = b.publisher; }
      }
      if (wanted.has("validator")) {
        const p = (await get("/v1/probes?limit=1"))?.probes?.[0];
        out.validator = p?.operator_address || p?.validator_address || undefined;
        if (!out.validator) {
          const vs: { operator_address?: string; address: string; voting_power: number }[] = (await get("/v1/validators?window=24h"))?.validators ?? [];
          const top = vs.reduce<(typeof vs)[number] | null>((m, v) => (!m || v.voting_power > m.voting_power ? v : m), null);
          out.validator = top ? top.operator_address || top.address : undefined;
        }
      }
      if (wanted.has("export")) {
        const ex: { name: string; day?: string }[] = (await get("/v1/exports?limit=1"))?.exports ?? [];
        const newest = ex.reduce<(typeof ex)[number] | null>((m, e) => (!m || (e.day ?? e.name) > (m.day ?? m.name) ? e : m), null);
        if (newest) out.export = `${newest.name}.sha256`;
      }
      if (on) setLive(out);
    })();
    return () => { on = false; };
  }, [other, wanted]);
  return other && wanted.size > 0 ? live ?? NONE_YET : null;
}

/** The request the inputs make:the path with its placeholders filled, and the query. */
function request(ep: Endpoint, vals: Record<string, string>): { url: string; missing: string[] } {
  let path = ep.path;
  const missing: string[] = [];
  const query = new URLSearchParams();
  for (const p of ep.params) {
    const v = (vals[p.name] ?? "").trim();
    if (p.in === "path") {
      if (!v) missing.push(p.name);
      path = path.replace(`{${p.name}}`, v ? encodeURIComponent(v) : `{${p.name}}`);
    } else if (v) {
      query.append(p.name, v);
    }
  }
  const qs = query.toString();
  return { url: qs ? `${path}?${qs}` : path, missing };
}

function TryIt({ ep }: { ep: Endpoint }) {
  const base = useBase();
  const [vals, setVals] = useState<Record<string, string>>(() => Object.fromEntries(ep.params.map((p) => [p.name, p.example ?? ""])));
  // On another network than the examples', a value that names a record is this API's own (useLive), empty until it is
  // read; a value the reader typed stays.
  const live = useLive(ep);
  const auto = useRef<Record<string, string>>({});
  useEffect(() => {
    if (!live) return;
    setVals((v) => {
      let changed = false;
      const next = { ...v };
      for (const p of ep.params) {
        const r = recordOf(ep, p);
        if (!r) continue;
        const want = live[r] ?? "";
        const untouched = v[p.name] === (p.example ?? "") || v[p.name] === auto.current[p.name];
        if (untouched && v[p.name] !== want) { next[p.name] = want; changed = true; }
        auto.current[p.name] = untouched ? want : auto.current[p.name];
      }
      return changed ? next : v;
    });
  }, [live, ep]);
  const [answer, setAnswer] = useState<Answer | null>(null);
  const [busy, setBusy] = useState(false);
  const req = request(ep, vals);

  const send = async () => {
    if (req.missing.length) {
      setAnswer({ url: req.url, error: `Fill in ${req.missing.join(" and ")} first.` });
      return;
    }
    setBusy(true);
    const t0 = performance.now();
    try {
      const r = await fetch(`${API_BASE}${req.url}`, { cache: "no-store" });
      const type = (r.headers.get("content-type") ?? "").split(";")[0].trim();
      if (/json|xml|text/.test(type)) {
        const body = await r.text();
        const ms = Math.round(performance.now() - t0);
        const bytes = new TextEncoder().encode(body).length;
        let shown = body, kind: "json" | "text" = "text";
        if (type.includes("json")) {
          try { shown = JSON.stringify(JSON.parse(body), null, 2); kind = "json"; } catch { /* shown as sent */ }
        }
        setAnswer({ url: req.url, status: r.status, ms, bytes, body: shown, kind, type });
      } else {
        // An archive: its size from the header, and the body left unread.
        const bytes = Number(r.headers.get("content-length") ?? 0);
        const ms = Math.round(performance.now() - t0);
        r.body?.cancel().catch(() => {});
        setAnswer({ url: req.url, status: r.status, ms, bytes, body: "", kind: "binary", type });
      }
    } catch (e) {
      setAnswer({ url: req.url, error: `The request failed: ${e instanceof Error ? e.message : String(e)}` });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="api-try">
      <h4 className="api-h4">Try it</h4>
      <form onSubmit={(e) => { e.preventDefault(); send(); }}>
        {ep.params.length > 0 && (
          <div className="api-fields">
            {ep.params.map((p) => {
              const id = `${ep.id}-${p.name}`;
              const size = p.in === "path" || WIDE.has(p.name) ? " wide" : p.type === "RFC 3339" ? " time" : "";
              return (
                <div className={`api-field${size}`} key={p.name}>
                  <label htmlFor={id}>{p.name}{p.required && <span aria-hidden="true"> *</span>}</label>
                  {p.values ? (
                    <select id={id} value={vals[p.name]} onChange={(e) => setVals({ ...vals, [p.name]: e.target.value })}>
                      <option value="">not set</option>
                      {p.values.map((v) => <option key={v} value={v}>{v}</option>)}
                    </select>
                  ) : (
                    <input id={id} value={vals[p.name]} onChange={(e) => setVals({ ...vals, [p.name]: e.target.value })}
                      placeholder={p.type === "RFC 3339" ? "2026-09-29T00:00:00Z" : p.default ?? ""} required={p.required}
                      spellCheck={false} autoComplete="off" autoCapitalize="off" />
                  )}
                </div>
              );
            })}
          </div>
        )}
        <div className="api-go">
          <code className="api-url">GET {base}{req.url}</code>
          <Copy text={`${base}${req.url}`} label="the request URL" />
          <button type="submit" className="api-send" disabled={busy}>{busy ? "Sending…" : "Send"}</button>
        </div>
      </form>
      <div aria-live="polite">
        {answer && ("error" in answer ? (
          <p className="api-meta">{answer.error}</p>
        ) : (
          <>
            <p className="api-meta">
              <span className={`api-st ${answer.status < 300 ? "good" : "bad"}`}>{answer.status} {REASON[answer.status] ?? ""}</span>
              <span>{answer.ms.toLocaleString("en-US")} ms</span>
              <span>{answer.bytes.toLocaleString("en-US")} bytes</span>
            </p>
            {answer.kind === "binary"
              ? <p className="api-meta">A file ({answer.type || "binary"}): not shown here.</p>
              : <Code text={answer.body} format={answer.kind === "json" ? undefined : "text"} label={`Response from ${answer.url}`} />}
          </>
        ))}
      </div>
    </div>
  );
}

function Body({ ep }: { ep: Endpoint }) {
  return (
    <>
      {ep.desc && <p className="api-desc">{ticks(ep.desc)}</p>}
      {ep.params.length > 0 && (
        <>
          <h4 className="api-h4">Parameters</h4>
          <Params ep={ep} />
        </>
      )}
      {ep.errors && <p className="api-errors"><b>Errors</b> {ticks(ep.errors)}</p>}
      <TryIt ep={ep} />
      <h4 className="api-h4">{ep.whole ? "Example response" : "Example response, trimmed"}</h4>
      <Code text={ep.example} format={ep.format} label={`Example response of ${ep.path}`} />
    </>
  );
}

function Row({ ep, open, seen, toggle }: { ep: Endpoint; open: boolean; seen: boolean; toggle: () => void }) {
  return (
    <article className="api-ep" id={ep.id}>
      <h3>
        <button type="button" className="api-ep-head" aria-expanded={open} aria-controls={`${ep.id}-body`} onClick={toggle}>
          <span className="api-get">GET</span>
          <code className="api-path"><PathText path={ep.path} /></code>
          <span className="api-sum">{ep.summary}</span>
          <svg className="api-chev" viewBox="0 0 16 16" aria-hidden="true"><path d="m4 6 4 4 4-4" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /></svg>
        </button>
      </h3>
      <div className="api-body" id={`${ep.id}-body`} hidden={!open}>
        {seen && <Body ep={ep} />}
      </div>
    </article>
  );
}

const ALL = GROUPS.flatMap((g) => g.endpoints.map((ep) => ({ g, ep })));
const IDS = new Set(ALL.map((x) => x.ep.id));

/** Every word of the query in the row's path, summary, group or parameter names. */
function matches(q: string, hay: string): boolean {
  return q.split(/\s+/).filter(Boolean).every((w) => hay.includes(w));
}

export function Reference() {
  const [q, setQ] = useState("");
  // true: open; false: opened once and closed again, its Try it kept
  const [state, setState] = useState<Record<string, boolean>>({});
  const [navOpen, setNavOpen] = useState(false);

  const hay = useMemo(() => new Map(ALL.map(({ g, ep }) => [ep.id,
    [ep.path, ep.summary, g.title, ...ep.params.map((p) => p.name)].join(" ").toLowerCase()])), []);
  const query = q.trim().toLowerCase();
  const shown = (id: string) => !query || matches(query, hay.get(id) ?? "");

  const openRow = (id: string) => setState((s) => (s[id] ? s : { ...s, [id]: true }));

  // A link to #id opens that row, on arrival and on every change of the hash.
  useEffect(() => {
    const go = () => {
      const id = fragmentId(window.location.hash);
      if (IDS.has(id)) {
        openRow(id);
        requestAnimationFrame(() => document.getElementById(id)?.scrollIntoView({ block: "start" }));
      }
    };
    go();
    window.addEventListener("hashchange", go);
    return () => window.removeEventListener("hashchange", go);
  }, []);

  const any = ALL.some(({ ep }) => shown(ep.id));

  return (
    <div className="api-ref">
      <aside className="api-side" aria-label="Endpoints">
        <div className="api-side-top">
          <label className="api-search">
            <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true"><circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" /></svg>
            <input type="search" value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search endpoints" aria-label="Search endpoints" autoComplete="off" spellCheck={false} />
          </label>
          <button type="button" className="api-navtoggle" aria-expanded={navOpen} aria-controls="api-nav" onClick={() => setNavOpen(!navOpen)}>
            Endpoints
            <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="m4 6 4 4 4-4" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /></svg>
          </button>
        </div>
        <nav id="api-nav" className={`api-nav${navOpen ? " open" : ""}`} aria-label="Endpoint list">
          {GROUPS.map((g) => {
            const eps = g.endpoints.filter((ep) => shown(ep.id));
            if (!eps.length) return null;
            return (
              <div key={g.id}>
                <p className="api-grp">{g.title}</p>
                <ul>
                  {eps.map((ep) => (
                    <li key={ep.id}>
                      <a href={`#${ep.id}`} onClick={() => { openRow(ep.id); setNavOpen(false); }}>
                        <span className="api-get">GET</span>
                        <code><PathText path={ep.path.replace(/^\/v1/, "")} /></code>
                      </a>
                    </li>
                  ))}
                </ul>
              </div>
            );
          })}
          {!any && <p className="api-none">No match.</p>}
        </nav>
      </aside>
      <div className="api-main">
        {GROUPS.map((g) => {
          const eps = g.endpoints.filter((ep) => shown(ep.id));
          if (!eps.length) return null;
          return (
            <section key={g.id} aria-labelledby={`group-${g.id}`}>
              <h2 id={`group-${g.id}`}>{g.title}</h2>
              {eps.map((ep) => (
                <Row key={ep.id} ep={ep} open={!!state[ep.id]} seen={ep.id in state}
                  toggle={() => setState((s) => ({ ...s, [ep.id]: !s[ep.id] }))} />
              ))}
            </section>
          );
        })}
        {!any && <p className="api-none">No endpoint matches &ldquo;{q.trim()}&rdquo;.</p>}
      </div>
    </div>
  );
}
