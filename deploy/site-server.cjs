#!/usr/bin/env node
// The site's own server: the static export in SITE_ROOT, and /api/v1/* passed
// to observer-api (/api itself is the static page that documents it). It
// exists for hosts where the front proxy is shared with other projects and
// cannot be given a file_server block for this site; where it
// can, deploy/Caddyfile does the same job without it.
//
// It replaces web/test/serve.cjs in production, which is a fixture server:
// synchronous file access on every request, no caching or security headers,
// the upstream's error text in a 502, and any method passed to the API.
//
//   SITE_ROOT    the static export (web/out), required
//   SITE_LISTEN  host:port to listen on, default 127.0.0.1:3112
//   API_LISTEN   observer-api's host:port, the same variable its unit reads
//
// Node's standard library only, so it runs from a copied file with no install.
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");

const ROOT = path.resolve(process.env.SITE_ROOT || "");
if (!process.env.SITE_ROOT || !fs.existsSync(path.join(ROOT, "index.html"))) {
  console.error(`site-server: SITE_ROOT (${process.env.SITE_ROOT || "unset"}) has no index.html`);
  process.exit(2);
}
function hostPort(v, def) {
  const s = (v || def).trim();
  const i = s.lastIndexOf(":");
  return { host: (i > 0 ? s.slice(0, i) : "") || "127.0.0.1", port: Number(s.slice(i + 1)) };
}
const LISTEN = hostPort(process.env.SITE_LISTEN, "127.0.0.1:3112");
const API = hostPort(process.env.API_LISTEN, "127.0.0.1:8081");

const TYPES = {
  ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8",
  ".json": "application/json", ".txt": "text/plain; charset=utf-8", ".svg": "image/svg+xml", ".ico": "image/x-icon",
  ".png": "image/png", ".webp": "image/webp", ".woff2": "font/woff2", ".webmanifest": "application/manifest+json",
};
const COMPRESSIBLE = new Set([".html", ".js", ".css", ".json", ".txt", ".svg", ".webmanifest"]);

// Everything the pages load comes from this origin: scripts, styles, fonts,
// flags and avatars (the API serves the Keybase pictures itself). Inline
// scripts stay allowed because the static export inlines its bootstrap and
// the theme script, and a nonce needs a server that renders; everything else
// is held to 'self'. The site is only reached through the front proxy over
// HTTPS, so HSTS is safe to send (no includeSubDomains: other hosts share the
// parent domain).
const SECURITY = {
  "x-content-type-options": "nosniff",
  "referrer-policy": "strict-origin-when-cross-origin",
  "x-frame-options": "DENY",
  "content-security-policy": [
    "default-src 'self'", "script-src 'self' 'unsafe-inline'", "style-src 'self' 'unsafe-inline'",
    "img-src 'self' data:", "font-src 'self'", "connect-src 'self'", "form-action 'self'",
    "frame-ancestors 'none'", "object-src 'none'", "base-uri 'self'",
  ].join("; "),
  "permissions-policy": "camera=(), microphone=(), geolocation=(), interest-cohort=()",
  "strict-transport-security": "max-age=31536000",
};

// /api is rate limited per client, and the number of requests in flight to
// the API is capped: a figure pinned to an arbitrary as_of bypasses the API's
// caches and costs a second or more of database work, so a loop of them from
// one address must not starve everyone else. A page load makes about ten
// data requests at once and then polls one every few seconds, well inside
// these limits. Avatars are exempt: the overview asks for one per validator
// in a burst, and the API serves them from its own cache.
const RATE = { burst: 120, perSec: 10 };        // token bucket per client address
const INFLIGHT_PER_CLIENT = 16, INFLIGHT_TOTAL = 48;
const buckets = new Map();                      // address -> { tokens, at, inflight }
let inflight = 0;
setInterval(() => {
  const now = Date.now();
  for (const [k, b] of buckets) if (b.inflight === 0 && now - b.at > 10 * 60_000) buckets.delete(k);
}, 60_000).unref();

// The client's address: the front proxy's X-Forwarded-For when the request
// comes from a private address (the proxy's container), else the socket's.
function clientOf(req) {
  const peer = (req.socket.remoteAddress || "").replace(/^::ffff:/, "");
  const priv = /^(127\.|10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.|::1$|f[cd])/.test(peer);
  const xff = priv && typeof req.headers["x-forwarded-for"] === "string" ? req.headers["x-forwarded-for"].split(",")[0].trim() : "";
  return xff || peer;
}

function admit(req, res) {
  const who = clientOf(req), now = Date.now();
  let b = buckets.get(who);
  if (!b) buckets.set(who, b = { tokens: RATE.burst, at: now, inflight: 0 });
  b.tokens = Math.min(RATE.burst, b.tokens + ((now - b.at) / 1000) * RATE.perSec);
  b.at = now;
  if (b.tokens < 1 || b.inflight >= INFLIGHT_PER_CLIENT || inflight >= INFLIGHT_TOTAL) {
    res.writeHead(429, { ...SECURITY, "content-type": "application/json", "retry-after": "5" });
    res.end('{"error":"too many requests"}');
    return null;
  }
  b.tokens -= 1;
  b.inflight++; inflight++;
  let done = false;
  const release = () => { if (!done) { done = true; b.inflight--; inflight--; } };
  res.on("close", release);
  return release;
}

// Next's hashed build output never changes under a name; everything else can.
function cacheControl(rel) {
  if (rel.startsWith("/_next/static/")) return "public, max-age=31536000, immutable";
  if (rel.startsWith("/fonts/")) return "public, max-age=2592000";
  return "no-cache";
}

// The file a URL path names, or null. Resolving and then checking the prefix
// is the traversal guard; URL parsing has already collapsed dot segments, so
// this is the second of two.
async function resolveFile(pathname) {
  let rel;
  try { rel = decodeURIComponent(pathname); } catch { return null; }
  if (rel.includes("\0")) return null;
  const base = path.resolve(ROOT, "." + path.posix.normalize("/" + rel));
  if (base !== ROOT && !base.startsWith(ROOT + path.sep)) return null;
  for (const f of [base, base.replace(/[\\/]$/, "") + ".html", path.join(base, "index.html")]) {
    try {
      const st = await fs.promises.stat(f);
      if (st.isFile()) return { file: f, size: st.size, mtime: st.mtime };
    } catch { /* next candidate */ }
  }
  return null;
}

// Compressed copies, made once per file and kept in memory: brotli for a client that takes it, else gzip, both at
// their best levels, since a file is compressed once per deploy rather than once per request. A copy is keyed by the
// file's path, size and modification time, so the files a deploy swaps in are compressed afresh; the oldest copies go
// first past PACKED_MAX bytes. Requests that arrive while a file is being compressed wait for that one compression.
const PACKED_MAX = 64 * 1024 * 1024;
const packed = new Map();     // key -> { br, gz, bytes }, oldest first
const packing = new Map();    // key -> the compression in progress
let packedBytes = 0;
const brotli = (buf) => new Promise((ok, no) => zlib.brotliCompress(buf, { params: {
  [zlib.constants.BROTLI_PARAM_QUALITY]: zlib.constants.BROTLI_MAX_QUALITY,
  [zlib.constants.BROTLI_PARAM_SIZE_HINT]: buf.length,
} }, (e, out) => (e ? no(e) : ok(out))));
const gzipped = (buf) => new Promise((ok, no) => zlib.gzip(buf, { level: zlib.constants.Z_BEST_COMPRESSION }, (e, out) => (e ? no(e) : ok(out))));
function packedOf(found) {
  const key = `${found.file}|${found.size}|${found.mtime.getTime()}`;
  const have = packed.get(key);
  if (have) { packed.delete(key); packed.set(key, have); return Promise.resolve(have); }
  if (packing.has(key)) return packing.get(key);
  const job = (async () => {
    const raw = await fs.promises.readFile(found.file);
    const [br, gz] = await Promise.all([brotli(raw), gzipped(raw)]);
    const p = { br, gz, bytes: br.length + gz.length };
    packed.set(key, p);
    packedBytes += p.bytes;
    for (const [k, v] of packed) {
      if (packedBytes <= PACKED_MAX || packed.size <= 1) break;
      packed.delete(k);
      packedBytes -= v.bytes;
    }
    return p;
  })().finally(() => packing.delete(key));
  packing.set(key, job);
  return job;
}

async function send(req, res, status, found, rel) {
  const ext = path.extname(found.file).toLowerCase();
  const headers = { ...SECURITY, "content-type": TYPES[ext] || "application/octet-stream", "cache-control": cacheControl(rel), "last-modified": found.mtime.toUTCString() };
  if (COMPRESSIBLE.has(ext)) headers.vary = "Accept-Encoding";
  const accepts = req.headers["accept-encoding"] || "";
  const coding = !COMPRESSIBLE.has(ext) || found.size <= 1024 ? "" : /\bbr\b/.test(accepts) ? "br" : /\bgzip\b/.test(accepts) ? "gzip" : "";
  if (coding) {
    // a file that cannot be compressed (a read or zlib error) goes out as it is
    const p = await packedOf(found).catch((e) => { console.error(`site-server: compress ${rel}: ${e.message}`); return null; });
    if (p) {
      const body = coding === "br" ? p.br : p.gz;
      res.writeHead(status, { ...headers, "content-encoding": coding, "content-length": body.length });
      return res.end(req.method === "HEAD" ? undefined : body);
    }
  }
  res.writeHead(status, { ...headers, "content-length": found.size });
  if (req.method === "HEAD") return res.end();
  const stream = fs.createReadStream(found.file);
  stream.on("error", () => res.destroy());
  stream.pipe(res);
}

// After start, every compressible file of the export is compressed in the background, one at a time, so the first
// visitors after a deploy are not the ones who wait for it.
async function warm(dir = ROOT) {
  let entries;
  try { entries = await fs.promises.readdir(dir, { withFileTypes: true }); } catch { return; }
  for (const e of entries) {
    const f = path.join(dir, e.name);
    if (e.isDirectory()) { await warm(f); continue; }
    if (!COMPRESSIBLE.has(path.extname(f).toLowerCase())) continue;
    try {
      const st = await fs.promises.stat(f);
      if (st.size > 1024) await packedOf({ file: f, size: st.size, mtime: st.mtime });
    } catch { /* a file that went away, or one that cannot be read: it is served as it is */ }
  }
}

// SLOW_MS is when an API answer is worth a journal line.
const SLOW_MS = 1000;

const HOP = new Set(["connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "te", "trailer", "host"]);

function proxy(req, res, u) {
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.writeHead(405, { ...SECURITY, allow: "GET, HEAD" });
    return res.end();
  }
  if (!u.pathname.startsWith("/api/v1/avatars/") && !admit(req, res)) return;
  // A page waiting on the API is the thing to catch before a visitor does:
  // every answer slower than SLOW_MS goes to the journal with its time.
  const t0 = Date.now();
  res.on("finish", () => {
    const ms = Date.now() - t0;
    if (ms >= SLOW_MS) console.log(`site-server: slow ${u.pathname}${u.search} ${res.statusCode} ${ms} ms`);
  });
  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) if (!HOP.has(k)) headers[k] = v;
  const up = http.request({ host: API.host, port: API.port, method: req.method, path: u.pathname.slice(4) + u.search, headers, timeout: 60_000 }, (r) => {
    const out = {};
    for (const [k, v] of Object.entries(r.headers)) if (!HOP.has(k)) out[k] = v;
    res.writeHead(r.statusCode || 502, { ...SECURITY, ...out });
    r.pipe(res);
  });
  up.on("timeout", () => up.destroy(new Error("upstream timeout")));
  up.on("error", (e) => {
    console.error(`site-server: api ${u.pathname}: ${e.message}`);
    if (!res.headersSent) { res.writeHead(502, { ...SECURITY, "content-type": "application/json" }); res.end('{"error":"observer API unavailable"}'); }
    else res.destroy();
  });
  up.end();
}

const server = http.createServer(async (req, res) => {
  try {
    const u = new URL(req.url || "/", "http://site");
    // /api/v1 is the API; /api itself is the page that documents it (web/src/app/api).
    if (u.pathname === "/api/v1" || u.pathname.startsWith("/api/v1/")) return proxy(req, res, u);
    // The API page's first address, kept for links made to it.
    if (u.pathname === "/developers" || u.pathname.startsWith("/developers/")) {
      res.writeHead(301, { ...SECURITY, location: "/api/" });
      return res.end();
    }
    if (req.method !== "GET" && req.method !== "HEAD") {
      res.writeHead(405, { ...SECURITY, allow: "GET, HEAD" });
      return res.end();
    }
    const found = await resolveFile(u.pathname);
    if (found) return await send(req, res, 200, found, u.pathname);
    const nf = await resolveFile("/404.html");
    if (nf) return await send(req, res, 404, nf, "/404.html");
    res.writeHead(404, { ...SECURITY, "content-type": "text/plain; charset=utf-8" });
    res.end("not found");
  } catch (e) {
    console.error(`site-server: ${req.method} ${req.url}: ${e && e.stack || e}`);
    if (!res.headersSent) { res.writeHead(500, SECURITY); res.end(); } else res.destroy();
  }
});
server.headersTimeout = 20_000;
server.requestTimeout = 60_000;
server.on("clientError", (_e, sock) => { if (sock.writable) sock.end("HTTP/1.1 400 Bad Request\r\n\r\n"); });
server.listen(LISTEN.port, LISTEN.host, () => {
  console.log(`site-server: ${ROOT} on ${LISTEN.host}:${LISTEN.port}, /api/v1 -> ${API.host}:${API.port}`);
  const t0 = Date.now();
  warm().then(() => console.log(`site-server: ${packed.size} files compressed in ${Date.now() - t0} ms (${Math.round(packedBytes / 1024)} KB kept)`));
});
for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, () => server.close(() => process.exit(0)));
