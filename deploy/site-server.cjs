#!/usr/bin/env node
// The site's own server: the static export in SITE_ROOT, and /api/* passed to
// observer-api. It exists for hosts where the front proxy is shared with other
// projects and cannot be given a file_server block for this site; where it
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

function send(req, res, status, found, rel) {
  const ext = path.extname(found.file).toLowerCase();
  const headers = { ...SECURITY, "content-type": TYPES[ext] || "application/octet-stream", "cache-control": cacheControl(rel), "last-modified": found.mtime.toUTCString() };
  const gzip = COMPRESSIBLE.has(ext) && found.size > 1024 && /\bgzip\b/.test(req.headers["accept-encoding"] || "");
  if (COMPRESSIBLE.has(ext)) headers.vary = "Accept-Encoding";
  if (gzip) headers["content-encoding"] = "gzip"; else headers["content-length"] = found.size;
  res.writeHead(status, headers);
  if (req.method === "HEAD") return res.end();
  const stream = fs.createReadStream(found.file);
  stream.on("error", () => res.destroy());
  (gzip ? stream.pipe(zlib.createGzip()) : stream).pipe(res);
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
    if (u.pathname === "/api" || u.pathname.startsWith("/api/")) return proxy(req, res, u);
    if (req.method !== "GET" && req.method !== "HEAD") {
      res.writeHead(405, { ...SECURITY, allow: "GET, HEAD" });
      return res.end();
    }
    const found = await resolveFile(u.pathname);
    if (found) return send(req, res, 200, found, u.pathname);
    const nf = await resolveFile("/404.html");
    if (nf) return send(req, res, 404, nf, "/404.html");
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
server.listen(LISTEN.port, LISTEN.host, () => console.log(`site-server: ${ROOT} on ${LISTEN.host}:${LISTEN.port}, /api -> ${API.host}:${API.port}`));
for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, () => server.close(() => process.exit(0)));
