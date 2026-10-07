// site-server.cjs (node --test; web's npm test runs it): the per-address limits on /api/v1. The server runs as it does
// in production, from its own file, in front of a fake observer API that answers every path; each test speaks for its
// own client address through X-Forwarded-For, which the server takes from a loopback peer as it does from the proxy.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { spawn } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SERVER = fileURLToPath(new URL("../site-server.cjs", import.meta.url));

const listening = (srv) => new Promise((ok) => srv.listen(0, "127.0.0.1", () => ok(srv.address().port)));
async function freePort() {
  const s = http.createServer();
  const port = await listening(s);
  await new Promise((ok) => s.close(ok));
  return port;
}

let api, site, base;
before(async () => {
  api = http.createServer((_req, res) => { res.writeHead(200, { "content-type": "application/json" }); res.end("{}"); });
  const apiPort = await listening(api);
  const root = mkdtempSync(path.join(tmpdir(), "site-server-"));
  writeFileSync(path.join(root, "index.html"), "<!doctype html><title>t</title>");
  const port = await freePort();
  site = spawn(process.execPath, [SERVER], {
    env: { ...process.env, SITE_ROOT: root, SITE_LISTEN: `127.0.0.1:${port}`, API_LISTEN: `127.0.0.1:${apiPort}` },
    stdio: ["ignore", "pipe", "inherit"],
  });
  await new Promise((ok, no) => {
    site.once("exit", (code) => no(new Error(`site-server exited with ${code}`)));
    site.stdout.on("data", (b) => { if (String(b).includes(" on 127.0.0.1:")) ok(); });
  });
  base = `http://127.0.0.1:${port}`;
});
after(() => {
  site?.kill();
  api?.closeAllConnections();
  api?.close();
});

const ask = async (p, who) => (await fetch(base + p, { headers: { "x-forwarded-for": who } })).status;

test("the tips of many pages behind one address do not spend its data requests' allowance", async () => {
  const who = "203.0.113.7";
  // more tips than either allowance holds, asked back to back: some are refused
  const tips = [];
  for (let i = 0; i < 150; i++) tips.push(await ask("/api/v1/tip", who));
  assert.ok(tips.includes(429), "a flood of tips from one address is still refused");
  assert.equal(tips[0], 200);
  // the page's own data requests from the same address are still answered
  assert.equal(await ask("/api/v1/validators?window=24h", who), 200);
  assert.equal(await ask("/api/v1/meta", who), 200);
});

test("data requests from one address keep their own limit", async () => {
  const who = "203.0.113.8";
  const got = [];
  for (let i = 0; i < 150; i++) got.push(await ask("/api/v1/blobs?limit=50", who));
  assert.equal(got[0], 200);
  assert.ok(got.includes(429), "a loop of data requests from one address is refused past the allowance");
  // and another address is not
  assert.equal(await ask("/api/v1/blobs?limit=50", "203.0.113.9"), 200);
});
