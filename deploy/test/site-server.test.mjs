// site-server.cjs (node --test; web's npm test runs it): the per-client limits on /api/v1, the two halves of a proxied
// request, and the files it streams. The server runs as it does in production, from its own file, in front of a fake
// observer API that answers every path; each test speaks for its own client address through X-Forwarded-For, which
// the server takes from a loopback peer as it does from the proxy.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import net from "node:net";
import { spawn } from "node:child_process";
import { EventEmitter, once } from "node:events";
import { existsSync, mkdtempSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const SERVER = fileURLToPath(new URL("../site-server.cjs", import.meta.url));

const listening = (srv) => new Promise((ok) => srv.listen(0, "127.0.0.1", () => ok(srv.address().port)));
async function freePort() {
  const s = http.createServer();
  const port = await listening(s);
  await new Promise((ok) => s.close(ok));
  return port;
}

// What the fake API was asked, for the tests that watch it: "hold" when a request for /v1/hold arrives, "hold-closed"
// when its connection closes.
const seen = new EventEmitter();
// a day's export: more bytes than any buffer between the API and a client holds, sent for as long as it is read
const CHUNK = Buffer.alloc(64 * 1024, 7);
function apiAnswer(req, res) {
  const p = req.url.split("?")[0];
  if (p.startsWith("/v1/exports/")) {
    res.writeHead(200, { "content-type": "application/gzip" });
    const more = () => { while (res.write(CHUNK)); };
    res.on("drain", more);
    return more();
  }
  // a request the API is slow to answer: it never does
  if (p === "/v1/hold") {
    res.on("close", () => seen.emit("hold-closed"));
    return seen.emit("hold");
  }
  // an API that goes away a part of the way into its answer
  if (p === "/v1/cut") {
    res.writeHead(200, { "content-type": "application/json", "content-length": "100000" });
    res.write(Buffer.alloc(1000, 32));
    return setTimeout(() => res.socket.destroy(), 50);
  }
  res.writeHead(200, { "content-type": "application/json" });
  res.end("{}");
}

let api, site, base, root;
before(async () => {
  api = http.createServer(apiAnswer);
  const apiPort = await listening(api);
  root = mkdtempSync(path.join(tmpdir(), "site-server-"));
  writeFileSync(path.join(root, "index.html"), "<!doctype html><title>t</title>");
  // a file streamed as it is (not compressible), larger than the socket buffers on the way to a client
  writeFileSync(path.join(root, "big.png"), Buffer.alloc(64 * 1024 * 1024));
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

// A request whose answer has begun and is then read no further: resolves with the request once its headers are in.
function opened(p, who) {
  return new Promise((ok, no) => {
    const req = http.get(base + p, { headers: { "x-forwarded-for": who }, agent: false }, (res) => {
      res.pause();
      ok(req);
    });
    req.on("error", no);
  });
}

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

test("the addresses of one IPv6 /64 are one client, however they are spelled", async () => {
  const got = [];
  for (let i = 1; i <= 150; i++) {
    const who = i % 3 === 0 ? `2001:0db8:0001:0002:0000:0000:0000:${i.toString(16).padStart(4, "0")}`
      : i % 3 === 1 ? `2001:db8:1:2::${i.toString(16)}` : `2001:DB8:1:2:${i.toString(16)}::1`;
    got.push(await ask("/api/v1/blobs?limit=50", who));
  }
  assert.equal(got[0], 200);
  assert.ok(got.includes(429), "a new address in the same /64 for each request gets no allowance of its own");
  // the next /64 is another client
  assert.equal(await ask("/api/v1/blobs?limit=50", "2001:db8:1:3::1"), 200);
});

test("answers read slowly do not hold the places every other client's requests need", async () => {
  // three clients, each with as many exports open as its own cap allows, every one of them read no further than its
  // headers: as many as the global cap
  const held = [];
  try {
    for (let c = 0; c < 3; c++) {
      for (let i = 0; i < 16; i++) held.push(await opened(`/api/v1/exports/2026-10-0${1 + (i % 7)}.tar.gz`, `198.51.100.${10 + c}`));
    }
    assert.equal(await ask("/api/v1/tip", "198.51.100.20"), 200);
    assert.equal(await ask("/api/v1/validators?window=24h", "198.51.100.20"), 200);
    // a client's own slow downloads still count against it
    assert.equal(await ask("/api/v1/meta", "198.51.100.10"), 429);
  } finally {
    held.forEach((r) => r.destroy());
  }
});

test("a client that goes away cancels its request to the API", async () => {
  const arrived = once(seen, "hold");
  const closed = once(seen, "hold-closed");
  const req = http.get(base + "/api/v1/hold", { headers: { "x-forwarded-for": "198.51.100.30" }, agent: false });
  req.on("error", () => {});
  await arrived;
  req.destroy();
  const got = await Promise.race([closed.then(() => "cancelled"), delay(5000).then(() => "still asked")]);
  assert.equal(got, "cancelled");
});

test("an API that goes away mid-answer ends the client's answer too", async () => {
  let req;
  const got = await new Promise((ok) => {
    req = http.get(base + "/api/v1/cut", { headers: { "x-forwarded-for": "198.51.100.31" }, agent: false }, (res) => {
      res.on("data", () => {});
      res.on("error", () => {});
      res.on("close", () => ok(res.complete ? "whole" : "cut"));
    });
    req.on("error", () => ok("cut"));
    setTimeout(() => ok("hanging"), 5000);
  });
  req.destroy();
  assert.equal(got, "cut");
});

test("a file a client stops reading and leaves is closed", { skip: !existsSync("/proc/self/fd") && "needs /proc" }, async () => {
  const fds = () => readdirSync(`/proc/${site.pid}/fd`).length;
  const leave = async () => {
    const sock = net.connect(Number(new URL(base).port), "127.0.0.1");
    sock.on("error", () => {});
    sock.write("GET /big.png HTTP/1.1\r\nHost: site\r\n\r\n");
    // the headers and the first bytes, then nothing more is read, and the client goes
    await once(sock, "data");
    sock.pause();
    await delay(20);
    sock.destroy();
  };
  await leave();
  await delay(300);
  const was = fds();
  for (let i = 0; i < 30; i++) await leave();
  await delay(500);
  assert.ok(fds() - was < 10, `${fds() - was} more files open after 30 clients left mid-file`);
});

test("past CLIENTS_MAX addresses a new one shares one allowance with the rest", async () => {
  // one request from each of 50,000 addresses (CLIENTS_MAX), 32 at a time
  const agent = new http.Agent({ keepAlive: true, maxSockets: 32 });
  const one = (who) => new Promise((ok) => {
    const req = http.get(base + "/api/v1/meta", { agent, headers: { "x-forwarded-for": who } }, (res) => { res.resume(); res.on("end", ok); });
    req.on("error", ok);
  });
  let next = 0;
  await Promise.all(Array.from({ length: 32 }, async () => {
    while (next < 50_000) { const i = next++; await one(`198.18.${i >> 8}.${i & 255}`); }
  }));
  agent.destroy();
  // then a new address for every request: together they are refused past one allowance
  const got = [];
  for (let i = 0; i < 150; i++) got.push(await ask("/api/v1/blobs?limit=50", `198.19.${i >> 8}.${i & 255}`));
  assert.ok(got.includes(429), "new addresses past the cap each got an allowance of their own");
});
