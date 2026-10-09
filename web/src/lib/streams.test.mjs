// api.ts's streams (npm test; node --test): when a stream asks the API again after an answer, a failure, a refusal and
// while the page is hidden. The module imports React for its hooks only, so the test compiles it with the project's own
// TypeScript with React's two hooks stubbed out, and runs it on mocked timers against a stub of fetch. It also passes on
// amounts.ts's formats, which a module loaded from a data: URL cannot import and this test does not use (amounts.test.mjs
// holds them), so that line is left out.
import { test, mock } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import ts from "typescript";

const load = (file) => {
  const src = readFileSync(new URL(file, import.meta.url), "utf8")
    .replace(/import \{[^}]*\} from "react";/, "const useEffect = () => {}, useState = (v) => [v, () => {}];")
    .replace(/^export \{[^}]*\} from "\.\/amounts";$/m, "");
  const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
  return import(`data:text/javascript;base64,${Buffer.from(js).toString("base64")}`);
};

// the page: hidden or shown, with its one visibilitychange listener list
const doc = { hidden: false, on: [], addEventListener(type, fn) { if (type === "visibilitychange") this.on.push(fn); } };
globalThis.document = doc;
const show = (hidden) => { doc.hidden = hidden; doc.on.forEach((fn) => fn()); };

// the API: each path answers from its own queue of answers, the last one standing once the queue is down to it
const answers = new Map();
const asked = [];
globalThis.fetch = async (url) => {
  const path = url.replace(/^\/api/, "");
  asked.push(path);
  const q = answers.get(path) ?? [];
  const a = q.length > 1 ? q.shift() : q[0];
  if (!a) throw new TypeError("fetch failed");
  if (a.throws) throw new TypeError(a.throws);
  return {
    ok: a.status >= 200 && a.status < 300, status: a.status,
    headers: { get: (h) => (a.headers ?? {})[h.toLowerCase()] ?? null },
    json: async () => a.body ?? {},
  };
};
const ok = (body = { n: 1 }) => ({ status: 200, body });
const answer = (path, ...q) => answers.set(path, q);
const count = (path) => asked.filter((p) => p === path).length;

mock.timers.enable({ apis: ["setTimeout", "setInterval", "Date"] });
const { subscribe, failRetryMs } = await load("./api.ts");

// lets the requests out come back: a few turns of the microtask queue past every await on the way
const settle = async () => { for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r)); };
/** moves the clock on by ms, letting each request that falls due come back before the next */
const pass = async (ms) => {
  for (let t = 0; t < ms; t += 250) { mock.timers.tick(Math.min(250, ms - t)); await settle(); }
};
const watch = (path, refreshMs) => {
  const seen = [];
  const stop = subscribe(`${refreshMs}|${path}`, path, refreshMs, (f) => seen.push(f));
  return { seen, stop };
};

test("a failed answer is asked again within seconds, doubling, not at the stream's interval", async () => {
  const path = "/v1/fail-then-ok";
  answer(path, { status: 502, body: { error: "observer API unavailable" } }, { status: 502, body: { error: "observer API unavailable" } }, ok());
  const s = watch(path, 30000);
  await settle();
  assert.equal(count(path), 1);
  await pass(1750);
  assert.equal(count(path), 1, "not before 2 s");
  await pass(250);
  assert.equal(count(path), 2, "again at 2 s");
  await pass(3750);
  assert.equal(count(path), 2);
  await pass(250);
  assert.equal(count(path), 3, "again 4 s later");
  assert.equal(s.seen.at(-1).error, null);
  // answered: back to the interval
  await pass(29750);
  assert.equal(count(path), 3);
  await pass(250);
  assert.equal(count(path), 4);
  s.stop();
});

test("a failure keeps what is on screen, with the error beside it", async () => {
  const path = "/v1/ok-then-fail";
  answer(path, ok({ v: 7 }), { status: 500, body: { error: "internal error" } });
  const s = watch(path, 30000);
  await settle();
  await pass(30000);
  const last = s.seen.at(-1);
  assert.deepEqual(last.data, { v: 7 });
  assert.equal(last.error, "internal error");
  assert.equal(last.status, 500);
  s.stop();
});

test("a stream with no interval asks again after a failure, until it is answered", async () => {
  const path = "/v1/params-once";
  answer(path, { throws: "connection refused" }, ok());
  const s = watch(path, 0);
  await settle();
  assert.equal(s.seen.at(-1).status, 0);
  await pass(2000);
  assert.equal(count(path), 2);
  assert.equal(s.seen.at(-1).error, null);
  // answered: nothing more
  await pass(600000);
  assert.equal(count(path), 2);
  s.stop();
});

test("a refusal (429) backs off past a fast interval and waits out its Retry-After", async () => {
  const path = "/v1/tip-refused";
  const refused = { status: 429, body: { error: "too many requests" }, headers: { "retry-after": "5" } };
  answer(path, refused, refused, { status: 502, body: { error: "observer API unavailable" } }, { status: 502 }, ok());
  const s = watch(path, 1000);
  await settle();
  assert.equal(count(path), 1);
  await pass(4750);
  assert.equal(count(path), 1, "Retry-After: 5 is waited out");
  await pass(250);
  assert.equal(count(path), 2);
  await pass(5000);
  assert.equal(count(path), 3, "4 s of back-off, 5 s of Retry-After");
  await pass(7750);
  assert.equal(count(path), 3, "8 s: a 1 s stream backs off to 8 times its interval");
  await pass(250);
  assert.equal(count(path), 4);
  await pass(8000);
  assert.equal(count(path), 5, "never longer than that");
  assert.equal(s.seen.at(-1).error, null);
  await pass(1000);
  assert.equal(count(path), 6, "answered: back to every second");
  s.stop();
});

test("a 404 says nothing about the API: asked again at the interval, not sooner", async () => {
  const path = "/v1/blobs/missing";
  answer(path, { status: 404, body: { error: "not found" } });
  const s = watch(path, 30000);
  await settle();
  await pass(29750);
  assert.equal(count(path), 1);
  await pass(250);
  assert.equal(count(path), 2);
  s.stop();
});

test("a figure being computed is asked again when the API says, whatever the interval", async () => {
  const path = "/v1/computing";
  answer(path, { status: 503, body: { error: "computing", computing: true, retry_after_s: 3 } }, ok());
  const s = watch(path, 30000);
  await settle();
  assert.equal(s.seen.at(-1).computing, true);
  assert.equal(s.seen.at(-1).error, null);
  await pass(3000);
  assert.equal(count(path), 2);
  assert.deepEqual(s.seen.at(-1).data, { n: 1 });
  s.stop();
});

test("a hidden page asks nothing at the interval, and asks at once when it is shown again", async () => {
  const path = "/v1/hidden";
  answer(path, ok());
  const s = watch(path, 1000);
  await settle();
  await pass(1000);
  assert.equal(count(path), 2);
  show(true);
  await pass(10000);
  assert.equal(count(path), 2, "nothing while hidden");
  show(false);
  await settle();
  assert.equal(count(path), 3, "at once when shown");
  await pass(1000);
  assert.equal(count(path), 4, "then at its interval");
  s.stop();
});

test("a stream not yet due when the page is shown again keeps its time", async () => {
  const path = "/v1/slow-hidden";
  answer(path, ok());
  const s = watch(path, 30000);
  await settle();
  show(true);
  await pass(5000);
  show(false);
  await settle();
  assert.equal(count(path), 1, "shown again before its time: no extra request");
  await pass(25000);
  assert.equal(count(path), 2);
  s.stop();
});

test("a stream nobody reads any more asks nothing", async () => {
  const path = "/v1/gone";
  answer(path, { status: 502 });
  const s = watch(path, 1000);
  await settle();
  s.stop();
  await pass(60000);
  assert.equal(count(path), 1);
});

test("the back-off: 2 s doubling, up to the interval, a fast stream to 8 intervals or 30 s, a Retry-After waited out", () => {
  assert.deepEqual([1, 2, 3, 4, 5, 6].map((n) => failRetryMs(n, 30000)), [2000, 4000, 8000, 16000, 30000, 30000]);
  assert.deepEqual([1, 2, 3, 4, 5].map((n) => failRetryMs(n, 1000)), [2000, 4000, 8000, 8000, 8000]);
  assert.equal(failRetryMs(9, 15000), 30000);
  assert.equal(failRetryMs(12, 300000), 300000);
  assert.equal(failRetryMs(20, 0), 300000);
  assert.equal(failRetryMs(1, 30000, 5000), 5000);
  assert.equal(failRetryMs(1, 30000, 3600000), 300000, "a Retry-After is waited out up to 5 minutes");
});
