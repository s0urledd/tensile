// apidown.ts (npm test; node --test): the API page's pill shows only when the API does not answer, and a refusal over
// the site's rate limit reads busy. The module is plain TypeScript, so the test compiles it with the project's own
// TypeScript.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import ts from "typescript";

const load = (file) => {
  const src = readFileSync(new URL(file, import.meta.url), "utf8");
  const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
  return import(`data:text/javascript;base64,${Buffer.from(js).toString("base64")}`);
};
const { apiDown } = await load("./apidown.ts");

test("an API that answers shows nothing, whatever its checks say", () => {
  assert.equal(apiDown(200, { status: "ok", checks: [] }), null);
  // a day the day-partials audit failed: every figure exact, the health degraded
  assert.equal(apiDown(503, { status: "degraded", checks: [{ name: "day_partials", ok: false }] }), null);
  assert.equal(apiDown(503, { status: "failing" }), null);
});

test("a refusal over the site's rate limit reads busy, not unreachable", () => {
  assert.deepEqual(apiDown(429, { error: "too many requests" }), { word: "Busy", why: "too many requests" });
  assert.deepEqual(apiDown(429, null), { word: "Busy", why: "too many requests" });
});

test("the proxy's answer while the API restarts, or no JSON at all, reads not answering", () => {
  assert.deepEqual(apiDown(502, { error: "observer API unavailable" }), { word: "Not answering", why: "observer API unavailable" });
  assert.deepEqual(apiDown(504, null), { word: "Not answering", why: "HTTP 504" });
  assert.deepEqual(apiDown(500, { error: "internal error" }), { word: "Not answering", why: "internal error" });
});
