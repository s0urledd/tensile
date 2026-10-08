// fragment.ts (npm test; node --test): the id a link's # points to, on the API page and the methodology page. The
// module is plain TypeScript, so the test compiles it with the project's own TypeScript.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import ts from "typescript";

const src = readFileSync(new URL("./fragment.ts", import.meta.url), "utf8");
const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
const { fragmentId } = await import(`data:text/javascript;base64,${Buffer.from(js).toString("base64")}`);

test("a link's fragment is decoded", () => {
  assert.equal(fragmentId("#get-v1-validators"), "get-v1-validators");
  assert.equal(fragmentId("#params%2Dchain"), "params-chain");
  assert.equal(fragmentId(""), "");
  assert.equal(fragmentId("#"), "");
});

test("a fragment that is no valid escape is taken as it stands, not thrown on", () => {
  assert.equal(fragmentId("#%"), "%");
  assert.equal(fragmentId("#100%"), "100%");
  assert.equal(fragmentId("#%E0%A4%A"), "%E0%A4%A");
});
