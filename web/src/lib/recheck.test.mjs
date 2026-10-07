// recheck.ts (npm test; node --test): when the overview's recent blobs ask again for a status they hold, so a blob
// Tensile read as available is never worded "not read" because the grid read it before its reading. The module is
// TypeScript with type imports only, so the test compiles it with the project's own TypeScript.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import ts from "typescript";

const load = (file) => {
  const src = readFileSync(new URL(file, import.meta.url), "utf8");
  const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
  return import(`data:text/javascript;base64,${Buffer.from(js).toString("base64")}`);
};
const { recheck } = await load("./recheck.ts");

const MIN = 60_000;
const END = Date.parse("2026-10-07T10:02:04Z");
const blob = (status) => ({ must_serve_until: new Date(END).toISOString(), reconstructable: status === null ? null : { status } });

test("a pending status read hours before the window's end is asked again after the reading, and before the end", () => {
  const readAt = END - 3 * 60 * MIN; // the grid's first read, 3 h before the end
  assert.equal(recheck(blob("pending"), readAt, END - 30 * MIN), false, "nothing yet: the reading is 10 minutes before the end");
  assert.equal(recheck(blob("pending"), readAt, END - 9 * MIN), false, "the reading is out; its answers not all in");
  assert.equal(recheck(blob("pending"), readAt, END - 8 * MIN), true, "2 minutes after the reading");
  // asked at the first check: the next is the one 2 minutes before the end
  const again = END - 8 * MIN + 500;
  assert.equal(recheck(blob("pending"), again, END - 3 * MIN), false);
  assert.equal(recheck(blob("pending"), again, END - 2 * MIN), true, "2 minutes before the end, while the word is still 'retention window'");
  // asked then too: nothing more, whatever the API answered
  assert.equal(recheck(blob("pending"), END - 2 * MIN + 500, END + 60 * MIN), false);
});

test("a page that slept through both checks asks once when it looks again", () => {
  assert.equal(recheck(blob("pending"), END - 3 * 60 * MIN, END + 2 * 60 * MIN), true);
  assert.equal(recheck(blob(null), END - 3 * 60 * MIN, END + 2 * 60 * MIN), true, "no status at all is no answer either");
});

test("a status the API answers for good is never asked again", () => {
  for (const s of ["yes", "no", "not_read", "unknown"]) assert.equal(recheck(blob(s), END - 3 * 60 * MIN, END + MIN), false, s);
});

test("a blob read after the checks holds what the API said then", () => {
  assert.equal(recheck(blob("pending"), END - MIN, END + 10 * MIN), false);
  assert.equal(recheck({ must_serve_until: "", reconstructable: { status: "pending" } }, 0, END), false, "no window end, no check");
});
