// recheck.ts (npm test; node --test): when the overview's recent blobs ask again for a status they hold, so a blob
// Tensile read as available is not left worded "not read" because the grid read it before its reading was stored. The
// module is TypeScript with type imports only, so the test compiles it with the project's own TypeScript.
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
});

test("a reading the collector stores late is asked for after the window's end, pending or not_read", () => {
  // the check 2 minutes before the end still found it pending: the collector is behind
  const late = END - 2 * MIN + 500;
  assert.equal(recheck(blob("pending"), late, END + 2 * MIN), false, "the window has just ended; the first check after it is at 3 minutes");
  assert.equal(recheck(blob("pending"), late, END + 5 * MIN), true, "lane() words it 'not read' now: asked again");
  // the API answered not_read 3 minutes after the end: the reading is not in yet
  const after = END + 3 * MIN + 500;
  assert.equal(recheck(blob("not_read"), after, END + 14 * MIN), false);
  assert.equal(recheck(blob("not_read"), after, END + 15 * MIN), true, "15 minutes after the end, once more");
  // asked then too: nothing more, whatever the API answered
  assert.equal(recheck(blob("not_read"), END + 15 * MIN + 500, END + 6 * 60 * MIN), false);
  assert.equal(recheck(blob("pending"), END + 15 * MIN + 500, END + 6 * 60 * MIN), false);
});

test("a page that slept through the checks asks once when it looks again", () => {
  assert.equal(recheck(blob("pending"), END - 3 * 60 * MIN, END + 2 * 60 * MIN), true);
  assert.equal(recheck(blob(null), END - 3 * 60 * MIN, END + 2 * 60 * MIN), true, "no status at all is no answer either");
});

test("a status the API answers for good is never asked again", () => {
  for (const s of ["yes", "no", "unknown"]) {
    assert.equal(recheck(blob(s), END - 3 * 60 * MIN, END - 8 * MIN), false, s);
    assert.equal(recheck(blob(s), END - 3 * 60 * MIN, END + 60 * MIN), false, s);
  }
});

test("a blob read after the checks holds what the API said then", () => {
  assert.equal(recheck(blob("not_read"), END + 16 * MIN, END + 60 * MIN), false);
  assert.equal(recheck({ must_serve_until: "", reconstructable: { status: "pending" } }, 0, END), false, "no window end, no check");
});
