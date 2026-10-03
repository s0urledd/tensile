// blobkey.ts against the identifiers a developer holds for the docs' example blob (npm test; node --test).
// The module is TypeScript with no imports, so the test compiles it with the project's own TypeScript.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import ts from "typescript";

const src = readFileSync(new URL("./blobkey.ts", import.meta.url), "utf8");
const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
const { blobKey, blobIdOf, keyText } = await import(`data:text/javascript;base64,${Buffer.from(js).toString("base64")}`);

// the example in Celestia's Fibre docs: its blob ID, the commitment under it, and the transaction that settled it
const ID = "AJ+0/lcm8YgL/06EFKHMnnMaQfJp/ChDQskMk7rkd8FQ";
const COMMITMENT = "9fb4fe5726f1880bff4e8414a1cc9e731a41f269fc284342c90c93bae477c150";
const TX = "ADD4C5190131D0AFB55AF3341B347700B2ADEC2B128FAF3C7CA06495852C5C16";
const PROMISE = "481498948d4e9ee30ad214ab22d1d2005c7c218e9db95dabfd5b4e42d5e88a95";
const asId = { kind: "id", hex: COMMITMENT, id: ID };

test("the docs' blob ID is version 0 and its commitment", () => {
  assert.equal(blobIdOf(COMMITMENT), ID);
  assert.equal(blobIdOf(COMMITMENT.toUpperCase()), ID);
  assert.deepEqual(blobKey(ID), asId);
});

test("every spelling of the blob ID reads as its commitment", () => {
  for (const s of [
    ID,
    ID.replace(/\+/g, "-").replace(/\//g, "_"), // URL-safe
    `${ID}=`, // padded, as some encoders write 33 bytes
    `  ${ID}\n`,
    ID.replace(/\+/g, " "), // an address that read its + as a space
    `00${COMMITMENT}`, // BlobID.String, hex
    `0x00${COMMITMENT.toUpperCase()}`,
  ]) assert.deepEqual(blobKey(s), asId, s);
});

test("66 hex is a blob ID, never a commitment; 64 hex is a hash, never a blob ID", () => {
  assert.equal(blobKey(`00${COMMITMENT}`).kind, "id");
  assert.equal(blobKey(`00${COMMITMENT}`).hex, COMMITMENT);
  assert.deepEqual(blobKey(COMMITMENT), { kind: "hash", hex: COMMITMENT });
  // a 64-hex hash that starts 00 is still a hash
  assert.deepEqual(blobKey(`00${COMMITMENT.slice(2)}`), { kind: "hash", hex: `00${COMMITMENT.slice(2)}` });
  // another version byte is no blob ID Fibre has
  assert.equal(blobKey(`01${COMMITMENT}`), null);
});

test("a transaction hash and a promise hash read as 64 hex, in lower case", () => {
  for (const s of [TX, TX.toLowerCase(), `0x${TX}`, `0X${TX}`]) assert.deepEqual(blobKey(s), { kind: "hash", hex: TX.toLowerCase() }, s);
  assert.deepEqual(blobKey(PROMISE), { kind: "hash", hex: PROMISE });
  assert.equal(keyText(blobKey(TX)), TX.toLowerCase());
  assert.equal(keyText(blobKey(`00${COMMITMENT}`)), ID);
});

test("anything else is no identifier", () => {
  const c = Buffer.from(COMMITMENT, "hex");
  for (const s of [
    "", "   ", "hello world", TX.slice(1), `${TX}0`,
    Buffer.concat([Buffer.from([1]), c]).toString("base64"), // version 1
    c.toString("base64"), // 32 bytes: a bare commitment in base64
    Buffer.concat([Buffer.from([0]), c, Buffer.from([7])]).toString("base64"), // 34 bytes
    `${ID.slice(0, 43)}!`, `${ID}===`,
  ]) assert.equal(blobKey(s), null, s);
  assert.equal(blobKey(null), null);
  assert.equal(blobKey(undefined), null);
});

test("blob IDs round-trip through every base64 spelling", () => {
  for (let i = 0; i < 500; i++) {
    const c = randomBytes(32);
    const want = Buffer.concat([Buffer.from([0]), c]).toString("base64");
    assert.equal(blobIdOf(c.toString("hex")), want);
    // an address reads each + as a space, the last one too: the pages put every one back before they read it
    const addr = want.replace(/\+/g, " ").replace(/ /g, "+");
    for (const s of [want, want.replace(/\+/g, "-").replace(/\//g, "_"), addr, `00${c.toString("hex")}`]) {
      assert.equal(blobKey(s)?.hex, c.toString("hex"), s);
    }
  }
});
