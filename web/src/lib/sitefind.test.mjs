// sitefind.ts against the identifiers the header's search takes, with the docs' example blob (npm test; node --test).
// The modules are TypeScript with type imports only, so the test compiles them with the project's own TypeScript.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import ts from "typescript";

const load = (file) => {
  const src = readFileSync(new URL(file, import.meta.url), "utf8");
  const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
  return import(`data:text/javascript;base64,${Buffer.from(js).toString("base64")}`);
};
const { siteTarget, validatorAddr, publisherAddr } = await load("./sitefind.ts");
const { blobKey } = await load("./blobkey.ts");
const go = (s) => siteTarget(s, blobKey);

const ID = "AJ+0/lcm8YgL/06EFKHMnnMaQfJp/ChDQskMk7rkd8FQ";
const COMMITMENT = "9fb4fe5726f1880bff4e8414a1cc9e731a41f269fc284342c90c93bae477c150";
const TX = "ADD4C5190131D0AFB55AF3341B347700B2ADEC2B128FAF3C7CA06495852C5C16";
const PROMISE = "481498948d4e9ee30ad214ab22d1d2005c7c218e9db95dabfd5b4e42d5e88a95";
// a Mocha validator's operator and consensus addresses, and a publisher's account address
const VALOPER = "celestiavaloper19jz75rcp26a6tkch208qm2wmt2ekk4a272cvlx";
const VALCONS = "celestiavalcons17dzlj8xncd3r3a2s5qjgqrq29ngd04yu6v4lcc";
const ACCOUNT = "celestia1las83d0dt9gew3faq2mxp2gtupq5drclee9snr";

test("a blob identifier is read as the site keeps it, with the Blobs list of what it matches", () => {
  assert.deepEqual(go(TX), { kind: "blob", id: TX.toLowerCase(), href: `/blobs/?blob=${TX.toLowerCase()}` });
  assert.deepEqual(go(`0x${PROMISE}`), { kind: "blob", id: PROMISE, href: `/blobs/?blob=${PROMISE}` });
  assert.deepEqual(go(COMMITMENT.toUpperCase()), { kind: "blob", id: COMMITMENT, href: `/blobs/?blob=${COMMITMENT}` });
  // a blob ID keeps its base64 in the address, encoded, whether it came as base64 or as hex
  const id = { kind: "blob", id: ID, href: `/blobs/?blob=${encodeURIComponent(ID)}` };
  assert.deepEqual(go(ID), id);
  assert.deepEqual(go(`00${COMMITMENT}`), id);
  assert.deepEqual(go(` ${ID}\n`), id);
});

test("a validator's operator or consensus address leads to its page, an account address to a publisher's", () => {
  assert.deepEqual(go(VALOPER), { kind: "validator", id: VALOPER, href: `/validator/?addr=${VALOPER}` });
  assert.deepEqual(go(VALOPER.toUpperCase()), { kind: "validator", id: VALOPER, href: `/validator/?addr=${VALOPER}` });
  assert.deepEqual(go(VALCONS), { kind: "validator", id: VALCONS, href: `/validator/?addr=${VALCONS}` });
  assert.deepEqual(go(ACCOUNT), { kind: "publisher", id: ACCOUNT, href: `/publisher/?addr=${ACCOUNT}` });
});

test("names, words and partial identifiers are no identifier", () => {
  for (const s of ["", "   ", null, undefined, "Qubelabs", "celestia", "celestiavaloper1", VALOPER.slice(0, -1), `${ACCOUNT}x`,
    ACCOUNT.replace("l", "b"), TX.slice(0, 63), `${TX}0`, ACCOUNT.replace("celestia", "osmo")]) {
    assert.equal(go(s), null, String(s));
  }
});

// the forms a validator's page, a publisher's and a blob's take from their address, and the hand-made links that put
// their own query, a dot segment or a fragment into the API's path through them
const CONS_HEX = "f1450ff2c4d43ba3d35f4d8d01e19a4040b7b4c6";
const BENT = [
  `${VALOPER}?as_of=2026-09-20T12:00:00Z&window=7d#`, `${ACCOUNT}?window=24h#`, "../tip", "..", ".", "./", "%2e%2e", `${VALOPER}/`,
  `${VALOPER}/feed.atom`, `${CONS_HEX}#x`, `${CONS_HEX}0`, ` ${VALOPER} x`,
];

test("a validator's page takes its consensus, operator or account address, and nothing that bends the API's path", () => {
  for (const a of [VALOPER, VALCONS, ACCOUNT, CONS_HEX]) assert.equal(validatorAddr(a), a);
  assert.equal(validatorAddr(` ${VALOPER.toUpperCase()}\n`), VALOPER);
  assert.equal(validatorAddr(CONS_HEX.toUpperCase()), CONS_HEX);
  for (const s of [...BENT, "", null, undefined, "celestiavaloper1"]) assert.equal(validatorAddr(s), null, String(s));
});

test("a publisher's page takes an account address of 20 or 32 bytes, and nothing that bends the API's path", () => {
  const ACCOUNT32 = "celestia1" + "q".repeat(52) + "sx8a9k";
  assert.equal(publisherAddr(ACCOUNT), ACCOUNT);
  assert.equal(publisherAddr(ACCOUNT32), ACCOUNT32);
  assert.equal(publisherAddr(` ${ACCOUNT.toUpperCase()}`), ACCOUNT);
  for (const s of [...BENT, VALOPER, VALCONS, CONS_HEX, "", null, undefined]) assert.equal(publisherAddr(s), null, String(s));
});

test("a blob's page takes a promise hash, and nothing that bends the API's path", () => {
  assert.deepEqual(blobKey(PROMISE), { kind: "hash", hex: PROMISE });
  for (const s of ["../tip", "..", `${PROMISE}?rows=1#`, `${PROMISE}/`, `../${PROMISE}`]) assert.notEqual(blobKey(s)?.kind, "hash", s);
});
