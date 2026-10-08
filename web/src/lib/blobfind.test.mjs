// blobfind-core.ts against a stub of /v1/blobs that answers as the observer API does (npm test; node --test): what an
// identifier finds, how many, as what, and that a lookup that fails is never read as "nothing matched". The modules
// are TypeScript with type imports only, so the test compiles them with the project's own TypeScript.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import ts from "typescript";

const load = (file) => {
  const src = readFileSync(new URL(file, import.meta.url), "utf8");
  const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
  return import(`data:text/javascript;base64,${Buffer.from(js).toString("base64")}`);
};
const { findBlobs, isShort } = await load("./blobfind-core.ts");
const { blobKey } = await load("./blobkey.ts");

// the example in Celestia's Fibre docs: its promise hash, its settlement transaction, its blob ID and the commitment under it
const PROMISE = "481498948d4e9ee30ad214ab22d1d2005c7c218e9db95dabfd5b4e42d5e88a95";
const TX = "ADD4C5190131D0AFB55AF3341B347700B2ADEC2B128FAF3C7CA06495852C5C16";
const ID = "AJ+0/lcm8YgL/06EFKHMnnMaQfJp/ChDQskMk7rkd8FQ";
const COMMITMENT = "9fb4fe5726f1880bff4e8414a1cc9e731a41f269fc284342c90c93bae477c150";

const hx = (s) => s.repeat(64 / s.length);
const blob = (promise_hash, commitment, tx, height, index = 0) =>
  ({ promise_hash, commitment, settlement_tx_hash: tx, settlement_height: height, settlement_tx_index: index });
const RECORD = [
  blob(PROMISE, COMMITMENT, TX.toLowerCase(), 1113071),
  // one transaction that settled two blobs
  blob(hx("11"), hx("21"), hx("b0"), 2000, 4),
  blob(hx("12"), hx("22"), hx("b0"), 2000, 4),
  // one commitment settled three times
  blob(hx("31"), hx("c3"), hx("41"), 3001),
  blob(hx("32"), hx("c3"), hx("42"), 3002),
  blob(hx("33"), hx("c3"), hx("43"), 3003),
  // a blob whose commitment is another blob's promise hash: one hash, two blobs, as two things
  blob(hx("51"), hx("11"), hx("61"), 4000),
];
const newest = (a, b) => b.settlement_height - a.settlement_height || b.settlement_tx_index - a.settlement_tx_index;

/**
 * /v1/blobs/{hash} and /v1/blobs?commitment=&tx=&limit= over RECORD: 404 for no such promise, 400 for a value that is
 * not 64 hex, a list that echoes its filters. old: an API from before ?tx=, which ignores it; failTx: ?tx= answers 503;
 * failPromise: /v1/blobs/{hash} answers 503; down: every request fails. failedTx: {hash: record}, a Fibre transaction
 * that failed in a block, answered as failed_tx to ?tx= of that hash. The observer sends it only beside no blob and with
 * the echo; the stub sends it whatever the list holds and whether or not it echoes (old, echo: another hash echoed),
 * so that the client's own checks are what the tests hold.
 */
function api({ old = false, failTx = false, failPromise = false, down = false, failedTx = {}, echo } = {}) {
  const asked = [];
  const answer = (status, body) => ({ ok: status >= 200 && status < 300, status, json: async () => body });
  const get = async (url) => {
    asked.push(url);
    if (down) throw new TypeError("Failed to fetch");
    const u = new URL(url, "http://observer");
    const one = u.pathname.match(/^\/v1\/blobs\/([^/]+)$/);
    if (one) {
      if (failPromise) return answer(503, { error: "busy" });
      if (!/^[0-9a-f]{64}$/.test(one[1])) return answer(400, { error: "promise hash must be 64 hex characters" });
      const b = RECORD.find((x) => x.promise_hash === one[1]);
      return b ? answer(200, { blob: b }) : answer(404, { error: "no publication with this promise hash" });
    }
    const c = u.searchParams.get("commitment"), t = old ? null : u.searchParams.get("tx");
    if (failTx && u.searchParams.has("tx")) return answer(503, { error: "busy" });
    for (const v of [c, t]) if (v !== null && !/^[0-9a-f]{64}$/.test(v)) return answer(400, { error: "must be 64 hex characters" });
    const rows = RECORD.filter((b) => (c === null || b.commitment === c) && (t === null || b.settlement_tx_hash === t)).sort(newest);
    const limit = Number(u.searchParams.get("limit") ?? 50);
    const failed = failedTx[u.searchParams.get("tx")];
    return answer(200, {
      blobs: rows.slice(0, limit), total: rows.length, ...(c !== null && { commitment: c }), ...(t !== null && { tx: echo ?? t }),
      ...(failed && { failed_tx: failed }),
    });
  };
  return { asked, get };
}
const find = (s, a = api(), opts = {}) => findBlobs(blobKey(s), { base: "", get: a.get, ...opts });

test("the docs' example: every identifier a developer holds reaches the same one blob", async () => {
  for (const [s, by] of [
    [TX, "tx"], [TX.toLowerCase(), "tx"], [`0x${TX}`, "tx"],
    [PROMISE, "promise"], [PROMISE.toUpperCase(), "promise"],
    [COMMITMENT, "commitment"],
    [ID, "commitment"], [ID.replace(/\+/g, "-").replace(/\//g, "_"), "commitment"], [`00${COMMITMENT}`, "commitment"], [`0x00${COMMITMENT.toUpperCase()}`, "commitment"],
  ]) {
    const f = await find(s);
    assert.equal(f.total, 1, s);
    assert.equal(f.rows[0].promise_hash, PROMISE, s);
    assert.deepEqual(f.by, [by], s);
    assert.equal(f.error, null, s);
    assert.equal(f.partial, null, s);
  }
});

test("a blob ID is asked as its commitment, never as a 64- or 66-hex hash", async () => {
  for (const s of [ID, `00${COMMITMENT}`]) {
    const a = api();
    await find(s, a);
    assert.deepEqual(a.asked, [`/v1/blobs?commitment=${COMMITMENT}&limit=25`], s);
  }
  // 64 hex is asked as all three things it can be
  const a = api();
  await find(COMMITMENT, a);
  assert.deepEqual(a.asked.sort(), [`/v1/blobs/${COMMITMENT}`, `/v1/blobs?commitment=${COMMITMENT}&limit=25`, `/v1/blobs?tx=${COMMITMENT}&limit=25`].sort());
});

test("several matches are several, newest first", async () => {
  const two = await find(hx("B0"));
  assert.equal(two.total, 2);
  assert.deepEqual(two.by, ["tx"]);
  const three = await find(hx("c3"));
  assert.equal(three.total, 3);
  assert.deepEqual(three.by, ["commitment"]);
  assert.deepEqual(three.rows.map((b) => b.promise_hash), [hx("33"), hx("32"), hx("31")]);
  // the same three by the commitment's blob ID
  const id = await find(`00${hx("c3")}`);
  assert.equal(id.total, 3);
});

test("the total counts past the limit of a list", async () => {
  const f = await find(hx("c3"), api(), { limit: 1 });
  assert.equal(f.rows.length, 1);
  assert.equal(f.total, 3);
});

test("one hash that names two blobs as two things counts each once", async () => {
  const f = await find(hx("11"));
  assert.equal(f.total, 2);
  assert.deepEqual(f.by, ["promise", "commitment"]);
  assert.deepEqual(f.rows.map((b) => b.promise_hash), [hx("51"), hx("11")]);
});

test("an API from before ?tx= finds nothing by a transaction, and says it could not ask", async () => {
  const f = await find(TX, api({ old: true }));
  assert.equal(f.total, 0);
  assert.equal(f.noTx, true);
  assert.equal(f.error, null);
  // what it can find, it still finds
  const p = await find(PROMISE, api({ old: true }));
  assert.equal(p.total, 1);
  assert.deepEqual(p.by, ["promise"]);
});

test("a lookup that fails is never 'nothing matched', and leaves one match unconfirmed", async () => {
  // the docs' tx is on record: with ?tx= failing and the others finding nothing, the answer is unknown, not none
  const tx = await find(TX, api({ failTx: true }));
  assert.equal(tx.total, 0);
  assert.equal(tx.error, "503");
  assert.equal(tx.partial, "503");
  // the commitment is found; the failed ?tx= might have found more, so the one match is partial
  const c = await find(COMMITMENT, api({ failTx: true }));
  assert.equal(c.total, 1);
  assert.equal(c.error, null);
  assert.equal(c.partial, "503");
  // nothing answers
  const down = await find(TX, api({ down: true }));
  assert.equal(down.error, "Failed to fetch");
});

test("a hash on no record is none, with nothing failed", async () => {
  const f = await find(hx("5a"));
  assert.equal(f.total, 0);
  assert.deepEqual(f.by, []);
  assert.equal(f.error, null);
  assert.equal(f.partial, null);
  assert.equal(f.noTx, false);
});

// A Fibre transaction that failed in a block, as /v1/blobs?tx= answers it beside no blob (observer/api/failedtx.go):
// a deposit after a send, its second message failed. ante_passed: the fee and sequence were taken (final), or the
// chain stopped it before running it (not final).
const failure = (ante_passed) => ({
  height: 1300123, time: "2026-10-10T08:01:02.123456789Z", code: 5, codespace: "sdk", reason: "Insufficient funds", failed_msg_index: 1,
  messages: [{ index: 0, type_url: "/cosmos.bank.v1beta1.MsgSend", fibre: false }, { index: 1, type_url: "/celestia.fibre.v1.MsgDepositToEscrow", fibre: true }],
  gas_wanted: 200000, gas_used: 91234, ante_passed, ...(ante_passed && { fee: "2000utia" }),
  log: "failed to execute message; message index: 1: failed to transfer funds to escrow: spendable balance 10utia is smaller than 1000000utia: insufficient funds",
});
// two transaction hashes on no blob's record, one failed for good and one not
const FINAL = hx("f1"), OPEN = hx("f2");
const FAILED = { [FINAL]: failure(true), [OPEN]: failure(false) };

test("a transaction that failed in a block is the answer, final or not: nothing failed, nothing left open", async () => {
  for (const [h, ante] of [[FINAL, true], [OPEN, false]]) {
    for (const s of [h, h.toUpperCase(), `0x${h}`]) {
      const f = await find(s, api({ failedTx: FAILED }));
      assert.deepEqual(f.failedTx, failure(ante), s);
      assert.equal(f.failedTx.ante_passed, ante, s);
      assert.equal(f.rows.length, 0, s);
      assert.equal(f.total, 0, s);
      assert.deepEqual(f.by, [], s);
      assert.equal(f.error, null, s);
      assert.equal(f.partial, null, s);
      assert.equal(isShort(f), false, s);
    }
    // as the blob page asks it, ?tx= alone and one row
    const page = await find(h, api({ failedTx: FAILED }), { as: ["tx"], limit: 1 });
    assert.deepEqual(page.failedTx, failure(ante));
    assert.equal(isShort(page), false);
  }
});

test("a failed_tx on a list that did not filter on the hash is not the transaction's", async () => {
  // an API from before ?tx= ignores the filter: its list of the newest blobs echoes no tx
  const old = await find(FINAL, api({ old: true, failedTx: FAILED }));
  assert.equal(old.failedTx, null);
  assert.equal(old.noTx, true);
  assert.equal(old.total, 0);
  assert.equal(isShort(old), true);
  // a list that echoes another hash
  const other = await find(FINAL, api({ echo: hx("0e"), failedTx: FAILED }));
  assert.equal(other.failedTx, null);
  assert.equal(other.total, 0);
  assert.equal(isShort(other), true);
});

test("a blob found beside a failed_tx wins: rows, and no failed transaction", async () => {
  // the same ?tx= answer lists blobs and carries failed_tx
  const both = await find(hx("b0"), api({ failedTx: { [hx("b0")]: failure(true) } }));
  assert.equal(both.failedTx, null);
  assert.equal(both.total, 2);
  assert.deepEqual(both.by, ["tx"]);
  assert.equal(isShort(both), false);
  // ?tx= finds no blob and carries failed_tx, while the same hash is a blob's promise hash
  const promise = await find(PROMISE, api({ failedTx: { [PROMISE]: failure(true) } }));
  assert.equal(promise.failedTx, null);
  assert.equal(promise.total, 1);
  assert.equal(promise.rows[0].promise_hash, PROMISE);
  assert.deepEqual(promise.by, ["promise"]);
  assert.equal(isShort(promise), false);
});

test("a failed transaction beside a lookup that failed is still the answer", async () => {
  // the promise lookup fails, the commitment finds nothing, ?tx= says the transaction failed: known, not unknown
  for (const [h, ante] of [[FINAL, true], [OPEN, false]]) {
    const f = await find(h, api({ failPromise: true, failedTx: FAILED }));
    assert.deepEqual(f.failedTx, failure(ante));
    assert.equal(f.partial, "503");
    assert.equal(f.error, null);
    assert.equal(isShort(f), false);
  }
  // with ?tx= itself failing nothing is known
  const lost = await find(FINAL, api({ failTx: true, failedTx: FAILED }));
  assert.equal(lost.failedTx, null);
  assert.equal(lost.error, "503");
  assert.equal(isShort(lost), true);
});

test("isShort is what it was for every answer without a failed transaction", async () => {
  // found whole: nothing more to ask
  assert.equal(isShort(await find(TX)), false);
  assert.equal(isShort(await find(hx("c3"))), false);
  // nothing found, nothing failed: asked again, for a blob not indexed yet
  assert.equal(isShort(await find(hx("5a"))), true);
  // one found while another lookup failed: incomplete
  const partial = await find(COMMITMENT, api({ failTx: true }));
  assert.equal(partial.total, 1);
  assert.equal(isShort(partial), true);
  // nothing answers
  assert.equal(isShort(await find(TX, api({ down: true }))), true);
  // every answer without one carries failedTx null
  for (const f of [await find(TX), await find(hx("5a")), partial]) assert.equal(f.failedTx, null);
});
