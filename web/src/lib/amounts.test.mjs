// amounts.ts (npm test; node --test): how the transaction page and the blob page write a coin the chain printed. The
// transaction fee in TIA down to the utia, the chain's figure with its digits grouped at any length, and a requested
// amount exact. The module is TypeScript with no runtime import, so the test compiles it with the project's own
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
const { coins, feeTia, exactCoin } = await load("./amounts.ts");

test("feeTia writes a transaction fee in TIA down to the utia", () => {
  assert.equal(feeTia("8000utia"), "0.008 TIA");
  assert.equal(feeTia("800utia"), "0.0008 TIA");
  assert.equal(feeTia("240utia"), "0.00024 TIA");
  assert.equal(feeTia("1utia"), "0.000001 TIA");
  assert.equal(feeTia("2500000utia"), "2.5 TIA");
});

test("feeTia leaves another denomination as coins() writes it", () => {
  assert.equal(feeTia("1500ufoo"), coins("1500ufoo"));
  assert.equal(feeTia("1500ufoo"), "1,500 ufoo");
  // a utia figure past a Number's exact digits is written as the chain printed it, grouped
  assert.equal(feeTia("1000000000000000utia"), coins("1000000000000000utia"));
});

test("coins groups the chain's figure at any length", () => {
  assert.equal(coins("8000utia"), "8,000 utia");
  assert.equal(coins("800utia"), "800 utia");
  assert.equal(coins("1000000000000000utia"), "1,000,000,000,000,000 utia");
  assert.equal(coins("123456789012345678901utia"), "123,456,789,012,345,678,901 utia");
  assert.equal(coins("8000utia,15ufoo"), "8,000 utia, 15 ufoo");
});

test("coins groups as int() does up to 15 digits", () => {
  for (const n of ["0", "7", "999", "1000", "65432", "1234567", "999999999999999"]) {
    assert.equal(coins(`${n}utia`), `${Number(n).toLocaleString("en-US")} utia`);
  }
});

test("exactCoin writes a requested amount exact, its trailing zeros trimmed", () => {
  assert.equal(exactCoin("1000000000000000utia"), "1,000,000,000 TIA");
  assert.equal(exactCoin("1utia"), "0.000001 TIA");
  assert.equal(exactCoin("0utia"), "0 TIA");
  assert.equal(exactCoin("1500000utia"), "1.5 TIA");
  assert.equal(exactCoin("123456789utia"), "123.456789 TIA");
  assert.equal(exactCoin("5ufoo"), "5 ufoo");
});
