// golden-compare.mjs <base dir> <head dir> <report.json>: compares two runs of TestGoldenAnswers request for request.
// An answer file is "GET <route>\nstatus <n>\ncontent-type <t>\n\n<body>"; "compute_ms" (a computation's own duration,
// written into some answers) is the only part allowed to differ. Prints a Markdown summary (for the job summary) and
// writes every difference, with the JSON paths that changed, to <report.json>. Exits 0 whatever it finds: whether a
// difference was intended is for the reader of the summary.
import { readFileSync, readdirSync, writeFileSync, existsSync } from "node:fs";

const [A, B, OUT] = process.argv.slice(2);
const norm = (s) => s.replace(/"compute_ms":\d+/g, '"compute_ms":0');
const list = (d) => (existsSync(`${d}/answers`) ? readdirSync(`${d}/answers`) : []);
const names = [...new Set([...list(A), ...list(B)])].sort();

function split(text) {
  const i = text.indexOf("\n\n");
  const head = i < 0 ? text : text.slice(0, i);
  const body = i < 0 ? "" : text.slice(i + 2);
  const route = (head.match(/^GET (.*)$/m) || [])[1] || "";
  const status = (head.match(/^status (\d+)$/m) || [])[1] || "";
  return { route, status, body };
}

// paths whose values differ, at most `max`
function diffPaths(a, b, path = "$", out = [], max = 25) {
  if (out.length >= max) return out;
  if (typeof a !== typeof b || Array.isArray(a) !== Array.isArray(b) || (a === null) !== (b === null)) {
    out.push({ path, base: brief(a), head: brief(b) });
    return out;
  }
  if (Array.isArray(a)) {
    if (a.length !== b.length) out.push({ path: `${path}.length`, base: a.length, head: b.length });
    for (let i = 0; i < Math.min(a.length, b.length) && out.length < max; i++) diffPaths(a[i], b[i], `${path}[${i}]`, out, max);
    return out;
  }
  if (a && typeof a === "object") {
    for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
      if (out.length >= max) break;
      if (!(k in b)) out.push({ path: `${path}.${k}`, base: brief(a[k]), head: "(removed)" });
      else if (!(k in a)) out.push({ path: `${path}.${k}`, base: "(added)", head: brief(b[k]) });
      else diffPaths(a[k], b[k], `${path}.${k}`, out, max);
    }
    return out;
  }
  if (a !== b) out.push({ path, base: brief(a), head: brief(b) });
  return out;
}
const brief = (v) => {
  const s = JSON.stringify(v);
  return s === undefined ? "undefined" : s.length > 120 ? s.slice(0, 117) + "..." : s;
};
// a path with its array indices folded, so one change repeated over every row reads as one kind of change
const fold = (p) => p.replace(/\[\d+\]/g, "[]");

let same = 0;
const differ = [], onlyBase = [], onlyHead = [];
const kinds = new Map();
for (const n of names) {
  const pa = `${A}/answers/${n}`, pb = `${B}/answers/${n}`;
  if (!existsSync(pb)) { onlyBase.push(n); continue; }
  if (!existsSync(pa)) { onlyHead.push(n); continue; }
  const a = readFileSync(pa, "utf8"), b = readFileSync(pb, "utf8");
  if (a === b || norm(a) === norm(b)) { same++; continue; }
  const x = split(norm(a)), y = split(norm(b));
  let paths = [];
  try { paths = diffPaths(JSON.parse(x.body), JSON.parse(y.body)); }
  catch { paths = [{ path: "(body is not JSON or differs as text)", base: brief(x.body.slice(0, 80)), head: brief(y.body.slice(0, 80)) }]; }
  if (x.status !== y.status) paths.unshift({ path: "(status)", base: x.status, head: y.status });
  differ.push({ name: n, route: x.route || y.route, paths });
  const route = (x.route || y.route).replace(/\?.*/, "").replace(/\/[0-9a-f]{20,}|\/celestiavalcons1[0-9a-z]+|\/celestia1[0-9a-z]+/g, "/<id>");
  for (const p of paths) {
    const k = `${route} ${fold(p.path)} ${p.head === "(removed)" ? "removed" : p.base === "(added)" ? "added" : "changed"}`;
    kinds.set(k, (kinds.get(k) || 0) + 1);
  }
}
const recompute = ["24h", "7d", "all"].map((w) => {
  const fa = `${A}/recompute-${w}.txt`, fb = `${B}/recompute-${w}.txt`;
  if (!existsSync(fa) || !existsSync(fb)) return `recompute ${w}: missing on one side`;
  const a = readFileSync(fa, "utf8"), b = readFileSync(fb, "utf8");
  const tail = (s) => (s.trim().split("\n").pop() || "").slice(0, 80);
  return `recompute ${w}: ${a === b ? "identical" : "DIFFERS"} (base: ${tail(a)}; head: ${tail(b)})`;
});

writeFileSync(OUT, JSON.stringify({ same, differ, onlyBase, onlyHead, kinds: [...kinds].sort((p, q) => q[1] - p[1]), recompute }, null, 1));
const lines = [];
lines.push(`### Golden answers: ${same} identical, ${differ.length} differ, ${onlyBase.length} only in base, ${onlyHead.length} only in head`);
lines.push("");
for (const r of recompute) lines.push(`- ${r}`);
if (kinds.size) {
  lines.push("", "| changes | route and JSON path (indices folded) |", "|---:|---|");
  for (const [k, n] of [...kinds].sort((p, q) => q[1] - p[1]).slice(0, 80)) lines.push(`| ${n} | \`${k}\` |`);
}
if (onlyBase.length) lines.push("", `Only in base (first 20): ${onlyBase.slice(0, 20).join(", ")}`);
if (onlyHead.length) lines.push("", `Only in head (first 20): ${onlyHead.slice(0, 20).join(", ")}`);
console.log(lines.join("\n"));
