#!/usr/bin/env bash
#
# persistence: the record survives a restart, the index is sound, and the
# index can be thrown away and rebuilt from the record to the same answer.
#
# Two kinds of check, kept apart because they need different things:
#
#   live (1-2): restart the collector and the scanner and every checkpoint
#     (state.json last_scanned_height, ingest cursors) must be at or past
#     where it was, the scanner must log that it resumed, and the row counts
#     must not drop; then integrity_check and foreign_key_check on the live
#     database. These read the live host twice and compare monotonically,
#     which holds under traffic.
#
#   cut (3-5): everything that compares one figure with another is done on
#     ONE consistent cut of the record, taken with backup-manifest snapshot
#     (state.json first, then byte lengths up to each file's last complete
#     line, dependents before what they refer to, then hashed and parsed;
#     the snapshot's state.json is the one read at the cut). The cut has
#     no duplicate line; the database rebuilt from it
#     alone holds exactly its records; and sentinel-recompute, reading the
#     same cut, agrees with a second observer-api serving the database
#     built from that cut, both evaluated as of the cut's own timestamp. No
#     live figure is compared with a cut figure: under traffic they differ
#     for no reason worth a red line.
#
# Usage: sudo deploy/test/persistence.sh [instance] [spare-port]   (mocha, 18082)
# Needs python3, the binaries in /usr/local/bin, and about the size of the
# data directory free under $TMPDIR. Exit 0 when every check passes.
set -o errexit -o nounset -o pipefail
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

INSTANCE="${1:-mocha}"
PORT="${2:-18082}"
ENVFILE=/etc/fibre-observer/$INSTANCE.env
MANIFEST_TOOL="$(dirname "$0")/../backup-manifest.py"
[ -x "$MANIFEST_TOOL" ] || MANIFEST_TOOL=/usr/local/bin/fibre-backup-manifest

[ -r "$ENVFILE" ] || { echo "no $ENVFILE" >&2; exit 2; }
DATA_DIR=$(envval "$ENVFILE" DATA_DIR); RPC=$(envval "$ENVFILE" RPC); VANTAGE=$(envval "$ENVFILE" VANTAGE)
DB="$DATA_DIR/observer.db"
[ -f "$DB" ] || { echo "no $DB" >&2; exit 2; }
[ -x "$MANIFEST_TOOL" ] || { echo "backup-manifest tool not found (deploy/backup-manifest.py or /usr/local/bin/fibre-backup-manifest)" >&2; exit 2; }

sql() {
  python3 - "$1" "$2" <<'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
for row in con.execute(sys.argv[2]):
    print("\t".join("" if v is None else str(v) for v in row))
PY
}
scanned() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("last_scanned_height", 0))' "$DATA_DIR/state.json" 2>/dev/null || echo 0; }
counts() { sql "$1" "SELECT (SELECT COUNT(*) FROM publications), (SELECT COUNT(*) FROM probes), (SELECT COUNT(*) FROM reachability)"; }
cursors() { sql "$1" "SELECT file, byte_offset FROM ingest_cursors ORDER BY file"; }
# recorded <db>: the measurement lines the store holds or stands for. A
# publication the prober wrote row by row as sampled out is one decision once
# collapsed (source = 'rows'), standing for points x validators lines.
recorded() { sql "$1" "SELECT (SELECT COUNT(*) FROM probes) + (SELECT COALESCE(SUM(points * validators), 0) FROM sampling_decisions WHERE source = 'rows')"; }
distinct_keys() { # distinct_keys <dir> -> "<publications lines> <distinct promises> <dup> <measurement lines> <distinct slots> <dup>"
  # the whole record of each file: archived segments first, then the live file
  python3 - "$1" "$MANIFEST_TOOL" <<'PY'
import json, os, subprocess, sys
d = sys.argv[1]
def scan(name, keyf):
    path = os.path.join(d, name)
    if not os.path.exists(path):
        return (0, 0, 0)
    seen, dup, n = set(), 0, 0
    cat = subprocess.Popen([sys.argv[2], "cat", d, name], stdout=subprocess.PIPE)
    for line in cat.stdout:
        line = line.strip()
        if not line: continue
        n += 1
        try: k = keyf(json.loads(line))
        except Exception: continue
        if k is None: continue
        if k in seen: dup += 1
        seen.add(k)
    if cat.wait() != 0:
        raise SystemExit(f"{name}: the manifest tool could not read the record")
    return (n, len(seen), dup)
p = scan("publications.jsonl", lambda r: r.get("promise_hash"))
m = scan("measurements.jsonl", lambda r: None if not all(k in r for k in ("vantage","promise_hash","validator_address","scheduled_at")) else (r["vantage"], r["promise_hash"], r["validator_address"], r["scheduled_at"]))
print(*p, *m)
PY
}

TMP=$(mktemp -d "${TMPDIR:-/tmp}/fibre-persistence.XXXXXX")
API_PID=""
cleanup() { [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true; rm -rf "$TMP"; }
trap cleanup EXIT

echo "== 1. restart keeps every checkpoint (live)"
before_h=$(scanned); before_c=$(counts "$DB"); before_cur=$(cursors "$DB")
echo "  before: scanned=$before_h counts(pub,probe,reach)=$before_c"
systemctl restart "fibre-collector@$INSTANCE" "fibre-scan@$INSTANCE"
sleep 25
for u in fibre-collector fibre-scan; do
  [ "$(systemctl is-active "$u@$INSTANCE")" = "active" ] && pass "$u@$INSTANCE active after restart" || fail "$u@$INSTANCE not active after restart"
done
after_h=$(scanned); after_c=$(counts "$DB"); after_cur=$(cursors "$DB")
[ "$after_h" -ge "$before_h" ] && pass "scanner checkpoint $before_h -> $after_h" || fail "scanner checkpoint went backwards: $before_h -> $after_h"
if journalctl -u "fibre-scan@$INSTANCE" --since '-2 min' --no-pager 2>/dev/null | grep -q -E 'resuming: last_scanned='; then
  pass "scanner logged that it resumed from its checkpoint"
else
  warn "no 'resuming' line in the scanner journal in the last 2 min"
fi
python3 - "$before_c" "$after_c" <<'PY' && pass "row counts did not drop ($before_c -> $after_c)" || fail "row counts dropped: $before_c -> $after_c"
import sys
b = [int(x) for x in sys.argv[1].split("\t")]; a = [int(x) for x in sys.argv[2].split("\t")]
sys.exit(0 if all(x >= y for x, y in zip(a, b)) else 1)
PY
python3 - "$before_cur" "$after_cur" <<'PY' && pass "ingest cursors did not go backwards" || fail "an ingest cursor went backwards"
import sys
def parse(s):
    return {l.split("\t")[0]: int(l.split("\t")[1]) for l in s.splitlines() if "\t" in l}
b, a = parse(sys.argv[1]), parse(sys.argv[2])
sys.exit(0 if all(a.get(f, 0) >= o for f, o in b.items()) else 1)
PY

echo "== 2. database integrity (live)"
ic=$(sql "$DB" "PRAGMA integrity_check" | head -3)
[ "$ic" = "ok" ] && pass "integrity_check ok" || fail "integrity_check: $ic"
fk=$(sql "$DB" "PRAGMA foreign_key_check" | wc -l)
[ "$fk" = 0 ] && pass "foreign_key_check clean" || fail "foreign_key_check: $fk violation(s)"

echo "== 3. one consistent cut of the record"
if "$MANIFEST_TOOL" snapshot "$DATA_DIR" "$TMP" > "$TMP/manifest.log" 2>&1; then
  pass "cut taken: $(head -1 "$TMP/manifest.log")"
else
  fail "snapshot failed: $(tail -3 "$TMP/manifest.log")"; exit 1
fi
[ -f "$TMP/sampling-master.key" ] && fail "the master key was copied into the cut" || pass "master key not in the cut"
TAKEN_AT=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["taken_at"])' "$TMP/manifest.json")
read -r plines pkeys pdup mlines mkeys mdup <<<"$(distinct_keys "$TMP")"
# A publication appended again is a line the store keeps once (ON CONFLICT
# DO NOTHING on its promise): a re-scan of heights older than the scanner's
# dedupe window, which is its live file since publications.jsonl rotates,
# writes one. Said, not failed; the rebuild is held to the distinct
# promises below.
[ "$pdup" = 0 ] && pass "publications: $plines lines, $pkeys distinct promises, 0 duplicates" || warn "publications: $pdup line(s) repeat a promise already on record (a re-scan); the store keeps the first"
[ "$mdup" = 0 ] && pass "measurements: $mlines lines, $mkeys distinct slots, 0 duplicates" || fail "measurements: $mdup duplicate slot line(s)"

echo "== 4. rebuild the index from the cut"
if timeout 1200 /usr/local/bin/observer-collector -rpc "$RPC" -data-dir "$TMP" -vantage "$VANTAGE" -once \
     -endpoints-every 0 -escrow-every 0 -avatars-every 0 -export-hour -1 > "$TMP/rebuild.log" 2>&1; then
  pass "observer-collector --once rebuilt the index ($(tail -1 "$TMP/rebuild.log"))"
else
  fail "rebuild failed: $(tail -3 "$TMP/rebuild.log")"; exit 1
fi
rc=$(counts "$TMP/observer.db"); IFS=$'\t' read -r rpub rprobe _ <<<"$rc"
[ "$rpub" = "$pkeys" ] && pass "rebuilt publications ($rpub) == distinct promises in the cut ($pkeys)" || fail "rebuilt publications $rpub != cut $pkeys"
rlines=$(recorded "$TMP/observer.db")
[ "$rlines" = "$mkeys" ] && pass "rebuilt probes ($rprobe, standing for $rlines lines) == distinct slots in the cut ($mkeys)" || fail "rebuilt probes stand for $rlines lines != cut $mkeys"
echo "  live counts now: $(counts "$DB" | tr '\t' ' ') (a different moment; not compared)"

echo "== 5. recompute from the cut == a second API serving the cut, as of $TAKEN_AT"
/usr/local/bin/observer-api -data-dir "$TMP" -listen "127.0.0.1:$PORT" -vantage "$VANTAGE" > "$TMP/api.log" 2>&1 &
API_PID=$!
if wait_http "http://127.0.0.1:$PORT/v1/meta" 60; then
  pass "observer-api on :$PORT serves the rebuilt index"
else
  fail "observer-api did not come up on :$PORT: $(tail -3 "$TMP/api.log")"; exit 1
fi
if [ -x /usr/local/bin/sentinel-recompute ]; then
  if out=$(timeout 900 /usr/local/bin/sentinel-recompute -data-dir "$TMP" -window all -as-of "$TAKEN_AT" -api "http://127.0.0.1:$PORT" 2>&1); then
    pass "sentinel-recompute agrees with the API over the same cut"
    printf '%s\n' "$out" | tail -4 | sed 's/^/       /'
  else
    fail "sentinel-recompute differs from the API over the same cut:"
    printf '%s\n' "$out" | tail -12 | sed 's/^/       /'
  fi
else
  fail "/usr/local/bin/sentinel-recompute missing"
fi

echo
if [ "$FAILED" = 0 ]; then echo "persistence: every check passed"; else echo "persistence: FAILED"; exit 1; fi
