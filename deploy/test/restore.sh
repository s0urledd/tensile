#!/usr/bin/env bash
#
# restore: a backup is only a backup once something has been brought back
# from it, checked against what was written down when it was taken, and
# served.
#
# backup.sh writes a manifest before it copies anything: the byte length
# of every record file at one cut, the SHA-256 of exactly those bytes, the
# record count in them, and the scanner's checkpoint. This pulls the copy
# and the manifest from BACKUP_REMOTE, and:
#
#   - verifies the copy against the manifest: every listed file present,
#     at least as long as the cut (the files only grow; a copy taken after
#     the cut is trimmed back to it), the hash of the cut equal, every line
#     a complete JSON record, the record count equal, and no sampling
#     master key. The copy's state.json was read later than the cut and
#     points past it; verify puts the cut's own state.json (carried whole
#     in the manifest) in its place, so the scanner resumes from the
#     checkpoint these records were cut with. Missing, truncated, altered
#     or unparseable fails. A segment retired on the host (its lines
#     proven in the exports, the store and the remote, its file removed)
#     is checked by its file when the remote has one, and otherwise read
#     back from the copy's exports/, every digest held, as the rebuild
#     below then reads it.
#   - rebuilds the database from the verified cut and requires it to hold
#     exactly the cut's records (publications one per promise, as the store
#     keeps them: a line appended again by a re-scan is said, not failed);
#   - starts a second observer-api on a spare port against it and reads
#     /v1/meta, /v1/network and /v1/validators, waiting out the first
#     computation of each window; the counts it serves must be the rebuilt
#     ones.
#
# The live host's counts are printed for orientation and compared with
# nothing: they are a different moment.
#
# Usage: sudo deploy/test/restore.sh [instance] [spare-port]   (mocha, 18081)
# Needs rclone with RCLONE_CONFIG (/etc/fibre-observer/rclone.conf) and
# BACKUP_REMOTE set in the env file. Exit 0 when every check passes.
set -o errexit -o nounset -o pipefail
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

INSTANCE="${1:-mocha}"
PORT="${2:-18081}"
ENVFILE=/etc/fibre-observer/$INSTANCE.env
MANIFEST_TOOL="$(dirname "$0")/../backup-manifest.py"
[ -x "$MANIFEST_TOOL" ] || MANIFEST_TOOL=/usr/local/bin/fibre-backup-manifest

[ -r "$ENVFILE" ] || { echo "no $ENVFILE" >&2; exit 2; }
REMOTE=$(envval "$ENVFILE" BACKUP_REMOTE); RPC=$(envval "$ENVFILE" RPC); VANTAGE=$(envval "$ENVFILE" VANTAGE); API_LISTEN=$(envval "$ENVFILE" API_LISTEN)
export RCLONE_CONFIG="${RCLONE_CONFIG:-/etc/fibre-observer/rclone.conf}"
[ -n "$REMOTE" ] || { echo "BACKUP_REMOTE is empty in $ENVFILE: there is no backup to restore from" >&2; exit 1; }
command -v rclone >/dev/null || { echo "rclone is not installed" >&2; exit 1; }
[ -x "$MANIFEST_TOOL" ] || { echo "backup-manifest tool not found" >&2; exit 2; }

TMP=$(mktemp -d "${TMPDIR:-/tmp}/fibre-restore.XXXXXX")
API_PID=""
cleanup() { [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true; rm -rf "$TMP"; }
trap cleanup EXIT
counts() {
  python3 - "$1" <<'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
# probes, and the lines of measurements.jsonl they stand for: a publication
# the prober recorded row by row as sampled out is one decision in the
# store once collapsed (source = 'rows'), standing for points x validators
# of those lines.
print(*con.execute("""SELECT (SELECT COUNT(*) FROM publications), (SELECT COUNT(*) FROM probes),
    (SELECT COUNT(*) FROM probes) + (SELECT COALESCE(SUM(points * validators), 0) FROM sampling_decisions WHERE source = 'rows')""").fetchone())
PY
}

# The remote is not printed: one given whole (":s3,access_key_id=...:bucket")
# carries its credentials.
echo "== 1. pull the copy from BACKUP_REMOTE/$INSTANCE"
if rclone copy "$REMOTE/$INSTANCE" "$TMP" --transfers 4 --checkers 8 --stats-one-line --stats 0 --log-level NOTICE; then
  pass "copied $(find "$TMP" -type f | wc -l) file(s), $(du -sh "$TMP" | cut -f1)"
else
  fail "rclone copy failed"; exit 1
fi
if [ ! -f "$TMP/backup-manifest.json" ]; then
  fail "no backup-manifest.json in the copy: this backup predates manifests, or backup.sh did not run since; nothing here can be verified"
  echo "restore: FAILED"; exit 1
fi

echo "== 2. verify the copy against the manifest"
if "$MANIFEST_TOOL" verify "$TMP" "$TMP/backup-manifest.json" > "$TMP/verify.log" 2>&1; then
  pass "every file matches the manifest"
  sed 's/^/       /' "$TMP/verify.log" | head -20
else
  fail "the copy does not match the manifest:"
  sed 's/^/       /' "$TMP/verify.log" | tail -20
  echo "restore: FAILED"; exit 1
fi
newest=$(find "$TMP" -name '*.jsonl' -printf '%T@\n' 2>/dev/null | sort -n | tail -1 | cut -d. -f1)
if [ -n "$newest" ]; then
  age=$(( $(date +%s) - newest ))
  echo "  the copy's newest record file is $((age / 3600))h $(( (age % 3600) / 60 ))m old; manifest taken $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["taken_at"])' "$TMP/backup-manifest.json")"
fi
# the live lines and the archived ones, retired included: the rebuild reads
# every file from its first byte (archive/, and the exports for a retired
# range), and observer-archive rotates publications.jsonl as it does
# measurements.jsonl. The store keeps one publication per promise, and a
# re-scan of heights older than the scanner's dedupe window (its live file)
# can append one again, so the rebuilt publications are the distinct
# promises of the cut, read as the rebuild reads them; the lines read must
# be every record the manifest names.
want_probe=$(manifest_records "$TMP/backup-manifest.json" measurements.jsonl)
pub_records=$(manifest_records "$TMP/backup-manifest.json" publications.jsonl)
if read -r pub_lines want_pub <<<"$(record_promises "$MANIFEST_TOOL" "$TMP")" && [ -n "$want_pub" ]; then
  [ "$pub_lines" = "$pub_records" ] && pass "publications.jsonl reads whole: $pub_lines line(s), every record the manifest names" || fail "publications.jsonl reads $pub_lines line(s), the manifest names $pub_records"
  [ "$pub_lines" = "$want_pub" ] || warn "publications.jsonl: $((pub_lines - want_pub)) line(s) repeat a promise already on record (a re-scan); the store keeps the first"
else
  fail "publications.jsonl could not be read through the manifest tool"; exit 1
fi

echo "== 3. rebuild the database from the verified cut"
if timeout 1200 /usr/local/bin/observer-collector -rpc "$RPC" -data-dir "$TMP" -vantage "$VANTAGE" -once \
     -endpoints-every 0 -escrow-every 0 -avatars-every 0 -export-hour -1 > "$TMP/rebuild.log" 2>&1; then
  pass "rebuilt: $(tail -1 "$TMP/rebuild.log")"
else
  fail "rebuild failed: $(tail -3 "$TMP/rebuild.log")"; exit 1
fi
read -r rpub rprobe rlines <<<"$(counts "$TMP/observer.db")"
[ "$rpub" = "$want_pub" ] && pass "rebuilt publications ($rpub) == distinct promises in the cut ($want_pub)" || fail "rebuilt publications $rpub != distinct promises in the cut $want_pub"
[ "$rlines" = "$want_probe" ] && pass "rebuilt probes ($rprobe, standing for $rlines lines) == manifest records ($want_probe)" || fail "rebuilt probes stand for $rlines lines != manifest records $want_probe"

echo "== 4. serve it on :$PORT"
/usr/local/bin/observer-api -data-dir "$TMP" -listen "127.0.0.1:$PORT" -vantage "$VANTAGE" > "$TMP/api.log" 2>&1 &
API_PID=$!
if wait_http "http://127.0.0.1:$PORT/v1/meta" 60; then
  pass "observer-api answers /v1/meta from the restored directory"
else
  fail "observer-api did not come up on :$PORT: $(tail -3 "$TMP/api.log")"; exit 1
fi
code=$(http_code "http://127.0.0.1:$PORT/v1/meta" "$TMP/meta.json")
read -r apub aprobe <<<"$(python3 -c 'import json,sys; c=json.load(open(sys.argv[1]))["counts"]; print(c["Publications"], c["Probes"])' "$TMP/meta.json")"
[ "$code" = 200 ] && [ "$apub" = "$rpub" ] && [ "$aprobe" = "$rprobe" ] && pass "/v1/meta counts are the rebuilt ones (publications=$apub probes=$aprobe)" || fail "/v1/meta -> $code counts publications=$apub probes=$aprobe, rebuilt $rpub/$rprobe"
# The restored directory has no snapshot files, so this API computes every
# window from its start, and a read that arrives before a window has been
# computed is a 503 with "computing": true: not an answer yet, and asked
# again (snapshot_code). The 24h windows take about half a minute on a store
# the live one's size; the ten minutes are for a slow disk.
for ep in network validators; do
  t0=$(date +%s)
  code=$(HTTP_TIMEOUT=30 snapshot_code "http://127.0.0.1:$PORT/v1/$ep?window=24h" 600)
  took=$(( $(date +%s) - t0 ))
  [ "$code" = 200 ] && pass "/v1/$ep answers from the restored data (after ${took}s)" || fail "/v1/$ep -> $code after ${took}s"
done
lc=$(curl -sS -m 10 "http://$API_LISTEN/v1/meta" 2>/dev/null | python3 -c 'import json,sys; c=json.load(sys.stdin)["counts"]; print(c["Publications"], c["Probes"])' 2>/dev/null || echo "? ?")
echo "  live host now: publications/probes = $lc (a different moment; not compared)"

echo
if [ "$FAILED" = 0 ]; then echo "restore: every check passed (restored API stopped, $TMP removed)"; else echo "restore: FAILED"; exit 1; fi
