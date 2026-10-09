#!/usr/bin/env bash
#
# selftest: the acceptance tests' own regression tests. Runs anywhere with
# bash, python3, curl and util-linux's flock — CI included — against fake
# servers on loopback and a fake rclone (fake-rclone.sh) over a local
# directory, and needs no root, no systemd, no rclone. Each case is a bug
# that was shipped once, or a guarantee something else rests on:
#
#   http_code       a closed port is 000, not 000000; 200 is 200; 503 is 503
#   health reasons  503 with the chain source named passes; 503 for the disk
#                   alone does not; "ok" does not; garbage does not
#   env parsing     values with spaces and quotes survive, comments and bad
#                   lines are skipped, every pair reaches the command as its
#                   own argument (the xargs bug)
#   manifest        a copy that grew after the cut verifies (and is trimmed);
#                   a truncated, altered or missing file fails; the master
#                   key in the copy fails; a state.json the copy took later
#                   (records at 100, checkpoint at 101) is replaced by the
#                   cut's own, whole, not just its height; a manifest that
#                   carries no state cannot accept one past its checkpoint;
#                   the cut ends on a line boundary while a writer is
#                   mid-line; a cut that ends inside a line fails verify; a
#                   line that is not a JSON record is listed in the cut, not
#                   refused, and a manifest that does not list it fails; a
#                   name in the manifest that is not a record file's (an
#                   absolute path, a ".."), a segment name with a directory
#                   in it, and a file or state.json in the copy that is a
#                   link out of it are never touched
#   manifest over   the cut names the archived segments and the live base
#   an archive      and counts both, and restore.sh expects a rebuild to
#                   hold both, publications as measurements (the drill once
#                   counted the live publications alone);
#                   cat reads the whole record in order;
#                   a snapshot carries the segments and verifies; a missing
#                   or altered segment fails; an index that places the live
#                   file elsewhere fails; a live file no generation
#                   describes is refused at the cut
#   manifest over   a segment retired to two exports: cat reads the same
#   a retired       record and end the same length; the cut lists it
#   segment         without its file; a snapshot carries the exports and
#                   verifies from them; a copy with the segment's file is
#                   checked by the file; an export altered or missing, an
#                   index entry that is not the member, or exports that
#                   leave part of the range out fail; cat over a bad export
#                   fails and hands out no byte of the segment; an index
#                   an older build rewrote without the retired record reads
#                   it from retired.json; another vantage's rotated
#                   heartbeats are cut, read and verified
#   manifest over   a copy whose live files were rotated after the cut (a
#   a rotated copy  file archived before, and one archived for the first
#                   time) verifies, with the cut's live lines read back from
#                   the newer segments into the live file's place; a newer
#                   segment altered or missing fails
#   remote proof    backup.sh with the fake rclone: the observer's segments
#                   and each other vantage's go before the live files, with
#                   their indexes and retired.json, and no lock or master
#                   key goes; each export is read back once and recorded in
#                   exports/remote.jsonl with the remote's fingerprint; a
#                   remote copy that differs or cannot be read is ok false
#                   and the run passes, and is read again the next night; a
#                   rebuilt tarball is read again; a failed copy fails the
#                   run, proves nothing and leaves exports/remote-copy.json
#                   as the last copy that finished; a new BACKUP_REMOTE
#                   reads every export back again; a torn last line, NUL
#                   bytes included, is dropped, not closed into a line that
#                   is not a check; the remote is never printed, rclone's
#                   own messages from a failed copy included
#   backup cut      backup.sh with the manifest tool: the manifest goes
#                   last, on its own; a line that is not a record is
#                   copied and listed, and the copy verifies; a night whose
#                   manifest does not land after a rotation fails and
#                   leaves the last one, which the remote's copy still
#                   verifies against; a cut that cannot be taken copies
#                   everything, records the copy, uploads no manifest and
#                   fails the run
#   failed          failed_txs.jsonl, in a data dir and manifest of its own:
#   transactions    the cut counts its records and a copy verifies; a copy
#                   with it altered in place (the output names it and its
#                   digest) or missing fails; a data dir without it cuts and
#                   verifies; record_distinct counts a line written twice
#                   once
#   transaction     tx_costs.jsonl, in a data dir and manifest of its own:
#   costs           the cut counts its records and a copy verifies; a copy
#                   with it altered in place (the output names it and its
#                   digest) or missing fails; a data dir without it cuts and
#                   verifies; record_distinct counts a line written twice
#                   once
#   vantage pull   vantage-sync.sh: the pull resumes from the local file's
#                   logical end, a rotated one included, and a remote file
#                   shorter than the record fetches nothing and fails; two
#                   pulls at once append the new bytes once; only whole
#                   lines are appended, from a remote line still being
#                   written or a link that breaks part-way
#   rpc-check       app version 9 + fibre code 6 passes; 10 + 6 fails; 10 + 0
#                   passes; no block_results fails; a second RPC that does
#                   not answer fails; two nodes disagreeing on a hash fails;
#                   two agreeing pass
#   healthwatch     one alert per fault; a second failing check alerts while
#                   the state stays degraded; recovery alerts; a two-line
#                   state file from the older build does not alert by
#                   itself; a failed nightly backup or archive unit is a
#                   failing check of its own; a run exits 0 whatever the
#                   observer's state; an alert every destination refused is
#                   not recorded, fails the run and is sent again by the
#                   next; a state that cannot be written fails the run
#                   after the alert went out, and the next alerts again; a
#                   backup copy older than 26 h (with BACKUP_REMOTE set),
#                   none at all beside two-day-old exports, yesterday's
#                   export missing after 04:00 UTC, a second vantage with
#                   nothing new for 30 min (its live file emptied by a
#                   rotation too), and a failed pull unit with nothing new
#                   for 10 min each fail a check of their own; so does
#                   yesterday's export whose newest line in
#                   exports/remote.jsonl is ok false, or that has none,
#                   from 06:00 UTC with BACKUP_REMOTE set; a new host
#                   with no exports directory yet completes its run; the
#                   webhook and the bot token never reach curl's command
#                   line
#   exposure        Telegram alone is an alert destination; an env file
#   helpers         others can read is not private; fibre-site@ loads no
#                   env file with credentials; the README installs the env
#                   file 0640 root:fibre-observer and reads it only as root
#   hosting-db      a gzip cut short or failing its CRC, with enough lines
#                   before the damage, does not replace the good file
#   snapshot_code   a 503 with "computing": true is asked again until the
#                   deadline; a 503 without it and a 200 are final at once
#
# Usage: deploy/test/selftest.sh     Exit 0 when every case passes.
set -o errexit -o nounset -o pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$HERE/lib.sh"
MANIFEST="$HERE/../backup-manifest.py"
T=$(mktemp -d)
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; rm -rf "$T"; }
trap cleanup EXIT
ok=0; bad=0
check() { # the command's own output goes to the log; only a failure is printed
  if "$@" >> "$T/check.log" 2>&1; then ok=$((ok+1)); else bad=$((bad+1)); echo "  FAIL: $*"; tail -5 "$T/check.log" | sed 's/^/        /'; fi
}
not() { ! "$@"; }
eq() { [ "$1" = "$2" ] || { echo "  expected [$2], got [$1]"; return 1; }; }
serve() { # serve <script> <args...>: starts a fake server, sets PORT
  # Not $(serve ...): a background child inherits the substitution's pipe and
  # the substitution never returns, and PIDS+= in a subshell is lost.
  PORT=$(free_port)
  python3 "$HERE/$1" --port "$PORT" "${@:2}" > "$T/$1.$PORT.log" 2>&1 &
  PIDS+=("$!")
  wait_http "http://127.0.0.1:$PORT/status" 10 "200 404 503" >/dev/null || true
}
rotate() { # rotate <data-dir> <file> <lines>: observer-archive's rotation by hand:
  # the live file's first <lines> lines into a new segment, the index given
  # the segment and the new live file's generation
  python3 - "$@" <<'PY'
import gzip, hashlib, json, os, sys
d, name, k = sys.argv[1], sys.argv[2], int(sys.argv[3])
p = os.path.join(d, name)
adir = os.path.join(os.path.dirname(p), "archive", os.path.basename(p))
os.makedirs(adir, exist_ok=True)
ip = os.path.join(adir, "index.json")
idx = json.load(open(ip)) if os.path.exists(ip) else {"version": 1, "file": os.path.basename(p), "time_field": "scheduled_at", "segments": [], "generations": []}
lines = open(p, "rb").read().splitlines(keepends=True)
old, rest = b"".join(lines[:k]), b"".join(lines[k:])
sha = lambda b: hashlib.sha256(b).hexdigest()
if not idx["generations"]:
    idx["generations"].append({"base": 0, "head_sha256": sha(lines[0])})
base = idx["generations"][-1]["base"]
seg = os.path.join(adir, "%06d-rotated.jsonl.gz" % (len(idx["segments"]) + 1))
with gzip.open(seg, "wb") as z:
    z.write(old)
gz = open(seg, "rb").read()
idx["segments"].append({"name": os.path.basename(seg), "from": base, "to": base + len(old), "lines": k,
                        "sha256": sha(old), "gz_sha256": sha(gz), "gz_bytes": len(gz)})
idx["generations"].append({"base": base + len(old), "head_sha256": sha(lines[k])})
json.dump(idx, open(ip, "w"))
open(p, "wb").write(rest)
PY
}

echo "== http_code"
closed=$(free_port)
check eq "$(http_code "http://127.0.0.1:$closed/v1/health")" 000
printf '{"status":"ok","checks":[]}' > "$T/ok.json"
serve fake-http.py --code 200 --body "$T/ok.json"; p200=$PORT
check eq "$(http_code "http://127.0.0.1:$p200/v1/health")" 200
cat > "$T/degraded.json" <<'J'
{"status":"degraded","checks":[{"name":"scanner","ok":true,"detail":"alive"},{"name":"chain_liveness","ok":false,"detail":"newest block 11m3s old (2026-09-21T10:00:00Z)"}]}
J
serve fake-http.py --code 503 --body "$T/degraded.json"; p503=$PORT
check eq "$(http_code "http://127.0.0.1:$p503/v1/health" "$T/body.json")" 503
check eq "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["status"])' "$T/body.json")" degraded

echo "== health reasons"
check health_has_reason "$T/degraded.json" "chain_liveness,scanner,collector"
printf '{"status":"degraded","checks":[{"name":"disk","ok":false,"detail":"3%% free"}]}' > "$T/disk.json"
check not health_has_reason "$T/disk.json" "chain_liveness,scanner,collector"
printf '{"status":"degraded","checks":[{"name":"chain_liveness","ok":false,"detail":""}]}' > "$T/nodetail.json"
check not health_has_reason "$T/nodetail.json" "chain_liveness"
check not health_has_reason "$T/ok.json" "chain_liveness"
printf 'not json' > "$T/garbage.json"
check not health_has_reason "$T/garbage.json" "chain_liveness"
check eq "$(health_bad_checks "$T/degraded.json")" "chain_liveness: newest block 11m3s old (2026-09-21T10:00:00Z)"

echo "== snapshot_code"
printf '{"error":"this figure is being computed; ask again in a few seconds","computing":true,"window":"24h","retry_after_s":1}' > "$T/computing.json"
serve fake-http.py --code 503 --body "$T/computing.json"; pcomp=$PORT
t0=$(date +%s)
check eq "$(snapshot_code "http://127.0.0.1:$pcomp/v1/network?window=24h" 2)" 503
check test "$(( $(date +%s) - t0 ))" -ge 2   # asked again until the deadline, not once
t0=$(date +%s)
check eq "$(snapshot_code "http://127.0.0.1:$p503/v1/network?window=24h" 30)" 503
check eq "$(snapshot_code "http://127.0.0.1:$p200/v1/network?window=24h" 30 "$T/snap.json")" 200
check test "$(( $(date +%s) - t0 ))" -lt 10  # neither waited for the deadline
check eq "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["status"])' "$T/snap.json")" ok

echo "== env parsing"
cat > "$T/test.env" <<'E'
# a comment
NETWORK=mocha
VANTAGE_LOCATION="Helsinki, Finland"
VANTAGE_PROVIDER='Hetzner Online'
ALERT_WEBHOOK=https://hooks.example.org/x/y?a=1&b=2
export START_HEIGHT=0
EMPTY=
not an assignment
E
mapfile -t pairs < <(parse_envfile "$T/test.env")
check eq "${#pairs[@]}" 6
check eq "$(envval "$T/test.env" VANTAGE_LOCATION)" "Helsinki, Finland"
check eq "$(envval "$T/test.env" VANTAGE_PROVIDER)" "Hetzner Online"
check eq "$(envval "$T/test.env" ALERT_WEBHOOK)" "https://hooks.example.org/x/y?a=1&b=2"
check eq "$(envval "$T/test.env" MISSING)" ""
# every pair is one argument, and the command sees the exact values
check eq "$(env "${pairs[@]}" sh -c 'printf "%s|%s|%s|%s" "$VANTAGE_LOCATION" "$VANTAGE_PROVIDER" "$START_HEIGHT" "$ALERT_WEBHOOK"')" "Helsinki, Finland|Hetzner Online|0|https://hooks.example.org/x/y?a=1&b=2"
check eq "$(run_as_service "$T/test.env" "$(id -un)" sh -c 'printf "%s" "$VANTAGE_LOCATION"')" "Helsinki, Finland"

echo "== backup manifest"
D="$T/data"; mkdir -p "$D"
for i in 1 2 3; do printf '{"promise_hash":"p%d","x":%d}\n' "$i" "$i" >> "$D/publications.jsonl"; done
for i in 1 2 3 4 5; do printf '{"vantage":"t","promise_hash":"p1","validator_address":"v%d","scheduled_at":"2026-09-21T00:00:0%dZ"}\n' "$i" "$i" >> "$D/measurements.jsonl"; done
printf '{"last_scanned_height":500,"last_scanned_time":"2026-09-21T00:00:00Z"}\n' > "$D/state.json"
printf 'secret' > "$D/sampling-master.key"
check python3 "$MANIFEST" write "$D" "$T/manifest.json" >/dev/null
check eq "$(python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print(m["files"]["publications.jsonl"]["records"], m["files"]["measurements.jsonl"]["records"], m["checkpoint"]["last_scanned_height"])' "$T/manifest.json")" "3 5 500"
# a copy taken after the cut: longer, verifies, is trimmed
copy() { rm -rf "$T/copy"; mkdir -p "$T/copy"; cp "$D/publications.jsonl" "$D/measurements.jsonl" "$D/state.json" "$T/copy/"; }
copy; printf '{"promise_hash":"p4","x":4}\n' >> "$T/copy/publications.jsonl"
check python3 "$MANIFEST" verify "$T/copy" "$T/manifest.json" >/dev/null
check eq "$(wc -l < "$T/copy/publications.jsonl")" 3
# truncated
copy; head -c 20 "$D/measurements.jsonl" > "$T/copy/measurements.jsonl"
check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest.json"
# altered in place, same length
copy; printf '{"promise_hash":"pX","x":1}\n' > "$T/copy/publications.jsonl"; tail -n +2 "$D/publications.jsonl" >> "$T/copy/publications.jsonl"
check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest.json"
# missing file
copy; rm "$T/copy/measurements.jsonl"
check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest.json"
# the key came along
copy; cp "$D/sampling-master.key" "$T/copy/"
check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest.json"
ckpt() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("last_scanned_height", 0))' "$1"; }
# records at 100, checkpoint at 101: after the cut the scanner appended a
# record and moved its checkpoint past it, and the copy carried both. The
# record is trimmed back to the cut and state.json is the cut's own — the
# whole file, gaps and all — not the copy's: a checkpoint past the records
# it sits beside would resume the scanner past blocks this cut never held.
copy; printf '{"promise_hash":"p101","x":101}\n' >> "$T/copy/publications.jsonl"
printf '{"last_scanned_height":501,"last_scanned_time":"2026-09-21T00:00:03Z","gaps":[{"from":501,"to":501}]}\n' > "$T/copy/state.json"
check python3 "$MANIFEST" verify "$T/copy" "$T/manifest.json" >/dev/null
check eq "$(wc -l < "$T/copy/publications.jsonl")" 3
check eq "$(ckpt "$T/copy/state.json")" 500
check cmp -s "$T/copy/state.json" "$D/state.json"
# a checkpoint behind the cut is replaced the same way, and a copy with no
# state.json gets the cut's
copy; printf '{"last_scanned_height":400}\n' > "$T/copy/state.json"
check python3 "$MANIFEST" verify "$T/copy" "$T/manifest.json" >/dev/null
check eq "$(ckpt "$T/copy/state.json")" 500
copy; rm "$T/copy/state.json"
check python3 "$MANIFEST" verify "$T/copy" "$T/manifest.json" >/dev/null
check cmp -s "$T/copy/state.json" "$D/state.json"
# a manifest from before the state was carried cannot repair one: a copy
# whose state.json is past (or behind) its checkpoint fails, not passes
python3 - "$T/manifest.json" "$T/manifest-v1.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1])); m.pop("state_raw"); m.pop("state"); m["version"] = 1
json.dump(m, open(sys.argv[2], "w"))
PY
copy; printf '{"last_scanned_height":501}\n' > "$T/copy/state.json"
check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest-v1.json"
copy; check python3 "$MANIFEST" verify "$T/copy" "$T/manifest-v1.json" >/dev/null
# the manifest's own state must match its hash: an altered manifest fails
python3 - "$T/manifest.json" "$T/manifest-forged.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1])); m["state_raw"] = '{"last_scanned_height":900}\n'
json.dump(m, open(sys.argv[2], "w"))
PY
copy; check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest-forged.json"
# a writer mid-line at the cut: the cut ends at the last complete line
printf '{"promise_hash":"p4","x":4}\n{"promise_hash":"p5","x":' >> "$D/publications.jsonl"
check python3 "$MANIFEST" write "$D" "$T/manifest2.json" >/dev/null
check eq "$(python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print(m["files"]["publications.jsonl"]["records"])' "$T/manifest2.json")" 4
check eq "$(python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print(m["files"]["publications.jsonl"]["bytes"])' "$T/manifest2.json")" \
         "$(python3 -c 'import sys; b=open(sys.argv[1],"rb").read(); print(b.rfind(b"\n")+1)' "$D/publications.jsonl")"
copy; check python3 "$MANIFEST" verify "$T/copy" "$T/manifest2.json" >/dev/null
check eq "$(tail -c 1 "$T/copy/publications.jsonl" | od -An -c | tr -d ' ')" '\n'
# a cut that ends inside a line — a manifest that hashes half a record —
# fails verify even though the hash over those bytes matches: every line
# is parsed, and the last one is not a record
python3 - "$T/manifest2.json" "$D/publications.jsonl" "$T/manifest-torn.json" <<'PY'
import hashlib, json, sys
m = json.load(open(sys.argv[1])); n = m["files"]["publications.jsonl"]["bytes"] - 3
b = open(sys.argv[2], "rb").read()[:n]
m["files"]["publications.jsonl"] = {"bytes": n, "sha256": hashlib.sha256(b).hexdigest(), "records": b.count(b"\n")}
json.dump(m, open(sys.argv[3], "w"))
PY
copy; check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest-torn.json"
# snapshot copies exactly the cut (no torn tail), never the key, and its
# state.json is the cut's
rm -rf "$T/snap"; check python3 "$MANIFEST" snapshot "$D" "$T/snap" >/dev/null
check test ! -e "$T/snap/sampling-master.key"
check eq "$(wc -l < "$T/snap/publications.jsonl")" 4
check cmp -s "$T/snap/state.json" "$D/state.json"
check python3 "$MANIFEST" verify "$T/snap" >/dev/null
# a line that is not a JSON record (a write a full disk cut short, the
# restart's line glued to it; an empty line) is listed in the cut, not
# refused: it is in the record's bytes for good, and a refused cut stopped
# every copy after it. A copy of the same bytes verifies; a manifest that
# does not list it (an older one, or one altered) does not
printf '{"vantage":"t","promise_hash":"p1","validator_address":"v6","scheduled_at":"2026-1{"vantage":"t","promise_hash":"p1","validator_address":"v7","scheduled_at":"2026-09-21T00:00:07Z"}\n\n' >> "$D/measurements.jsonl"
check python3 "$MANIFEST" write "$D" "$T/manifest3.json"
check eq "$(python3 -c 'import json,sys; f=json.load(open(sys.argv[1]))["files"]["measurements.jsonl"]; print(f["records"], f["bad_lines"], f["bad_count"])' "$T/manifest3.json")" "5 [6, 7] 2"
copy; check python3 "$MANIFEST" verify "$T/copy" "$T/manifest3.json"
python3 - "$T/manifest3.json" "$T/manifest3-unlisted.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1])); f = m["files"]["measurements.jsonl"]; f.pop("bad_lines"); f.pop("bad_count")
json.dump(m, open(sys.argv[2], "w"))
PY
copy; check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest3-unlisted.json"
# The manifest comes back from the remote with the copy, and verify trims
# and writes the files it names. A name that is not a record file's (an
# absolute path, a ".."), or a file in the copy that is a link out of it,
# is not touched: here the live record outside the copy, longer than the
# cut, would have been cut to it
{ head -c 4096 /dev/zero | tr '\0' x; echo; } > "$T/victim.jsonl"
vsize=$(wc -c < "$T/victim.jsonl" | tr -d ' ')
python3 - "$T/manifest3.json" "$T/victim.jsonl" "$T/manifest-escape.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
for n in (sys.argv[2], "../victim.jsonl", "vantages/../../victim.jsonl"):
    m["files"][n] = {"bytes": 1, "sha256": "x", "records": 0}
json.dump(m, open(sys.argv[3], "w"))
PY
copy; check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest-escape.json"
check eq "$(wc -c < "$T/victim.jsonl" | tr -d ' ')" "$vsize"
copy; rm "$T/copy/measurements.jsonl"; ln -s "$T/victim.jsonl" "$T/copy/measurements.jsonl"
check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest3.json"
check eq "$(wc -c < "$T/victim.jsonl" | tr -d ' ')" "$vsize"
copy; rm "$T/copy/state.json"; ln -s "$T/victim.jsonl" "$T/copy/state.json"
check not python3 "$MANIFEST" verify "$T/copy" "$T/manifest3.json"
check eq "$(wc -c < "$T/victim.jsonl" | tr -d ' ')" "$vsize"

echo "== backup manifest over an archive"
# A file observer-archive rotated: three lines in a gzip segment, two live,
# index.json placing the live file at the segment's end by its first line.
A="$T/adata"; mkdir -p "$A/archive/measurements.jsonl"
python3 - "$A" <<'PY'
import gzip, hashlib, json, os, sys
d = sys.argv[1]
lines = [('{"vantage":"t","promise_hash":"p1","validator_address":"v%d","scheduled_at":"2026-09-1%dT00:00:00Z"}\n' % (i, i)).encode() for i in range(5)]
old, live = b"".join(lines[:3]), b"".join(lines[3:])
seg = os.path.join(d, "archive", "measurements.jsonl", "000001-2026-09-13.jsonl.gz")
with gzip.open(seg, "wb") as z:
    z.write(old)
gz = open(seg, "rb").read()
open(os.path.join(d, "measurements.jsonl"), "wb").write(live)
sha = lambda b: hashlib.sha256(b).hexdigest()
idx = {"version": 1, "file": "measurements.jsonl", "time_field": "scheduled_at", "live_since": "2026-09-13T00:00:00Z",
       "segments": [{"name": os.path.basename(seg), "from": 0, "to": len(old), "lines": 3, "sha256": sha(old), "gz_sha256": sha(gz), "gz_bytes": len(gz)}],
       "generations": [{"base": 0, "head_sha256": sha(lines[0])}, {"base": len(old), "head_sha256": sha(lines[3])}]}
json.dump(idx, open(os.path.join(d, "archive", "measurements.jsonl", "index.json"), "w"))
open(os.path.join(d, "state.json"), "w").write('{"last_scanned_height":7}\n')
PY
check python3 "$MANIFEST" write "$A" "$T/am.json" >/dev/null
check eq "$(python3 -c 'import json,sys; f=json.load(open(sys.argv[1]))["files"]["measurements.jsonl"]; print(f["records"], f["archived_records"], f["archive"]["base"], len(f["archive"]["segments"]))' "$T/am.json")" "2 3 $(gzip -dc "$A"/archive/measurements.jsonl/*.gz | wc -c | tr -d ' ') 1"
# restore.sh expects a rebuild to read every line of a file, archived ones
# included, for publications.jsonl (rotated too) as for measurements.jsonl
check eq "$(manifest_records "$T/am.json" measurements.jsonl)" 5
printf '{"files":{"publications.jsonl":{"bytes":1,"sha256":"","records":2,"archived_records":3}}}' > "$T/pm.json"
check eq "$(manifest_records "$T/pm.json" publications.jsonl)" 5
check eq "$(manifest_records "$T/pm.json" payments.jsonl)" 0
# and holds the rebuilt publications to the distinct promises of the whole
# record, since the store keeps one row per promise and a re-scan can append
# a publication again (the drill once failed a good backup on that)
PD="$T/pdata"; mkdir -p "$PD"
printf '{"promise_hash":"a"}\n{"promise_hash":"b"}\n{"promise_hash":"a"}\n' > "$PD/publications.jsonl"
check eq "$(record_promises "$MANIFEST" "$PD")" "3 2"
# the whole record reads archived lines first, and its logical end is base + live
check eq "$(python3 "$MANIFEST" cat "$A" measurements.jsonl | grep -o '"validator_address":"v[0-9]"' | tr -d '\n')" '"validator_address":"v0""validator_address":"v1""validator_address":"v2""validator_address":"v3""validator_address":"v4"'
check eq "$(python3 "$MANIFEST" end "$A" measurements.jsonl)" "$(python3 "$MANIFEST" cat "$A" measurements.jsonl | wc -c | tr -d ' ')"
# a snapshot carries the segments and the index, and verifies
rm -rf "$T/asnap"; check python3 "$MANIFEST" snapshot "$A" "$T/asnap" >/dev/null
check test -f "$T/asnap/archive/measurements.jsonl/index.json"
check python3 "$MANIFEST" verify "$T/asnap" >/dev/null
# a copy missing a segment, or with one altered, fails
acopy() { rm -rf "$T/acopy"; cp -r "$T/asnap" "$T/acopy"; }
acopy; rm "$T/acopy/archive/measurements.jsonl/"*.gz
check not python3 "$MANIFEST" verify "$T/acopy"
acopy; printf 'x' >> "$T/acopy/archive/measurements.jsonl/000001-2026-09-13.jsonl.gz"
check not python3 "$MANIFEST" verify "$T/acopy"
# an index that places the live file elsewhere fails
acopy; python3 - "$T/acopy/archive/measurements.jsonl/index.json" <<'PY'
import json, sys
i = json.load(open(sys.argv[1])); i["generations"][-1]["base"] += 1; json.dump(i, open(sys.argv[1], "w"))
PY
check not python3 "$MANIFEST" verify "$T/acopy"
# a live file the index does not describe is refused at the cut
printf '{"promise_hash":"stranger"}\n' > "$T/stranger.jsonl"; cp "$A/measurements.jsonl" "$T/live.bak"
cp "$T/stranger.jsonl" "$A/measurements.jsonl"
check not python3 "$MANIFEST" write "$A" "$T/am2.json"
cp "$T/live.bak" "$A/measurements.jsonl"

echo "== backup manifest over a retired segment"
# The segment above, retired as observer-archive -retire leaves it: its
# bytes are in two daily exports (the second one's member runs on into the
# live file), index.json names them, and its gzip file is gone.
RD="$T/rdata"; rm -rf "$RD"; cp -r "$A" "$RD"
python3 "$MANIFEST" cat "$A" measurements.jsonl > "$T/whole.jsonl"
python3 - "$RD" <<'PY'
import gzip, hashlib, io, json, os, sys, tarfile
d = sys.argv[1]
adir = os.path.join(d, "archive", "measurements.jsonl")
idx = json.load(open(os.path.join(adir, "index.json")))
seg = idx["segments"][0]
whole = gzip.open(os.path.join(adir, seg["name"])).read() + open(os.path.join(d, "measurements.jsonl"), "rb").read()
cut1 = whole.index(b"\n") + 1             # the first day: the first line
cut2 = whole.index(b"\n", seg["to"]) + 1  # the second: the rest, and the live file's first line
sha = lambda b: hashlib.sha256(b).hexdigest()
os.makedirs(os.path.join(d, "exports"))
entries = []
for day, lo, hi in (("2026-09-11", 0, cut1), ("2026-09-12", cut1, cut2)):
    member, buf = whole[lo:hi], io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in (("publications.jsonl", b""), ("measurements.jsonl", member), ("manifest.json", b"{}")):
            ti = tarfile.TarInfo(name)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))
    tgz, name = buf.getvalue(), f"tensile-t-{day}.tar.gz"
    open(os.path.join(d, "exports", name), "wb").write(tgz)
    entries.append({"name": name, "bytes": len(tgz), "sha256": sha(tgz), "files": [
        {"name": "publications.jsonl", "lines": 0, "bytes": 0, "sha256": sha(b""), "source_from": 0, "source_to": 0},
        {"name": "measurements.jsonl", "lines": member.count(b"\n"), "bytes": len(member), "sha256": sha(member), "source_from": lo, "source_to": hi}]})
json.dump(entries, open(os.path.join(d, "exports", "index.json"), "w"))
seg["retired"] = {"at": "2026-09-20T04:40:00Z", "exports": [e["name"] for e in entries], "member": "measurements.jsonl",
                  "exports_dir": "../../exports", "proof": "selftest"}
json.dump(idx, open(os.path.join(adir, "index.json"), "w"))
os.remove(os.path.join(adir, seg["name"]))
PY
# the whole record reads as before, the retired lines from the exports, and ends where it did
check eq "$(python3 "$MANIFEST" cat "$RD" measurements.jsonl | sha256sum)" "$(sha256sum < "$T/whole.jsonl")"
check eq "$(python3 "$MANIFEST" end "$RD" measurements.jsonl)" "$(wc -c < "$T/whole.jsonl" | tr -d ' ')"
# the cut lists the segment with its retired record and needs no file for it
check python3 "$MANIFEST" write "$RD" "$T/rm.json"
check eq "$(python3 -c 'import json,sys; f=json.load(open(sys.argv[1]))["files"]["measurements.jsonl"]; print(f["archived_records"], len(f["archive"]["segments"][0]["retired"]["exports"]))' "$T/rm.json")" "3 2"
# a snapshot carries the exports the segment is read from, and verifies from them
rm -rf "$T/rsnap"; check python3 "$MANIFEST" snapshot "$RD" "$T/rsnap"
check test -f "$T/rsnap/exports/tensile-t-2026-09-12.tar.gz"
check test ! -e "$T/rsnap/archive/measurements.jsonl/000001-2026-09-13.jsonl.gz"
check python3 "$MANIFEST" verify "$T/rsnap"
rcopy() { rm -rf "$T/rcopy"; cp -r "$T/rsnap" "$T/rcopy"; }
# a copy that has the segment's file (the remote has it when a backup ran
# while the file existed) is checked by the file: the right one verifies,
# another fails
rcopy; cp "$A/archive/measurements.jsonl/000001-2026-09-13.jsonl.gz" "$T/rcopy/archive/measurements.jsonl/"
check python3 "$MANIFEST" verify "$T/rcopy"
rcopy; printf 'x' > "$T/rcopy/archive/measurements.jsonl/000001-2026-09-13.jsonl.gz"
check not python3 "$MANIFEST" verify "$T/rcopy"
# an export altered or missing, or an index entry that is not its member's bytes, fails
rcopy; printf 'x' >> "$T/rcopy/exports/tensile-t-2026-09-11.tar.gz"
check not python3 "$MANIFEST" verify "$T/rcopy"
rcopy; rm "$T/rcopy/exports/tensile-t-2026-09-12.tar.gz"
check not python3 "$MANIFEST" verify "$T/rcopy"
rcopy; python3 - "$T/rcopy/exports/index.json" <<'PY'
import json, sys
i = json.load(open(sys.argv[1])); i[1]["files"][1]["sha256"] = "0" * 64; json.dump(i, open(sys.argv[1], "w"))
PY
check not python3 "$MANIFEST" verify "$T/rcopy"
# exports that leave part of the range out fail
python3 - "$T/rsnap/manifest.json" "$T/rm-gap.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1])); m["files"]["measurements.jsonl"]["archive"]["segments"][0]["retired"]["exports"].pop(0)
json.dump(m, open(sys.argv[2], "w"))
PY
rcopy; check not python3 "$MANIFEST" verify "$T/rcopy" "$T/rm-gap.json"
# cat over an altered export fails, and hands out no byte of the segment
cp "$RD/exports/tensile-t-2026-09-12.tar.gz" "$T/tgz.bak"; printf 'x' >> "$RD/exports/tensile-t-2026-09-12.tar.gz"
check not python3 "$MANIFEST" cat "$RD" measurements.jsonl
check eq "$(python3 "$MANIFEST" cat "$RD" measurements.jsonl 2>/dev/null | wc -c | tr -d ' ')" 0
cp "$T/tgz.bak" "$RD/exports/tensile-t-2026-09-12.tar.gz"
# an observer-archive from before retirement rewrote index.json without the
# retired record; retired.json, which it never touches, keeps it, so the
# record still reads whole, the cut still lists the segment as retired, and
# a snapshot carries retired.json and verifies
cp "$RD/archive/measurements.jsonl/index.json" "$T/rindex.bak"
python3 - "$RD/archive/measurements.jsonl" <<'PY'
import json, os, sys
d = sys.argv[1]
idx = json.load(open(os.path.join(d, "index.json")))
json.dump({"version": 1, "file": "measurements.jsonl", "segments": [s for s in idx["segments"] if s.get("retired")]}, open(os.path.join(d, "retired.json"), "w"))
for s in idx["segments"]:
    s.pop("retired", None)
json.dump(idx, open(os.path.join(d, "index.json"), "w"))
PY
check not grep -q '"retired"' "$RD/archive/measurements.jsonl/index.json"
check eq "$(python3 "$MANIFEST" cat "$RD" measurements.jsonl | sha256sum)" "$(sha256sum < "$T/whole.jsonl")"
check python3 "$MANIFEST" write "$RD" "$T/rm-older.json"
check eq "$(python3 -c 'import json,sys; f=json.load(open(sys.argv[1]))["files"]["measurements.jsonl"]; print(len(f["archive"]["segments"][0]["retired"]["exports"]))' "$T/rm-older.json")" 2
rm -rf "$T/rosnap"; check python3 "$MANIFEST" snapshot "$RD" "$T/rosnap"
check test -f "$T/rosnap/archive/measurements.jsonl/retired.json"
check python3 "$MANIFEST" verify "$T/rosnap"
cp "$T/rindex.bak" "$RD/archive/measurements.jsonl/index.json"
# another vantage's heartbeats, rotated under vantages/<name>/archive/, are
# cut, read, copied and verified like the observer's own
python3 - "$RD" <<'PY'
import gzip, hashlib, json, os, sys
d = os.path.join(sys.argv[1], "vantages", "de-1")
lines = [('{"vantage":"de-1","validator_address":"v%d","scheduled_at":"2026-09-1%dT00:00:00Z"}\n' % (i, i)).encode() for i in range(3)]
adir = os.path.join(d, "archive", "reachability.jsonl")
os.makedirs(adir)
seg = os.path.join(adir, "000001-2026-09-11.jsonl.gz")
with gzip.open(seg, "wb") as z:
    z.write(lines[0])
gz = open(seg, "rb").read()
open(os.path.join(d, "reachability.jsonl"), "wb").write(b"".join(lines[1:]))
sha = lambda b: hashlib.sha256(b).hexdigest()
json.dump({"version": 1, "file": "reachability.jsonl", "time_field": "scheduled_at", "live_since": "2026-09-11T00:00:00Z",
           "segments": [{"name": os.path.basename(seg), "from": 0, "to": len(lines[0]), "lines": 1, "sha256": sha(lines[0]), "gz_sha256": sha(gz), "gz_bytes": len(gz)}],
           "generations": [{"base": 0, "head_sha256": sha(lines[0])}, {"base": len(lines[0]), "head_sha256": sha(lines[1])}]},
          open(os.path.join(adir, "index.json"), "w"))
PY
check python3 "$MANIFEST" write "$RD" "$T/rmv.json"
check eq "$(python3 -c 'import json,sys; f=json.load(open(sys.argv[1]))["files"]["vantages/de-1/reachability.jsonl"]; print(f["records"], f["archived_records"])' "$T/rmv.json")" "2 1"
check eq "$(python3 "$MANIFEST" cat "$RD" vantages/de-1/reachability.jsonl | wc -l | tr -d ' ')" 3
rm -rf "$T/rvsnap"; check python3 "$MANIFEST" snapshot "$RD" "$T/rvsnap"
check test -f "$T/rvsnap/vantages/de-1/archive/reachability.jsonl/000001-2026-09-11.jsonl.gz"
check python3 "$MANIFEST" verify "$T/rvsnap"
# a segment name that is not a file name is never read
python3 - "$T/rvsnap/manifest.json" "$T/rvsnap-escape.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1])); m["files"]["measurements.jsonl"]["archive"]["segments"][0]["name"] = "../../../victim.jsonl"
json.dump(m, open(sys.argv[2], "w"))
PY
check not python3 "$MANIFEST" verify "$T/rvsnap" "$T/rvsnap-escape.json"
check grep -q 'is not a file name' "$T/check.log"

echo "== backup manifest over a copy rotated after the cut"
# A night whose copy stopped part-way, or whose manifest did not reach the
# remote, leaves the remote's manifest older than its live files, and the
# rotation since moved the cut's live lines into a new segment. The copy
# still holds the cut; verify reads it back from there, into the live
# file's place.
O="$T/odata"; mkdir -p "$O/vantages/de-1"
mline() { printf '{"vantage":"t","promise_hash":"p1","validator_address":"v%d","scheduled_at":"2026-10-0%dT00:00:00Z"}\n' "$1" "$1"; }
vline() { printf '{"vantage":"de-1","validator_address":"v%d","scheduled_at":"2026-10-0%dT00:00:00Z"}\n' "$1" "$1"; }
for i in 1 2 3 4; do mline "$i" >> "$O/measurements.jsonl"; done
for i in 1 2 3; do vline "$i" >> "$O/vantages/de-1/reachability.jsonl"; done
rotate "$O" measurements.jsonl 1   # archived once before the cut: its base is past 0
check python3 "$MANIFEST" write "$O" "$T/om.json"
# after the cut: a line more each, then the night's rotation: two of the
# cut's three live lines into a segment (the third stays live), and the
# other vantage's first, two of its three
mline 5 >> "$O/measurements.jsonl"; vline 4 >> "$O/vantages/de-1/reachability.jsonl"
rotate "$O" measurements.jsonl 2
rotate "$O" vantages/de-1/reachability.jsonl 2
ocopy() { rm -rf "$T/ocopy"; cp -r "$O" "$T/ocopy"; }
ocopy; check python3 "$MANIFEST" verify "$T/ocopy" "$T/om.json"
# the copy is the cut again: each live file holds the cut's live lines, and
# the whole record reads up to the cut
check eq "$(wc -l < "$T/ocopy/measurements.jsonl" | tr -d ' ')" 3
check eq "$(python3 "$MANIFEST" cat "$T/ocopy" measurements.jsonl | grep -c '"vantage"')" 4
check eq "$(wc -l < "$T/ocopy/vantages/de-1/reachability.jsonl" | tr -d ' ')" 3
check eq "$(python3 "$MANIFEST" cat "$T/ocopy" vantages/de-1/reachability.jsonl | grep -c '"vantage"')" 3
# a newer segment that is not the bytes the cut had fails, and so does one
# missing; the copy's live file is left as it was
ocopy; python3 -c 'import gzip, sys; gzip.open(sys.argv[1], "wb").write(open(sys.argv[2], "rb").read())' \
  "$T/ocopy/archive/measurements.jsonl/000002-rotated.jsonl.gz" "$T/ocopy/measurements.jsonl"
check not python3 "$MANIFEST" verify "$T/ocopy" "$T/om.json"
check cmp -s "$O/measurements.jsonl" "$T/ocopy/measurements.jsonl"
ocopy; rm "$T/ocopy/archive/measurements.jsonl/000002-rotated.jsonl.gz"
check not python3 "$MANIFEST" verify "$T/ocopy" "$T/om.json"

echo "== backup remote proof"
# backup.sh against fake-rclone.sh, which serves a local directory as the
# remote. The manifest step is not under test here (true stands in for it).
B="$T/bdata"; BR="$T/bremote"; mkdir -p "$B/exports" "$B/vantages/de-1" "$BR"
printf '{"promise_hash":"p1"}\n' > "$B/publications.jsonl"
printf '{"vantage":"de-1"}\n' > "$B/vantages/de-1/reachability.jsonl"   # its archive lock is taken too
printf 'secret' > "$B/sampling-master.key"
# a segment of the observer's own and one of the other vantage's, each with
# its index (the bytes are not read here, only copied)
for a in "$B/archive/measurements.jsonl" "$B/vantages/de-1/archive/reachability.jsonl"; do
  mkdir -p "$a"; printf 'segment' | gzip -c > "$a/000001-2026-09-30.jsonl.gz"; printf '{"version":1}\n' > "$a/index.json"
  printf '{"version":1,"segments":[]}\n' > "$a/retired.json"
done
BRT="$BR/bucket/sekret-token/t"
mkexport() { # mkexport <day> <bytes>: an export tarball and its .sha256 sidecar
  local n="tensile-t-$1.tar.gz"
  printf '%s' "$2" | gzip -c > "$B/exports/$n"
  printf '%s  %s\n' "$(sha256sum < "$B/exports/$n" | cut -d' ' -f1)" "$n" > "$B/exports/$n.sha256"
}
sha_of() { sha256sum < "$1" | cut -d' ' -f1; }
backup() { # backup [VAR=value ...]: one run as the timer runs it, its output in $T/backup.out
  env DATA_DIR="$B" BACKUP_REMOTE="fake:bucket/sekret-token" RCLONE="$HERE/fake-rclone.sh" \
    FAKE_RCLONE_ROOT="$BR" FAKE_RCLONE_LOG="$T/rclone.log" FIBRE_BACKUP_MANIFEST="$(type -P true)" "$@" \
    bash "$HERE/../backup.sh" t > "$T/backup.out" 2>&1
}
proofs() { # proofs <export>: each line exports/remote.jsonl holds for it, "<sha256> <ok>"
  python3 - "$B/exports/remote.jsonl" "$1" <<'PY'
import json, sys
for l in open(sys.argv[1]):
    r = json.loads(l)
    if r["name"] == sys.argv[2]:
        print(r["sha256"], str(r["ok"]).lower())
PY
}
e1=tensile-t-2026-10-01.tar.gz; e2=tensile-t-2026-10-02.tar.gz; e3=tensile-t-2026-10-03.tar.gz
e4=tensile-t-2026-10-04.tar.gz; e5=tensile-t-2026-10-05.tar.gz
# each export is copied, read back, hashed and recorded once
mkexport 2026-10-01 one; mkexport 2026-10-02 two
: > "$T/rclone.log"
check backup
check cmp -s "$B/exports/$e1" "$BRT/exports/$e1"
# the segments and their indexes, the other vantage's included, go first,
# each archive in a pass of its own, so a rotated live file never reaches
# the remote before the lines it no longer holds; the locks and the master
# key do not go at all
for a in archive/measurements.jsonl vantages/de-1/archive/reachability.jsonl; do
  check cmp -s "$B/$a/000001-2026-09-30.jsonl.gz" "$BRT/$a/000001-2026-09-30.jsonl.gz"
  check cmp -s "$B/$a/index.json" "$BRT/$a/index.json"
  check cmp -s "$B/$a/retired.json" "$BRT/$a/retired.json"
done
check cmp -s "$B/vantages/de-1/reachability.jsonl" "$BRT/vantages/de-1/reachability.jsonl"
check eq "$(grep '^copy ' "$T/rclone.log" | cut -d' ' -f2 | tr '\n' ' ')" "$B/archive $B/vantages/de-1/archive $B "
check test ! -e "$BRT/archive/.lock"
check test ! -e "$BRT/vantages/de-1/archive/.lock"
check test ! -e "$BRT/sampling-master.key"
check eq "$(proofs $e1)" "$(sha_of "$B/exports/$e1") true"
check eq "$(proofs $e2)" "$(sha_of "$B/exports/$e2") true"
check not grep -q sekret "$T/backup.out"   # the remote is never printed
# the next night reads nothing back: both are proven
: > "$T/rclone.log"
check backup
check not grep -q '^cat ' "$T/rclone.log"
check eq "$(wc -l < "$B/exports/remote.jsonl" | tr -d ' ')" 2
# a remote copy that is not the local one is ok false and the run passes;
# the next night reads it again and the newest line proves it
mkexport 2026-10-03 three
check backup FAKE_RCLONE_GARBLE=$e3
check eq "$(proofs $e3)" "$(sha_of "$B/exports/$e3") false"
check backup
check eq "$(proofs $e3 | tail -n 1)" "$(sha_of "$B/exports/$e3") true"
# a tarball rebuilt under a new digest is read back again
mkexport 2026-10-01 one-rebuilt
check backup
check eq "$(proofs $e1 | wc -l | tr -d ' ')" 2
check eq "$(proofs $e1 | tail -n 1)" "$(sha_of "$B/exports/$e1") true"
# a remote that cannot be read is ok false, the run passes, and rclone's
# message (which names the remote) is not printed
mkexport 2026-10-04 four
check backup FAKE_RCLONE_FAIL=cat
check eq "$(proofs $e4)" "$(sha_of "$B/exports/$e4") false"
check not grep -q sekret "$T/backup.out"
# a failed copy fails the run and proves nothing
mkexport 2026-10-05 five
check not backup FAKE_RCLONE_FAIL=copy
check eq "$(proofs $e5)" ""
# a last line a crash cut short (no newline) is dropped before the next one
# goes on: closed with a newline it would be a line that is not a check,
# which observer-archive refuses, and no segment would be retired again
printf '{"name":"%s","sha256":"%s","checked_at":"2026-10-06T03:40:00Z","ok":fa' "$e5" "$(sha_of "$B/exports/$e5")" >> "$B/exports/remote.jsonl"
check backup
check python3 -c 'import json, sys; [json.loads(l) for l in open(sys.argv[1])]' "$B/exports/remote.jsonl"
check eq "$(proofs $e5)" "$(sha_of "$B/exports/$e5") true"
check eq "$(wc -l < "$B/exports/remote.jsonl" | tr -d ' ')" 8   # e4 and e5 proven, nothing of the torn line
# every proof names the remote it read (the first 16 hex digits of the
# SHA-256 of the destination, never the destination), and a copy that
# finished is recorded with it: observer-archive counts a proof only while
# the copies to that remote go on finishing
fp=$(printf '%s' "fake:bucket/sekret-token/t" | sha256sum | cut -c1-16)
check eq "$(grep -c "\"remote\":\"$fp\"}" "$B/exports/remote.jsonl" | tr -d ' ')" 8
check python3 -c 'import json, sys; c = json.load(open(sys.argv[1])); assert c["copied_at"].endswith("Z") and c["remote"] == sys.argv[2], c' "$B/exports/remote-copy.json" "$fp"
check not grep -q sekret "$B/exports/remote-copy.json"
# a new BACKUP_REMOTE: every export is read back from it, since a proof of
# another remote proves nothing of this one, and the copy recorded names it
: > "$T/rclone.log"
check backup BACKUP_REMOTE=fake:moved/sekret-token
check eq "$(grep -c '^cat ' "$T/rclone.log" | tr -d ' ')" 5
fp2=$(printf '%s' "fake:moved/sekret-token/t" | sha256sum | cut -c1-16)
check eq "$(proofs $e1 | tail -n 1)" "$(sha_of "$B/exports/$e1") true"
check grep -q "\"remote\":\"$fp2\"}" "$B/exports/remote-copy.json"
# a copy that fails names no remote: rclone's message for a remote it
# cannot set up gives the remote as given, and it is replaced; the copy
# recorded stays the last one that finished
check not backup BACKUP_REMOTE=fake:third/sekret-token FAKE_RCLONE_FAIL=copy
check grep -q 'Failed to create file system for "BACKUP_REMOTE/t/archive"' "$T/backup.out"
check not grep -q sekret "$T/backup.out"
check grep -q "\"remote\":\"$fp2\"}" "$B/exports/remote-copy.json"
# a torn last line that ends in NUL bytes (the size reached the disk, the
# data did not) is dropped too: read through $(...) the NUL would vanish,
# the line would pass for a whole one, and the next proof would be written
# onto it
printf '{"name":"%s","sh\0\0\0\0' "$e5" >> "$B/exports/remote.jsonl"
: > "$T/rclone.log"
check backup BACKUP_REMOTE=fake:moved/sekret-token
check not grep -q '^cat ' "$T/rclone.log"
check python3 -c 'import json, sys; [json.loads(l) for l in open(sys.argv[1])]' "$B/exports/remote.jsonl"
check eq "$(tail -c 1 "$B/exports/remote.jsonl" | od -An -tx1 | tr -d ' \n')" 0a

echo "== backup: the cut and the manifest"
# backup.sh with the manifest tool itself, against the fake rclone
C="$T/cdata"; CR="$T/cremote"; CRT="$CR/bucket/t"; mkdir -p "$C/exports" "$C/vantages/de-1" "$CR"
printf '{"promise_hash":"c1"}\n' > "$C/publications.jsonl"
for i in 1 2 3; do mline "$i" >> "$C/measurements.jsonl"; done
for i in 1 2; do vline "$i" >> "$C/vantages/de-1/reachability.jsonl"; done
printf '{"last_scanned_height":7}\n' > "$C/state.json"
cbackup() { # cbackup [VAR=value ...]: one night, its output in $T/cbackup.out
  env DATA_DIR="$C" BACKUP_REMOTE="fake:bucket" RCLONE="$HERE/fake-rclone.sh" FAKE_RCLONE_ROOT="$CR" \
    FAKE_RCLONE_LOG="$T/crclone.log" FIBRE_BACKUP_MANIFEST="$MANIFEST" "$@" bash "$HERE/../backup.sh" t > "$T/cbackup.out" 2>&1
}
crestore() { # the remote's copy pulled whole, as restore.sh pulls it, and verified against its own manifest
  rm -rf "$T/crestore"; cp -r "$CRT" "$T/crestore"
  python3 "$MANIFEST" verify "$T/crestore" "$T/crestore/backup-manifest.json"
}
# the manifest goes last, on its own, after every file it describes
: > "$T/crclone.log"
check cbackup
check eq "$(cut -d' ' -f1 "$T/crclone.log" | tr '\n' ' ')" "copy copy copy copyto "
check cmp -s "$C/backup-manifest.json" "$CRT/backup-manifest.json"
check crestore
# a line that is not a record no longer stops every copy: it is listed in
# the manifest and copied as it is, and the copy verifies; the drill counts
# the records, not the lines
printf '{"promise_hash":"c2"}\n{"promise_ha{"promise_hash":"c3"}\n' >> "$C/publications.jsonl"
check cbackup
check eq "$(python3 -c 'import json,sys; f=json.load(open(sys.argv[1]))["files"]["publications.jsonl"]; print(f["records"], f["bad_lines"])' "$CRT/backup-manifest.json")" "2 [3]"
check cmp -s "$C/publications.jsonl" "$CRT/publications.jsonl"
check crestore
check eq "$(record_promises "$MANIFEST" "$T/crestore")" "2 2"
cp "$CRT/backup-manifest.json" "$T/cm2.json"
# the rotation, then a night whose manifest does not reach the remote: the
# run fails, the remote keeps the last manifest beside the rotated live
# files, and its copy still verifies against it (the manifest, uploaded
# with the files, used to land first and leave no cut that verified)
mline 4 >> "$C/measurements.jsonl"; vline 3 >> "$C/vantages/de-1/reachability.jsonl"
rotate "$C" measurements.jsonl 3
rotate "$C" vantages/de-1/reachability.jsonl 2
check not cbackup FAKE_RCLONE_FAIL=copyto
check cmp -s "$C/measurements.jsonl" "$CRT/measurements.jsonl"
check cmp -s "$T/cm2.json" "$CRT/backup-manifest.json"
check crestore
check eq "$(wc -l < "$T/crestore/measurements.jsonl" | tr -d ' ')" 3
# a cut that cannot be taken (a live file the archive index does not place)
# does not stop the copy either: the files go, the finished copy is
# recorded, the remote keeps the last manifest, and the run fails at the end
cp "$C/measurements.jsonl" "$T/clive.bak"
printf '{"promise_hash":"stranger"}\n' > "$C/measurements.jsonl"
printf '{"promise_hash":"c4"}\n' >> "$C/publications.jsonl"
rm -f "$C/exports/remote-copy.json"
check not cbackup
check grep -q 'the cut failed' "$T/cbackup.out"
check cmp -s "$C/publications.jsonl" "$CRT/publications.jsonl"
check test -f "$C/exports/remote-copy.json"
check cmp -s "$T/cm2.json" "$CRT/backup-manifest.json"
cp "$T/clive.bak" "$C/measurements.jsonl"
# the next night that cuts uploads its manifest, and the copy verifies
check cbackup
check not cmp -s "$T/cm2.json" "$CRT/backup-manifest.json"
check crestore

echo "== backup manifest with failed transactions"
# failed_txs.jsonl is a record file like the others: cut, counted, copied
# and verified. Its own data dir and manifest, so the copies above and
# their failing cases stay as they are.
F="$T/fdata"; mkdir -p "$F"
printf '{"promise_hash":"f1","x":1}\n' > "$F/publications.jsonl"
printf '{"vantage":"t","promise_hash":"f1","validator_address":"v1","scheduled_at":"2026-10-08T00:00:01Z"}\n' > "$F/measurements.jsonl"
printf '{"last_scanned_height":901,"last_scanned_time":"2026-10-08T00:00:06Z"}\n' > "$F/state.json"
fline() { # fline <height> <tx index>: a failed_txs.jsonl line as the scanner writes it
  printf '{"schema_version":1,"dedupe_key":"h%d:%d","height":%d,"time":"2026-10-08T00:00:05Z","app_version":10,"tx_hash":"%064d","tx_index":%d,"code":5,"codespace":"sdk","log":"insufficient funds","gas_wanted":200000,"gas_used":91234,"ante_passed":true,"fee":"2000utia","messages":[{"index":0,"type_url":"/celestia.fibre.v1.MsgDepositToEscrow"}],"recorded_at":"2026-10-08T00:00:06Z"}\n' \
    "$1" "$2" "$1" "$1$2" "$2"
}
{ fline 900 1; fline 901 0; } > "$F/failed_txs.jsonl"
check python3 "$MANIFEST" write "$F" "$T/fmanifest.json" >/dev/null
check eq "$(python3 -c 'import json,sys; f=json.load(open(sys.argv[1]))["files"]["failed_txs.jsonl"]; print(f["records"], f["bytes"])' "$T/fmanifest.json")" \
         "2 $(wc -c < "$F/failed_txs.jsonl" | tr -d ' ')"
fcopy() { rm -rf "$T/fcopy"; mkdir -p "$T/fcopy"; cp "$F/publications.jsonl" "$F/measurements.jsonl" "$F/failed_txs.jsonl" "$F/state.json" "$T/fcopy/"; }
fcopy; check python3 "$MANIFEST" verify "$T/fcopy" "$T/fmanifest.json" >/dev/null
# altered in place, same length: fails, and says which file and its digest
fcopy; { fline 900 1 | sed 's/"code":5/"code":6/'; fline 901 0; } > "$T/fcopy/failed_txs.jsonl"
check eq "$(wc -c < "$T/fcopy/failed_txs.jsonl" | tr -d ' ')" "$(wc -c < "$F/failed_txs.jsonl" | tr -d ' ')"
check not python3 "$MANIFEST" verify "$T/fcopy" "$T/fmanifest.json"
python3 "$MANIFEST" verify "$T/fcopy" "$T/fmanifest.json" > "$T/fverify.out" 2>&1 || true
fsha=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["files"]["failed_txs.jsonl"]["sha256"][:16])' "$T/fmanifest.json")
check grep -q 'FAIL failed_txs.jsonl: sha256 differs' "$T/fverify.out"
check grep -Eq "failed_txs\.jsonl +[0-9]+ bytes +2 records $fsha" "$T/fverify.out"
# missing
fcopy; rm "$T/fcopy/failed_txs.jsonl"
check not python3 "$MANIFEST" verify "$T/fcopy" "$T/fmanifest.json"
# a data dir without the file (no failure yet) cuts and verifies, and its
# manifest does not name it
rm -rf "$T/fnone" "$T/fnone-copy"; mkdir -p "$T/fnone"
cp "$F/publications.jsonl" "$F/measurements.jsonl" "$F/state.json" "$T/fnone/"
check python3 "$MANIFEST" write "$T/fnone" "$T/fmanifest-none.json" >/dev/null
check eq "$(python3 -c 'import json,sys; print("failed_txs.jsonl" in json.load(open(sys.argv[1]))["files"])' "$T/fmanifest-none.json")" False
cp -r "$T/fnone" "$T/fnone-copy"
check python3 "$MANIFEST" verify "$T/fnone-copy" "$T/fmanifest-none.json" >/dev/null
# a re-scan wrote the first line again: two records with distinct keys of
# three lines, as the store keeps them
fline 900 1 >> "$F/failed_txs.jsonl"
check eq "$(record_distinct "$MANIFEST" "$F" failed_txs.jsonl dedupe_key)" "3 2"

echo "== backup manifest with transaction costs"
# tx_costs.jsonl is a record file like the others: cut, counted, copied and
# verified. Its own data dir and manifest, so the copies above and their
# failing cases stay as they are.
TD="$T/tdata"; mkdir -p "$TD"
printf '{"promise_hash":"t1","x":1}\n' > "$TD/publications.jsonl"
printf '{"vantage":"t","promise_hash":"t1","validator_address":"v1","scheduled_at":"2026-10-09T00:00:01Z"}\n' > "$TD/measurements.jsonl"
printf '{"last_scanned_height":1001,"last_scanned_time":"2026-10-09T00:00:06Z"}\n' > "$TD/state.json"
tline() { # tline <height> <tx index>: a tx_costs.jsonl line as the scanner writes it
  printf '{"schema_version":1,"dedupe_key":"h%d:%d","height":%d,"tx_index":%d,"time":"2026-10-09T00:00:05Z","tx_hash":"%064d","gas_wanted":400000,"gas_used":219118,"fee":"8000utia","fee_payer":"celestia1pub","messages":[{"index":0,"type_url":"/celestia.fibre.v1.MsgPayForFibre","signer":"celestia1pub"}],"recorded_at":"2026-10-09T00:00:06Z"}\n' \
    "$1" "$2" "$1" "$2" "$1$2"
}
{ tline 1000 1; tline 1001 0; } > "$TD/tx_costs.jsonl"
check python3 "$MANIFEST" write "$TD" "$T/tmanifest.json" >/dev/null
check eq "$(python3 -c 'import json,sys; f=json.load(open(sys.argv[1]))["files"]["tx_costs.jsonl"]; print(f["records"], f["bytes"])' "$T/tmanifest.json")" \
         "2 $(wc -c < "$TD/tx_costs.jsonl" | tr -d ' ')"
tcopy() { rm -rf "$T/tcopy"; mkdir -p "$T/tcopy"; cp "$TD/publications.jsonl" "$TD/measurements.jsonl" "$TD/tx_costs.jsonl" "$TD/state.json" "$T/tcopy/"; }
tcopy; check python3 "$MANIFEST" verify "$T/tcopy" "$T/tmanifest.json" >/dev/null
# altered in place, same length: fails, and says which file and its digest
tcopy; { tline 1000 1 | sed 's/"gas_used":219118/"gas_used":219119/'; tline 1001 0; } > "$T/tcopy/tx_costs.jsonl"
check eq "$(wc -c < "$T/tcopy/tx_costs.jsonl" | tr -d ' ')" "$(wc -c < "$TD/tx_costs.jsonl" | tr -d ' ')"
check not python3 "$MANIFEST" verify "$T/tcopy" "$T/tmanifest.json"
python3 "$MANIFEST" verify "$T/tcopy" "$T/tmanifest.json" > "$T/tverify.out" 2>&1 || true
tsha=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["files"]["tx_costs.jsonl"]["sha256"][:16])' "$T/tmanifest.json")
check grep -q 'FAIL tx_costs.jsonl: sha256 differs' "$T/tverify.out"
check grep -Eq "tx_costs\.jsonl +[0-9]+ bytes +2 records $tsha" "$T/tverify.out"
# missing
tcopy; rm "$T/tcopy/tx_costs.jsonl"
check not python3 "$MANIFEST" verify "$T/tcopy" "$T/tmanifest.json"
# a data dir without the file (no Fibre success yet) cuts and verifies, and
# its manifest does not name it
rm -rf "$T/tnone" "$T/tnone-copy"; mkdir -p "$T/tnone"
cp "$TD/publications.jsonl" "$TD/measurements.jsonl" "$TD/state.json" "$T/tnone/"
check python3 "$MANIFEST" write "$T/tnone" "$T/tmanifest-none.json" >/dev/null
check eq "$(python3 -c 'import json,sys; print("tx_costs.jsonl" in json.load(open(sys.argv[1]))["files"])' "$T/tmanifest-none.json")" False
cp -r "$T/tnone" "$T/tnone-copy"
check python3 "$MANIFEST" verify "$T/tnone-copy" "$T/tmanifest-none.json" >/dev/null
# a re-scan wrote the first line again: two records with distinct keys of
# three lines, as the store keeps them
tline 1000 1 >> "$TD/tx_costs.jsonl"
check eq "$(record_distinct "$MANIFEST" "$TD" tx_costs.jsonl dedupe_key)" "3 2"

echo "== vantage pull"
# deploy/vantage-pull.sh against the fake rclone, with util-linux's flock
# and the system's sh (vantage-sync.sh): run here so that CI runs it
check bash "$HERE/vantage-sync.sh"

echo "== rpc-check"
RC="$HERE/rpc-check.sh"
serve fake-rpc.py --app-version 9 --fibre-code 6; r9=$PORT
check "$RC" "http://127.0.0.1:$r9"
serve fake-rpc.py --app-version 10 --fibre-code 6; r10bad=$PORT
check not "$RC" "http://127.0.0.1:$r10bad"
serve fake-rpc.py --app-version 10 --fibre-code 0; r10=$PORT
check "$RC" "http://127.0.0.1:$r10"
serve fake-rpc.py --no-block-results; rnores=$PORT
check not "$RC" "http://127.0.0.1:$rnores"
check not "$RC" "http://127.0.0.1:$r9" "http://127.0.0.1:$closed"
serve fake-rpc.py --hash-salt other; rsalt=$PORT
check not "$RC" "http://127.0.0.1:$r9" "http://127.0.0.1:$rsalt"
serve fake-rpc.py --app-version 9 --fibre-code 6; r9b=$PORT
check "$RC" "http://127.0.0.1:$r9" "http://127.0.0.1:$r9b"

echo "== healthwatch"
HW="$HERE/../healthwatch.sh"
serve fake-http.py --code 200 --record "$T/hook.log"; hook=$PORT
cat > "$T/degraded2.json" <<'J'
{"status":"degraded","checks":[{"name":"chain_liveness","ok":false,"detail":"newest block 11m3s old"},{"name":"prober","ok":false,"detail":"stopped 3m ago (signal)"}]}
J
serve fake-http.py --code 503 --body "$T/degraded2.json"; p503b=$PORT
hwstate="$T/hw/status/healthwatch.state"
# a fake systemctl: is-failed --quiet <unit> passes for each unit listed in
# $T/failed-units
cat > "$T/systemctl" <<'SH'
#!/bin/sh
[ "$1" = is-failed ] && [ "$2" = --quiet ] || exit 2
grep -qxF "$3" "$FAKE_FAILED_UNITS" 2>/dev/null
SH
chmod +x "$T/systemctl"; : > "$T/failed-units"
hw() { # hw <api port>: one healthwatch run; its exit code says ok or not
  API_LISTEN="127.0.0.1:$1" DATA_DIR="$T/hw" NETWORK=t ALERT_REPEAT_MIN=60 \
    SYSTEMCTL="$T/systemctl" FAKE_FAILED_UNITS="$T/failed-units" \
    ALERT_WEBHOOK="http://127.0.0.1:$hook/" bash "$HW" t >> "$T/check.log" 2>&1 || true
}
posts() { if [ -f "$T/hook.log" ]; then wc -l < "$T/hook.log" | tr -d ' '; else echo 0; fi; }
lastpost() { tail -n 1 "$T/hook.log"; }
contains() { case "$1" in *"$2"*) ;; *) echo "  [$1] lacks [$2]"; return 1 ;; esac; }
hw "$p503"; check eq "$(posts)" 1
check eq "$(sed -n 3p "$hwstate")" chain_liveness
hw "$p503"; check eq "$(posts)" 1              # same fault: quiet until the repeat
# a second fault while the first persists: same state, new set, one alert
hw "$p503b"; check eq "$(posts)" 2
check contains "$(lastpost)" "failing checks changed (was: chain_liveness)"
check eq "$(sed -n 3p "$hwstate")" chain_liveness,prober
hw "$p503b"; check eq "$(posts)" 2
hw "$p200"; check eq "$(posts)" 3
check contains "$(lastpost)" recovered
# a two-line state file from the older build: the set is unknown, not
# empty, so the upgrade alone does not alert
printf 'degraded\n%s\n' "$(date +%s)" > "$hwstate"
hw "$p503"; check eq "$(posts)" 3
# a nightly unit that failed (the backup, which the remote proofs rest on,
# or the archive run) is a failing check of its own, which /v1/health
# cannot see: it alerts beside a healthy API, joins the set beside a
# degraded one, and its next good run recovers
printf 'ok\n%s\n\n' "$(date +%s)" > "$hwstate"
echo "fibre-backup@t.service" > "$T/failed-units"
hw "$p200"; check eq "$(posts)" 4
check contains "$(lastpost)" "fibre-backup@t.service failed"
check eq "$(sed -n 3p "$hwstate")" fibre-backup@t
echo "fibre-archive@t.service" >> "$T/failed-units"
hw "$p503"; check eq "$(posts)" 5
check eq "$(sed -n 3p "$hwstate")" chain_liveness,fibre-archive@t,fibre-backup@t
: > "$T/failed-units"
hw "$p200"; check eq "$(posts)" 6
check contains "$(lastpost)" recovered
# hwx <api port> [VAR=value...]: one run with the environment given on top
# of hw's; prints its exit status
hwx() {
  local port=$1 st=0
  shift
  env API_LISTEN="127.0.0.1:$port" DATA_DIR="$T/hw" NETWORK=t ALERT_REPEAT_MIN=60 \
    SYSTEMCTL="$T/systemctl" FAKE_FAILED_UNITS="$T/failed-units" \
    ALERT_WEBHOOK="http://127.0.0.1:$hook/" "$@" bash "$HW" t >> "$T/check.log" 2>&1 || st=$?
  echo "$st"
}
hwreset() { printf 'ok\n%s\n\n' "${1:-$(date +%s)}" > "$hwstate"; } # a state with nothing failing
# the observer's state is the alert's, not the watcher's: a run that
# alerted about a degraded observer exits 0
hwreset
check eq "$(hwx "$p503")" 0
check eq "$(sed -n 1p "$hwstate")" degraded
check eq "$(hwx "$p200")" 0
# an alert every destination refused is not recorded, fails the run, and
# is sent again by the next one
serve fake-http.py --code 500 --record "$T/hook500.log"; hook500=$PORT
hwreset
check eq "$(hwx "$p503" ALERT_WEBHOOK="http://127.0.0.1:$hook500/")" 1
check eq "$(sed -n 1p "$hwstate")" ok
n=$(posts)
check eq "$(hwx "$p503")" 0
check eq "$(posts)" $((n + 1))
check eq "$(sed -n 1p "$hwstate")" degraded
# a state that cannot be written (a full disk; here a file where its
# directory goes) fails the run after the alert went out, and the next run
# alerts again rather than staying silent
mkdir -p "$T/hw3"; : > "$T/hw3/status"
n=$(posts)
check eq "$(hwx "$p503" DATA_DIR="$T/hw3")" 1
check eq "$(posts)" $((n + 1))
check eq "$(hwx "$p503" DATA_DIR="$T/hw3")" 1
check eq "$(posts)" $((n + 2))
# Outcomes, at a fixed clock: noon UTC on 2026-10-07, with yesterday's
# export in place and proven on the remote.
noon=$(date -u -d 2026-10-07T12:00:00Z +%s)
mkdir -p "$T/hw/exports"
touch "$T/hw/exports/tensile-t-2026-10-06.tar.gz"
proof() { # proof <export> <true|false>: one line of the backup's remote proof
  printf '{"name":"%s","sha256":"%064d","checked_at":"2026-10-07T03:40:00Z","ok":%s,"remote":"0123456789abcdef"}\n' "$1" 0 "$2" >> "$T/hw/exports/remote.jsonl"
}
proof tensile-t-2026-10-06.tar.gz true
# the backup's last finished copy: 32 hours old fails with BACKUP_REMOTE
# set, and says nothing without it; a fresh one recovers
printf '{"copied_at":"2026-10-06T03:20:00Z","remote":"0123456789abcdef"}\n' > "$T/hw/exports/remote-copy.json"
hwreset "$noon"
check eq "$(hwx "$p200" HEALTHWATCH_NOW="$noon")" 0
check eq "$(sed -n 1p "$hwstate")" ok
check eq "$(hwx "$p200" HEALTHWATCH_NOW="$noon" BACKUP_REMOTE=r:bucket)" 0
check eq "$(sed -n 3p "$hwstate")" backup-copy
check contains "$(lastpost)" "no backup copy has finished for 32h"
printf '{"copied_at":"2026-10-07T03:20:00Z","remote":"0123456789abcdef"}\n' > "$T/hw/exports/remote-copy.json"
hwx "$p200" HEALTHWATCH_NOW="$noon" BACKUP_REMOTE=r:bucket >/dev/null
check contains "$(lastpost)" recovered
# no copy ever recorded, with exports there for two days, fails too
rm -f "$T/hw/exports/remote-copy.json"
touch -d 2026-10-05T03:05:00Z "$T/hw/exports/tensile-t-2026-10-04.tar.gz"
hwx "$p200" HEALTHWATCH_NOW="$noon" BACKUP_REMOTE=r:bucket >/dev/null
check eq "$(sed -n 3p "$hwstate")" backup-copy
check contains "$(lastpost)" "no backup copy has ever finished"
rm -f "$T/hw/exports/tensile-t-2026-10-04.tar.gz"
# yesterday's export not proven on the remote, with BACKUP_REMOTE set and
# the copies finishing: its newest line there ok false (a remote that takes
# copies and cannot give them back), or none at all, fails from 06:00 UTC;
# a proof recovers
printf '{"copied_at":"2026-10-07T03:20:00Z","remote":"0123456789abcdef"}\n' > "$T/hw/exports/remote-copy.json"
proof tensile-t-2026-10-06.tar.gz false
hwreset "$noon"
hwx "$p200" HEALTHWATCH_NOW="$(date -u -d 2026-10-07T05:30:00Z +%s)" BACKUP_REMOTE=r:bucket >/dev/null
check eq "$(sed -n 1p "$hwstate")" ok
hwx "$p200" HEALTHWATCH_NOW="$noon" >/dev/null
check eq "$(sed -n 1p "$hwstate")" ok
check eq "$(hwx "$p200" HEALTHWATCH_NOW="$noon" BACKUP_REMOTE=r:bucket)" 0
check eq "$(sed -n 3p "$hwstate")" backup-proof
check contains "$(lastpost)" "tensile-t-2026-10-06.tar.gz not proven on the remote"
proof tensile-t-2026-10-06.tar.gz true
hwx "$p200" HEALTHWATCH_NOW="$noon" BACKUP_REMOTE=r:bucket >/dev/null
check contains "$(lastpost)" recovered
: > "$T/hw/exports/remote.jsonl"
proof tensile-t-2026-10-05.tar.gz true
hwx "$p200" HEALTHWATCH_NOW="$noon" BACKUP_REMOTE=r:bucket >/dev/null
check eq "$(sed -n 3p "$hwstate")" backup-proof
check contains "$(lastpost)" "tensile-t-2026-10-06.tar.gz not proven on the remote"
proof tensile-t-2026-10-06.tar.gz true
hwx "$p200" HEALTHWATCH_NOW="$noon" BACKUP_REMOTE=r:bucket >/dev/null
check contains "$(lastpost)" recovered
# yesterday's export missing: not yet at 03:30 UTC, a failing check from
# 04:00 on, recovered once it is there
rm -f "$T/hw/exports/tensile-t-2026-10-06.tar.gz"
hwreset "$noon"
hwx "$p200" HEALTHWATCH_NOW="$(date -u -d 2026-10-07T03:30:00Z +%s)" >/dev/null
check eq "$(sed -n 1p "$hwstate")" ok
hwx "$p200" HEALTHWATCH_NOW="$noon" >/dev/null
check eq "$(sed -n 3p "$hwstate")" export
check contains "$(lastpost)" "no daily export for 2026-10-06 after 04:00 UTC"
touch "$T/hw/exports/tensile-t-2026-10-06.tar.gz"
hwx "$p200" HEALTHWATCH_NOW="$noon" >/dev/null
check contains "$(lastpost)" recovered
# a second vantage: nothing new for 40 minutes fails; a failed pull unit
# fails once nothing came for 15 minutes, and is noise after 2; a vantage
# that never sent anything is not judged
mkdir -p "$T/hw/vantages/de-1" "$T/hw/vantages/de-2"
echo '{}' > "$T/hw/vantages/de-1/reachability.jsonl"
: > "$T/hw/vantages/de-2/reachability.jsonl"
touch -d "@$((noon - 40 * 60))" "$T/hw/vantages/de-1/reachability.jsonl" "$T/hw/vantages/de-2/reachability.jsonl"
hwreset "$noon"
hwx "$p200" HEALTHWATCH_NOW="$noon" VANTAGE_PULL_NAMES="de-1 de-2" >/dev/null
check eq "$(sed -n 3p "$hwstate")" vantage-pull
check contains "$(lastpost)" "nothing new from vantage de-1 for 40m"
check not contains "$(lastpost)" de-2
touch -d "@$((noon - 15 * 60))" "$T/hw/vantages/de-1/reachability.jsonl"
hwreset "$noon"
hwx "$p200" HEALTHWATCH_NOW="$noon" VANTAGE_PULL_NAMES="de-1 de-2" >/dev/null
check eq "$(sed -n 1p "$hwstate")" ok
echo "fibre-vantage-pull@t.service" > "$T/failed-units"
hwx "$p200" HEALTHWATCH_NOW="$noon" VANTAGE_PULL_NAMES="de-1 de-2" >/dev/null
check eq "$(sed -n 3p "$hwstate")" fibre-vantage-pull@t
touch -d "@$((noon - 2 * 60))" "$T/hw/vantages/de-1/reachability.jsonl"
hwx "$p200" HEALTHWATCH_NOW="$noon" VANTAGE_PULL_NAMES="de-1 de-2" >/dev/null
check contains "$(lastpost)" recovered
: > "$T/failed-units"
# a vantage whose lines the nightly rotation moved to the archive, live file
# left empty, is still judged: it went quiet, it did not stop being expected
mkdir -p "$T/hw/vantages/de-2/archive/reachability.jsonl"
echo '{}' > "$T/hw/vantages/de-2/archive/reachability.jsonl/index.json"
touch -d "@$((noon - 40 * 60))" "$T/hw/vantages/de-2/reachability.jsonl"
hwx "$p200" HEALTHWATCH_NOW="$noon" VANTAGE_PULL_NAMES="de-1 de-2" >/dev/null
check eq "$(sed -n 3p "$hwstate")" vantage-pull
check contains "$(lastpost)" "nothing new from vantage de-2 for 40m"
rm -rf "$T/hw/vantages/de-2/archive"
hwx "$p200" HEALTHWATCH_NOW="$noon" VANTAGE_PULL_NAMES="de-1 de-2" >/dev/null
check contains "$(lastpost)" recovered
# a new host: BACKUP_REMOTE set, no copy and no exports directory yet; the
# run completes (a find over the missing directory used to end it under
# pipefail, before any alert or state) and finds nothing wrong
check eq "$(hwx "$p200" HEALTHWATCH_NOW="$noon" BACKUP_REMOTE=r:bucket DATA_DIR="$T/hw4")" 0
check eq "$(sed -n 1p "$T/hw4/status/healthwatch.state")" ok
check eq "$(hwx "$p503" HEALTHWATCH_NOW="$noon" BACKUP_REMOTE=r:bucket DATA_DIR="$T/hw4")" 0
check eq "$(sed -n 1p "$T/hw4/status/healthwatch.state")" degraded
# Telegram: the bot API's sendMessage with the chat and the text; a refused
# post (an unknown token is a 404 there) is said, and the token never printed
tgtest() { # tgtest <bot api port>: healthwatch --test against it
  API_LISTEN="127.0.0.1:$p200" DATA_DIR="$T/hw2" NETWORK=t TELEGRAM_BOT_TOKEN=123:sekret TELEGRAM_CHAT_ID=-1001 \
    TELEGRAM_API="http://127.0.0.1:$1" bash "$HW" t --test 2>&1
}
serve fake-http.py --code 200 --record "$T/tg.log" --record-path; tgok=$PORT
serve fake-http.py --code 404 --record "$T/tg404.log"; tgbad=$PORT
if out=$(tgtest "$tgok"); then check eq delivered delivered; else check eq "telegram test: $out" delivered; fi
check contains "$(tail -n 1 "$T/tg.log")" "/bot123:sekret/sendMessage"
check contains "$(tail -n 1 "$T/tg.log")" '"chat_id": "-1001"'
check contains "$(tail -n 1 "$T/tg.log")" "alert delivery check"
if out=$(tgtest "$tgbad"); then check eq "a refused post passed" refused; else check eq refused refused; fi
check contains "$out" "telegram post failed (HTTP 404)"
check not contains "$out" sekret
# The URL (the bot token inside Telegram's, the webhook) reaches curl
# through its config on stdin, never its command line, which every account
# on the host can read in the process list while a post is in flight. A
# curl in front of the real one writes down every command line it is given.
mkdir -p "$T/curlspy"
cat > "$T/curlspy/curl" <<SH
#!/bin/sh
printf '%s\n' "\$*" >> "$T/curl-argv.log"
exec $(command -v curl) "\$@"
SH
chmod +x "$T/curlspy/curl"; : > "$T/curl-argv.log"
check env PATH="$T/curlspy:$PATH" API_LISTEN="127.0.0.1:$p200" DATA_DIR="$T/hw2" NETWORK=t TELEGRAM_BOT_TOKEN=123:sekret \
  TELEGRAM_CHAT_ID=-1001 TELEGRAM_API="http://127.0.0.1:$tgok" bash "$HW" t --test
check contains "$(tail -n 1 "$T/tg.log")" "/bot123:sekret/sendMessage"
n=$(posts)
check env PATH="$T/curlspy:$PATH" API_LISTEN="127.0.0.1:$p200" DATA_DIR="$T/hw2" NETWORK=t \
  ALERT_WEBHOOK="http://127.0.0.1:$hook/hook-sekret" bash "$HW" t --test
check eq "$(posts)" $((n + 1))
check grep -q -- '-K -' "$T/curl-argv.log"
check not grep -q sekret "$T/curl-argv.log"

echo "== exposure helpers"
# where healthwatch posts, by its own rule: Telegram alone is a destination
# (exposure.sh took ALERT_WEBHOOK alone for one, and never sent the test)
printf 'TELEGRAM_BOT_TOKEN=123:abc\nTELEGRAM_CHAT_ID=-1001\n' > "$T/tg.env"
check eq "$(alert_destinations "$T/tg.env")" telegram
printf 'ALERT_WEBHOOK=https://hooks.example.org/x\nTELEGRAM_BOT_TOKEN=123:abc\nTELEGRAM_CHAT_ID=-1001\n' > "$T/both.env"
check eq "$(alert_destinations "$T/both.env")" "webhook telegram"
printf 'ALERT_WEBHOOK=\nTELEGRAM_BOT_TOKEN=123:abc\nTELEGRAM_CHAT_ID=\n' > "$T/half.env"
check eq "$(alert_destinations "$T/half.env")" ""
check eq "$(alert_destinations "$T/test.env")" webhook
# an env file other accounts can read is not private; 0640 and 0600 are
printf 'x\n' > "$T/p.env"
chmod 0644 "$T/p.env"; check not private_file "$T/p.env"
chmod 0640 "$T/p.env"; check private_file "$T/p.env"
chmod 0600 "$T/p.env"; check private_file "$T/p.env"
# the internet-facing site-server gets its own two settings, not the env
# file that holds the alert and backup credentials; and the README never
# puts an env file in place with root's umask (0644)
check not grep -q '^EnvironmentFile=/etc/fibre-observer/%i\.env' "$HERE/../systemd/fibre-site@.service"
check grep -q '^EnvironmentFile=/etc/fibre-observer/site-%i\.env' "$HERE/../systemd/fibre-site@.service"
check not grep -qE 'cp deploy/observer\.env\.example /etc/' "$HERE/../README.md"
check grep -q 'install -m 0640 -o root -g fibre-observer deploy/observer.env.example /etc/fibre-observer/mocha.env' "$HERE/../README.md"
# once closed, an env file is read as root: a grep without sudo got
# "Permission denied", and its tee wrote an empty site-<network>.env that
# left site-server on mocha's ports whatever the network
check not grep -qE '^[[:space:]]*(grep|cat|sed|awk|head|tail|cut)[[:space:]].*/etc/fibre-observer/[^ ]*\.env' "$HERE/../README.md"
check grep -qF "sudo grep -E '^(SITE_LISTEN|API_LISTEN)=' /etc/fibre-observer/mocha.env | sudo tee /etc/fibre-observer/site-mocha.env" "$HERE/../README.md"
check grep -qF 'sudo cat /etc/fibre-observer/site-mocha.env' "$HERE/../README.md"

echo "== hosting-db"
# deploy/hosting-db.sh against a curl that serves one file, whatever the
# URL. A gzip cut short, or failing its CRC, that still gives the old check
# its hundred thousand lines and its first line is refused, and the good
# file stays
HD="$T/hosting"; mkdir -p "$T/fakecurl"
cat > "$T/fakecurl/curl" <<'SH'
#!/bin/sh
out=""
while [ $# -gt 0 ]; do case $1 in -o) out=$2; shift 2 ;; *) shift ;; esac; done
cp "$FAKE_CURL_FILE" "$out"
SH
chmod +x "$T/fakecurl/curl"
awk 'BEGIN { for (i = 0; i < 200000; i++) printf "1.%d.%d.0\t1.%d.%d.255\t13335\tUS\tCLOUDFLARENET\n", int(i / 256) % 256, i % 256, int(i / 256) % 256, i % 256 }' | gzip -c > "$T/asn-good.gz"
gzsize=$(wc -c < "$T/asn-good.gz" | tr -d ' ')
head -c $((gzsize * 9 / 10)) "$T/asn-good.gz" > "$T/asn-cut.gz"
cp "$T/asn-good.gz" "$T/asn-crc.gz"
printf '\0\0\0\0' | dd of="$T/asn-crc.gz" bs=1 seek=$((gzsize - 8)) conv=notrunc 2>/dev/null
check test "$(gzip -dc "$T/asn-cut.gz" 2>/dev/null | wc -l)" -ge 100000
check test "$(gzip -dc "$T/asn-crc.gz" 2>/dev/null | wc -l)" -ge 100000
hdb() { env PATH="$T/fakecurl:$PATH" FAKE_CURL_FILE="$1" HOSTING_SKIP_COUNTRY=1 HOSTING_SKIP_CITY=1 sh "$HERE/../hosting-db.sh" "$HD"; }
check hdb "$T/asn-good.gz"
check cmp -s "$T/asn-good.gz" "$HD/ip2asn-combined.tsv.gz"
check not hdb "$T/asn-cut.gz"
check cmp -s "$T/asn-good.gz" "$HD/ip2asn-combined.tsv.gz"
check not hdb "$T/asn-crc.gz"
check cmp -s "$T/asn-good.gz" "$HD/ip2asn-combined.tsv.gz"

echo
echo "selftest: $ok passed, $bad failed"
[ "$bad" = 0 ]
