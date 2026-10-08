#!/usr/bin/env bash
# shellcheck shell=bash
#
# lib.sh: what the acceptance tests share. Sourced, never run.
#
# Every function here exists because a script got it wrong once:
#   http_code    curl's -w prints "000" on a transport failure AND exits
#                non-zero, so `... || echo 000` printed "000000" and every
#                comparison against it was false. One normalised code, one
#                place.
#   health_*     "not 200" is not "degraded": a dead API is 000 and a
#                degraded one is 503 with a body that names the check. A
#                test that accepts either proves neither.
#   parse_envfile `env "$(grep ... | xargs)"` handed every variable to env as
#                one argument. This reads KEY=VALUE lines the way systemd
#                reads EnvironmentFile= for the values it uses (one layer of
#                matching quotes), prints one KEY=VALUE per line, and never
#                evaluates anything.
#   run_as_service runs a command exactly as the units do: the service user
#                and the instance's EnvironmentFile, through systemd-run when
#                it is there, through the parser above when it is not.
#   snapshot_code a fresh API's first read of a window is a 503 with
#                "computing": true until the window's first computation
#                lands, about half a minute on a store the live one's size;
#                the restore drill read it once and called a good backup
#                broken.
#   manifest_records the restore drill compared the rebuilt publications
#                with the live file's records alone, while the rebuild
#                reads the archived lines as well: once observer-archive
#                rotated publications.jsonl, a good backup failed the drill.
#   record_promises the drill then compared the rebuilt publications with
#                the lines, while the store keeps one row per promise: a
#                publication appended again by a re-scan would have failed a
#                good backup.
#   alert_destinations exposure.sh took ALERT_WEBHOOK alone for "somebody
#                is told": a host that alerts through Telegram only failed
#                the check for a false reason and never sent its test.
#   private_file the README copied the env file under root's umask, 0644:
#                the bot token, the webhook and the backup remote readable
#                by every account on a host shared with a validator.

: "${FAILED:=0}"
pass() { echo "  ok   $*"; }
fail() { echo "  FAIL $*"; FAILED=1; }
warn() { echo "  warn $*"; }
ts() { date -u +%H:%M:%S; }

# envval <file> <KEY>: the value of KEY in an env file, first match, one layer
# of matching quotes removed. Empty when absent.
envval() {
  local line
  line=$(grep -m1 -E "^$2=" "$1" 2>/dev/null || true)
  strip_quotes "${line#*=}"
}
strip_quotes() {
  local v=$1
  case "$v" in
    \"*\") v=${v#\"}; v=${v%\"} ;;
    \'*\') v=${v#\'}; v=${v%\'} ;;
  esac
  printf '%s' "$v"
}

# alert_destinations <envfile>: where healthwatch posts with this env file,
# by its own rule: "webhook" for ALERT_WEBHOOK, "telegram" for
# TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID both set, space-separated. Empty
# when none is: then it only logs.
alert_destinations() {
  local d=""
  if [ -n "$(envval "$1" ALERT_WEBHOOK)" ]; then d="webhook"; fi
  if [ -n "$(envval "$1" TELEGRAM_BOT_TOKEN)" ] && [ -n "$(envval "$1" TELEGRAM_CHAT_ID)" ]; then d="${d:+$d }telegram"; fi
  printf '%s' "$d"
}

# private_file <path>: prints "<mode> <owner>:<group>" of the file, and
# exits 0 only when other accounts have no access to it at all (no bit set
# for others). The env files and rclone.conf hold the alert and backup
# credentials; systemd reads EnvironmentFile= as root, so nothing needs them
# open to others.
private_file() {
  local m
  m=$(stat -c '%a %U:%G' "$1") || return 1
  printf '%s\n' "$m"
  [ $(( 8#${m%% *} & 7 )) = 0 ]
}

# parse_envfile <file>: KEY=VALUE per line for every assignment in the file,
# comments and blank lines skipped, quotes stripped as above. Consume with
# `mapfile -t pairs < <(parse_envfile f)` and pass "${pairs[@]}" to env.
parse_envfile() {
  local line key val
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in ''|'#'*) continue ;; esac
    key=${line%%=*}
    [ "$key" = "$line" ] && continue          # no '=' at all
    key=${key#export }; key=${key// /}
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    val=$(strip_quotes "${line#*=}")
    printf '%s=%s\n' "$key" "$val"
  done < "$1"
}

# http_code <url> [outfile]: the HTTP status as three digits, or 000 when
# there was no HTTP answer at all. Never anything else. The body goes to
# outfile (default /dev/null). HTTP_TIMEOUT seconds (default 10).
http_code() {
  local out="${2:-/dev/null}" code
  code=$(curl -sS -m "${HTTP_TIMEOUT:-10}" -o "$out" -w '%{http_code}' "$1" 2>/dev/null) || code=000
  case "$code" in
    [1-5][0-9][0-9]) printf '%s\n' "$code" ;;
    *) printf '000\n' ;;
  esac
}

# health_bad_checks <health.json>: "name: detail; ..." for every check that
# is not ok. Empty when none, or when the body is not a health body.
health_bad_checks() {
  python3 - "$1" <<'PY' 2>/dev/null || true
import json, sys
try:
    h = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
print("; ".join(f"{c.get('name','?')}: {c.get('detail','')}" for c in h.get("checks", []) if not c.get("ok")))
PY
}

# health_has_reason <health.json> <name,name,...>: exit 0 when the body's
# status is not "ok" AND at least one failing check has one of the given
# names AND that check carries a non-empty detail. A failing check with
# another name (disk, say) does not count: the test that calls this is
# asking whether the observer reported a specific thing.
health_has_reason() {
  python3 - "$1" "$2" <<'PY' 2>/dev/null
import json, sys
try:
    h = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(1)
if h.get("status") == "ok":
    sys.exit(1)
names = set(sys.argv[2].split(","))
for c in h.get("checks", []):
    if not c.get("ok") and c.get("name") in names and (c.get("detail") or "").strip():
        sys.exit(0)
sys.exit(1)
PY
}

# run_as_service <envfile> <user> <cmd...>: run cmd as the service user with
# the instance's environment, the way the units run. As root on a systemd
# host, systemd-run reads the EnvironmentFile with systemd's own parser and
# runs the command under the unit's user; otherwise (no systemd, or not
# root — a transient unit with --uid needs root, and CI is neither) the
# fallback parses the file here and passes each pair as its own argument
# to env. The self-test covers the fallback; the systemd-run path is what
# exposure.sh takes under sudo on the host.
run_as_service() {
  local envfile=$1 user=$2; shift 2
  if [ "$(id -u)" = 0 ] && command -v systemd-run >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    systemd-run --quiet --wait --pipe --collect --uid="$user" -p "EnvironmentFile=$envfile" "$@"
  else
    local pairs=()
    mapfile -t pairs < <(parse_envfile "$envfile")
    if [ "$(id -un)" = "$user" ]; then env "${pairs[@]}" "$@"; else sudo -u "$user" env "${pairs[@]}" "$@"; fi
  fi
}

# snapshot_code <url> <seconds> [outfile]: http_code for a read of a window
# snapshot (/v1/network, /v1/validators, /v1/market), asking again while the
# API answers that the window is still being computed. An API with no
# snapshot file for a window (a first start, a restored directory) holds the
# first reader a few seconds and then answers 503 with "computing": true and
# retry_after_s until the computation lands; that answer is asked again after
# retry_after_s seconds (5 when it names none) until <seconds> have passed.
# Any other answer is final. The last body goes to outfile when one is given.
# HTTP_TIMEOUT as for http_code.
snapshot_code() {
  local url=$1 secs=$2 out="${3:-}" body code after deadline
  body=$(mktemp)
  deadline=$(( $(date +%s) + secs ))
  while :; do
    code=$(http_code "$url" "$body")
    after=""
    if [ "$code" = 503 ]; then
      after=$(python3 -c 'import json, sys
b = json.load(open(sys.argv[1]))
s = b.get("retry_after_s")
if b.get("computing") is True:
    print(max(1, int(s)) if isinstance(s, (int, float)) else 5)' "$body" 2>/dev/null || true)
    fi
    if [ -z "$after" ] || [ "$(date +%s)" -ge "$deadline" ]; then break; fi
    sleep "$after"
  done
  if [ -n "$out" ]; then cp "$body" "$out"; fi
  rm -f "$body"
  printf '%s\n' "$code"
}

# manifest_records <manifest.json> <file>: the lines of <file> that a
# rebuild from the manifest's cut reads. The collector reads a record file
# from its first byte, so that is the live file's records and its archived
# segments' (archived_records, retired segments included). 0 when the
# manifest does not list the file.
manifest_records() {
  python3 -c 'import json, sys
f = json.load(open(sys.argv[1]))["files"].get(sys.argv[2], {})
print(f.get("records", 0) + f.get("archived_records", 0))' "$1" "$2"
}

# record_promises <manifest-tool> <dir>: "<records> <distinct promises>" of
# publications.jsonl's whole record under dir (archived segments, a retired
# one read back from the exports, then the live file), read with the
# manifest tool's cat as a rebuild reads it. The store keeps one row per
# promise (ON CONFLICT DO NOTHING): a publication appended again, which a
# re-scan of heights older than the scanner's dedupe window (its live file)
# can do, is a line and not a row. A line that is not a JSON object (a
# write a full disk cut short) is not a record, as the manifest counts
# records, and the rebuild skips it.
record_promises() {
  python3 - "$1" "$2" <<'PY'
import json, subprocess, sys
cat = subprocess.Popen([sys.argv[1], "cat", sys.argv[2], "publications.jsonl"], stdout=subprocess.PIPE)
n, seen = 0, set()
for line in cat.stdout:
    try:
        r = json.loads(line)
    except (ValueError, RecursionError):
        continue
    if not isinstance(r, dict):
        continue
    n += 1
    seen.add(r.get("promise_hash"))
if cat.wait() != 0:
    raise SystemExit("publications.jsonl: the manifest tool could not read the record")
print(n, len(seen))
PY
}

# free_port: a TCP port nothing listens on right now.
free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

# wait_http <url> <seconds> [codes]: wait until http_code is one of codes
# (default 200), polling every 2 s. Exit 1 on timeout.
wait_http() {
  local url=$1 secs=$2 codes="${3:-200}" until c
  until=$(( $(date +%s) + secs ))
  while :; do
    c=$(http_code "$url")
    case " $codes " in *" $c "*) return 0 ;; esac
    [ "$(date +%s)" -ge "$until" ] && return 1
    sleep 2
  done
}
