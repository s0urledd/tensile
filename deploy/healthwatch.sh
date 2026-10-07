#!/usr/bin/env bash
#
# fibre-healthwatch <instance>: ask this instance's API whether the observer
# is healthy, and if not, tell somebody. Runs from fibre-healthwatch@.timer
# every five minutes as the service user, with the instance's env file.
#
# /v1/health is 200 when every process is alive and the disk has room, 503
# with the failing checks otherwise. The check is deliberately outside the
# processes it judges: a dead prober cannot report itself, and this script
# has no state to lose. It posts to ALERT_WEBHOOK (a Discord, Slack or
# Matrix URL that accepts a JSON body with "content") and to a Telegram chat
# through a bot (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID), whichever are
# set, once when the state or the set of failing checks changes, and again
# every ALERT_REPEAT_MIN minutes while it stays bad, so a broken observer
# nags, a second fault is heard even while the first persists, and a fixed
# one says so once.
#
# The site shows no check, only an API that does not answer: a failing
# check reaches the operator here alone. So do the jobs /v1/health cannot
# see, judged by what they left behind rather than by their unit's state
# alone (a timer never enabled leaves a unit that never failed): the
# nightly backup and archive units, the backup's last finished copy, the
# day's export and its proof on the remote, and the second vantages'
# pulls. Each that is wrong is a failing check of its own (below).
#
# With neither set it only logs, which journalctl -u
# fibre-healthwatch@<instance> shows; point any external uptime monitor at
# /api/v1/health for the same signal without this script.
#
# An alert is delivered before the state file records it: one that every
# destination refused (Telegram's 429, a link down) is sent again on the
# next run, and a state file that cannot be written (a full disk) costs a
# repeated alert, never a lost one. The exit status is the script's own: 0
# for a run that did its job whatever the observer's state (which is in
# the alert and in the log line), 1 when an alert was refused or the state
# could not be written, so a failed fibre-healthwatch@ unit means the
# watcher itself is in trouble.
set -o errexit -o nounset -o pipefail

instance="${1:?instance}"
listen="${API_LISTEN:-127.0.0.1:8080}"
url="http://${listen}/v1/health"
webhook="${ALERT_WEBHOOK:-}"
tg_token="${TELEGRAM_BOT_TOKEN:-}"
tg_chat="${TELEGRAM_CHAT_ID:-}"
tg_api="${TELEGRAM_API:-https://api.telegram.org}" # the selftest points it at a fake
tg=0; if [ -n "$tg_token" ] && [ -n "$tg_chat" ]; then tg=1; fi
repeat="${ALERT_REPEAT_MIN:-60}"
data="${DATA_DIR:-/var/lib/fibre-observer/$instance}"
state="$data/status/healthwatch.state"
name="${NETWORK:-$instance}"
backup_remote="${BACKUP_REMOTE:-}"
pull_names="${VANTAGE_PULL_NAMES:-}"
# The run's clock, in Unix seconds; the selftest sets HEALTHWATCH_NOW to put
# a run before or after the hour the day's export is due by.
epoch="${HEALTHWATCH_NOW:-$(date +%s)}"

# post URL JSON prints the HTTP status, 000 when nothing answered. Only the
# status is ever printed: curl's own error text can carry the URL, and the
# URL is the secret (the webhook, or the bot token inside Telegram's).
post() {
  curl -sS -m 20 -o /dev/null -w '%{http_code}' -X POST -H 'content-type: application/json' -d "$2" "$1" 2>/dev/null || echo 000
}

# deliver MSG posts to every destination that is set, says which one
# refused it (never its URL), counts in delivered how many took it, and
# fails if any refused it.
delivered=0
deliver() {
  local msg=$1 failed=0 code payload
  delivered=0
  if [ -n "$webhook" ]; then
    # Read stdin once. Reading it twice in one dict literal left "text"
    # empty, because Python evaluates the values in order and the first read
    # exhausts it: Discord reads "content" and worked, Slack reads "text",
    # rejected the empty payload with a 400, and nothing said so.
    payload=$(printf '%s' "$msg" | python3 -c 'import json,sys; m=sys.stdin.read()[:1900]; print(json.dumps({"content": m, "text": m}))' 2>/dev/null \
      || printf '{"content":"%s"}' "$msg")
    code=$(post "$webhook" "$payload")
    case "$code" in 2*) delivered=$((delivered + 1)) ;; *) echo "healthwatch[$name]: webhook post failed (HTTP $code)" >&2; failed=1 ;; esac
  fi
  if [ "$tg" = 1 ]; then
    payload=$(printf '%s' "$msg" | TG_CHAT="$tg_chat" python3 -c 'import json,os,sys; m=sys.stdin.read()[:3500]; print(json.dumps({"chat_id": os.environ["TG_CHAT"], "text": m, "disable_web_page_preview": True}))')
    code=$(post "${tg_api}/bot${tg_token}/sendMessage" "$payload")
    case "$code" in 2*) delivered=$((delivered + 1)) ;; *) echo "healthwatch[$name]: telegram post failed (HTTP $code)" >&2; failed=1 ;; esac
  fi
  return $failed
}

# --test: post one message to every destination set and exit with the
# verdict, touching no state. A webhook pasted wrong, a bot not in the chat,
# or a channel that dropped the integration looks exactly like a healthy
# observer until the day it is not; this is how an operator proves delivery
# before that day. deploy/test/exposure.sh runs it.
if [ "${2:-}" = "--test" ]; then
  if [ -z "$webhook" ] && [ "$tg" = 0 ]; then
    echo "healthwatch[$name]: neither ALERT_WEBHOOK nor TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are set; nothing to test" >&2; exit 1
  fi
  if deliver "Fibre observer [$name] test: alert delivery check from $(hostname) at $(date -u +%Y-%m-%dT%H:%M:%SZ); no action needed"; then
    echo "healthwatch[$name]: test message delivered"; exit 0
  fi
  exit 1
fi

body=$(curl -sS -m 20 -o /dev/stdout -w '\n%{http_code}' "$url" 2>/dev/null || echo -e '\n000')
code="${body##*$'\n'}"
json="${body%$'\n'*}"

if [ "$code" = "200" ]; then
  now="ok"
  failing=""
  summary="every observer process is alive"
elif [ "$code" = "000" ]; then
  now="down"
  failing=""
  summary="observer-api at $listen does not answer"
else
  now="degraded"
  # Two lines out: the sorted, comma-joined names of the failing checks,
  # then the human summary. The names are what the state file remembers
  # (below); the summary is what the alert says. No f-strings: a nested
  # c["name"] inside one is a syntax error before Python 3.12, and this
  # runs on whatever python3 the host has.
  parsed=$(printf '%s' "$json" | python3 -c '
import json, sys
try:
    h = json.load(sys.stdin)
except Exception:
    print("?"); print("unreadable health body"); sys.exit()
bad = [c for c in h.get("checks", []) if not c.get("ok")]
print(",".join(sorted(set(str(c.get("name", "?")) for c in bad))))
print(str(h.get("status", "?")) + ": " + "; ".join(str(c.get("name", "?")) + ": " + str(c.get("detail", "")) for c in bad))
' 2>/dev/null) || parsed=$(printf '?\nhealth %s' "$code")
  failing=$(printf '%s\n' "$parsed" | sed -n 1p)
  summary=$(printf '%s\n' "$parsed" | sed -n 2p)
  [ -n "$summary" ] || summary="health $code"
fi

# fail_check NAME WHAT: a check of this host's own joins the failing set
# under NAME (no spaces), so it alerts once, nags while it stays failed,
# and the run that finds it fixed recovers; WHAT joins the summary.
fail_check() {
  # shellcheck disable=SC2086 # the check names hold no spaces
  failing=$(printf '%s\n' ${failing//,/ } "$1" | LC_ALL=C sort -u | paste -sd, -)
  case $now in
    ok) now="degraded"; summary="every observer process is alive; $2" ;;
    *) summary="$summary; $2" ;;
  esac
}

# hours SECONDS: whole hours, for a summary.
hours() { echo "$(($1 / 3600))h"; }

# The nightly jobs fail where /v1/health cannot see them: a backup that no
# longer copies (and with it the remote proofs that retiring a local copy
# rests on, which then stops), or an archive run that failed (which skips
# the retirement after it). A failed unit is a failing check under its own
# name. SYSTEMCTL is the systemctl binary (the selftest puts a fake there);
# with none, nothing is asked.
sysctl_bin="${SYSTEMCTL:-systemctl}"
have_sysctl=0
if command -v "$sysctl_bin" >/dev/null 2>&1; then have_sysctl=1; fi
unit_failed() { [ "$have_sysctl" = 1 ] && "$sysctl_bin" is-failed --quiet "$1" 2>/dev/null; }
for unit in "fibre-backup@$instance.service" "fibre-archive@$instance.service"; do
  if unit_failed "$unit"; then fail_check "${unit%.service}" "$unit failed (journalctl -u $unit)"; fi
done

# A unit that never failed is not a job that ran: a timer never enabled on
# a new host leaves nothing failed. So the outcomes are checked too. The
# backup records each copy that finished in exports/remote-copy.json; with
# BACKUP_REMOTE set, one older than 26 hours (a night missed, and some) is
# a failing check, and so is none at all once exports have been there that
# long.
if [ -n "$backup_remote" ]; then
  copy_rec="$data/exports/remote-copy.json"
  if [ -r "$copy_rec" ]; then
    copied=$(sed -n 's/.*"copied_at" *: *"\([^"]*\)".*/\1/p' "$copy_rec" | head -n 1 || true)
    copied_s=$(date -u -d "$copied" +%s 2>/dev/null || true)
    if [ -z "$copied" ] || [ -z "$copied_s" ]; then
      fail_check backup-copy "exports/remote-copy.json says no time a copy finished"
    elif [ $((epoch - copied_s)) -gt $((26 * 3600)) ]; then
      fail_check backup-copy "no backup copy has finished for $(hours $((epoch - copied_s))) (the last at $copied)"
    fi
  else
    oldest=$(find "$data/exports" -maxdepth 1 -name '*.tar.gz' -printf '%T@\n' 2>/dev/null | sort -n | head -n 1 || true)
    oldest=${oldest%%.*}
    if [ -n "$oldest" ] && [ $((epoch - oldest)) -gt $((26 * 3600)) ]; then
      fail_check backup-copy "no backup copy has ever finished (no exports/remote-copy.json), with exports there for $(hours $((epoch - oldest)))"
    fi
  fi
fi

# The run's hour of the day (UTC) and yesterday's date, which the daily
# export and its remote proof are due by.
hour=$((10#$(date -u -d "@$epoch" +%H)))
yday=$(date -u -d "@$((epoch - 86400))" +%F)

# The collector builds each day's export from 03:00 UTC; from 04:00 on,
# yesterday's must be there. An export that stopped (its index lost, any
# error it keeps hitting) used to be a log line every pass and nothing else.
if [ -d "$data/exports" ] && [ "$hour" -ge 4 ]; then
  found=0
  for f in "$data/exports/"*"-$yday.tar.gz"; do
    if [ -e "$f" ]; then found=1; fi
  done
  if [ "$found" = 0 ]; then fail_check export "no daily export for $yday after 04:00 UTC"; fi
fi

# The backup reads each export back from the remote and appends what it
# found to exports/remote.jsonl, the newest line for a name being the one
# that counts; observer-archive -retire removes a segment only once the
# exports holding it are proven there. A remote that takes the copies but
# cannot give them back writes "ok": false lines while the backup unit
# succeeds, and retirement keeps every segment with nothing saying why. So
# with BACKUP_REMOTE set, from 06:00 UTC (the backup runs from 03:17),
# yesterday's export whose newest line there is not a proof, or that has
# none, is a failing check of its own. An export not there at all is the
# export check's.
if [ -n "$backup_remote" ] && [ -d "$data/exports" ] && [ "$hour" -ge 6 ]; then
  for f in "$data/exports/"*"-$yday.tar.gz"; do
    [ -e "$f" ] || continue
    n=${f##*/}
    last=$(grep -aF "\"name\":\"$n\"" "$data/exports/remote.jsonl" 2>/dev/null | tail -n 1 || true)
    case $last in
      *'"ok":true'*) ;;
      *) fail_check backup-proof "$n not proven on the remote" ;;
    esac
  done
fi

# The second vantages: each pull appends whatever the vantage's heartbeat
# wrote since the last, every few minutes while it runs. Nothing new for
# 30 minutes is a pull that keeps failing or a heartbeat that died there,
# which look the same from here. The pull unit runs every minute, and one
# failed run is noise the next run clears, so its failure counts once
# nothing has come for 10 minutes too. A vantage that never sent anything
# (before Fibre is live there is nothing to dial) is not judged; one whose
# lines were all archived (the nightly rotation can leave its live file
# empty, or none) still is, from when the rotation left it.
if [ -n "$pull_names" ]; then
  pull_unit="fibre-vantage-pull@$instance.service"
  pull_failed=0
  if unit_failed "$pull_unit"; then pull_failed=1; fi
  for n in $pull_names; do
    f="$data/vantages/$n/reachability.jsonl"
    idx="$data/vantages/$n/archive/reachability.jsonl/index.json"
    if [ ! -s "$f" ]; then
      [ -e "$idx" ] || continue
      [ -e "$f" ] || f=$idx
    fi
    at=$(stat -c %Y "$f" 2>/dev/null || true)
    [ -n "$at" ] || continue
    quiet=$((epoch - at))
    if [ "$quiet" -gt 1800 ]; then
      fail_check vantage-pull "nothing new from vantage $n for $((quiet / 60))m"
    elif [ "$pull_failed" = 1 ] && [ "$quiet" -gt 600 ]; then
      fail_check "${pull_unit%.service}" "$pull_unit failed and nothing new from vantage $n for $((quiet / 60))m (journalctl -u $pull_unit)"
    fi
  done
fi

# The state file: line 1 the state (ok, degraded, down), line 2 the epoch of
# the last alert, line 3 the failing check names at that alert. Line 3 is
# new; a file written by an older build has two lines, and its missing set
# is "unknown" rather than "empty", so the upgrade alone does not alert.
prev_state=""; prev_at=0; prev_failing=""; have_prev_failing=0
if [ -r "$state" ]; then
  prev_state=$(sed -n 1p "$state"); prev_at=$(sed -n 2p "$state")
  if [ "$(wc -l < "$state")" -ge 3 ]; then
    prev_failing=$(sed -n 3p "$state"); have_prev_failing=1
  fi
fi
case "$prev_at" in ''|*[!0-9]*) prev_at=0 ;; esac
echo "healthwatch[$name]: $now ($summary)"

# Alert when the state changes, when the set of failing checks changes
# while the state does not, and every ALERT_REPEAT_MIN while it stays bad.
# The set matters because "degraded" is one word for many faults: without
# it, a disk filling up an hour after a stuck scan would be the same state,
# already alerted, and nobody would hear about the second fault until the
# repeat — or ever, if the first one was the kind that lingers.
notify=0; changed=0
if [ "$now" != "$prev_state" ]; then notify=1
elif [ "$have_prev_failing" = 1 ] && [ "$failing" != "$prev_failing" ]; then notify=1; changed=1
elif [ "$now" != "ok" ] && [ $((epoch - prev_at)) -ge $((repeat * 60)) ]; then notify=1
fi

# write_state records the alert just made. One that cannot be written is
# said, and fails the run, but stops nothing: the alert went out first, and
# the next run, finding the old state, alerts again.
rc=0
write_state() {
  if ! { mkdir -p "$(dirname "$state")" && printf '%s\n%s\n%s\n' "$now" "$epoch" "$failing" > "$state"; } 2>/dev/null; then
    echo "healthwatch[$name]: could not write $state; the next run alerts again" >&2
    rc=1
  fi
}

if [ "$notify" = 1 ]; then
  if [ -n "$webhook" ] || [ "$tg" = 1 ]; then
    msg="Fibre observer [$name] $now: $summary"
    if [ "$changed" = 1 ]; then msg="Fibre observer [$name] $now, failing checks changed (was: ${prev_failing:-none}): $summary"; fi
    if [ "$now" = "ok" ] && [ -n "$prev_state" ]; then msg="Fibre observer [$name] recovered: $summary"; fi
    # The state is recorded once somebody has the alert. Refused by every
    # destination, it is not, and the next run sends it again; a refusal is
    # said in the log either way, and fails the run.
    if deliver "$msg"; then
      write_state
    else
      rc=1
      if [ "$delivered" -gt 0 ]; then write_state; fi
    fi
  else
    write_state
  fi
fi
exit "$rc"
