#!/usr/bin/env bash
#
# vantage-sync: deploy/vantage-pull.sh against a fake sftp on local files.
# Needs bash and coreutils; no network, no root. The fake speaks the batch
# command the script sends (reget) with OpenSSH's semantics where they matter
# here: a command fails the batch unless it is prefixed with '-'; reget
# appends what the remote file has beyond the local copy.
#
#   pull     reachability is fetched, then appended, not re-fetched
#   failure  a vantage whose reachability.jsonl is missing, or a server that
#            is down, fails the run and says so
set -u
cd "$(dirname "$0")"
. ./lib.sh

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
remote="$tmp/remote"
data="$tmp/data"
log="$tmp/sftp.log"
mkdir -p "$remote/vantage/de-1" "$data"

cat >"$tmp/sftp" <<'EOF'
#!/usr/bin/env bash
# fake sftp: batch on stdin, files under $FAKE_REMOTE
[ -n "${FAKE_SFTP_DOWN:-}" ] && exit 255
sz() { if [ -f "$1" ]; then wc -c <"$1" | tr -d ' '; else echo -1; fi; }
while read -r cmd a b c; do
  soft=0
  case $cmd in -*) soft=1; cmd=${cmd#-} ;; esac
  ok=1
  case $cmd in
    reget) r="$FAKE_REMOTE/$a"; l=$b; rs=$(sz "$r"); ls=$(sz "$l"); [ "$ls" -lt 0 ] && ls=0
      if [ "$rs" -lt 0 ] || [ "$ls" -gt "$rs" ]; then ok=0
      else tail -c +$((ls + 1)) "$r" >>"$l"; echo "reget $a $((rs - ls))" >>"$FAKE_LOG"; fi ;;
    *) ok=0 ;;
  esac
  if [ "$ok" = 0 ] && [ "$soft" = 0 ]; then exit 1; fi
done
exit 0
EOF
chmod +x "$tmp/sftp"

run() {
  FAKE_REMOTE="$remote" FAKE_LOG="$log" SFTP="$tmp/sftp" DATA_DIR="$data" \
    VANTAGE_PULL_HOST=tensile-backup@example VANTAGE_PULL_NAMES=de-1 \
    VANTAGE_PULL_KEY=/dev/null VANTAGE_PULL_KNOWN=/dev/null \
    sh ../vantage-pull.sh mocha "$@" 2>"$tmp/stderr"
}
same() { cmp -s "$1" "$2"; }

rreach="$remote/vantage/de-1/reachability.jsonl"
lreach="$data/vantages/de-1/reachability.jsonl"

echo "vantage-sync"
echo '{"beat":1}' >"$rreach"

if run; then pass "first pull: exit 0"; else fail "exit $? on the first pull ($(cat "$tmp/stderr"))"; fi
same "$rreach" "$lreach" && pass "reachability pulled" || fail "reachability not pulled"

# more heartbeats arrive: only the new bytes come back
echo '{"beat":2}' >>"$rreach"
: >"$log"
run || fail "second pull: exit $?"
same "$rreach" "$lreach" && pass "reachability appended" || fail "reachability differs"
grep -q "^reget vantage/de-1/reachability.jsonl 11\$" "$log" && pass "only the new heartbeat is fetched" || fail "pull: $(cat "$log")"

# the server is down: the run fails and says so
if FAKE_SFTP_DOWN=1 run; then fail "a failed fetch exited 0"; else pass "a failed fetch exits non-zero"; fi
grep -q "fetch failed" "$tmp/stderr" && pass "the failed fetch is named" || fail "stderr: $(cat "$tmp/stderr")"

# reachability.jsonl missing on the vantage is a failure
rm "$rreach"
if run; then fail "a missing reachability.jsonl passed"; else pass "a missing reachability.jsonl fails the fetch"; fi

[ "$FAILED" = 0 ] && echo "vantage-sync: all passed" || { echo "vantage-sync: FAILED"; exit 1; }
