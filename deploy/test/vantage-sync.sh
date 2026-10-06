#!/usr/bin/env bash
#
# vantage-sync: deploy/vantage-pull.sh against fake-rclone.sh on local files
# and a fake observer-archive. Needs bash, coreutils and util-linux's flock;
# no network, no root, no rclone. The fake observer-archive answers
# -logical-end with the live file's size plus FAKE_BASE, which stands for
# the base of a live file that observer-archive rotated (0: never rotated).
#
#   pull      the first pull fetches the whole file over the sftp remote;
#             the next fetches only the bytes past the local logical end,
#             and one with nothing new fetches nothing
#   rotated   a rotated local file resumes from its logical end, not its size
#   shorter   a remote file shorter than the local logical end fails the
#             run, says so, and leaves the local file as it was
#   source    VANTAGE_PULL_SOURCE reads a local directory in place of the
#             account
#   one pull  a pull that finds another pull of the vantage running fetches
#             nothing and passes; two at once append the new bytes once
#   whole     only whole lines are appended: a line the vantage is still
#             writing waits for the next pull, and a fetch the link cuts
#             short appends the whole lines that arrived and fails
#   failure   a vantage whose reachability.jsonl is missing, or a server
#             that is down, fails the run and says so
set -u
cd "$(dirname "$0")"
. ./lib.sh

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
remote="$tmp/remote"
data="$tmp/data"
log="$tmp/rclone.log"
mkdir -p "$remote/vantage/de-1" "$data"

cat >"$tmp/observer-archive" <<'EOF'
#!/usr/bin/env bash
# fake observer-archive -data-dir D -logical-end F
d="" f=""
while [ $# -gt 0 ]; do
  case $1 in
    -data-dir) d=$2; shift 2 ;;
    -logical-end) f=$2; shift 2 ;;
    *) echo "fake observer-archive: unexpected $1" >&2; exit 2 ;;
  esac
done
[ -f "$d/$f" ] || { echo "$d/$f: no such file" >&2; exit 1; }
echo $(( ${FAKE_BASE:-0} + $(wc -c <"$d/$f") ))
EOF
chmod +x "$tmp/observer-archive"

run() {
  FAKE_RCLONE_ROOT="$remote" FAKE_RCLONE_LOG="$log" RCLONE="$PWD/fake-rclone.sh" OBSERVER_ARCHIVE="$tmp/observer-archive" \
    DATA_DIR="$data" VANTAGE_PULL_HOST=tensile-backup@example VANTAGE_PULL_NAMES=de-1 \
    VANTAGE_PULL_KEY=/dev/null VANTAGE_PULL_KNOWN=/dev/null \
    sh ../vantage-pull.sh mocha "$@" 2>"$tmp/stderr"
}
same() { cmp -s "$1" "$2"; }

rreach="$remote/vantage/de-1/reachability.jsonl"
lreach="$data/vantages/de-1/reachability.jsonl"
sftp=":sftp,host=example,user=tensile-backup,key_file=/dev/null,known_hosts_file=/dev/null,shell_type=none:vantage/de-1/reachability.jsonl"

echo "vantage-sync"
echo '{"beat":1}' >"$rreach"

if run; then pass "first pull: exit 0"; else fail "exit $? on the first pull ($(cat "$tmp/stderr"))"; fi
same "$rreach" "$lreach" && pass "reachability pulled" || fail "reachability not pulled"
grep -qF "cat --offset 0 -q --contimeout 20s --timeout 60s $sftp" "$log" && pass "fetched over the sftp remote from offset 0" || fail "rclone calls: $(cat "$log")"

# more heartbeats arrive: only the new bytes come back
echo '{"beat":2}' >>"$rreach"
: >"$log"
run || fail "second pull: exit $? ($(cat "$tmp/stderr"))"
same "$rreach" "$lreach" && pass "reachability appended" || fail "reachability differs"
grep -q "^cat --offset 11 " "$log" && pass "only the new heartbeat is fetched" || fail "pull: $(cat "$log")"

# nothing new: nothing is fetched
: >"$log"
run || fail "third pull: exit $?"
grep -q "^cat " "$log" && fail "a pull with nothing new fetched: $(cat "$log")" || pass "nothing new, nothing fetched"

# observer-archive rotated the local file: its first heartbeat moved to the
# archive (base 11), and the live file holds the second. The pull resumes
# from the logical end, 22, not from the live file's 11 bytes.
tail -c +12 "$rreach" >"$lreach"
echo '{"beat":3}' >>"$rreach"
: >"$log"
FAKE_BASE=11 run || fail "pull after a rotation: exit $? ($(cat "$tmp/stderr"))"
grep -q "^cat --offset 22 " "$log" && pass "a rotated file resumes from its logical end" || fail "pull: $(cat "$log")"
same <(tail -c +12 "$rreach") "$lreach" && pass "the live file holds the remote's bytes past the base" || fail "the live file differs"

# the remote file is shorter than what the record holds of it: nothing is fetched
cp "$lreach" "$tmp/before"
if FAKE_BASE=100 run; then fail "a remote shorter than the local end passed"; else pass "a remote shorter than the local end fails the run"; fi
grep -q "shorter than the 122 this record holds" "$tmp/stderr" && pass "and says so" || fail "stderr: $(cat "$tmp/stderr")"
same "$tmp/before" "$lreach" && pass "the local file is left as it was" || fail "the local file changed"

# VANTAGE_PULL_SOURCE: a local directory in place of the account
src="$tmp/source"; mkdir -p "$src/vantage/de-1" "$tmp/data2"
echo '{"beat":"s"}' >"$src/vantage/de-1/reachability.jsonl"
if FAKE_RCLONE_LOG="$log" RCLONE="$PWD/fake-rclone.sh" OBSERVER_ARCHIVE="$tmp/observer-archive" DATA_DIR="$tmp/data2" \
    VANTAGE_PULL_SOURCE="$src" VANTAGE_PULL_NAMES=de-1 sh ../vantage-pull.sh mocha 2>"$tmp/stderr"; then
  pass "a pull from VANTAGE_PULL_SOURCE: exit 0"
else
  fail "a pull from VANTAGE_PULL_SOURCE: exit $? ($(cat "$tmp/stderr"))"
fi
same "$src/vantage/de-1/reachability.jsonl" "$tmp/data2/vantages/de-1/reachability.jsonl" && pass "read from the local directory" || fail "not read from the local directory"

# another pull of the vantage holds its .pull.lock: this one fetches
# nothing, says so and passes, and the one holding it fetches
echo '{"beat":4}' >>"$rreach"
exec 7>>"$data/vantages/de-1/.pull.lock"
flock -n 7 || fail "the test could not take .pull.lock"
cp "$lreach" "$tmp/before"
: >"$log"
if FAKE_BASE=11 run >"$tmp/stdout"; then pass "a pull beside another passes"; else fail "a pull beside another: exit $? ($(cat "$tmp/stderr"))"; fi
exec 7>&-
grep -q "^cat " "$log" && fail "a pull beside another fetched: $(cat "$log")" || pass "a pull beside another fetches nothing"
grep -q "another pull of it is running" "$tmp/stdout" && pass "and says so" || fail "stdout: $(cat "$tmp/stdout")"
same "$tmp/before" "$lreach" && pass "the local file is left to the other pull" || fail "the local file changed"

# two pulls at once over a slow link: one fetches and the other leaves the
# vantage to it, so the new heartbeats are appended once, not twice (two
# appends would put every later byte at the wrong offset)
echo '{"beat":5}' >>"$rreach"
FAKE_BASE=11 FAKE_RCLONE_SLOW=2 run >/dev/null & p1=$!
FAKE_BASE=11 FAKE_RCLONE_SLOW=2 run >/dev/null & p2=$!
wait "$p1" && wait "$p2" && pass "two pulls at once pass" || fail "two pulls at once: one failed"
same <(tail -c +12 "$rreach") "$lreach" && pass "two pulls at once append the new bytes once" || fail "two pulls at once: the local file is not the remote's bytes past the base ($(cat "$lreach"))"

# the vantage is writing a line as it is read: only the whole lines are
# appended, so the record never ends inside a line (which nothing here
# would complete, and which observer-archive leaves unrotated), and the
# next pull fetches the rest of that line, from the local logical end
past() { tail -c +12 "$rreach" | head -c $(( $(wc -c <"$rreach") - 11 - $1 )); } # the remote's bytes past the base, but its last $1
printf '{"beat":6}\n{"be' >>"$rreach"
FAKE_BASE=11 run || fail "a pull of a line being written: exit $? ($(cat "$tmp/stderr"))"
same <(past 4) "$lreach" && pass "a line being written is not appended" || fail "a line being written: the local file holds $(cat "$lreach")"
printf 'at":7}\n' >>"$rreach"
: >"$log"
FAKE_BASE=11 run || fail "the pull after it: exit $? ($(cat "$tmp/stderr"))"
same <(tail -c +12 "$rreach") "$lreach" && pass "the next pull appends the whole line" || fail "the next pull: the local file holds $(cat "$lreach")"
test ! -e "$data/vantages/de-1/.pull.part" && pass "no fetched bytes are left beside it" || fail ".pull.part left behind"

# the link breaks part-way: the whole lines that arrived are appended and
# the run fails and says so; the next pull fetches the rest
printf '{"beat":8}\n{"beat":9}\n' >>"$rreach"
if FAKE_BASE=11 FAKE_RCLONE_CUT=15 run; then fail "a fetch cut short passed"; else pass "a fetch cut short fails the run"; fi
grep -q "fetch failed" "$tmp/stderr" && pass "and says so" || fail "stderr: $(cat "$tmp/stderr")"
same <(past 11) "$lreach" && pass "the whole line that arrived is appended, the cut one is not" || fail "a fetch cut short: the local file holds $(cat "$lreach")"
FAKE_BASE=11 run || fail "the pull after a fetch cut short: exit $? ($(cat "$tmp/stderr"))"
same <(tail -c +12 "$rreach") "$lreach" && pass "the next pull appends the rest" || fail "after a fetch cut short: the local file holds $(cat "$lreach")"

# the server is down: the run fails and says so
if FAKE_RCLONE_FAIL=lsf run; then fail "a failed fetch exited 0"; else pass "a failed fetch exits non-zero"; fi
grep -q "fetch failed" "$tmp/stderr" && pass "the failed fetch is named" || fail "stderr: $(cat "$tmp/stderr")"

# reachability.jsonl missing on the vantage is a failure
rm "$rreach"
if run; then fail "a missing reachability.jsonl passed"; else pass "a missing reachability.jsonl fails the fetch"; fi

[ "$FAILED" = 0 ] && echo "vantage-sync: all passed" || { echo "vantage-sync: FAILED"; exit 1; }
