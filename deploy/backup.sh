#!/usr/bin/env bash
#
# fibre-backup <instance>: copy the raw record (the JSONL files and
# state.json) off the host. Runs nightly from fibre-backup@.timer as the
# service user with the instance's env file.
#
# Litestream replicates only the derived database; the JSONL files are what
# everything is rebuilt from and they had no shipped copy until this. The
# copy goes to BACKUP_REMOTE, an rclone remote path ("r2:fibre-observer",
# "b2:bucket/path", "sftp-host:/backups"), configured once in
# /etc/fibre-observer/rclone.conf (RCLONE_CONFIG below) with any provider
# rclone supports. The files are append-only, so a nightly sync moves only
# what was added since yesterday.
#
# A copy that finishes is recorded (exports/remote-copy.json), and each
# daily export not yet proven on the remote is then read back from it and
# hashed (the remote proof, at the end): observer-archive -retire removes a
# local segment only once the exports holding its bytes are proven on the
# remote the backup copies to, and only while its copies go on finishing.
#
# Never copied: the sampling master key. It is what makes the published
# sample commitments checkable and unpredictable, and a copy that leaves the
# host is a copy a publisher might read.
#
# With BACKUP_REMOTE empty the script says so and exits 0, so the timer can
# be enabled everywhere and armed by setting one variable. RCLONE names the
# rclone binary (the tests put a fake there).
set -o errexit -o nounset -o pipefail

instance="${1:?instance}"
data="${DATA_DIR:-/var/lib/fibre-observer/$instance}"
remote="${BACKUP_REMOTE:-}"
rclone="${RCLONE:-rclone}"
export RCLONE_CONFIG="${RCLONE_CONFIG:-/etc/fibre-observer/rclone.conf}"
manifest_tool="${FIBRE_BACKUP_MANIFEST:-/usr/local/bin/fibre-backup-manifest}"
[ -x "$manifest_tool" ] || manifest_tool="$(dirname "$0")/backup-manifest.py"

if [ -z "$remote" ]; then
  echo "fibre-backup[$instance]: BACKUP_REMOTE is not set; nothing copied (litestream still covers the database if enabled)"
  exit 0
fi
command -v "$rclone" >/dev/null || { echo "fibre-backup: rclone is not installed" >&2; exit 1; }

dest="$remote/$instance"
# The remote itself is never printed: one given whole on the command line
# (":s3,access_key_id=...,secret_access_key=...:bucket") carries its
# credentials in the string, and this output goes to the journal. This
# script's own lines name it BACKUP_REMOTE, and rclone's messages from the
# copies below, which can name it as given (a remote it cannot set up),
# have it replaced with BACKUP_REMOTE (quiet_remote). rclone's command line
# still holds it while rclone runs, for anyone on the host to read in the
# process list: a named remote in rclone.conf (RCLONE_CONFIG) keeps
# credentials out of both.
echo "fibre-backup[$instance]: $data -> BACKUP_REMOTE/$instance"
# The remote's fingerprint: the first 16 hex digits of the SHA-256 of the
# destination, which names it in exports/remote.jsonl and
# exports/remote-copy.json without its credentials. A proof read from
# another remote is not a proof of this one.
fp=$(printf '%s' "$dest" | sha256sum | cut -c1-16)

# quiet_remote <command...>: the command, with its stderr passed on and
# every occurrence of the remote in it replaced with BACKUP_REMOTE; stdout
# untouched, and the command's exit status (pipefail) as its own.
quiet_remote() {
  { "$@" 2>&1 1>&3 3>&- | R="$remote" awk 'BEGIN { r = ENVIRON["R"]; n = length(r) }
      { s = $0; out = ""; while (n > 0 && (i = index(s, r)) > 0) { out = out substr(s, 1, i - 1) "BACKUP_REMOTE"; s = substr(s, i + n) } print out s; fflush() }' >&2; } 3>&1
}
# observer-archive rotates the biggest files daily: their older lines move
# into archive/<file>/*.jsonl.gz and the live file keeps the rest. It holds
# archive/.lock exclusively while it does; the cut and the copy below hold
# it shared, so they never straddle a rotation (the manifest tool takes it
# shared too, which a shared lock allows). Another vantage's heartbeats,
# vantages/<name>/reachability.jsonl, rotate the same way under their own
# vantages/<name>/archive/.lock, which is held here as well.
mkdir -p "$data/archive"
exec 9>>"$data/archive/.lock"
flock -s -w 7200 9 || { echo "fibre-backup[$instance]: observer-archive held archive/.lock for two hours" >&2; exit 1; }
for v in "$data"/vantages/*; do
  [ -f "$v/reachability.jsonl" ] || continue
  mkdir -p "$v/archive"
  exec {lockfd}>>"$v/archive/.lock"
  flock -s -w 7200 "$lockfd" || { echo "fibre-backup[$instance]: observer-archive held ${v#"$data"/}/archive/.lock for two hours" >&2; exit 1; }
done
# One consistent cut of the record before anything is copied: state.json
# first (read whole and carried in the manifest), then the byte length of
# every record file up to its last complete line (dependents before what
# they refer to), then the SHA-256, the parse and the record count of
# exactly those bytes. The files keep growing while rclone reads them, so
# the copy is at least the cut; deploy/test/restore.sh trims a restored copy
# back to the cut, checks every hash and puts the cut's state.json beside
# it. Missing, short, different or unparseable is a failed restore — and a
# record that is not a sequence of JSON lines fails the cut here, before
# anything is copied, so the timer unit shows it.
if [ -x "$manifest_tool" ]; then
  "$manifest_tool" write "$data" "$data/backup-manifest.json"
else
  echo "fibre-backup[$instance]: backup-manifest tool not found; copying without a manifest (restore.sh will refuse to verify this copy)" >&2
fi
# copy, not sync. The record is append-only, so copy is the correct verb, and
# sync would mirror a deletion: deploy/README.md tells the operator that the
# answer to a full disk is to move the oldest JSONL files off the box, and the
# next nightly run would then delete exactly those files from the remote —
# which is the only copy, since litestream replicates the derived database and
# not the record. --max-delete 0 is belt and braces for the same reason.
# --local-no-check-updated: the JSONL files are being appended while they
# are read, and rclone would otherwise abort with "source file is being
# updated". The copy is whatever length the file had when the transfer
# began, which is at least the manifest's cut.
#
# The archive first, in its own pass (each other vantage's after the
# observer's own): a rotated live file is shorter than the copy the remote
# holds and replaces it there, so the segments holding its older lines must
# be on the remote before it is. Segments never change once written; copy
# uploads each once, with index.json and retired.json (the second copy of
# the retired records, which an older observer-archive drops from the
# index). A segment retired since (observer-archive -retire) keeps its copy
# on the remote if it was ever sent, since nothing here deletes; one
# archived and retired in the same night never was, and its lines are on
# the remote in the exports alone.
quiet_remote "$rclone" copy "$data/archive" "$dest/archive" \
  --include '*.jsonl.gz' --include 'index.json' --include 'retired.json' \
  --transfers 4 --checkers 8 --stats-one-line --stats 0 --log-level NOTICE
for a in "$data"/vantages/*/archive; do
  [ -d "$a" ] || continue
  quiet_remote "$rclone" copy "$a" "$dest/${a#"$data"/}" \
    --include '*.jsonl.gz' --include 'index.json' --include 'retired.json' \
    --transfers 4 --checkers 8 --stats-one-line --stats 0 --log-level NOTICE
done
quiet_remote "$rclone" copy "$data" "$dest" \
  --include '*.jsonl' --include 'state.json' --include 'backup-manifest.json' --include 'exports/**' \
  --exclude 'sampling-master.key' --exclude 'observer.db*' --exclude 'snapshots/**' \
  --local-no-check-updated \
  --transfers 4 --checkers 8 --stats-one-line --stats 0 --log-level NOTICE
# The status files are replaced (written beside, renamed over) every few
# seconds, so one can be another file by the time rclone opens the name it
# listed, and rclone calls the size it then reads a corrupted transfer and
# fails the night. They are not the record: they go from a copy taken first,
# each file whole as one of its versions.
snap="$(mktemp -d)"
trap 'rm -rf "$snap"' EXIT
if [ -d "$data/status" ]; then
  cp -p "$data"/status/* "$snap/" 2>/dev/null || true
  quiet_remote "$rclone" copy "$snap" "$dest/status" --transfers 4 --stats-one-line --stats 0 --log-level NOTICE
fi

# The copy finished: every export on this host is on the remote now, since
# rclone copy uploads whatever the remote lacks (a run that failed above
# stopped there, errexit). observer-archive -retire counts the remote
# proofs below only while this record is recent and names the remote they
# were read from: a proof is taken once per tarball, and a remote that died,
# lost its credentials or was replaced since shows here as a copy that no
# longer finishes, or one to another remote. Written beside and renamed
# over, so it is always one whole record.
if [ -d "$data/exports" ]; then
  printf '{"copied_at":"%s","remote":"%s"}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$fp" >"$data/exports/remote-copy.json.tmp"
  mv -f "$data/exports/remote-copy.json.tmp" "$data/exports/remote-copy.json"
fi

# The remote proof. observer-archive -retire removes a local segment's file
# only when every export holding its bytes is on the remote with the digest
# this host has, and the archive unit has no network (PrivateNetwork=true),
# so the proof is taken here, once the exports are copied: each export
# tarball not yet proven is read back from the remote whole (rclone cat),
# hashed, and compared with its .sha256 sidecar. Each check is one JSON line
# appended to exports/remote.jsonl in one write, never rewritten:
# {"name", "sha256" (the local digest checked against), "checked_at", "ok",
# "remote" (the fingerprint of the remote read)}. The newest line for a
# name is the one that counts, so a tarball rebuilt under a new digest, one
# whose last check failed, or one last proven on another remote (a new
# BACKUP_REMOTE) is read again the next night, and one proven on this
# remote is not read again.
#
# A failed check is a line with "ok": false, not a failed run: the unit's
# status is the copy's, and a tarball not proven only keeps the segments it
# holds on the disk until a later night proves it. rclone's own messages
# are not printed here, because they name the remote.
#
# A last line that a crash or a full disk cut short has no newline. It
# proves nothing, since observer-archive reads only complete lines, but
# closed with a newline it would be a complete line that is not a check,
# which observer-archive refuses rather than skip, and the retirement would
# stop every night until someone edited the file. So those bytes are cut
# off before anything is appended; no complete line is ever touched. The
# last byte is read as a number: a crash can leave the end of an append as
# NUL bytes (the size reached the disk, the data did not), and a NUL read
# through $(...) is dropped, which would make that line look whole.
prove_remote() {
  local ledger="$data/exports/remote.jsonl" t name want got ok last line torn checked=0 proven=0
  if [ -s "$ledger" ] && [ "$(tail -c 1 "$ledger" | od -An -tx1 | tr -d ' \n')" != 0a ]; then
    # tail -n 1 prints the unterminated last line alone, NUL bytes and
    # all: its length is the torn bytes'.
    torn=$(tail -n 1 "$ledger" | wc -c)
    if truncate -s "$(( $(wc -c <"$ledger") - torn ))" "$ledger"; then
      echo "fibre-backup[$instance]: remote proof: dropped a torn last line ($((torn)) bytes) from exports/remote.jsonl" >&2
    else
      echo "fibre-backup[$instance]: remote proof: cannot drop the torn last line of exports/remote.jsonl" >&2
      return 1
    fi
  fi
  for t in "$data"/exports/*.tar.gz; do
    [ -f "$t" ] || continue
    name=${t##*/}
    # Export names are [A-Za-z0-9._-] (export.NamePattern), which is also
    # what lets the line below be written without escaping.
    case $name in
      *[!A-Za-z0-9._-]*) echo "fibre-backup[$instance]: remote proof: an export with an unexpected name is not checked" >&2; continue ;;
    esac
    want=""
    if [ -f "$t.sha256" ]; then read -r want _ <"$t.sha256" || true; fi
    case $want in
      ''|*[!0-9a-f]*) echo "fibre-backup[$instance]: remote proof: $name has no readable .sha256 beside it; not checked" >&2; continue ;;
    esac
    [ "${#want}" = 64 ] || { echo "fibre-backup[$instance]: remote proof: $name.sha256 holds no SHA-256; not checked" >&2; continue; }
    # -a: a line grep took for binary would come back as grep's own message,
    # or as nothing, and the export would be read back every night.
    last=$(grep -aF "\"name\":\"$name\"" "$ledger" 2>/dev/null | tail -n 1 || true)
    case $last in
      *"\"sha256\":\"$want\""*'"ok":true'*"\"remote\":\"$fp\""*) continue ;;
    esac
    if got=$("$rclone" cat "$dest/exports/$name" 2>/dev/null | sha256sum); then
      got=${got%% *}
    else
      got=""
    fi
    ok=false
    [ "$got" = "$want" ] && ok=true
    line=$(printf '{"name":"%s","sha256":"%s","checked_at":"%s","ok":%s,"remote":"%s"}' "$name" "$want" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$ok" "$fp")
    # One write. One that fails part-way leaves a torn line, which the next
    # run drops (above); this run stops here.
    printf '%s\n' "$line" >>"$ledger" || { echo "fibre-backup[$instance]: remote proof: cannot append to exports/remote.jsonl" >&2; return 1; }
    checked=$((checked + 1))
    if [ "$ok" = true ]; then
      proven=$((proven + 1))
    else
      echo "fibre-backup[$instance]: remote proof: $name: the remote copy could not be read or is not the local one; checked again next night" >&2
    fi
  done
  echo "fibre-backup[$instance]: remote proof: $checked export(s) read back, $proven proven"
}
if [ -d "$data/exports" ]; then
  prove_remote || echo "fibre-backup[$instance]: the remote proof did not finish; the copy stands" >&2
fi
echo "fibre-backup[$instance]: done"
