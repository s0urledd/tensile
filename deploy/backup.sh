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
# After the copy, each daily export not yet proven on the remote is read
# back from it and hashed (the remote proof, at the end): observer-archive
# -retire removes a local segment only once the exports holding its bytes
# are proven there.
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
# credentials in the string, and this output goes to the journal.
echo "fibre-backup[$instance]: $data -> BACKUP_REMOTE/$instance"
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
# uploads each once. A segment retired since (observer-archive -retire)
# keeps its copy on the remote, since nothing here deletes.
"$rclone" copy "$data/archive" "$dest/archive" \
  --include '*.jsonl.gz' --include 'index.json' \
  --transfers 4 --checkers 8 --stats-one-line --stats 0 --log-level NOTICE
for a in "$data"/vantages/*/archive; do
  [ -d "$a" ] || continue
  "$rclone" copy "$a" "$dest/${a#"$data"/}" \
    --include '*.jsonl.gz' --include 'index.json' \
    --transfers 4 --checkers 8 --stats-one-line --stats 0 --log-level NOTICE
done
"$rclone" copy "$data" "$dest" \
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
  "$rclone" copy "$snap" "$dest/status" --transfers 4 --stats-one-line --stats 0 --log-level NOTICE
fi

# The remote proof. observer-archive -retire removes a local segment's file
# only when every export holding its bytes is on the remote with the digest
# this host has, and the archive unit has no network (PrivateNetwork=true),
# so the proof is taken here, once the exports are copied: each export
# tarball not yet proven is read back from the remote whole (rclone cat),
# hashed, and compared with its .sha256 sidecar. Each check is one JSON line
# appended to exports/remote.jsonl in one write, never rewritten:
# {"name", "sha256" (the local digest checked against), "checked_at", "ok"}.
# The newest line for a name is the one that counts, so a tarball rebuilt
# under a new digest, or one whose last check failed, is read again the
# next night, and one proven is not read again.
#
# A failed check is a line with "ok": false, not a failed run: the unit's
# status is the copy's, and a tarball not proven only keeps the segments it
# holds on the disk until a later night proves it. rclone's own messages
# are not printed here, because they name the remote.
prove_remote() {
  local ledger="$data/exports/remote.jsonl" t name want got ok last line checked=0 proven=0
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
    last=$(grep -F "\"name\":\"$name\"" "$ledger" 2>/dev/null | tail -n 1 || true)
    case $last in
      *"\"sha256\":\"$want\""*'"ok":true'*) continue ;;
    esac
    if got=$("$rclone" cat "$dest/exports/$name" 2>/dev/null | sha256sum); then
      got=${got%% *}
    else
      got=""
    fi
    ok=false
    [ "$got" = "$want" ] && ok=true
    line=$(printf '{"name":"%s","sha256":"%s","checked_at":"%s","ok":%s}' "$name" "$want" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$ok")
    # A last line torn by a crash is closed first, so that this one is a
    # line of its own; it is still one write.
    if [ -s "$ledger" ] && [ -n "$(tail -c 1 "$ledger")" ]; then
      line=$'\n'"$line"
    fi
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
