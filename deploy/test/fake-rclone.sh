#!/usr/bin/env bash
#
# fake-rclone: the rclone subcommands the deploy scripts use, over local
# files, for their tests (no network, no rclone). A remote path
# "<remote>:<path>", the on-the-fly ":sftp,host=...,...:<path>" included, is
# $FAKE_RCLONE_ROOT/<path>, the part after the last ':'; a path without ':'
# is a local path, as rclone takes it. Every flag is accepted and ignored but
# cat's --offset and lsf's --format.
#
#   copy <src> <dst> [flags]    every file under src into dst (filters ignored)
#   cat [--offset N] <path>     the file from byte N
#   lsf --format s <path>       the file's size
#
# A path that does not exist is rclone's "directory not found", exit 3, with
# the path in the message, as rclone prints it.
#
#   FAKE_RCLONE_LOG     one line per call: the subcommand and its arguments
#   FAKE_RCLONE_FAIL    a subcommand that fails (exit 1) whatever it is asked
#   FAKE_RCLONE_GARBLE  a file name whose bytes cat serves altered (a remote
#                       copy that is not the one uploaded)
set -u
sub=${1:-}
shift || true
[ -n "${FAKE_RCLONE_LOG:-}" ] && printf '%s %s\n' "$sub" "$*" >>"$FAKE_RCLONE_LOG"
if [ -n "${FAKE_RCLONE_FAIL:-}" ] && [ "$sub" = "$FAKE_RCLONE_FAIL" ]; then
  echo "fake-rclone: $sub failed (FAKE_RCLONE_FAIL)" >&2
  exit 1
fi
local_path() {
  case $1 in
    *:*) printf '%s/%s' "${FAKE_RCLONE_ROOT:?FAKE_RCLONE_ROOT not set}" "${1##*:}" ;;
    *) printf '%s' "$1" ;;
  esac
}
offset=0 format=""
paths=()
while [ $# -gt 0 ]; do
  case $1 in
    --offset) offset=$2; shift 2 ;;
    --offset=*) offset=${1#--offset=}; shift ;;
    --format) format=$2; shift 2 ;;
    --format=*) format=${1#--format=}; shift ;;
    # flags that take a value, so the value is not taken for a path
    --include|--exclude|--transfers|--checkers|--stats|--log-level|--contimeout|--timeout|--config|--count) shift 2 ;;
    -*) shift ;;
    *) paths+=("$1"); shift ;;
  esac
done
missing() { echo "fake-rclone: error: $1: directory not found" >&2; exit 3; }
case $sub in
  copy)
    [ ${#paths[@]} = 2 ] || { echo "fake-rclone: copy wants <src> <dst>" >&2; exit 2; }
    src=$(local_path "${paths[0]}"); dst=$(local_path "${paths[1]}")
    [ -d "$src" ] || missing "${paths[0]}"
    mkdir -p "$dst" && cp -R "$src/." "$dst/"
    ;;
  cat)
    [ ${#paths[@]} = 1 ] || { echo "fake-rclone: cat wants one path" >&2; exit 2; }
    f=$(local_path "${paths[0]}")
    [ -f "$f" ] || missing "${paths[0]}"
    if [ -n "${FAKE_RCLONE_GARBLE:-}" ] && [ "${f##*/}" = "$FAKE_RCLONE_GARBLE" ]; then
      { tail -c +$((offset + 1)) "$f"; printf 'x'; }
    else
      tail -c +$((offset + 1)) "$f"
    fi
    ;;
  lsf)
    [ ${#paths[@]} = 1 ] || { echo "fake-rclone: lsf wants one path" >&2; exit 2; }
    [ "$format" = s ] || { echo "fake-rclone: lsf supports --format s only" >&2; exit 2; }
    f=$(local_path "${paths[0]}")
    [ -f "$f" ] || missing "${paths[0]}"
    wc -c <"$f" | tr -d ' '
    ;;
  *)
    echo "fake-rclone: $sub is not faked" >&2
    exit 2
    ;;
esac
