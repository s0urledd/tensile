#!/usr/bin/env bash
#
# fake-rclone: the rclone subcommands the deploy scripts use, over local
# files, for their tests (no network, no rclone). A remote path
# "<remote>:<path>", the on-the-fly ":sftp,host=...,...:<path>" included, is
# $FAKE_RCLONE_ROOT/<path>, the part after the last ':'; a path without ':'
# is a local path, as rclone takes it. Every flag is accepted and ignored but
# cat's --offset, lsf's --format and copy's --include and --exclude.
#
#   copy <src> <dst> [flags]    every file under src that the filters pass
#                               into dst
#   cat [--offset N] <path>     the file from byte N
#   lsf --format s <path>       the file's size
#
# The filters are rclone's: a pattern matches the end of the path relative
# to src (from its root when it starts with '/'), '*' and '?' stop at a '/'
# and '**' does not; the includes are tried first, then the excludes, in the
# order given, and the first that matches decides; a file none matches is
# copied only when there is no include. rclone adds the excludes after the
# includes whatever their order on the command line, as here.
#
# A path that does not exist is rclone's "directory not found", exit 3, with
# the path in the message, as rclone prints it.
#
#   FAKE_RCLONE_LOG     one line per call: the subcommand and its arguments
#   FAKE_RCLONE_FAIL    a subcommand that fails (exit 1) whatever it is asked
#   FAKE_RCLONE_GARBLE  a file name whose bytes cat serves altered (a remote
#                       copy that is not the one uploaded)
#   FAKE_RCLONE_SLOW    seconds cat waits before it serves anything (a slow
#                       link, so that two callers overlap)
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
paths=() includes=() excludes=()
while [ $# -gt 0 ]; do
  case $1 in
    --offset) offset=$2; shift 2 ;;
    --offset=*) offset=${1#--offset=}; shift ;;
    --format) format=$2; shift 2 ;;
    --format=*) format=${1#--format=}; shift ;;
    --include) includes+=("$2"); shift 2 ;;
    --include=*) includes+=("${1#--include=}"); shift ;;
    --exclude) excludes+=("$2"); shift 2 ;;
    --exclude=*) excludes+=("${1#--exclude=}"); shift ;;
    # flags that take a value, so the value is not taken for a path
    --transfers|--checkers|--stats|--log-level|--contimeout|--timeout|--config|--count) shift 2 ;;
    -*) shift ;;
    *) paths+=("$1"); shift ;;
  esac
done
missing() { echo "fake-rclone: error: $1: directory not found" >&2; exit 3; }
# glob_re <pattern>: an rclone filter pattern as an extended regular
# expression over a path relative to the copy's source.
glob_re() {
  local p=$1 re="" c anchored=0
  case $p in /*) anchored=1; p=${p#/} ;; esac
  while [ -n "$p" ]; do
    case $p in
      '**'*) re+='.*'; p=${p#'**'} ;;
      '*'*) re+='[^/]*'; p=${p#'*'} ;;
      '?'*) re+='[^/]'; p=${p#'?'} ;;
      *)
        c=${p:0:1}; p=${p:1}
        case $c in
          '.'|'['|']'|'('|')'|'{'|'}'|'^'|'$'|'|'|'+'|'\') re+="\\$c" ;;
          *) re+=$c ;;
        esac
        ;;
    esac
  done
  if [ "$anchored" = 1 ]; then printf '^%s$' "$re"; else printf '(^|/)%s$' "$re"; fi
}
# passes <path>: whether the filters let the file at <path> (relative to
# the source) be copied.
passes() {
  local p re
  for p in ${includes[@]+"${includes[@]}"}; do re=$(glob_re "$p"); [[ $1 =~ $re ]] && return 0; done
  for p in ${excludes[@]+"${excludes[@]}"}; do re=$(glob_re "$p"); [[ $1 =~ $re ]] && return 1; done
  [ ${#includes[@]} = 0 ]
}
case $sub in
  copy)
    [ ${#paths[@]} = 2 ] || { echo "fake-rclone: copy wants <src> <dst>" >&2; exit 2; }
    src=$(local_path "${paths[0]}"); dst=$(local_path "${paths[1]}")
    [ -d "$src" ] || missing "${paths[0]}"
    mkdir -p "$dst" || exit 1
    while IFS= read -r f; do
      f=${f#./}
      passes "$f" || continue
      mkdir -p "$dst/$(dirname "$f")" && cp -p "$src/$f" "$dst/$f" || exit 1
    done < <(cd "$src" && find . -type f)
    ;;
  cat)
    [ ${#paths[@]} = 1 ] || { echo "fake-rclone: cat wants one path" >&2; exit 2; }
    f=$(local_path "${paths[0]}")
    [ -f "$f" ] || missing "${paths[0]}"
    [ -n "${FAKE_RCLONE_SLOW:-}" ] && sleep "$FAKE_RCLONE_SLOW"
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
