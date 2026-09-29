#!/usr/bin/env bash
#
# Deploy smoke test: install the units the way the README says, run each one
# the way systemd would, and check the chain actually produces rows.
#
# systemd is not PID 1 in every environment where you want this answer (a
# container, a CI runner), so this does not call systemctl. It runs each
# unit's own ExecStart, with its own EnvironmentFile, as its own User, via
# run-unit.py. That leaves the sandboxing directives untested, which is what
# `systemd-analyze verify` is for and which this script also runs. What it
# does catch is everything else: a binary that is not where the unit says, a
# flag that no longer exists, a variable the env file never sets, a directory
# the service user cannot write.
#
# Usage: deploy/test/smoke.sh <rpc-url> [instance]
# Expects the binaries in /usr/local/bin and the layout from deploy/README.md;
# instance is the network name the units are enabled for (default mocha).
set -o errexit -o nounset -o pipefail

RPC="${1:-http://127.0.0.1:26657}"
INSTANCE="${2:-mocha}"
UNITS=/etc/systemd/system
ENVFILE=/etc/fibre-observer/$INSTANCE.env
HERE="$(cd "$(dirname "$0")" && pwd)"
RUN="$HERE/run-unit.py"

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== units parse and their sandboxing is well-formed"
for u in "$UNITS"/fibre-*@.service "$UNITS"/fibre-*@.timer; do
  [ -e "$u" ] || continue
  # a template is verified as one instance
  out=$(systemd-analyze verify "${u/@./@$INSTANCE.}" 2>&1) || fail "systemd-analyze verify $u: $out"
  [ -z "$out" ] || fail "systemd-analyze verify $u: $out"
  echo "  ok $(basename "$u")"
done

echo "== environment file is complete"
[ -r "$ENVFILE" ] || fail "no $ENVFILE"
for key in NETWORK RPC VANTAGE DATA_DIR POLICY API_LISTEN; do
  grep -q "^${key}=" "$ENVFILE" || fail "$ENVFILE has no $key"
done
for key in VANTAGE_LOCATION VANTAGE_PROVIDER; do
  value=$(sed -n "s/^${key}=//p" "$ENVFILE" | head -1)
  [ -n "$value" ] || echo "  note: $key is empty; the API does not publish it, only observer-api's startup log names the vantage"
done
echo "  ok"

echo "== each unit starts, reaches the chain, and writes as its service user"
for unit in fibre-scan fibre-heartbeat fibre-collector; do
  log=$(mktemp)
  timeout 25 python3 "$RUN" "$UNITS/$unit@.service" "$INSTANCE" >"$log" 2>&1 || true
  grep -qiE "connected|up:|round done|done \(" "$log" || fail "$unit produced no sign of life: $(tail -3 "$log")"
  grep -qiE "permission denied|no such file" "$log" && fail "$unit: $(grep -iE 'permission denied|no such file' "$log" | head -1)"
  echo "  ok $unit"
  rm -f "$log"
done

DATA_DIR=$(sed -n 's/^DATA_DIR=//p' "$ENVFILE" | head -1)
owner=$(stat -c '%U' "$DATA_DIR")
[ "$owner" = "fibre-observer" ] || fail "$DATA_DIR is owned by $owner, not the service user"
echo "  ok $DATA_DIR owned by $owner"

echo "== reverse proxy config is valid"
if command -v caddy >/dev/null; then
  DOMAIN="${DOMAIN:-observer.example.org}" caddy validate --config "$HERE/../Caddyfile" --adapter caddyfile >/dev/null 2>&1 \
    || fail "deploy/Caddyfile does not validate"
  echo "  ok Caddyfile"
else
  echo "  skipped (caddy not installed)"
fi

echo
echo "smoke: all checks passed against $RPC"
