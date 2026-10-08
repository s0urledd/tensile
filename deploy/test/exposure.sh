#!/usr/bin/env bash
#
# exposure: what the outside can see, and whether the host comes back on
# its own.
#
#   - listeners: every socket bound to a non-loopback address must be one of
#     ssh, http, https (ALLOWED_PORTS to widen). The API listens on
#     127.0.0.1 and reaches the outside only through Caddy; the database is
#     a file; there is no admin port. Anything else is a mistake.
#   - units: the five services and two timers are enabled, so a reboot
#     brings them back without a hand on the box. (The reboot itself is not
#     done here; do it once, then run this script again.)
#   - HTTPS: the Caddyfile validates and https://$DOMAIN/api/v1/health
#     answers 200 or 503 — either proves the proxy, the certificate and the
#     API are wired; 000 means one of them is not.
#   - alerts: fibre-healthwatch --test posts a real message to every
#     destination the env file sets (ALERT_WEBHOOK, and the Telegram chat
#     with TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID), run as the service user
#     with the instance's EnvironmentFile exactly as the timer runs it.
#     Delivery is the point of an alert; a destination that was never
#     exercised is a guess. Neither set fails: nobody would be told. No
#     webhook or token is ever printed.
#   - secrets: the env file, rclone.conf and litestream's env file hold the
#     alert and backup credentials, and no other account on the host may
#     read them (0640 root:fibre-observer; systemd reads EnvironmentFile= as
#     root).
#
# Usage: sudo deploy/test/exposure.sh [instance]   (default mocha)
# Reads /etc/fibre-observer/<instance>.env. Exit 0 when every check passes.
set -o errexit -o nounset -o pipefail
# shellcheck source=lib.sh
. "$(dirname "$0")/lib.sh"

INSTANCE="${1:-mocha}"
ENVFILE=/etc/fibre-observer/$INSTANCE.env
ALLOWED="${ALLOWED_PORTS:-22 80 443}"
SERVICE_USER=fibre-observer

[ -r "$ENVFILE" ] || { echo "no $ENVFILE" >&2; exit 2; }
API_LISTEN=$(envval "$ENVFILE" API_LISTEN)
DOMAIN=$(envval "$ENVFILE" DOMAIN)
DATA_DIR=$(envval "$ENVFILE" DATA_DIR)

echo "== listeners reachable from outside"
exposed=0
while read -r addr; do
  [ -n "$addr" ] || continue
  host="${addr%:*}"; port="${addr##*:}"
  case "$host" in 127.*|"[::1]"|"::1"|"[::ffff:127."*) continue ;; esac
  allowed=0
  for p in $ALLOWED; do [ "$port" = "$p" ] && allowed=1; done
  if [ "$allowed" = 1 ]; then pass "port $port on $host (allowed)"; else fail "port $port on $host is reachable from outside"; exposed=1; fi
done < <(ss -H -ltn 2>/dev/null | awk '{print $4}')
[ "$exposed" = 0 ] && pass "no unexpected listener"
case "$API_LISTEN" in
  127.0.0.1:*|localhost:*|"[::1]":*) pass "API_LISTEN=$API_LISTEN is loopback" ;;
  *) fail "API_LISTEN=$API_LISTEN is not loopback: the API is meant to be reached through Caddy only" ;;
esac

echo "== units come back after a reboot"
for u in fibre-scan@$INSTANCE fibre-probe@$INSTANCE fibre-heartbeat@$INSTANCE fibre-collector@$INSTANCE fibre-api@$INSTANCE \
         fibre-healthwatch@$INSTANCE.timer fibre-backup@$INSTANCE.timer; do
  en=$(systemctl is-enabled "$u" 2>/dev/null || true)
  ac=$(systemctl is-active "$u" 2>/dev/null || true)
  if [ "$en" = "enabled" ]; then pass "$u enabled, $ac"; else fail "$u is '${en:-absent}' (want enabled): it will not start after a reboot"; fi
done
if command -v caddy >/dev/null 2>&1; then
  en=$(systemctl is-enabled caddy 2>/dev/null || true)
  [ "$en" = "enabled" ] && pass "caddy enabled" || fail "caddy is '${en:-absent}'"
fi

echo "== HTTPS through the proxy"
if command -v caddy >/dev/null 2>&1 && [ -r /etc/caddy/Caddyfile ]; then
  if out=$(caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile 2>&1); then pass "/etc/caddy/Caddyfile validates"; else fail "Caddyfile: $out"; fi
else
  warn "caddy not installed or no /etc/caddy/Caddyfile: the site is not served publicly from this host"
fi
if [ -n "$DOMAIN" ] && [ "$DOMAIN" != "observer.example.org" ]; then
  code=$(HTTP_TIMEOUT=20 http_code "https://$DOMAIN/api/v1/health")
  case "$code" in
    200|503) pass "https://$DOMAIN/api/v1/health -> $code (TLS, proxy and API are wired)" ;;
    *) fail "https://$DOMAIN/api/v1/health -> $code" ;;
  esac
  hsts=$(curl -sS -m 20 -I "https://$DOMAIN/" 2>/dev/null | grep -i -c '^strict-transport-security' || true)
  [ "${hsts:-0}" -ge 1 ] && pass "HSTS header present" || warn "no Strict-Transport-Security header on https://$DOMAIN/"
else
  warn "DOMAIN is unset or the example value; HTTPS not checked"
fi

echo "== alert delivery"
dests=$(alert_destinations "$ENVFILE")
if [ -z "$dests" ]; then
  fail "neither ALERT_WEBHOOK nor TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are set: healthwatch only logs; nobody is told when the observer breaks"
elif [ -x /usr/local/bin/fibre-healthwatch ]; then
  if run_as_service "$ENVFILE" "$SERVICE_USER" /usr/local/bin/fibre-healthwatch "$INSTANCE" --test; then
    pass "test alert delivered to every configured destination ($dests) as $SERVICE_USER with $ENVFILE; check that it arrived where a human looks"
  else
    fail "test alert not delivered to every configured destination ($dests); healthwatch said which refused it, above"
  fi
else
  fail "/usr/local/bin/fibre-healthwatch missing"
fi

echo "== secrets readable by the service only"
for f in "$ENVFILE" /etc/fibre-observer/rclone.conf "/etc/fibre-observer/litestream-$INSTANCE.env"; do
  [ -e "$f" ] || continue
  if m=$(private_file "$f"); then
    pass "$f is $m"
  else
    fail "$f is ${m:-unreadable}: other accounts on the host can read it (sudo chown root:$SERVICE_USER $f; sudo chmod 0640 $f)"
  fi
done

echo "== the master key stays on the host"
if [ -n "$DATA_DIR" ] && [ -f "$DATA_DIR/sampling-master.key" ]; then
  mode=$(stat -c '%a %U' "$DATA_DIR/sampling-master.key")
  case "$mode" in "600 $SERVICE_USER") pass "sampling-master.key is 600 $SERVICE_USER" ;; *) fail "sampling-master.key is $mode (want 600 $SERVICE_USER)" ;; esac
else
  pass "no sampling-master.key under $DATA_DIR (none is needed with POLICY empty)"
fi

echo
if [ "$FAILED" = 0 ]; then echo "exposure: every check passed"; else echo "exposure: FAILED"; exit 1; fi
