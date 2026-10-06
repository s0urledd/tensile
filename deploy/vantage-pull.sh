#!/bin/sh
# vantage-pull.sh <network> — fetch every second vantage's heartbeats.
#
# A second vantage is observer-heartbeat run on another host under its own
# -vantage name (deploy/README.md, "Second vantage"). Once a minute, from the
# timer, for each vantage: append to <DATA_DIR>/vantages/<name>/reachability.jsonl
# the bytes that vantage/<name>/reachability.jsonl on that host gained since
# the last pull, where the collector ingests them.
#
# The local file holds the remote one's bytes at the same offsets, and
# observer-archive rotates it like the observer's own record files: its
# older lines move into vantages/<name>/archive/, and may later be retired
# to the daily exports. The local copy's length is therefore not its size
# but its logical end, the live file's base plus its size, which
# observer-archive -logical-end reads from the archive index; the pull
# fetches from that offset (rclone cat --offset) over the sftp account.
#
# It holds the local file's shared flock from reading the end to the last
# byte appended, as every writer of a record file does
# (internal/record.Appender): a rotation swaps the file only under the
# exclusive lock, so the end cannot move under a pull, and a pull that finds
# the path renamed over the file it locked opens the new one.
#
# Both files are append-only. A remote file shorter than the local logical
# end was replaced or cut on the vantage: the pull says so and fetches
# nothing, since appending from a lower offset would put other bytes at
# offsets the record already holds.
#
# Environment (the network's env file):
#   VANTAGE_PULL_HOST    user@host of an sftp-only account on that server
#   VANTAGE_PULL_NAMES   space-separated vantage names; each is read from
#                        vantage/<name>/ under that account
#   VANTAGE_PULL_KEY     ssh key (default /etc/fibre-observer/backup_ed25519)
#   VANTAGE_PULL_KNOWN   known_hosts (default /etc/fibre-observer/backup_known_hosts)
#   VANTAGE_PULL_SOURCE  a local directory read in place of the account (tests)
#   RCLONE               the rclone binary (default rclone; the tests put a fake here)
#   OBSERVER_ARCHIVE     observer-archive (default /usr/local/bin/observer-archive)
set -eu
net=${1:?usage: vantage-pull.sh <network>}
host=${VANTAGE_PULL_HOST:-}
names=${VANTAGE_PULL_NAMES:-}
source_dir=${VANTAGE_PULL_SOURCE:-}
if [ -z "$names" ] || { [ -z "$host" ] && [ -z "$source_dir" ]; }; then
	echo "vantage-pull[$net]: VANTAGE_PULL_HOST / VANTAGE_PULL_NAMES not set; nothing to fetch"
	exit 0
fi
key=${VANTAGE_PULL_KEY:-/etc/fibre-observer/backup_ed25519}
known=${VANTAGE_PULL_KNOWN:-/etc/fibre-observer/backup_known_hosts}
data=${DATA_DIR:?DATA_DIR not set}
rclone=${RCLONE:-rclone}
archive=${OBSERVER_ARCHIVE:-/usr/local/bin/observer-archive}
# -q: errors only (rclone otherwise says, every minute, that it has no
# config file, which it does not need: the remote is given whole below).
# The timeouts are the old sftp session's: a host that does not answer
# fails the pull, and the next minute tries again.
flags="-q --contimeout 20s --timeout 60s"

remote=""
if [ -z "$source_dir" ]; then
	case $host in
		?*@?*) ;;
		*) echo "vantage-pull[$net]: VANTAGE_PULL_HOST must be user@host" >&2; exit 1 ;;
	esac
	user=${host%%@*}
	h=${host#*@}
	# The values go into rclone's connection string, whose parameters are
	# separated by ',' and which ends at ':'.
	for v in "$user" "$h" "$key" "$known"; do
		case $v in
			*[,:\"\']*) echo "vantage-pull[$net]: '$v' cannot be written into an rclone connection string" >&2; exit 1 ;;
		esac
	done
	remote=":sftp,host=$h,user=$user,key_file=$key,known_hosts_file=$known:"
fi

# pull <name>: one vantage, under the shared lock of its local file. Every
# step is checked here: the caller's `if` turns errexit off inside.
pull() {
	n=$1
	rel="vantages/$n/reachability.jsonl"
	file="$data/$rel"
	if [ -n "$source_dir" ]; then
		src="$source_dir/vantage/$n/reachability.jsonl"
	else
		src="${remote}vantage/$n/reachability.jsonl"
	fi
	mkdir -p "$data/vantages/$n" || return 1
	tries=0
	while :; do
		exec 9>>"$file"
		if ! flock -s -w 60 9; then
			echo "vantage-pull[$net]: $n: the local file stayed locked for a minute" >&2
			exec 9>&-
			return 1
		fi
		[ "$file" -ef /dev/fd/9 ] && break
		# A rotation came between the open and the lock: the path names a
		# new file, which is opened again.
		exec 9>&-
		tries=$((tries + 1))
		if [ "$tries" -ge 5 ]; then
			echo "vantage-pull[$net]: $n: the local file was replaced five times while the pull waited for it" >&2
			return 1
		fi
	done
	if ! end=$("$archive" -data-dir "$data" -logical-end "$rel"); then
		echo "vantage-pull[$net]: $n: the local file's logical end is not known; fetched nothing" >&2
		exec 9>&-
		return 1
	fi
	# $flags is split into its words on purpose.
	# shellcheck disable=SC2086
	if ! size=$("$rclone" lsf --format s $flags "$src"); then
		echo "vantage-pull[$net]: $n: fetch failed" >&2
		exec 9>&-
		return 1
	fi
	for x in "$end" "$size"; do
		case $x in
			''|*[!0-9]*)
				echo "vantage-pull[$net]: $n: unexpected sizes (local end '$end', remote '$size'); fetched nothing" >&2
				exec 9>&-
				return 1
				;;
		esac
	done
	if [ "$size" -lt "$end" ]; then
		echo "vantage-pull[$net]: $n: the vantage's file is $size bytes, shorter than the $end this record holds of it; fetched nothing (it was replaced or cut on the vantage)" >&2
		exec 9>&-
		return 1
	fi
	if [ "$size" -gt "$end" ]; then
		# shellcheck disable=SC2086
		if ! "$rclone" cat --offset "$end" $flags "$src" >&9; then
			# Whatever arrived before the failure is the remote's bytes at
			# their offsets: the next pull goes on from there.
			echo "vantage-pull[$net]: $n: fetch failed" >&2
			exec 9>&-
			return 1
		fi
	fi
	exec 9>&-
}

rc=0
for n in $names; do
	case $n in *[!a-z0-9-]*|"") echo "vantage-pull[$net]: bad vantage name '$n'" >&2; rc=1; continue ;; esac
	if ! pull "$n"; then
		rc=1
	fi
done
exit $rc
