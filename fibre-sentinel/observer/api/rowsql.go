package api

import (
	"strconv"

	"github.com/plsgiveup/fibre/fibre-sentinel/observer/rollup"
)

// The statements the validator list reads its row figures with, one per
// figure, over the rows started in [?1, ?2] (the window's start and end).
// filter is appended to the WHERE clause (a validator, or nothing); its
// argument goes last. They are written here rather than inline in
// validatorRows so that the day partials (dayparts.go) can run the very
// same text over a day's bounds instead of the window's, and the definition
// digest of the kept partials can name it.

// valClassesSQL is the effective-class tally of the assigned in-window rows,
// per validator.
func valClassesSQL(filter string) string {
	cls := rollup.EffectiveClass("")
	return `SELECT validator_address, ` + cls + `, COUNT(*) FROM probe_rows
		WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'` + filter + `
		GROUP BY validator_address, ` + cls
}

// latencyPopSQL is the population the service-time and transfer-rate
// figures are drawn from: the assigned in-window readings that came back
// HEALTHY (the stored class, not the effective one) with a duration.
const latencyPopSQL = `started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'
			  AND classification = 'HEALTHY' AND total_duration_ms > 0`

// valLatencySQL ranks the population per validator by duration and by
// transfer rate and reads the median and the 95th percentile of the first
// and the median of the second (see validatorRows). Its first three
// arguments are throughputMinBytes; the window's bounds follow.
func valLatencySQL(filter string) string {
	return `SELECT validator_address,
			MAX(CASE WHEN rn = (c + 1) / 2          THEN ms END),
			MAX(CASE WHEN rn = (c * 95 + 99) / 100  THEN ms END),
			MAX(c),
			MAX(CASE WHEN rb = (cb + 1) / 2         THEN bps END),
			MAX(cb)
		FROM (
			SELECT validator_address AS validator_address,
			       total_duration_ms AS ms,
			       CASE WHEN bytes_returned >= ? AND download_ms > 0 THEN bytes_returned * 1000 / download_ms END AS bps,
			       ROW_NUMBER() OVER (PARTITION BY validator_address ORDER BY total_duration_ms) AS rn,
			       COUNT(*)     OVER (PARTITION BY validator_address)                            AS c,
			       ROW_NUMBER() OVER (PARTITION BY validator_address
			                          ORDER BY (bytes_returned IS NULL OR bytes_returned < ? OR download_ms <= 0), bytes_returned * 1000.0 / NULLIF(download_ms, 0)) AS rb,
			       SUM(CASE WHEN bytes_returned >= ? AND download_ms > 0 THEN 1 ELSE 0 END)
			                    OVER (PARTITION BY validator_address)                            AS cb
			FROM probes
			WHERE ` + latencyPopSQL + filter + `
		) GROUP BY validator_address`
}

// valAttestationSQL is the proven, unproven and unknown (validator, blob)
// pairs of the assigned in-window rows, per validator: one per pair, by
// MAX(attested) over its rows in the window.
func valAttestationSQL(filter string) string {
	return `SELECT validator_address,
			COALESCE(SUM(CASE WHEN a = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a IS NULL THEN 1 ELSE 0 END), 0)
		FROM (
			SELECT validator_address AS validator_address, MAX(attested) AS a
			FROM probe_rows WHERE started_at >= ? AND started_at <= ? AND assigned = 1 AND phase = 'in_window'` + filter + `
			GROUP BY validator_address, promise_hash
		) GROUP BY validator_address`
}

// valSeenSQL is every reading row per validator, the newest one's start and
// the newest whose effective class is HEALTHY.
func valSeenSQL(filter string) string {
	return `SELECT validator_address, COUNT(*), MAX(started_at),
			MAX(CASE WHEN ` + rollup.EffectiveClass("") + ` = 'HEALTHY' THEN started_at END)
		FROM probe_rows
		WHERE started_at >= ? AND started_at <= ?` + filter + ` GROUP BY validator_address`
}

// valBeatsSQL is this observer's own heartbeats per validator (the third
// argument is its vantage): how many, how many completed TLS, how many of
// those presented an endorsed certificate, and the newest of each kind.
func valBeatsSQL(filter string) string {
	return `SELECT validator_address, COUNT(*),
			COALESCE(SUM(CASE WHEN tcp_ok = 1 AND tls_ok = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN tcp_ok = 1 AND tls_ok = 1 AND identity_ok = 1 THEN 1 ELSE 0 END), 0),
			MAX(CASE WHEN tcp_ok = 1 AND tls_ok = 1 THEN NULL ELSE started_at END),
			MAX(CASE WHEN tcp_ok = 1 AND tls_ok = 1 THEN started_at END)
		FROM reachability WHERE started_at >= ? AND started_at <= ? AND outcome <> 'PROBE_ERROR' AND +vantage = ?` + filter + `
		GROUP BY validator_address`
}

// The statements the day partials add beside those: the same populations,
// read as what a merge needs rather than as the figure itself.

// valGapsSQL is the gap rows (NOT_PROBED, PROBE_ERROR) per validator and
// outcome: the network's probe_gaps and probe_gaps_by_outcome, split by
// validator.
func valGapsSQL(filter string) string {
	return `SELECT validator_address, outcome, COUNT(*) FROM probe_rows
		WHERE started_at >= ? AND started_at <= ? AND classification IN ('NOT_PROBED','PROBE_ERROR')` + filter + `
		GROUP BY validator_address, outcome`
}

// valLatencyHistSQL is the service-time population as a histogram per
// validator: how many readings took each whole number of milliseconds.
// The rank the figures read depends only on these counts.
func valLatencyHistSQL(filter string) string {
	return `SELECT validator_address, total_duration_ms, COUNT(*) FROM probes
		WHERE ` + latencyPopSQL + filter + `
		GROUP BY validator_address, total_duration_ms`
}

// valThroughputHistSQL is the transfer-rate population as a histogram per
// validator: how many readings of a large enough shard moved each whole
// number of bytes per second (the figure published), over a shard of at
// least throughputMinBytes.
func valThroughputHistSQL(filter string) string {
	return `SELECT validator_address, bytes_returned * 1000 / download_ms, COUNT(*) FROM probes
		WHERE bytes_returned >= ` + strconv.Itoa(throughputMinBytes) + ` AND download_ms > 0 AND ` + latencyPopSQL + filter + `
		GROUP BY validator_address, bytes_returned * 1000 / download_ms`
}
