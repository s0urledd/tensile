// Package store is the observer's relational store: SQLite for the single-node
// MVP, with a schema kept inside the subset SQLite and Postgres share.
//
// The store never computes verdicts. It keeps the raw records the sentinel
// tools already produce (publications.jsonl, measurements.jsonl), plus the
// facts the dashboard needs that those files do not carry: endpoint history,
// observer run spans (for gaps), and ingest cursors.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/probe"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/status"
)

//go:embed schema.sql
var schemaSQL string

// SchemaVersion is the schema this binary expects. schema.sql is the frozen
// version-1 baseline; every later version is a numbered entry in migrations,
// applied in order. A fresh database therefore takes exactly the same path as
// an upgraded one — baseline, then every migration — so the two end up
// identical in shape and the migration code is exercised by every test run
// rather than only on upgrade day.
const SchemaVersion = 24

// migration is one numbered step above the baseline. The statements run in a
// single transaction: SQLite supports transactional DDL, so a failed step
// leaves the database at the previous version rather than half-migrated.
type migration struct {
	version int
	note    string
	stmts   []string
}

// migrations must stay append-only and in ascending order. Never edit a
// released entry: a database that already applied it will not re-run it.
var migrations = []migration{
	{
		version: 2,
		note:    "verified attestation: which validators a settled promise proves stored the blob",
		stmts: []string{
			// Nullable on purpose. A row written before this migration, or
			// ingested from a record whose schema_version predates the
			// attestation field, has no attestation evidence either way.
			// NULL is "unknown"; 0 would claim the validator did not attest,
			// which the record does not say.
			`ALTER TABLE assignments ADD COLUMN attested INTEGER`,
			`ALTER TABLE probes ADD COLUMN attested INTEGER`,
			`ALTER TABLE publications ADD COLUMN attested_with_rows INTEGER`,
			`ALTER TABLE publications ADD COLUMN attested_voting_power INTEGER`,
			`ALTER TABLE publications ADD COLUMN signature_entries INTEGER`,
			`ALTER TABLE publications ADD COLUMN signatures_verified INTEGER`,
			`ALTER TABLE publications ADD COLUMN signatures_unmatched INTEGER`,
			`ALTER TABLE publications ADD COLUMN signatures_out_of_position INTEGER`,
			`CREATE INDEX IF NOT EXISTS assignments_attested ON assignments (promise_hash, attested)`,
		},
	},
	{
		version: 3,
		note:    "endpoint rows say why they closed: the observer cannot tell deregistration from unbonding",
		stmts: []string{
			// The only signal behind a closure is that the (validator, host)
			// pair stopped appearing in AllBondedFibreProviders. That happens
			// on every jailing and every unbonding without the operator
			// touching its Fibre registration, and x/valaddr has no
			// deregistration message at all, so "closed" never meant
			// "deregistered". The column says what was actually observed.
			`ALTER TABLE endpoints ADD COLUMN closed_reason TEXT`,
			`UPDATE endpoints SET closed_reason = 'left_bonded_provider_list' WHERE closed_at IS NOT NULL AND closed_reason IS NULL`,
		},
	},
	{
		version: 4,
		note:    "validator identities from the staking module, so rows carry the name the operator chose",
		stmts: []string{
			// Read from the chain's own staking module, not from an explorer
			// API: an observer whose validator names come from somebody
			// else's index is that much less independent, and it inherits
			// that index's rate limits, terms and coverage gaps.
			//
			// The key is the 20-byte consensus address in lower-case hex,
			// the same identifier every probe row and assignment already
			// uses, so no join needs a bech32 conversion.
			`CREATE TABLE IF NOT EXISTS validator_identities (
				cons_address     TEXT PRIMARY KEY,
				operator_address TEXT NOT NULL DEFAULT '',
				moniker          TEXT NOT NULL DEFAULT '',
				identity         TEXT NOT NULL DEFAULT '',
				website          TEXT NOT NULL DEFAULT '',
				tokens           TEXT NOT NULL DEFAULT '',
				jailed           INTEGER NOT NULL DEFAULT 0,
				status           TEXT NOT NULL DEFAULT '',
				first_seen_at    TEXT NOT NULL,
				updated_at       TEXT NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS validator_identities_moniker ON validator_identities (moniker)`,
		},
	},
	{
		version: 5,
		note:    "covering index for the in-window window aggregates the network summary runs",
		stmts: []string{
			// Every rate on /v1/network is an aggregate over the same
			// population — probes of an assigned shard, in window, since a
			// timestamp — and there were five separate scans of it per
			// request with no index to seek by. Measured on a store with
			// 2,200 publications and 714,000 probes, /v1/network?window=7d
			// took 27.9s.
			//
			// The leading columns are the two equalities and the range, in
			// that order, so SQLite can seek instead of scanning the table.
			// The rest are there so it never has to: every column these
			// aggregates read is in the index, which is what turns a scan of
			// forty-column rows into a scan of the index alone. Measured on
			// the same store: attestation 1.63s to 0.20s, the per-point
			// breakdown 2.37s to 0.89s, the correlated-failure guard 2.41s
			// to 0.88s, the verdict tally 1.72s to 0.50s.
			//
			// It costs about 200 bytes a probe. `outcome` is deliberately not
			// in it: it is the widest column none of these queries reads, and
			// the one query that does read it seeks by promise_hash instead.
			`CREATE INDEX IF NOT EXISTS probes_window ON probes
				(assigned, phase, started_at, classification, schedule_label,
				 attested, validator_address, promise_hash, scheduled_at)`,
		},
	},
	{
		version: 6,
		note:    "the same covering index led by validator, for the per-validator page",
		stmts: []string{
			// probes_window above leads with the two equalities and the range,
			// which is right for the network aggregates and wrong for a page
			// about one validator: validator_address sits seventh, so a query
			// for one validator seeks to the start of the window and then walks
			// every in-window probe of every validator, discarding all but its
			// own. On an 85,000-probe store that made /v1/validators/{addr}
			// 1.4s, and it is the page an operator opens about themselves.
			//
			// This is the same column set led by validator_address. Measured on
			// that store: the per-validator class tally 13.2ms to 0.4ms and the
			// obligation rate 10.8ms to 0.9ms, with the detail page computing
			// eight of them (four windows, two queries each).
			//
			// probes_validator_time is kept: it orders by started_at directly,
			// which this one cannot, and the recent-probes list needs that.
			`CREATE INDEX IF NOT EXISTS probes_validator_window ON probes
				(validator_address, assigned, phase, started_at, classification,
				 schedule_label, attested, promise_hash, scheduled_at)`,
		},
	},
	{
		version: 7,
		note:    "the escrow side of x/fibre: who paid what, and which promises were abandoned",
		stmts: []string{
			// One row per escrow movement the chain recorded: a settlement
			// (MsgPayForFibre), a timeout (MsgPaymentPromiseTimeout, the
			// same charge, submitted by whoever held the abandoned promise),
			// a deposit, a withdrawal request and a withdrawal payout. The
			// amount of a settlement or timeout is not in any chain event; it
			// is recomputed from the promise's blob_size with the module's
			// own gas formula, which is exactly what the module charges.
			//
			// publisher is the account the module charged (derived from the
			// promise's signer key), whoever broadcast the transaction.
			// processor is that broadcaster: the publisher for a settlement,
			// anyone for a timeout.
			`CREATE TABLE IF NOT EXISTS payments (
				dedupe_key    TEXT PRIMARY KEY,
				kind          TEXT NOT NULL,
				height        INTEGER NOT NULL,
				time          TEXT NOT NULL,
				tx_hash       TEXT NOT NULL DEFAULT '',
				tx_index      INTEGER NOT NULL DEFAULT -1,
				msg_index     INTEGER NOT NULL DEFAULT 0,
				publisher     TEXT NOT NULL,
				processor     TEXT NOT NULL DEFAULT '',
				promise_hash  TEXT NOT NULL DEFAULT '',
				namespace     TEXT NOT NULL DEFAULT '',
				blob_size     INTEGER NOT NULL DEFAULT 0,
				gas_units     INTEGER NOT NULL DEFAULT 0,
				denom         TEXT NOT NULL DEFAULT '',
				amount_utia   INTEGER NOT NULL DEFAULT 0,
				available_at  TEXT,
				raw_json      TEXT NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS payments_time ON payments (time)`,
			`CREATE INDEX IF NOT EXISTS payments_publisher_time ON payments (publisher, time)`,
			`CREATE INDEX IF NOT EXISTS payments_kind_time ON payments (kind, time)`,
			`CREATE INDEX IF NOT EXISTS payments_promise ON payments (promise_hash)`,
			`CREATE INDEX IF NOT EXISTS payments_processor ON payments (processor, kind)`,
			// A publisher's escrow balance as the chain holds it, read by
			// state query (there is no list-all query, so only publishers
			// the payments table already knows are polled). available is
			// balance minus a pending withdrawal.
			`CREATE TABLE IF NOT EXISTS escrow_accounts (
				publisher      TEXT PRIMARY KEY,
				found          INTEGER NOT NULL DEFAULT 0,
				denom          TEXT NOT NULL DEFAULT '',
				balance_utia   INTEGER NOT NULL DEFAULT 0,
				available_utia INTEGER NOT NULL DEFAULT 0,
				height         INTEGER NOT NULL DEFAULT 0,
				updated_at     TEXT NOT NULL
			)`,
		},
	},
	{
		version: 8,
		note:    "bytes handed over per probe, so a transfer rate can be stated over the download alone",
		stmts: []string{
			// Throughput used to be rows per second over the whole probe:
			// dial, TLS, identity check, download, verification. The fixed
			// cost of the first three is amortised over a big shard and not
			// over a small one, so the figure rose with stake by
			// construction, and a row is as wide as its blob's square, so
			// rows/s was not comparable across blobs either. Bytes over the
			// download step alone answer both. Nullable: a record written
			// before the field existed says nothing about size.
			`ALTER TABLE probes ADD COLUMN bytes_returned INTEGER`,
		},
	},
	{
		version: 9,
		note:    "the evidence behind a verdict, on the row: returned row indices and digest, the gRPC code, the shadowing promise, and the code that judged it",
		stmts: []string{
			// A classification is a function of the wire result and the
			// code. Without the returned indices nobody can re-run the
			// assignment check behind a WRONG_ROWS or PARTIAL verdict;
			// without the digest an INVALID_ROWS claim is "we saw it";
			// without the gRPC code SERVER_ERROR and THROTTLED are a
			// substring match on free text; without the build and pin the
			// verdict cannot be traced to the code that made it. All
			// nullable: rows from before the fields existed say nothing.
			`ALTER TABLE probes ADD COLUMN row_indices TEXT`,
			`ALTER TABLE probes ADD COLUMN rows_sha256 TEXT`,
			`ALTER TABLE probes ADD COLUMN rpc_code TEXT`,
			`ALTER TABLE probes ADD COLUMN shadowed_by TEXT`,
			`ALTER TABLE probes ADD COLUMN observer_build TEXT`,
			`ALTER TABLE probes ADD COLUMN app_version INTEGER`,
		},
	},
	{
		version: 10,
		note:    "run records with their configuration, replayed from runs.jsonl; revealed sampling day secrets",
		stmts: []string{
			// A run used to be the collector's own row only, written straight
			// to the database and lost with it. Every component now appends
			// its starts and stops to runs.jsonl with the configuration it
			// ran under (status.RunEvent), and the collector replays them
			// here. pid and hostname make the natural key: the same
			// component can start twice in one second on two hosts.
			`ALTER TABLE observer_runs ADD COLUMN pid INTEGER`,
			`ALTER TABLE observer_runs ADD COLUMN hostname TEXT`,
			`ALTER TABLE observer_runs ADD COLUMN config_json TEXT`,
			`CREATE UNIQUE INDEX IF NOT EXISTS observer_runs_natural ON observer_runs (component, vantage, started_at, pid)`,
			// The prober reveals each day's sampling secret once the day is
			// old enough that every publication settled on it has left its
			// window; from then on the day's admission draws can be
			// recomputed by anyone (see /v1/sampling).
			`CREATE TABLE IF NOT EXISTS sampling_secrets (
				day         TEXT PRIMARY KEY,
				commitment  TEXT NOT NULL,
				secret      TEXT NOT NULL,
				revealed_at TEXT NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS sampling_secrets_commitment ON sampling_secrets (commitment)`,
		},
	},
	{
		version: 11,
		note:    "retention: the last fields the API read out of raw_json become columns; daily rollups of obligations and probe counts; raw_json may be dropped",
		stmts: []string{
			// raw_json is the bulk of a probe row and is dropped after the
			// raw-json retention (rollup.Prune); everything the API still
			// reads from it becomes a typed column, back-filled here.
			`ALTER TABLE probes ADD COLUMN sampling_p REAL`,
			`ALTER TABLE probes ADD COLUMN sampling_binding TEXT`,
			`ALTER TABLE probes ADD COLUMN sampling_commitment TEXT`,
			`ALTER TABLE probes ADD COLUMN retry_first_outcome TEXT`,
			`ALTER TABLE probes ADD COLUMN clock_offset_ms INTEGER`,
			`UPDATE probes SET
				sampling_p = json_extract(raw_json, '$.sampling.p'),
				sampling_binding = json_extract(raw_json, '$.sampling.binding'),
				sampling_commitment = json_extract(raw_json, '$.sampling.day_commitment'),
				retry_first_outcome = json_extract(raw_json, '$.retry.first_outcome'),
				clock_offset_ms = json_extract(raw_json, '$.clock_offset_ms')
			 WHERE raw_json <> '' AND json_valid(raw_json)`,
			`CREATE INDEX IF NOT EXISTS probes_sampling ON probes (sampling_commitment, started_at)`,
			// obligation_daily: one row per (settlement day, validator) with
			// the day's obligation buckets, computed once the day is final
			// (rollup.Run) exactly as the API computes them live. Beyond the
			// raw retention the "all" window rests on these.
			`CREATE TABLE IF NOT EXISTS obligation_daily (
				day                    TEXT NOT NULL,
				validator_address      TEXT NOT NULL,
				total                  INTEGER NOT NULL,
				served                 INTEGER NOT NULL,
				broken                 INTEGER NOT NULL,
				end_unobserved         INTEGER NOT NULL,
				unobserved_reachable   INTEGER NOT NULL,
				unobserved_unreachable INTEGER NOT NULL,
				unobserved_not_probed  INTEGER NOT NULL,
				pending                INTEGER NOT NULL,
				computed_at            TEXT NOT NULL,
				PRIMARY KEY (day, validator_address)
			)`,
			// probe_daily: one row per (start day, validator) with the row
			// counts the "all" window prints: every row, the in-window
			// assigned population by class (JSON), faults, gaps, and the
			// heartbeats.
			`CREATE TABLE IF NOT EXISTS probe_daily (
				day               TEXT NOT NULL,
				validator_address TEXT NOT NULL,
				probes            INTEGER NOT NULL,
				gaps              INTEGER NOT NULL,
				faults            INTEGER NOT NULL,
				classes_json      TEXT NOT NULL,
				beats             INTEGER NOT NULL,
				beats_up          INTEGER NOT NULL,
				computed_at       TEXT NOT NULL,
				PRIMARY KEY (day, validator_address)
			)`,
		},
	},
	{
		version: 12,
		note:    "deferred shadow verdicts: shadow_gap as a column, the verdict at probe time kept beside the amended one, and the amendment log",
		stmts: []string{
			// The Fibre store serves the first shard of a commitment by
			// promise-hash order, so a promise settling after a probe can own
			// the rows it returned; the prober defers such a verdict and the
			// collector judges it once the scanner has read past probe time +
			// payment_promise_timeout (LateShadowVerdicts). The row keeps the
			// verdict it was stamped with beside the amended one.
			`ALTER TABLE probes ADD COLUMN shadow_gap TEXT`,
			`ALTER TABLE probes ADD COLUMN classification_at_probe TEXT`,
			`ALTER TABLE probes ADD COLUMN amended_at TEXT`,
			`UPDATE probes SET shadow_gap = json_extract(raw_json, '$.download.shadow_gap')
			 WHERE raw_json <> '' AND json_valid(raw_json) AND json_extract(raw_json, '$.download.shadow_gap') IS NOT NULL`,
			`CREATE INDEX IF NOT EXISTS probes_deferred ON probes (classification, amended_at) WHERE shadow_gap IS NOT NULL`,
			`CREATE TABLE IF NOT EXISTS probe_amendments (
				dedupe_key          TEXT PRIMARY KEY REFERENCES probes(dedupe_key) ON DELETE CASCADE,
				from_classification TEXT NOT NULL,
				to_classification   TEXT NOT NULL,
				reason              TEXT NOT NULL,
				shadowed_by         TEXT,
				judged_at           TEXT NOT NULL,
				scanner_frontier    TEXT NOT NULL
			)`,
		},
	},
	{
		version: 13,
		note:    "the host registered at settlement on the obligation and on the row, and what it answered when the current host did not serve",
		stmts: []string{
			// NULL: the scanner could not read the registry at the settlement
			// height (or the record predates the field); '' : no host was
			// registered. The two are different facts.
			`ALTER TABLE assignments ADD COLUMN host_at_settlement TEXT`,
			`ALTER TABLE probes ADD COLUMN host_at_settlement TEXT`,
			`ALTER TABLE probes ADD COLUMN settlement_host_outcome TEXT`,
			`ALTER TABLE probes ADD COLUMN settlement_host_served INTEGER`,
		},
	},
	{
		version: 14,
		note:    "host_events: every Fibre host registration the scanner read from the chain's events, plus its seed; the source of host_at_settlement",
		stmts: []string{
			// Distinct from endpoints, which is the collector's own poll of
			// the bonded registry (the current registry, for the prober and
			// the heartbeat). This is the chain's history, replayed from
			// host_history.jsonl, and it is what a verifier derives
			// host_at_settlement from.
			`CREATE TABLE IF NOT EXISTS host_events (
				from_height   INTEGER NOT NULL,
				from_tx_index INTEGER NOT NULL,
				cons_address  TEXT NOT NULL,
				host          TEXT NOT NULL,
				source        TEXT NOT NULL,
				time          TEXT NOT NULL,
				PRIMARY KEY (cons_address, from_height, from_tx_index)
			)`,
		},
	},
	{
		version: 15,
		note:    "validator_avatars: the Keybase picture behind a validator's identity field, fetched once by the collector and served by the API",
		stmts: []string{
			// Keyed by the identity (key suffix) rather than the validator:
			// several validators can share one operator's Keybase, and the
			// picture belongs to the identity. status is ok (picture held),
			// none (Keybase knows no picture for it), or error (the last
			// attempt failed; retried after the max age like the others).
			`CREATE TABLE IF NOT EXISTS validator_avatars (
				identity     TEXT PRIMARY KEY,
				url          TEXT NOT NULL DEFAULT '',
				content_type TEXT NOT NULL DEFAULT '',
				data         BLOB,
				status       TEXT NOT NULL DEFAULT '',
				checked_at   TEXT NOT NULL
			)`,
		},
	},
	{
		version: 16,
		note:    "probe_daily.identity_up: the endorsed share of a rolled day's completed handshakes, so identity_rate_window survives the prune the way reachability_window does",
		stmts: []string{
			// identity_rate_window is endorsed handshakes over completed
			// ones, so its denominator is beats_up, already rolled. Only the
			// numerator was missing, and without it the figure quietly
			// narrowed to the unpruned days while the reachability figure
			// printed beside it kept covering everything — two spans, one
			// row, no way for a reader to tell.
			`ALTER TABLE probe_daily ADD COLUMN identity_up INTEGER NOT NULL DEFAULT 0`,
		},
	},
	{
		version: 17,
		note:    "probe_daily attestation split: the proven / unproven / unrecorded counts of a rolled day, so the response's own reconciliation identity still holds once the rollup is folded in",
		stmts: []string{
			// docs/verdicts.md tells a reader to check the answer against
			// itself: serve_rate_coverage.den equals attested + unattested +
			// unknown, and serve_rate_held_out.UNATTESTED equals
			// attestation.unattested_probes. The classes are folded in from
			// the rollup on the "all" window and the attestation counts were
			// not, so after the first prune both identities failed — on the
			// one window a reader is most likely to check.
			`ALTER TABLE probe_daily ADD COLUMN attested INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE probe_daily ADD COLUMN unattested INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE probe_daily ADD COLUMN unknown_att INTEGER NOT NULL DEFAULT 0`,
		},
	},
	{
		version: 18,
		note:    "indexes for the two public routes that had none: /v1/probes?at= and /v1/sampling; and the sampling index no query could use is replaced",
		stmts: []string{
			// ?at= filters on scheduled_at, which no index led with — and the
			// dashboard itself links to it, from every correlated-failure
			// point it publishes. A full scan behind a public link is an
			// amplifier: one cheap GET buys the table.
			`CREATE INDEX IF NOT EXISTS probes_scheduled ON probes (scheduled_at)`,
			// probes_sampling was (sampling_commitment, started_at). The one
			// query reading those columns constrains started_at, the second
			// column, and wraps the first in COALESCE, so the index could
			// never be used for it: pure cost, a b-tree write on every probe
			// insert and its share of the WAL. Replaced with one the handler
			// can seek and that covers every column it reads.
			`DROP INDEX IF EXISTS probes_sampling`,
			`CREATE INDEX IF NOT EXISTS probes_sampling_started ON probes
				(started_at, sampling_commitment, sampling_binding, sampling_p, classification, promise_hash)`,
		},
	},
	{
		version: 19,
		note:    "params uncertainty: the height ranges this observer cannot say which x/fibre params were in force over, the hold they place on the verdicts they cover, and the append-only log of every deadline and verdict a verification moved",
		stmts: []string{
			// The ranges. Everything but the resolution is written once;
			// the resolution latches from open to verified or unresolvable
			// and is never cleared, because a range that was closed cannot
			// become open again.
			`CREATE TABLE IF NOT EXISTS param_uncertainty (
				id                             TEXT PRIMARY KEY,
				chain_id                       TEXT NOT NULL,
				kind                           TEXT NOT NULL,
				from_height                    INTEGER NOT NULL,
				to_height                      INTEGER NOT NULL,
				effective_from_height          INTEGER NOT NULL DEFAULT 0,
				interval_start_known           INTEGER NOT NULL DEFAULT 1,
				direction                      TEXT NOT NULL DEFAULT '',
				window_before_s                INTEGER NOT NULL DEFAULT 0,
				window_after_s                 INTEGER NOT NULL DEFAULT 0,
				publications_affected          INTEGER NOT NULL DEFAULT 0,
				publications_affected_is_floor INTEGER NOT NULL DEFAULT 0,
				detected_at                    TEXT NOT NULL,
				to_time                        TEXT,
				last_error                     TEXT NOT NULL DEFAULT '',
				holds                          INTEGER NOT NULL DEFAULT 0,
				resolution                     TEXT NOT NULL DEFAULT '',
				resolved_at                    TEXT,
				resolve_method                 TEXT NOT NULL DEFAULT '',
				heights_read                   INTEGER NOT NULL DEFAULT 0,
				resolve_error                  TEXT NOT NULL DEFAULT '',
				raw_json                       TEXT NOT NULL
			)`,
			// The one query that matters runs every collector pass: which
			// ranges still hold, and which heights do they cover.
			`CREATE INDEX IF NOT EXISTS param_uncertainty_holding
				ON param_uncertainty (holds, from_height, to_height)`,

			// The correction log. No FOREIGN KEY to probes, deliberately:
			// probe_amendments references probes(dedupe_key) ON DELETE
			// CASCADE, so the retention prune destroys the log of the
			// amendments it once published. A record of what this observer
			// withdrew has to outlive the row it withdrew it from.
			`CREATE TABLE IF NOT EXISTS publication_corrections (
				promise_hash          TEXT NOT NULL,
				uncertainty_id        TEXT NOT NULL,
				from_must_serve_until TEXT NOT NULL,
				to_must_serve_until   TEXT NOT NULL,
				from_basis            TEXT NOT NULL,
				to_basis              TEXT NOT NULL,
				reason                TEXT NOT NULL,
				judged_at             TEXT NOT NULL,
				PRIMARY KEY (promise_hash, uncertainty_id)
			)`,
			`CREATE TABLE IF NOT EXISTS probe_corrections (
				dedupe_key            TEXT NOT NULL,
				uncertainty_id        TEXT NOT NULL,
				promise_hash          TEXT NOT NULL,
				validator_address     TEXT NOT NULL,
				scheduled_at          TEXT NOT NULL,
				from_phase            TEXT NOT NULL,
				to_phase              TEXT NOT NULL,
				from_classification   TEXT NOT NULL,
				to_classification     TEXT NOT NULL,
				from_must_serve_until TEXT NOT NULL,
				to_must_serve_until   TEXT NOT NULL,
				reason                TEXT NOT NULL,
				prune_tolerance_s     INTEGER NOT NULL DEFAULT 0,
				judged_at             TEXT NOT NULL,
				PRIMARY KEY (dedupe_key, uncertainty_id)
			)`,
			`CREATE INDEX IF NOT EXISTS probe_corrections_promise ON probe_corrections (promise_hash)`,

			// The verdict as it was stamped, kept beside the corrected one.
			// NULL means uncorrected, never "the same as".
			`ALTER TABLE publications ADD COLUMN must_serve_until_at_scan       TEXT`,
			`ALTER TABLE publications ADD COLUMN must_serve_until_basis_at_scan TEXT`,
			`ALTER TABLE publications ADD COLUMN corrected_at                   TEXT`,
			`ALTER TABLE probes       ADD COLUMN must_serve_until_at_probe      TEXT`,
			`ALTER TABLE probes       ADD COLUMN phase_at_probe                 TEXT`,
			`ALTER TABLE probes       ADD COLUMN corrected_at                   TEXT`,

			// The hold, denormalised onto the rows so every rate query is a
			// column test rather than a join against a range table. It is
			// derived state: SyncParamHolds recomputes it from
			// param_uncertainty every pass, in both directions, so it
			// cannot drift or stick.
			`ALTER TABLE publications ADD COLUMN retention_unverified INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE probes       ADD COLUMN retention_unverified INTEGER NOT NULL DEFAULT 0`,
			`CREATE INDEX IF NOT EXISTS probes_held ON probes (promise_hash) WHERE retention_unverified = 1`,

			// Publication.must_serve_until_ambiguous has been written to
			// publications.jsonl since the field was added and read by
			// nothing: no column, no ingest, no API. Shipping a second
			// uncertainty axis while the first stays invisible would be
			// worse than having one. Backfilled from raw_json, which the
			// retention pass strips after 30 days; older rows keep 0, which
			// understates rather than invents.
			// The ninth obligation bucket. Rolled days from before this
			// migration keep 0, which is honest: nothing was held then.
			`ALTER TABLE obligation_daily ADD COLUMN held_param_unverified INTEGER NOT NULL DEFAULT 0`,

			`ALTER TABLE publications ADD COLUMN must_serve_until_ambiguous INTEGER NOT NULL DEFAULT 0`,
			`UPDATE publications SET must_serve_until_ambiguous = 1
			 WHERE raw_json <> '' AND json_valid(raw_json)
			   AND json_extract(raw_json, '$.must_serve_until_ambiguous') = 1`,
		},
	},
	{
		version: 20,
		note:    "param_uncertainty.corrected_at: verifying a range is not the same fact as having applied its corrections, and conflating the two released the rows before anything re-graded them",
		stmts: []string{
			// Set when every deadline and verdict the range covers has been
			// re-derived. Until then the range withholds, whatever its
			// resolution says: reading every height tells the observer what
			// the deadline should have been, it does not move the deadlines
			// already stamped or re-grade the rows drawn against them.
			//
			// holds was previously written from the record alone, which made
			// it false the moment a range was verified — so the corrector,
			// which reads the holding set, never saw a verified range and
			// never ran, while the rows it would have corrected were already
			// released. The column is now derived from both facts.
			`ALTER TABLE param_uncertainty ADD COLUMN corrected_at TEXT`,
			`UPDATE param_uncertainty SET holds = 1
			 WHERE kind = 'silent_change' AND corrected_at IS NULL`,
		},
	},
	// Version 21's statements live beside the code that writes them, in
	// withdrawals.go (withdrawalQueueMigration): the withdrawal queue read
	// from state, and the block time each params value took effect at.
	withdrawalQueueMigration,
	{
		version: 22,
		note:    "indexes so the collector pass and the API's per-request queries cost what is new, not what is stored: the covering window indexes regain what migration 19 took from them, and every hot query that walked a whole table gets something to seek",
		stmts: []string{
			// Migration 19 made every published rate read EffectiveClass,
			// which reads retention_unverified and outcome. Neither is in
			// probes_window or probes_validator_window, so from that
			// migration on neither index covered the queries it was built
			// for: SQLite still sought by the leading columns but went back
			// to the table for every row in the window, which is exactly
			// the forty-column read migration 5 existed to avoid. Measured
			// on a 990,000-probe fixture: the 7-day network tally 0.67s to
			// 0.32s, the per-validator tally 0.91s to 0.51s.
			//
			// outcome was left out of migration 5 on purpose as "the widest
			// column none of these queries reads"; that is no longer true,
			// and it is a short enum, not a message. Same names, so every
			// comment and plan that talks about them stays right.
			`DROP INDEX IF EXISTS probes_window`,
			`CREATE INDEX IF NOT EXISTS probes_window ON probes
				(assigned, phase, started_at, classification, schedule_label,
				 attested, validator_address, promise_hash, scheduled_at,
				 retention_unverified, outcome)`,
			`DROP INDEX IF EXISTS probes_validator_window`,
			`CREATE INDEX IF NOT EXISTS probes_validator_window ON probes
				(validator_address, assigned, phase, started_at, classification,
				 schedule_label, attested, promise_hash, scheduled_at,
				 retention_unverified, outcome)`,

			// SyncParamHolds runs every collector pass, inside a write
			// transaction, and found its rows by scanning: probes for
			// retention_unverified = 0, publications for = 1. Held rows are
			// the rare case and the one every statement starts from, so
			// both tables get a partial index on it (probes_held already
			// exists) and the statements are rewritten to start from them
			// (store/uncertainty.go). Partial, so an unheld row — nearly
			// all of them — costs nothing to write.
			`CREATE INDEX IF NOT EXISTS publications_held ON publications (promise_hash) WHERE retention_unverified = 1`,
			// The other reason a row is held is a deadline that disagrees
			// with its publication's, and a publication's deadline only
			// ever moves through ApplyPublicationCorrection, which stamps
			// corrected_at. So the publications a stale row can belong to
			// are exactly these: a handful, and the only ones the sync and
			// the corrector's sweep need to look under.
			`CREATE INDEX IF NOT EXISTS publications_corrected ON publications (promise_hash) WHERE corrected_at IS NOT NULL`,
			// /v1/meta and /v1/health count unassignable publications on
			// every call; nothing indexed assignment_error, so each call
			// read every publication. Partial, and led by
			// settlement_height so the recent-only count seeks by height
			// too.
			`CREATE INDEX IF NOT EXISTS publications_unassignable ON publications (settlement_height) WHERE assignment_error <> ''`,

			// "Latest reachability answer per validator" is MAX(rowid)
			// among the rows that carry one, and the rows that do not are
			// excluded by outcome, which no index held: the validators
			// table walked every probe and every heartbeat through the
			// table to find ~80 rows. An index on validator_address alone
			// keeps each validator's entries in rowid order, so the newest
			// qualifying row is the last entry — one seek per validator —
			// and partial on the same outcome test the query applies, so
			// the walk never meets a row it would discard. See
			// reachabilityNow.
			`CREATE INDEX IF NOT EXISTS probes_latest_answer ON probes (validator_address)
				WHERE outcome NOT IN ('MISSED','PROBE_ERROR')`,
			`CREATE INDEX IF NOT EXISTS reachability_latest_answer ON reachability (validator_address)
				WHERE outcome <> 'PROBE_ERROR'`,

			// How many vantages wrote to this store is a UNION of both
			// tables' vantage column, which nothing indexed: /v1/meta
			// re-ran it whenever either table had grown, which under
			// traffic is every collector pass. With these the distinct
			// values are a loose index scan, one seek per vantage (there
			// are one or two), however many rows each wrote. The column is
			// a short label, so each entry is a few bytes plus the rowid.
			`CREATE INDEX IF NOT EXISTS probes_vantage ON probes (vantage)`,
			`CREATE INDEX IF NOT EXISTS reachability_vantage ON reachability (vantage)`,

			// "Voting power and rows from the newest publication each
			// validator appears in" joined every assignment to its
			// publication to find one height per validator. Assignments
			// are never pruned and grow by one per validator per blob, so
			// that query grew without bound. The settlement height is
			// copied onto the assignment (it never changes once written:
			// nothing updates publications.settlement_height), and the
			// index below makes the newest height per validator a single
			// seek. It replaces assignments_validator, whose every use is
			// served by the new index's leading column.
			//
			// Nullable, not NOT NULL DEFAULT 0: an assignment whose
			// publication is missing has no height, and 0 would claim one.
			`ALTER TABLE assignments ADD COLUMN settlement_height INTEGER`,
			`UPDATE assignments SET settlement_height =
				(SELECT p.settlement_height FROM publications p WHERE p.promise_hash = assignments.promise_hash)`,
			`DROP INDEX IF EXISTS assignments_validator`,
			`CREATE INDEX IF NOT EXISTS assignments_validator_height ON assignments (validator_address, settlement_height)`,
		},
	},
	// confirm.go (confirmMigration): other vantages' answers to this
	// observer's faults; read by nothing any more.
	confirmMigration,
	// sampledout.go (sampledOutMigration): a publication the sampler drew
	// out is one decision, not a NOT_PROBED row per validator per point.
	sampledOutMigration,
}

// Store wraps one SQLite database.
type Store struct {
	db *sql.DB
	// counts keeps Count's running totals: see Count.
	counts rowCounts
}

// Open opens or creates the SQLite database at path, applies the pragmas the
// observer relies on (WAL, busy timeout) and the schema. ":memory:" is
// accepted for tests.
func Open(path string) (*Store, error) {
	dsn := path
	if path != ":memory:" {
		// auto_vacuum(incremental) has to be set before the first table is
		// created; on an existing file it takes a full VACUUM, which is why
		// it is here and not in a migration. Without it the retention pass
		// deletes rows and returns nothing to the operating system: the
		// pages go on SQLite's free list and observer.db never shrinks, so
		// the pruning deploy/README.md presents as the answer to a full disk
		// reclaims none of it. Incremental rather than full because a
		// blocking VACUUM of a multi-gigabyte database is not something to
		// run behind a live API; the collector releases pages after a prune
		// (Store.ReclaimSpace).
		dsn = "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=auto_vacuum(incremental)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// One writer at a time: SQLite serialises writers anyway, and a single
	// connection avoids "database is locked" between our own goroutines.
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// OpenReadOnly opens an existing database for queries only: no migration,
// no DDL, every statement runs under PRAGMA query_only. The API uses it so a
// read-only process cannot race the collector's schema setup or write by
// accident. It fails if the database does not exist or its schema is not the
// version this binary knows.
func OpenReadOnly(path string) (*Store, error) {
	if path == ":memory:" {
		return nil, errors.New("read-only open needs a file")
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("observer database %s: %w (start the collector first)", path, err)
	}
	// Tuning, in the order it matters.
	//
	// query_only is the safety property this constructor exists for: the file
	// is still opened read-write at the OS level, which is what keeps WAL
	// working (a WAL reader writes to the -shm file), and every statement is
	// refused if it would write.
	//
	// cache_size is negative, which SQLite reads as kibibytes rather than
	// pages: 48 MiB per connection. The aggregates here scan hundreds of
	// thousands of index entries and the 2 MiB default evicts most of the
	// index between one query and the next.
	//
	// mmap_size lets reads come from the page cache without a copy into
	// SQLite's own. 1 GiB is a ceiling, not a reservation: only pages actually
	// touched are mapped.
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=query_only(1)" +
		"&_pragma=cache_size(-49152)&_pragma=mmap_size(1073741824)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Readers run concurrently. The writable Open above pins the pool to one
	// connection because SQLite serialises writers anyway and a single
	// connection avoids "database is locked" between our own goroutines; that
	// reasoning does not carry over here, and copying it did real damage. In
	// WAL mode any number of readers proceed at once without blocking each
	// other, and this process never writes. With the pool at one, a background
	// snapshot refresh — seconds of aggregate over hundreds of thousands of
	// rows — held the only connection, so every unrelated request behind it
	// (a blob page, the footer's counts) waited for the whole refresh. Each
	// connection carries its own page cache, so the count is bounded rather
	// than left to grow with concurrency.
	conns := runtime.NumCPU()
	if conns < 4 {
		conns = 4
	}
	if conns > 8 {
		conns = 8
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	// Every version from 1 to SchemaVersion must be present, not just the
	// highest: a database with a gap is missing that migration's columns, and
	// MAX alone would wave it through.
	var highest, distinct int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0), COUNT(DISTINCT version) FROM schema_migrations`).
		Scan(&highest, &distinct); err != nil {
		db.Close()
		return nil, fmt.Errorf("read schema version: %w (is this an observer database?)", err)
	}
	if highest > SchemaVersion {
		// A newer collector has added columns this binary does not know. The
		// queries here might still run, and the first one that does not would
		// fail at request time on a public site; refuse now instead.
		db.Close()
		return nil, fmt.Errorf("database schema version %d is newer than this binary's %d: upgrade this binary", highest, SchemaVersion)
	}
	if highest != SchemaVersion || distinct != SchemaVersion {
		db.Close()
		return nil, fmt.Errorf("schema version %d (%d of %d migrations applied), this binary expects %d: "+
			"run the collector once to migrate, or upgrade this binary", highest, distinct, SchemaVersion, SchemaVersion)
	}
	return &Store{db: db}, nil
}

// DB exposes the underlying handle for read-only queries (the API).
func (s *Store) DB() *sql.DB { return s.db }

// Querier is what a read needs: the database itself, or one transaction on
// it, so that every read of one computation can see the same snapshot of the
// store (the API computes a whole window in one read transaction).
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	// The baseline is idempotent (every statement is CREATE ... IF NOT
	// EXISTS), so running it against an existing database is a no-op and
	// against a new one creates version 1.
	for _, stmt := range splitSQL(schemaSQL) {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("schema: %w\n%s", err, stmt)
		}
	}
	if _, err := s.db.Exec(`INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (1, ?)`,
		ts(time.Now())); err != nil {
		return fmt.Errorf("record baseline: %w", err)
	}

	applied, err := s.appliedVersions()
	if err != nil {
		return err
	}
	var highest int
	for v := range applied {
		if v > highest {
			highest = v
		}
	}
	if highest > SchemaVersion {
		return fmt.Errorf("database schema version %d is newer than this binary's %d: "+
			"upgrade the binary, or point it at a different database", highest, SchemaVersion)
	}

	for _, m := range migrations {
		if m.version > SchemaVersion {
			return fmt.Errorf("migration %d is above SchemaVersion %d: bump the constant", m.version, SchemaVersion)
		}
		if applied[m.version] {
			continue
		}
		if err := s.applyMigration(m); err != nil {
			return err
		}
	}

	// Every version from 1 to SchemaVersion must now be recorded. A gap means
	// migrations is missing an entry, which would let OpenReadOnly's version
	// check pass over a database that never got the columns.
	applied, err = s.appliedVersions()
	if err != nil {
		return err
	}
	for v := 1; v <= SchemaVersion; v++ {
		if !applied[v] {
			return fmt.Errorf("schema version %d is missing from migrations", v)
		}
	}
	return nil
}

func (s *Store) appliedVersions() (map[int]bool, error) {
	rows, err := s.db.Query(`SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema versions: %w", err)
	}
	defer rows.Close()
	out := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

func (s *Store) applyMigration(m migration) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, stmt := range m.stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("migration %d (%s): %w\n%s", m.version, m.note, err, stmt)
		}
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`,
		m.version, ts(time.Now())); err != nil {
		return fmt.Errorf("migration %d: record: %w", m.version, err)
	}
	return tx.Commit()
}

// splitSQL turns a schema file into executable statements. Comments are
// stripped before splitting on ";" so a semicolon inside a comment cannot cut
// a statement in half.
func splitSQL(src string) []string {
	var sb strings.Builder
	for _, line := range strings.Split(src, "\n") {
		sb.WriteString(stripSQLComment(line))
		sb.WriteByte('\n')
	}
	var out []string
	for _, stmt := range strings.Split(sb.String(), ";") {
		if stmt = strings.TrimSpace(stmt); stmt != "" {
			out = append(out, stmt)
		}
	}
	return out
}

// TimeLayout is the fixed-width UTC layout every timestamp column uses, so
// that string comparison in SQL (>=, ORDER BY, MAX) is chronological.
// RFC3339Nano trims trailing zeros, which breaks that: "...:00Z" sorts after
// "...:00.5Z".
// MetaParamHoldsRev is bumped whenever a params hold is raised or lifted
// or a correction moves a verdict, so the API's cached aggregates — which
// run to a fifteen-minute TTL — can tell that a figure they hold has been
// withdrawn instead of republishing it until the TTL runs out. Its value is
// a counter; see bumpParamHoldsRev for why it is not a timestamp.
const MetaParamHoldsRev = "param_holds_rev"

// execer is satisfied by *sql.DB and by *sql.Tx, so a revision bump can
// join a transaction that is already open or stand on its own.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// bumpParamHoldsRev advances the revision the API's cached aggregates key
// on, as one atomic statement.
//
// It counts rather than stamping the clock. The collector takes one
// time.Now() at the top of a pass and threads it through every record it
// ingests, so two ranges landing in the same pass wrote the same
// nanosecond: a snapshot computed between them carried that value, still
// matched it afterwards, and went on serving the verdicts the second range
// had just withheld. Two bumps must never produce the same token, and a
// counter cannot.
//
// The increment is done by SQLite inside the statement, not read-then-
// written in Go, so two writers cannot both read the same value first.
//
// A value left by the earlier timestamp scheme is an integer, so it
// increments from there and the token keeps rising across the change.
// Anything that will not parse casts to 0 and the next value is 1, which is
// still a change — the API only ever compares for equality, and treats the
// token as opaque.
func bumpParamHoldsRev(db execer, now time.Time) error {
	_, err := db.Exec(`INSERT INTO meta (key, value, updated_at) VALUES (?, '1', ?)
		ON CONFLICT(key) DO UPDATE SET
			value      = CAST(CAST(meta.value AS INTEGER) + 1 AS TEXT),
			updated_at = excluded.updated_at`, MetaParamHoldsRev, ts(now))
	return err
}

// BumpParamHoldsRev advances the revision from outside a transaction, for
// the collector's hold sync and correction passes. Every path that moves
// this key goes through the same counter; none of them writes a timestamp.
func (s *Store) BumpParamHoldsRev(now time.Time) error { return bumpParamHoldsRev(s.db, now) }

const TimeLayout = "2006-01-02T15:04:05.000000000Z"

// TS formats a time for a timestamp column or a comparison argument.
func TS(t time.Time) string { return t.UTC().Format(TimeLayout) }

func ts(t time.Time) string { return TS(t) }

// stripSQLComment removes a trailing "--" comment from one SQL line, ignoring
// a "--" that falls inside a single-quoted string literal (a DEFAULT or a
// CHECK constraint may legitimately contain one).
func stripSQLComment(line string) string {
	inStr := false
	for i := 0; i < len(line); i++ {
		switch {
		case line[i] == '\'':
			// '' inside a literal is an escaped quote, which this toggle
			// handles correctly: it closes and immediately reopens.
			inStr = !inStr
		case !inStr && line[i] == '-' && i+1 < len(line) && line[i+1] == '-':
			return line[:i]
		}
	}
	return line
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ---- observer runs (gap tracking) ----

// StartRun records a process start and returns the run id.
func (s *Store) StartRun(component, vantage, version string, now time.Time) (int64, error) {
	host, _ := os.Hostname()
	res, err := s.db.Exec(`INSERT INTO observer_runs (component, vantage, version, started_at, last_heartbeat_at, pid, hostname)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, component, vantage, version, ts(now), ts(now), os.Getpid(), host)
	if err != nil {
		return 0, fmt.Errorf("start run: %w", err)
	}
	return res.LastInsertId()
}

// ReplayRunEvent applies one runs.jsonl record (status.RunEvent). A start
// inserts the run with its configuration, or is a no-op when the same
// (component, vantage, started_at, pid) is already there; a stop closes the
// matching open run. Returns whether a row changed.
func (s *Store) ReplayRunEvent(e status.RunEvent) (bool, error) {
	switch e.Kind {
	case status.RunStarted:
		var cfg any
		if len(e.Config) > 0 {
			b, err := json.Marshal(e.Config)
			if err != nil {
				return false, err
			}
			cfg = string(b)
		}
		res, err := s.db.Exec(`INSERT INTO observer_runs (component, vantage, version, started_at, last_heartbeat_at, pid, hostname, config_json)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
			e.Component, e.Vantage, e.Version, ts(e.At), ts(e.At), e.PID, nullIfEmpty(e.Hostname), cfg)
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		return n > 0, nil
	case status.RunStopped:
		res, err := s.db.Exec(`UPDATE observer_runs SET stopped_at = ?, last_heartbeat_at = ?, stop_reason = ?
			WHERE component = ? AND vantage = ? AND pid = ? AND started_at <= ? AND stopped_at IS NULL`,
			ts(e.At), ts(e.At), e.Reason, e.Component, e.Vantage, e.PID, ts(e.At))
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		return n > 0, nil
	default:
		return false, fmt.Errorf("unknown run event kind %q", e.Kind)
	}
}

// SamplingSecret is one revealed per-day sampling secret, as the prober
// appends it to sampling-secrets.jsonl (policy.Reveal).
type SamplingSecret struct {
	Day        string    `json:"day"`
	Commitment string    `json:"commitment"`
	Secret     string    `json:"secret"`
	RevealedAt time.Time `json:"revealed_at"`
}

// UpsertSamplingSecret stores a revealed day secret; a day already revealed
// is left as first recorded.
func (s *Store) UpsertSamplingSecret(e SamplingSecret) (bool, error) {
	res, err := s.db.Exec(`INSERT INTO sampling_secrets (day, commitment, secret, revealed_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (day) DO NOTHING`, e.Day, e.Commitment, e.Secret, ts(e.RevealedAt))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// Heartbeat extends a run's observed span.
func (s *Store) Heartbeat(runID int64, now time.Time) error {
	_, err := s.db.Exec(`UPDATE observer_runs SET last_heartbeat_at = ? WHERE id = ?`, ts(now), runID)
	return err
}

// StopRun closes a run cleanly. A run without stopped_at whose heartbeat is
// stale is a crash; the dashboard treats the span after the last heartbeat as
// a gap either way.
func (s *Store) StopRun(runID int64, now time.Time, reason string) error {
	_, err := s.db.Exec(`UPDATE observer_runs SET stopped_at = ?, last_heartbeat_at = ?, stop_reason = ? WHERE id = ?`,
		ts(now), ts(now), reason, runID)
	return err
}

// ---- ingest cursors ----

// Cursor returns the byte offset and line number already ingested for file.
func (s *Store) Cursor(file string) (offset, line int64, err error) {
	err = s.db.QueryRow(`SELECT byte_offset, line_no FROM ingest_cursors WHERE file = ?`, file).Scan(&offset, &line)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	return offset, line, err
}

// SetCursor persists the ingest position for file.
func (s *Store) SetCursor(file string, offset, line int64, now time.Time) error {
	_, err := s.db.Exec(`INSERT INTO ingest_cursors (file, byte_offset, line_no, updated_at) VALUES (?, ?, ?, ?)
		ON CONFLICT(file) DO UPDATE SET byte_offset = excluded.byte_offset, line_no = excluded.line_no, updated_at = excluded.updated_at`,
		file, offset, line, ts(now))
	return err
}

// ---- meta ----

// SetMeta writes one key.
func (s *Store) SetMeta(key, value string, now time.Time) error {
	_, err := s.db.Exec(`INSERT INTO meta (key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`, key, value, ts(now))
	return err
}

// Meta reads one key ("" if absent).
func (s *Store) Meta(key string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

// ---- chain pace ----

// paceStep is how long the newer pace anchor is kept before the pair
// advances, so the older anchor always spans between paceStep and twice it.
const paceStep = 12 * time.Hour

// NotePace keeps two anchors of the chain's own clock against its height
// (meta chain_pace_from_* and chain_pace_mid_*), so the API can state the
// chain's recent average block time as a measurement over the last twelve
// to twenty-four hours rather than a nominal figure. Called on every status
// poll; the pair advances once the newer anchor is paceStep old. A tip
// below the older anchor is a different chain: both anchors start over.
func (s *Store) NotePace(height int64, tipTime, now time.Time) error {
	if height <= 0 || tipTime.IsZero() {
		return nil
	}
	fromH, fromT, err := s.paceAnchor("from")
	if err != nil {
		return err
	}
	if fromH == 0 || height < fromH {
		if err := s.setPaceAnchor("mid", 0, time.Time{}, now); err != nil {
			return err
		}
		return s.setPaceAnchor("from", height, tipTime, now)
	}
	midH, midT, err := s.paceAnchor("mid")
	if err != nil {
		return err
	}
	if midH == 0 {
		if tipTime.Sub(fromT) >= paceStep {
			return s.setPaceAnchor("mid", height, tipTime, now)
		}
		return nil
	}
	if tipTime.Sub(midT) >= paceStep {
		if err := s.setPaceAnchor("from", midH, midT, now); err != nil {
			return err
		}
		return s.setPaceAnchor("mid", height, tipTime, now)
	}
	return nil
}

func (s *Store) paceAnchor(name string) (int64, time.Time, error) {
	h, err := s.Meta("chain_pace_" + name + "_height")
	if err != nil {
		return 0, time.Time{}, err
	}
	t, err := s.Meta("chain_pace_" + name + "_time")
	if err != nil {
		return 0, time.Time{}, err
	}
	height, _ := strconv.ParseInt(h, 10, 64)
	at, perr := time.Parse(TimeLayout, t)
	if height <= 0 || perr != nil {
		return 0, time.Time{}, nil
	}
	return height, at, nil
}

func (s *Store) setPaceAnchor(name string, height int64, at, now time.Time) error {
	h, t := "", ""
	if height > 0 {
		h, t = strconv.FormatInt(height, 10), ts(at)
	}
	if err := s.SetMeta("chain_pace_"+name+"_height", h, now); err != nil {
		return err
	}
	return s.SetMeta("chain_pace_"+name+"_time", t, now)
}

// ---- params history ----

// UpsertParams stores the scanner's param history. Idempotent.
func (s *Store) UpsertParams(entries []scan.ParamEntry) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, e := range entries {
		p := e.ParamsJSON
		if _, err := tx.Exec(`INSERT INTO params_history
			(effective_from_height, effective_from_tx_index, source, withdrawal_delay_s, payment_promise_timeout_s,
			 payment_promise_height_window, shard_retention_s, full_stake_storage_budget)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(effective_from_height, effective_from_tx_index) DO NOTHING`,
			e.FromHeight, e.FromTxIndex, e.Source, p.WithdrawalDelaySeconds, p.PaymentPromiseTimeoutSeconds,
			p.PaymentPromiseHeightWindow, p.ShardRetentionSeconds, p.FullStakeStorageBudget); err != nil {
			return fmt.Errorf("params h=%d: %w", e.FromHeight, err)
		}
	}
	return tx.Commit()
}

// ---- publications and assignments ----

// UpsertPublication stores one scanner record and its per-validator
// assignment rows in one transaction. raw is the JSONL line as read from the
// file; it is kept verbatim for provenance. Re-inserting the same promise
// hash is a no-op.
//
// Both rows are write-once, and the API relies on it: it remembers each
// publication's original_rows from raw_json instead of parsing the record on
// every snapshot (api/origrows.go), and it keeps each validator's newest
// endorsements up to date from the assignments added since it last looked
// (api/signing.go, endorsementLedger). An UPDATE of raw_json, or of an
// assignment's attested, host, row count or settlement columns, would need
// both to learn about it.
func (s *Store) UpsertPublication(p scan.Publication, raw []byte) (inserted bool, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	a := p.Assignment
	// A record written before AttestationSchemaVersion has no attestation
	// evidence, so these columns stay NULL. Writing 0 would assert that no
	// validator attested, which the record does not say.
	att := func(v int64) any {
		if !p.HasAttestation() {
			return nil
		}
		return v
	}
	res, err := tx.Exec(`INSERT INTO publications
		(promise_hash, commitment, blob_version, blob_size, namespace, chain_id, promise_height, creation_timestamp,
		 signer, signer_public_key, validator_signature_count, settlement_height, settlement_time, settlement_tx_hash,
		 settlement_tx_index, settlement_tx_code, must_serve_until, must_serve_until_basis, shard_retention_s,
		 payment_promise_timeout_s, assignment_error, protocol_params_fingerprint, pinned_celestia_app,
		 validator_set_height, total_voting_power, sigma_rows, distinct_rows, wrap_overlaps, validators_with_rows,
		 recorded_at, raw_json,
		 attested_with_rows, attested_voting_power, signature_entries, signatures_verified,
		 signatures_unmatched, signatures_out_of_position, retention_unverified)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		        ?, ?, ?, ?, ?, ?,
		-- Born withheld when a range that still withholds already covers
		-- this publication's upload interval. Publications are ingested
		-- before the ranges in a pass, so one settling into a range
		-- recorded on an earlier pass would otherwise arrive unheld and
		-- stay that way until the hold sync at the end of the pass.
		EXISTS (SELECT 1 FROM param_uncertainty u
			WHERE u.holds = 1 AND ? - 1 <= u.to_height AND ? >= u.from_height))
		ON CONFLICT(promise_hash) DO NOTHING`,
		p.PromiseHash, p.Promise.Commitment, p.Promise.BlobVersion, p.Promise.BlobSize, p.Promise.Namespace,
		p.Promise.ChainID, p.Promise.Height, ts(p.Promise.CreationTimestamp),
		p.Signer, p.Promise.SignerPublicKey, p.ValidatorSignatureCount, p.SettlementHeight, ts(p.SettlementTime),
		p.SettlementTxHash, p.SettlementTxIndex, p.SettlementTxCode, ts(p.MustServeUntil), p.MustServeUntilBasis,
		p.ParamsAtPublication.ShardRetentionSeconds, p.ParamsAtPublication.PaymentPromiseTimeoutSeconds,
		a.Error, a.ProtocolParams.Fingerprint, a.ProtocolParams.PinnedCelestiaApp,
		a.ValidatorSetHeight, a.TotalVotingPower, a.Sigma, a.Distinct, a.WrapOverlaps, a.ValidatorsWithRows,
		ts(p.RecordedAt), string(raw),
		att(int64(a.AttestedWithRows)), att(a.AttestedVotingPower), att(int64(a.SignatureEntries)),
		att(int64(a.SignaturesVerified)), att(int64(a.SignaturesUnmatched)), att(int64(a.SignaturesOutOfPosition)),
		p.Promise.Height, p.SettlementHeight)
	if err != nil {
		return false, fmt.Errorf("publication %s: %w", p.PromiseHash, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, tx.Commit()
	}
	for _, v := range a.Validators {
		var rowsJSON any
		if v.Rows != nil {
			b, err := json.Marshal(v.Rows)
			if err != nil {
				return false, err
			}
			rowsJSON = string(b)
		}
		var attested any
		if p.HasAttestation() {
			attested = b2i(v.Attested)
		}
		// NULL: unknown (a scan gap, or no seed); '' : no host on record;
		// else the host the chain's events or the seed named.
		var host any
		switch v.HostSource {
		case scan.HostFromEvent, scan.HostFromSeed, scan.HostFromSeedLazy, scan.HostFromSeedCurrent, scan.HostFromReseed:
			host = v.Host
		case scan.HostNone:
			host = ""
		}
		// settlement_height is the publication's, copied so the newest
		// assignment per validator is an index seek (migration 22).
		if _, err := tx.Exec(`INSERT INTO assignments (promise_hash, validator_address, voting_power, row_count, rows_json, attested, host_at_settlement, settlement_height)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(promise_hash, validator_address) DO NOTHING`,
			p.PromiseHash, v.Address, v.VotingPower, v.RowCount, rowsJSON, attested, host, p.SettlementHeight); err != nil {
			return false, fmt.Errorf("assignment %s/%s: %w", p.PromiseHash, v.Address, err)
		}
	}
	return true, tx.Commit()
}

// ---- probes ----

// InsertProbe stores one prober measurement. Re-inserting the same dedupe key
// is a no-op, so a file can be re-ingested safely. A sampled-out row whose
// publication is already stored as a decision is not stored: the decision
// stands for it (see sampledout.go), and it was deleted when the decision
// was made from it.
func (s *Store) InsertProbe(m probe.Measurement, raw []byte) (inserted bool, err error) {
	if decided, err := s.sampledOutDecided(m); err != nil || decided {
		return false, err
	}
	res, err := s.db.Exec(`INSERT INTO probes
		(dedupe_key, vantage, promise_hash, commitment, blob_version, must_serve_until, validator_set_height,
		 validator_address, validator_host, assigned, assigned_row_count, schedule_label, scheduled_at, started_at,
		 finished_at, lateness_ms, dns_ok, dns_ms, tcp_ok, tcp_ms, tls_ok, tls_ms, tls_version, peer_cert_sha256,
		 identity_ok, identity_reason, download_ok, download_ms, rows_returned, rows_expected, commitment_verified,
		 assignment_verified, phase, outcome, classification, classification_reason, raw_error, total_duration_ms, raw_json,
		 attested, bytes_returned, row_indices, rows_sha256, rpc_code, shadowed_by, observer_build, app_version,
		 sampling_p, sampling_binding, sampling_commitment, retry_first_outcome, clock_offset_ms, shadow_gap,
		 host_at_settlement, settlement_host_outcome, settlement_host_served, retention_unverified)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		-- Born withheld when this row must be: see ProbeHeldAtInsert for
		-- the three cases and why each is needed. Deciding it here rather
		-- than in the pass that follows is what makes the withholding a
		-- property of the row instead of a race the collector usually
		-- wins.
		`+ProbeHeldAtInsert+`)
		ON CONFLICT(dedupe_key) DO NOTHING`,
		m.DedupeKey(), m.Vantage, m.PromiseHash, m.Commitment, m.BlobVersion, ts(m.MustServeUntil), m.ValidatorSetHeight,
		m.ValidatorAddress, m.ValidatorHost, b2i(m.Assigned), m.AssignedRowCount, m.ScheduleLabel, ts(m.ScheduledAt),
		ts(m.StartedAt), ts(m.FinishedAt), m.LatenessMS,
		b2i(m.DNS.OK), m.DNS.DurationMS, b2i(m.TCP.OK), m.TCP.DurationMS, b2i(m.TLS.OK), m.TLS.DurationMS,
		m.TLS.Version, m.TLS.PeerCertSHA256, b2i(m.Identity.OK), m.Identity.Reason,
		b2i(m.Download.OK), m.Download.DurationMS, m.Download.RowsReturned, m.Download.RowsExpected,
		b2i(m.Download.CommitmentVerified), b2i(m.Download.AssignmentVerified),
		string(m.Phase), string(m.Outcome), string(m.Classification), m.ClassificationReason, m.RawError,
		m.TotalDurationMS, string(raw),
		probeAttested(m), probeBytes(m),
		nullIfEmpty(rowIndicesJSON(m)), nullIfEmpty(m.Download.RowsSHA256), nullIfEmpty(m.Download.RPCCode),
		nullIfEmpty(m.Download.ShadowedBy), nullIfEmpty(observerBuild(m)), observerAppVersion(m),
		samplingP(m), samplingField(m, func(d *probe.SamplingDecision) string { return d.Binding }),
		samplingField(m, func(d *probe.SamplingDecision) string { return d.DayCommitment }), retryFirstOutcome(m), m.ClockOffsetMS,
		nullIfEmpty(m.Download.ShadowGap), nullIfEmpty(m.HostAtSettlement), settlementOutcome(m), settlementServed(m),
		ts(m.MustServeUntil), m.PromiseHash)
	if err != nil {
		return false, fmt.Errorf("probe %s: %w", m.DedupeKey(), err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func settlementOutcome(m probe.Measurement) any {
	if m.SettlementHost == nil {
		return nil
	}
	return string(m.SettlementHost.Outcome)
}

func settlementServed(m probe.Measurement) any {
	if m.SettlementHost == nil {
		return nil
	}
	return b2i(m.SettlementHost.Outcome == probe.OutcomeServedOK && m.SettlementHost.AssignmentVerified)
}

func samplingP(m probe.Measurement) any {
	if m.Sampling == nil {
		return nil
	}
	return m.Sampling.P
}

func samplingField(m probe.Measurement, f func(*probe.SamplingDecision) string) any {
	if m.Sampling == nil {
		return nil
	}
	return nullIfEmpty(f(m.Sampling))
}

func retryFirstOutcome(m probe.Measurement) any {
	if m.Retry == nil {
		return nil
	}
	return nullIfEmpty(string(m.Retry.FirstOutcome))
}

// nullIfEmpty stores "" as NULL: an absent fact, not an empty one.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// rowIndicesJSON is the returned row indices as a JSON array, "" when none.
func rowIndicesJSON(m probe.Measurement) string {
	if len(m.Download.RowIndices) == 0 {
		return ""
	}
	b, err := json.Marshal(m.Download.RowIndices)
	if err != nil {
		return ""
	}
	return string(b)
}

func observerBuild(m probe.Measurement) string {
	if m.Observer == nil {
		return ""
	}
	return m.Observer.Build
}

func observerAppVersion(m probe.Measurement) any {
	if m.Observer == nil || m.Observer.AppVersion == 0 {
		return nil
	}
	return m.Observer.AppVersion
}

// probeBytes maps the bytes handed over to a nullable column. A record from
// before the field existed carries zero, and zero bytes beside a positive row
// count is not a measurement: it stays NULL so no rate is drawn over it.
func probeBytes(m probe.Measurement) any {
	if m.Download.BytesReturned <= 0 {
		return nil
	}
	return m.Download.BytesReturned
}

// probeAttested maps a measurement's attestation to its nullable column. A
// record written before AttestationSchemaVersion carries no evidence, so the
// column stays NULL: its Attested is false only because the field did not
// exist, and the API must not read that as "this validator did not attest".
func probeAttested(m probe.Measurement) any {
	if !m.HasAttestation() {
		return nil
	}
	return b2i(m.Attested)
}

// ---- endpoints ----

// ObserveEndpoints reconciles one AllBondedFibreProviders snapshot with the
// endpoint history: unseen (validator, host) pairs open a row, pairs seen
// again extend last_seen, open rows missing from the snapshot are closed.
// Returns how many rows were opened and closed.
//
// "Closed" means only that the pair stopped appearing in the bonded provider
// list, which is what closed_reason records. It is not deregistration:
// x/valaddr has no message for that, and its msg server rejects an empty
// host, so a registration once made stays on chain. Jailing and unbonding
// both remove a provider from the bonded list while the entry survives, so
// treating a closure as the operator withdrawing its endpoint would be
// reading an event that cannot happen.
func (s *Store) ObserveEndpoints(ctx context.Context, providers []scan.FibreProvider, height int64, now time.Time) (opened, closed int, err error) {
	evs, err := s.ObserveEndpointEvents(ctx, providers, height, now)
	for _, e := range evs {
		if e.Kind == EndpointOpened {
			opened++
		} else {
			closed++
		}
	}
	return opened, closed, err
}

// Endpoint event kinds, as written to registry.jsonl.
const (
	EndpointOpened = "endpoint_opened"
	EndpointClosed = "endpoint_closed"
)

// EndpointEvent is one change to the Fibre endpoint registry as this
// observer saw it: a (validator, host) pair appearing in or leaving
// AllBondedFibreProviders. The collector appends these to registry.jsonl so
// the endpoint history, which has no other source than the live polls,
// survives a rebuild of the database from the JSONL files.
type EndpointEvent struct {
	Kind        string    `json:"kind"`
	ConsAddress string    `json:"validator_cons_address"`
	Host        string    `json:"host"`
	Height      int64     `json:"height"`
	At          time.Time `json:"at"`
	Reason      string    `json:"reason,omitempty"`
}

// ObserveEndpointEvents is ObserveEndpoints returning what changed.
func (s *Store) ObserveEndpointEvents(ctx context.Context, providers []scan.FibreProvider, height int64, now time.Time) ([]EndpointEvent, error) {
	var events []EndpointEvent
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	type key struct{ addr, host string }
	open := map[key]int64{}
	rows, err := tx.Query(`SELECT id, validator_cons_address, host FROM endpoints WHERE closed_at IS NULL`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		var k key
		if err := rows.Scan(&id, &k.addr, &k.host); err != nil {
			rows.Close()
			return nil, err
		}
		open[k] = id
	}
	rows.Close()

	seen := map[key]bool{}
	for _, p := range providers {
		k := key{p.ConsAddressBech32, p.Host}
		seen[k] = true
		if id, ok := open[k]; ok {
			if _, err := tx.Exec(`UPDATE endpoints SET last_seen_at = ?, last_seen_height = ? WHERE id = ?`, ts(now), height, id); err != nil {
				return nil, err
			}
			continue
		}
		if _, err := tx.Exec(`INSERT INTO endpoints (validator_cons_address, host, first_seen_at, first_seen_height, last_seen_at, last_seen_height)
			VALUES (?, ?, ?, ?, ?, ?)`, k.addr, k.host, ts(now), height, ts(now), height); err != nil {
			return nil, err
		}
		events = append(events, EndpointEvent{Kind: EndpointOpened, ConsAddress: k.addr, Host: k.host, Height: height, At: now.UTC()})
	}
	for k, id := range open {
		if seen[k] {
			continue
		}
		if _, err := tx.Exec(`UPDATE endpoints SET closed_at = ?, closed_height = ?, closed_reason = ? WHERE id = ?`,
			ts(now), height, "left_bonded_provider_list", id); err != nil {
			return nil, err
		}
		events = append(events, EndpointEvent{Kind: EndpointClosed, ConsAddress: k.addr, Host: k.host, Height: height, At: now.UTC(), Reason: "left_bonded_provider_list"})
	}
	return events, tx.Commit()
}

// ReplayEndpointEvent applies one registry.jsonl record. It is idempotent:
// an open already present (by the same first_seen time, or by a later live
// poll) and a close already applied are no-ops, so the file can be tailed
// from zero as often as needed. Returns whether a row changed.
func (s *Store) ReplayEndpointEvent(e EndpointEvent) (bool, error) {
	switch e.Kind {
	case EndpointOpened:
		var n int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM endpoints WHERE validator_cons_address = ? AND host = ?
			AND (first_seen_at = ? OR closed_at IS NULL)`, e.ConsAddress, e.Host, ts(e.At)).Scan(&n); err != nil {
			return false, err
		}
		if n > 0 {
			return false, nil
		}
		_, err := s.db.Exec(`INSERT INTO endpoints (validator_cons_address, host, first_seen_at, first_seen_height, last_seen_at, last_seen_height)
			VALUES (?, ?, ?, ?, ?, ?)`, e.ConsAddress, e.Host, ts(e.At), e.Height, ts(e.At), e.Height)
		return err == nil, err
	case EndpointClosed:
		reason := e.Reason
		if reason == "" {
			reason = "left_bonded_provider_list"
		}
		res, err := s.db.Exec(`UPDATE endpoints SET closed_at = ?, closed_height = ?, closed_reason = ?
			WHERE validator_cons_address = ? AND host = ? AND closed_at IS NULL AND first_seen_at <= ?`,
			ts(e.At), e.Height, reason, e.ConsAddress, e.Host, ts(e.At))
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		return n > 0, nil
	default:
		return false, fmt.Errorf("unknown endpoint event kind %q", e.Kind)
	}
}

// UpsertValidatorIdentities stores what the staking module says about each
// validator. It is upsert-only: a validator that stops appearing keeps its
// last known name, because a row in the probe tables with no name is worse
// than a row with a stale one, and the chain does not forget validators.
// UpsertPayment inserts one escrow movement; a key already present is left
// alone (the scanner's dedupe key is stable across re-scans).
func (s *Store) UpsertPayment(p scan.Payment, raw []byte) (inserted bool, err error) {
	if p.DedupeKey == "" || p.Publisher == "" || p.Kind == "" {
		return false, fmt.Errorf("payment without dedupe_key, publisher or kind")
	}
	var avail any
	if p.AvailableAt != nil {
		avail = ts(*p.AvailableAt)
	}
	res, err := s.db.Exec(`INSERT INTO payments
		(dedupe_key, kind, height, time, tx_hash, tx_index, msg_index, publisher, processor,
		 promise_hash, namespace, blob_size, gas_units, denom, amount_utia, available_at, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(dedupe_key) DO NOTHING`,
		p.DedupeKey, p.Kind, p.Height, ts(p.Time), p.TxHash, p.TxIndex, p.MsgIndex, p.Publisher, p.Processor,
		p.PromiseHash, p.Namespace, int64(p.BlobSize), int64(p.GasUnits), p.Denom, int64(p.AmountUtia), avail, string(raw))
	if err != nil {
		return false, fmt.Errorf("payment %s: %w", p.DedupeKey, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// UpsertEscrowAccount records what the chain currently holds for one
// publisher. A publisher not found on chain is stored with found=0 and zero
// balances, so the page can say "no escrow" rather than nothing.
func (s *Store) UpsertEscrowAccount(e scan.Escrow, now time.Time) error {
	if e.Signer == "" {
		return fmt.Errorf("escrow account without a signer")
	}
	_, err := s.db.Exec(`INSERT INTO escrow_accounts
		(publisher, found, denom, balance_utia, available_utia, height, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(publisher) DO UPDATE SET
			found          = excluded.found,
			denom          = excluded.denom,
			balance_utia   = excluded.balance_utia,
			available_utia = excluded.available_utia,
			height         = excluded.height,
			updated_at     = excluded.updated_at`,
		e.Signer, b2i(e.Found), e.Denom, int64(e.BalanceUtia), int64(e.AvailableUtia), e.Height, ts(now))
	if err != nil {
		return fmt.Errorf("escrow account %s: %w", e.Signer, err)
	}
	return nil
}

// Publishers lists every account the payments table has seen as an escrow
// owner, for the collector's escrow poll.
func (s *Store) Publishers() ([]string, error) {
	rows, err := s.db.Query(`SELECT DISTINCT publisher FROM payments ORDER BY publisher`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) UpsertValidatorIdentities(ids []scan.ValidatorIdentity, now time.Time) (int, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	n := 0
	for _, v := range ids {
		if v.ConsAddressHex == "" {
			// No consensus key this build could parse, so nothing to join
			// against. Counted as skipped rather than stored under a blank
			// key, which would collide every such validator into one row.
			continue
		}
		if _, err := tx.Exec(`INSERT INTO validator_identities
			(cons_address, operator_address, moniker, identity, website, tokens, jailed, status, first_seen_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(cons_address) DO UPDATE SET
				operator_address = excluded.operator_address,
				moniker          = excluded.moniker,
				identity         = excluded.identity,
				website          = excluded.website,
				tokens           = excluded.tokens,
				jailed           = excluded.jailed,
				status           = excluded.status,
				updated_at       = excluded.updated_at`,
			strings.ToLower(v.ConsAddressHex), v.OperatorAddress, v.Moniker, v.Identity, v.Website,
			v.Tokens, b2i(v.Jailed), v.Status, ts(now), ts(now)); err != nil {
			return 0, fmt.Errorf("validator identity %s: %w", v.ConsAddressHex, err)
		}
		n++
	}
	return n, tx.Commit()
}

// AvatarsDue lists the Keybase identities of known validators whose picture
// has never been resolved or was last checked before now - maxAge, at most
// limit of them, oldest check first. Only well-formed key suffixes are
// candidates; a moniker or URL in the identity field is never looked up.
func (s *Store) AvatarsDue(ctx context.Context, now time.Time, maxAge time.Duration, limit int) ([]string, error) {
	// Sixteen hex characters, the same test keybase.ValidIdentity applies.
	// Selecting on length alone handed the lookup identities it refuses, so
	// a validator with a 16-character non-hex identity produced a guaranteed
	// failure every refresh cycle, forever, and an error row keyed by it.
	//
	// Joined case-insensitively, because the chain carries whatever the
	// operator typed: keybase.ValidIdentity accepts both cases, and a
	// mixed-case identity must not look unfetched forever beside the row
	// already held for it.
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT vi.identity, COALESCE(a.checked_at, '')
		FROM validator_identities vi LEFT JOIN validator_avatars a ON UPPER(a.identity) = UPPER(vi.identity)
		WHERE vi.identity GLOB '[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]'
		  AND (a.checked_at IS NULL OR a.checked_at < ?)
		ORDER BY COALESCE(a.checked_at, '') ASC LIMIT ?`, ts(now.Add(-maxAge)), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, checked string
		if err := rows.Scan(&id, &checked); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// PutAvatar records the result of one resolution: the picture (status ok),
// its absence (none) or a failed attempt (error, with the reason in url).
func (s *Store) PutAvatar(identity, status, url, contentType string, data []byte, now time.Time) error {
	// One canonical spelling for the key. The chain carries the operator's,
	// which may be either case; the API serves /v1/avatars/<upper>.
	identity = strings.ToUpper(identity)
	_, err := s.db.Exec(`INSERT INTO validator_avatars (identity, url, content_type, data, status, checked_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(identity) DO UPDATE SET url = excluded.url, content_type = excluded.content_type,
			data = excluded.data, status = excluded.status, checked_at = excluded.checked_at`,
		identity, url, contentType, data, status, ts(now))
	return err
}

// Avatar returns the picture held for an identity; ok is false when none is
// held (never resolved, no picture, or the last attempt failed).
func (s *Store) Avatar(ctx context.Context, identity string) (contentType string, data []byte, checkedAt time.Time, ok bool, err error) {
	var checked string
	err = s.db.QueryRowContext(ctx, `SELECT content_type, data, checked_at FROM validator_avatars WHERE UPPER(identity) = ? AND status = 'ok'`, strings.ToUpper(identity)).Scan(&contentType, &data, &checked)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, time.Time{}, false, nil
	}
	if err != nil {
		return "", nil, time.Time{}, false, err
	}
	checkedAt, _ = time.Parse(TimeLayout, checked)
	return contentType, data, checkedAt, len(data) > 0, nil
}

// Endpoint is one open or closed endpoint-history row.
type Endpoint struct {
	ID                   int64
	ValidatorConsAddress string
	Host                 string
	FirstSeenAt          string
	FirstSeenHeight      int64
	LastSeenAt           string
	LastSeenHeight       int64
	ClosedAt             *string
}

// ReclaimSpace returns pages freed by the retention pass to the operating
// system, up to a bounded number so the call cannot stall a live database.
//
// It is a no-op on a database created before auto_vacuum was set, which
// cannot free pages incrementally at all; the count returned is then zero and
// the caller says nothing. freed is the number of pages released.
func (s *Store) ReclaimSpace(ctx context.Context, maxPages int) (freed int64, err error) {
	if maxPages <= 0 {
		maxPages = 2000
	}
	var before, after int64
	if err := s.db.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&before); err != nil {
		return 0, err
	}
	if before == 0 {
		return 0, nil
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("PRAGMA incremental_vacuum(%d)", maxPages)); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&after); err != nil {
		return 0, err
	}
	if before <= after {
		return 0, nil
	}
	return before - after, nil
}

// CheckpointWAL copies the write-ahead log back into the database and, when
// no reader is holding a snapshot, truncates it.
//
// SQLite's own auto-checkpoint copies pages back but never resets the file,
// and it cannot reset one while any reader has a snapshot open. The API holds
// one through every snapshot refresh, which on a busy vantage is much of the
// time, so a batch ingest — a collector restarting with a day of measurements
// unread, or the first pass after an outage — leaves the -wal at its
// high-water mark for good: measured at about a gigabyte for one mocha day of
// probe rows, on the same volume as the database and counted by the health
// check's disk threshold.
//
// Called once per collector pass. It returns busy without doing anything when
// a reader is in the way, which is not an error: the next pass tries again.
// The counts are the SQLite pragma's own: log frames and frames checkpointed.
func (s *Store) CheckpointWAL(ctx context.Context) (busy bool, inLog, checkpointed int64, err error) {
	var b int64
	err = s.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&b, &inLog, &checkpointed)
	return b == 1, inLog, checkpointed, err
}

// CurrentEndpoints lists open endpoint rows.
func (s *Store) CurrentEndpoints(ctx context.Context) ([]Endpoint, error) {
	return CurrentEndpointsIn(ctx, s.db)
}

// CurrentEndpointsIn is CurrentEndpoints read through q.
func CurrentEndpointsIn(ctx context.Context, q Querier) ([]Endpoint, error) {
	rows, err := q.QueryContext(ctx, `SELECT id, validator_cons_address, host, first_seen_at, first_seen_height, last_seen_at, last_seen_height, closed_at
		FROM endpoints WHERE closed_at IS NULL ORDER BY validator_cons_address`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		var e Endpoint
		if err := rows.Scan(&e.ID, &e.ValidatorConsAddress, &e.Host, &e.FirstSeenAt, &e.FirstSeenHeight, &e.LastSeenAt, &e.LastSeenHeight, &e.ClosedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Counts is a quick health summary used by tests and the collector log.
type Counts struct {
	Publications, Assignments, Probes, OpenEndpoints, Runs int64
}

// countedTables are the tables Count keeps running totals for: the three
// that grow with traffic. The other two figures are small tables counted
// outright.
var countedTables = [3]string{"publications", "assignments", "probes"}

// countRecountEvery bounds how long a running total goes without a full
// recount. See Count.
const countRecountEvery = time.Hour

// rowCounts is Count's running state: per counted table, the rows counted
// and the highest rowid they were counted up to, plus what forces the next
// full recount.
type rowCounts struct {
	mu      sync.Mutex
	ok      bool
	rawFrom string
	fullAt  time.Time
	n, mark [len(countedTables)]int64
}

// metaRawFrom is rollup's meta key for the first day whose raw rows are
// all still present (rollup.metaRawFrom; the store cannot import rollup).
// The prune moves it every time it deletes a day.
const metaRawFrom = "raw_from"

// Count returns row counts of the main tables.
//
// The collector calls it every pass and /v1/meta on every request, and a
// COUNT(*) in SQLite is a walk of a whole index: it grows with every row
// ever stored, which is the wrong shape for something run every ten
// seconds and on every page load. So the three tables that grow are
// counted once and then kept up to date by counting only the rows written
// since: rowid is assigned in increasing order, so `rowid > mark` is a
// seek to the new rows, and the total stays exact as long as nothing was
// deleted.
//
// Deletes are what a running total cannot see, so the rules are:
//
//   - publications and assignments are never deleted;
//   - probes are deleted only by the retention prune (rollup.Run), which
//     moves raw_from in meta every time it deletes a day. A raw_from other
//     than the one the totals were taken under forces a full recount, so a
//     prune is reflected by the first call that sees it;
//   - and, as a backstop against a delete this reasoning does not know
//     about, nothing goes more than countRecountEvery without one.
//
// The figures therefore keep their meaning — the rows in each table — and
// only the cost changes.
func (s *Store) Count(ctx context.Context) (Counts, error) {
	var c Counts
	q := func(dst *int64, sqlText string) error { return s.db.QueryRowContext(ctx, sqlText).Scan(dst) }
	n, err := s.growingCounts(ctx)
	if err != nil {
		return c, err
	}
	c.Publications, c.Assignments, c.Probes = n[0], n[1], n[2]
	if err := q(&c.OpenEndpoints, `SELECT COUNT(*) FROM endpoints WHERE closed_at IS NULL`); err != nil {
		return c, err
	}
	if err := q(&c.Runs, `SELECT COUNT(*) FROM observer_runs`); err != nil {
		return c, err
	}
	return c, nil
}

// growingCounts is the running-total half of Count. Each table's count and
// mark come from one statement, so they describe the same snapshot of that
// table; the three tables are not one snapshot, which Count never was.
func (s *Store) growingCounts(ctx context.Context) ([len(countedTables)]int64, error) {
	var rawFrom string
	_ = s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, metaRawFrom).Scan(&rawFrom)
	rc := &s.counts
	rc.mu.Lock()
	defer rc.mu.Unlock()
	full := !rc.ok || rc.rawFrom != rawFrom || time.Since(rc.fullAt) >= countRecountEvery
	n, mark := rc.n, rc.mark
	for i, t := range countedTables {
		var got, top int64
		var err error
		if full {
			err = s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(rowid), 0) FROM `+t).Scan(&got, &top)
			n[i], mark[i] = got, top
		} else {
			err = s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MAX(rowid), ?) FROM `+t+` WHERE rowid > ?`, mark[i], mark[i]).Scan(&got, &top)
			n[i], mark[i] = n[i]+got, top
		}
		if err != nil {
			return n, err
		}
	}
	rc.n, rc.mark, rc.ok, rc.rawFrom = n, mark, true, rawFrom
	if full {
		rc.fullAt = time.Now()
	}
	return n, nil
}

// ---- reachability ----

// InsertReachability stores one heartbeat measurement. Idempotent on
// (vantage, validator, scheduled_at).
func (s *Store) InsertReachability(m probe.Measurement, raw []byte) (inserted bool, err error) {
	key := m.Vantage + "|" + m.ValidatorAddress + "|" + m.ScheduledAt.UTC().Format(time.RFC3339Nano)
	res, err := s.db.Exec(`INSERT INTO reachability
		(dedupe_key, vantage, validator_address, validator_host, height, scheduled_at, started_at,
		 dns_ok, tcp_ok, tcp_ms, tls_ok, tls_ms, peer_cert_sha256, identity_ok, identity_reason,
		 outcome, raw_error, total_duration_ms, raw_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(dedupe_key) DO NOTHING`,
		key, m.Vantage, m.ValidatorAddress, m.ValidatorHost, m.ValidatorSetHeight, ts(m.ScheduledAt), ts(m.StartedAt),
		b2i(m.DNS.OK), b2i(m.TCP.OK), m.TCP.DurationMS, b2i(m.TLS.OK), m.TLS.DurationMS, m.TLS.PeerCertSHA256,
		b2i(m.Identity.OK), m.Identity.Reason, string(m.Outcome), m.RawError, m.TotalDurationMS, string(raw))
	if err != nil {
		return false, fmt.Errorf("reachability %s: %w", key, err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ReplayHostEvent applies one host_history.jsonl record. Idempotent by
// natural key.
func (s *Store) ReplayHostEvent(e scan.HostEvent) (bool, error) {
	res, err := s.db.Exec(`INSERT INTO host_events (from_height, from_tx_index, cons_address, host, source, time)
		VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		e.FromHeight, e.FromTxIndex, strings.ToLower(e.ConsAddress), e.Host, e.Source, ts(e.Time))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// Amendment is one late verdict on a probe row (probe_amendments), as the
// collector appends it to amendments.jsonl. The prober defers the verdict on
// genuine rows that no scanned promise assigns, because the Fibre store
// serves the first shard of a commitment by promise-hash order and a promise
// settling after the probe can own them; once the scanner has read past
// probe time + payment_promise_timeout every candidate is on record and the
// verdict is drawn: SHADOWED_SHARD when a settled promise over the
// commitment, alive at the probe, assigns exactly the returned rows,
// UNMATCHED_GENUINE otherwise (held out: an upload that never settled can
// answer under hash-order serving, so no fault is supported), and
// PROBE_ERROR for good when a scan gap covers the interval or the
// assignment rows were not recorded.
type Amendment struct {
	DedupeKey        string    `json:"dedupe_key"`
	PromiseHash      string    `json:"promise_hash"`
	ValidatorAddress string    `json:"validator_address"`
	ScheduledAt      time.Time `json:"scheduled_at"`
	From             string    `json:"from"`
	To               string    `json:"to"`
	Reason           string    `json:"reason"`
	ShadowedBy       string    `json:"shadowed_by,omitempty"`
	JudgedAt         time.Time `json:"judged_at"`
	ScannerFrontier  time.Time `json:"scanner_frontier"`
	// PruneToleranceS is the collector's -prune-tolerance when the verdict
	// was drawn (how long past must_serve_until a candidate's shard was
	// taken to be on disk), so sentinel-recompute redraws it with the same
	// bound rather than a constant. Zero on lines from before the field.
	PruneToleranceS int64 `json:"prune_tolerance_s,omitempty"`
}

// ErrNoSuchRow is returned by ApplyAmendment when no probe row carries the
// amendment's key: the row was never ingested (a line skipped as
// undecodable, or a vantage whose file is not tailed here), which is not
// the same as a row already amended.
var ErrNoSuchRow = errors.New("no probe row with this key")

// LateShadowVerdicts draws the deferred verdicts the scanner frontier now
// allows. frontier is the block time of the last scanned height; a row is
// judged once frontier >= started_at + payment_promise_timeout. tolerance
// is how long past must_serve_until a candidate's shard is still taken to
// be on disk (the store's prune lag). Nothing is written: the caller applies
// each amendment (ApplyAmendment) and records it.
func (s *Store) LateShadowVerdicts(ctx context.Context, frontier, now time.Time, tolerance time.Duration) ([]Amendment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT pr.dedupe_key, pr.promise_hash, pr.commitment, pr.validator_address, pr.scheduled_at, pr.started_at,
			COALESCE(pr.row_indices, ''), pr.shadow_gap, pr.classification, pb.payment_promise_timeout_s
		FROM probes pr JOIN publications pb ON pb.promise_hash = pr.promise_hash
		WHERE pr.classification = 'PROBE_ERROR' AND pr.shadow_gap IS NOT NULL AND pr.amended_at IS NULL
		  AND pr.outcome IN ('WRONG_ROWS','PARTIAL') AND pr.commitment_verified = 1
		ORDER BY pr.started_at`)
	if err != nil {
		return nil, err
	}
	type pending struct {
		key, hash, commitment, addr, scheduled, started, indices, gap, cls string
		timeoutS                                                           int64
	}
	var todo []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.key, &p.hash, &p.commitment, &p.addr, &p.scheduled, &p.started, &p.indices, &p.gap, &p.cls, &p.timeoutS); err != nil {
			rows.Close()
			return nil, err
		}
		todo = append(todo, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []Amendment
	for _, p := range todo {
		started, err := time.Parse(TimeLayout, p.started)
		if err != nil {
			continue
		}
		scheduled, _ := time.Parse(TimeLayout, p.scheduled)
		a := Amendment{DedupeKey: p.key, PromiseHash: p.hash, ValidatorAddress: p.addr, ScheduledAt: scheduled, From: p.cls,
			JudgedAt: now.UTC(), ScannerFrontier: frontier.UTC(), PruneToleranceS: int64(tolerance / time.Second)}
		if strings.HasPrefix(p.gap, probe.ShadowGapScanPrefix) {
			// A block the scanner could not read can hold the promise that
			// owns these rows; nothing later fills it.
			a.To, a.Reason = string(probe.ClassProbeError), "no verdict: a scan gap covers the interval the owning promise would have settled in ("+p.gap+")"
			out = append(out, a)
			continue
		}
		if p.timeoutS <= 0 {
			// Without the promise timeout on record the bound by which
			// every candidate has settled cannot be named, so no frontier
			// ever closes the question; leaving the row deferred would hold
			// its day's rollup and prune forever.
			a.To, a.Reason = string(probe.ClassProbeError), "no verdict: the publication carries no payment_promise_timeout, so the bound by which every promise that could own these rows has settled cannot be named"
			out = append(out, a)
			continue
		}
		deadline := started.Add(time.Duration(p.timeoutS) * time.Second)
		if frontier.Before(deadline) {
			continue // not every candidate is on record yet
		}
		var got []int
		if p.indices == "" || json.Unmarshal([]byte(p.indices), &got) != nil {
			a.To, a.Reason = string(probe.ClassProbeError), "no verdict: the returned row indices were not recorded on this row"
			out = append(out, a)
			continue
		}
		cands, err := s.db.QueryContext(ctx, `SELECT pb.promise_hash, a.rows_json FROM publications pb
				JOIN assignments a ON a.promise_hash = pb.promise_hash
			WHERE pb.commitment = ? AND pb.promise_hash <> ? AND a.validator_address = ?
			  AND pb.settlement_time <= ? AND pb.must_serve_until >= ?
			ORDER BY pb.promise_hash`, p.commitment, p.hash, p.addr, ts(deadline), ts(started.Add(-tolerance)))
		if err != nil {
			return out, err
		}
		match, unrecorded := "", false
		for cands.Next() {
			var h string
			var rj sql.NullString
			if err := cands.Scan(&h, &rj); err != nil {
				cands.Close()
				return out, err
			}
			if !rj.Valid {
				unrecorded = true
				continue
			}
			var want []int
			if json.Unmarshal([]byte(rj.String), &want) != nil {
				unrecorded = true
				continue
			}
			if sameRowSet(got, want) {
				match = h
				break
			}
		}
		cands.Close()
		switch {
		case match != "":
			a.To, a.ShadowedBy = string(probe.ClassShadowedShard), match
			a.Reason = "returned rows of this blob that verify against the commitment and are exactly another settled promise's assignment for this validator; DownloadShard is addressed by commitment alone, so that promise answers in this one's place (judged once every promise settling by " + deadline.UTC().Format(time.RFC3339) + " was on record)"
		case unrecorded:
			a.To, a.Reason = string(probe.ClassProbeError), "no verdict: a candidate promise's assignment rows were not recorded (scanner ran without -rows)"
		default:
			// Not a fault: a shard uploaded for a promise that never settled
			// is on disk until its prune and never on chain, and it answers
			// when its hash sorts first. Held out of the rate, beside it.
			a.To, a.Reason = string(probe.ClassUnmatchedGenuine), "genuine rows of this blob that no promise over it settled by "+deadline.UTC().Format(time.RFC3339)+" assigns to this validator; under promise-hash-order serving an upload that never settled can answer, so this is held out of the rate, not a fault; the row carries the indices"
		}
		out = append(out, a)
	}
	return out, nil
}

func sameRowSet(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[int]int{}
	for _, x := range a {
		seen[x]++
	}
	for _, x := range b {
		if seen[x] == 0 {
			return false
		}
		seen[x]--
	}
	return true
}

// ApplyAmendment writes one late verdict: the row's classification moves,
// the verdict it was stamped with is kept, and the amendment is logged. A
// row already amended is left alone. Returns whether a row changed.
func (s *Store) ApplyAmendment(a Amendment) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE probes SET classification_at_probe = COALESCE(classification_at_probe, classification),
			classification = ?, classification_reason = ?, shadowed_by = COALESCE(?, shadowed_by), amended_at = ?
		WHERE dedupe_key = ? AND amended_at IS NULL`, a.To, a.Reason, nullIfEmpty(a.ShadowedBy), ts(a.JudgedAt), a.DedupeKey)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var exists int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM probes WHERE dedupe_key = ?`, a.DedupeKey).Scan(&exists); err != nil {
			return false, err
		}
		if exists == 0 {
			return false, ErrNoSuchRow
		}
		return false, nil
	}
	if _, err := tx.Exec(`INSERT INTO probe_amendments (dedupe_key, from_classification, to_classification, reason, shadowed_by, judged_at, scanner_frontier)
		VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(dedupe_key) DO NOTHING`,
		a.DedupeKey, a.From, a.To, a.Reason, nullIfEmpty(a.ShadowedBy), ts(a.JudgedAt), ts(a.ScannerFrontier)); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// QueryPlan returns SQLite's plan for q, one line per step, as EXPLAIN
// QUERY PLAN prints it ("SEARCH probes USING COVERING INDEX ..."). It
// exists so tests can pin that the queries run on every request or every
// collector pass seek an index rather than walk a table: a plan regression
// is invisible on a test-sized store and only shows once the table is big.
func (s *Store) QueryPlan(ctx context.Context, q string, args ...any) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `EXPLAIN QUERY PLAN `+q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			return nil, err
		}
		out = append(out, detail)
	}
	return out, rows.Err()
}

// FullScans is the steps of a plan that read a whole table, or a whole
// index that is not partial: a "SCAN x" with no index, or a "SCAN x USING
// ... INDEX i" whose i is not in partial. Scans of the query's own CTEs
// (named in ctes), of subquery results and of a constant row are not reads
// of the store.
func FullScans(plan []string, ctes, partial []string) []string {
	skip := map[string]bool{"CONSTANT": true}
	for _, c := range ctes {
		skip[c] = true
	}
	ok := map[string]bool{}
	for _, p := range partial {
		ok[p] = true
	}
	var out []string
	for _, step := range plan {
		f := strings.Fields(step)
		if len(f) < 2 || f[0] != "SCAN" || skip[f[1]] || strings.HasPrefix(f[1], "(") {
			continue
		}
		if i := len(f) - 1; len(f) > 3 && f[i-1] == "INDEX" && ok[f[i]] {
			continue
		}
		out = append(out, step)
	}
	return out
}
