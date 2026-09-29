package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"
	"go.yaml.in/yaml/v3"

	"github.com/celestiaorg/celestia-app/v10/pkg/appconsts"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// The publisher side of Fibre. Everything on these endpoints is a count of
// something the chain recorded, in the payments table the scanner fills from
// MsgPayForFibre, MsgPaymentPromiseTimeout, MsgDepositToEscrow,
// MsgRequestWithdrawal and the begin-block withdrawal payout. Nothing here
// was measured by this observer, and the response says so.
//
// Two honesty rules shape the numbers:
//
//   - A settlement amount is not in any chain event. It is recomputed from
//     the promise's blob_size with the module's own gas formula, which is
//     what the module charges. PriceFormula publishes that formula.
//   - A promise that was handed out and never settled leaves no trace on
//     chain until someone submits its timeout, so "timed out" is a floor on
//     abandoned promises, never a total. The settlement rate is stated over
//     settlements plus known timeouts, and its denominator says so.

// MiB is the unit "paid per MiB" is quoted in.
const MiB = 1 << 20

// priceFormula is the module's charge, published so a reader can recompute
// every fee on the site from a blob size.
type priceFormula struct {
	BaseGas     uint64 `json:"base_gas"`
	GasPerChunk uint64 `json:"gas_per_chunk"`
	ChunkBytes  uint64 `json:"chunk_bytes"`
	UtiaPerGas  uint64 `json:"utia_per_gas"`
	Note        string `json:"note"`
}

var formula = priceFormula{
	BaseGas:     uint64(appconsts.PFBFibreGasFixedCost),
	GasPerChunk: uint64(appconsts.PFBFibreGasPerChunk),
	ChunkBytes:  uint64(appconsts.PFBFibreChunkSize),
	UtiaPerGas:  1,
	Note:        "fee = (base_gas + gas_per_chunk × ⌈blob_size / chunk_bytes⌉) × utia_per_gas; the same charge whether the promise is settled or timed out",
}

// sum is a count and a total, the shape every money figure here takes.
type sum struct {
	Count int64 `json:"count"`
	Utia  int64 `json:"utia"`
}

type dayBucket struct {
	Day          string `json:"day"` // YYYY-MM-DD, UTC
	FeesUtia     int64  `json:"fees_utia"`
	Bytes        int64  `json:"bytes"`
	Settlements  int64  `json:"settlements"`
	Timeouts     int64  `json:"timeouts"`
	TimedOutUtia int64  `json:"timed_out_utia"`
}

// hourBucket is one UTC hour of settlements, for the one-day charts: a day
// of daily buckets is one bar.
type hourBucket struct {
	Hour        string `json:"hour"` // YYYY-MM-DDTHH, UTC
	Bytes       int64  `json:"bytes"`
	Settlements int64  `json:"settlements"`
	// FeesUtia is what those settlements paid, as a day bucket's fees_utia.
	FeesUtia int64 `json:"fees_utia"`
}

// dayPublisher is one publisher's share of one day, for the stacked daily
// chart: the window's top five publishers by fees keep their identity, the
// rest fold into one "other" row per day (publisher empty).
type dayPublisher struct {
	Day         string `json:"day"`
	Publisher   string `json:"publisher"`
	Label       string `json:"label,omitempty"`
	FeesUtia    int64  `json:"fees_utia"`
	Bytes       int64  `json:"bytes"`
	Settlements int64  `json:"settlements"`
}

// hourPublisher is one publisher's share of one UTC hour, for the one-day
// stacked chart, split as dayPublisher splits a day (splitByPublisher).
type hourPublisher struct {
	Hour        string `json:"hour"`
	Publisher   string `json:"publisher"`
	Label       string `json:"label,omitempty"`
	FeesUtia    int64  `json:"fees_utia"`
	Bytes       int64  `json:"bytes"`
	Settlements int64  `json:"settlements"`
}

// publisherShare is one slice of the top-N breakdown.
type publisherShare struct {
	Publisher   string   `json:"publisher"` // empty for the "other" bucket
	Label       string   `json:"label,omitempty"`
	FeesUtia    int64    `json:"fees_utia"`
	FeesShare   *float64 `json:"fees_share"`
	Bytes       int64    `json:"bytes"`
	BytesShare  *float64 `json:"bytes_share"`
	Settlements int64    `json:"settlements"`
	// Publishers is how many accounts the "other" bucket folds together.
	Publishers int64 `json:"publishers,omitempty"`
}

type marketResponse struct {
	Window     Window `json:"window"`
	Vantage    string `json:"vantage"`
	ComputedAt string `json:"computed_at,omitempty"`
	// RecordThrough is the point of the chain these figures rest on.
	RecordThrough *recordThrough `json:"record_through,omitempty"`
	ComputeMs     int64          `json:"compute_ms,omitempty"`
	// Source says where every number on this response comes from.
	Source string `json:"source"`

	Settlements int64 `json:"settlements"`
	// Blobs is how many distinct blobs those settlements paid for, over the
	// same window. A blob is addressed by its BlobID, blob_version ||
	// commitment, so one uploaded and paid for twice is one blob and two
	// settlements. A settlement whose publication is not recorded counts as a
	// blob of its own.
	Blobs            int64    `json:"blobs"`
	FeesSettledUtia  int64    `json:"fees_settled_utia"`
	Bytes            int64    `json:"bytes"`
	PublishersActive int64    `json:"publishers_active"`
	PaidPerMiBUtia   *float64 `json:"paid_per_mib_utia"` // fees / (bytes / MiB); null with no bytes
	// Timeouts is the floor described above; TimedOutUtia what those
	// promises were charged.
	Timeouts       int64 `json:"timeouts"`
	TimedOutUtia   int64 `json:"timed_out_utia"`
	SettlementRate Rate  `json:"settlement_rate"` // settlements / (settlements + timeouts)
	// TimeoutProcessors is how many distinct accounts submitted a timeout.
	TimeoutProcessors int64 `json:"timeout_processors"`

	Deposits             sum `json:"deposits"`
	WithdrawalsRequested sum `json:"withdrawals_requested"`
	WithdrawalsExecuted  sum `json:"withdrawals_executed"`
	// EscrowHeldUtia is the sum of every known publisher's current balance,
	// from state queries; EscrowAccounts how many were polled.
	EscrowHeldUtia int64 `json:"escrow_held_utia"`
	EscrowAccounts int64 `json:"escrow_accounts"`
	// EscrowTotalUtia is the x/fibre module account's balance: every
	// escrow on the chain, whether or not its owner ever published, read
	// at EscrowTotalAt. Absent until the collector has read it once.
	EscrowTotalUtia *int64  `json:"escrow_total_utia,omitempty"`
	EscrowTotalAt   *string `json:"escrow_total_at,omitempty"`
	// WithdrawalQueue is the queue read from state (withdrawals.go): pending
	// now, outcomes and payout delays over the window. Absent on an as_of
	// request, because the queue is not kept as a series of past states.
	WithdrawalQueue *withdrawalQueue `json:"withdrawal_queue,omitempty"`

	Daily []dayBucket `json:"daily"`
	// Hourly is set for windows of a day or less, where a daily chart is one
	// bar.
	Hourly []hourBucket `json:"hourly,omitempty"`
	// HourlyByPub is Hourly split by publisher as DailyByPub splits Daily,
	// set with it.
	HourlyByPub  []hourPublisher  `json:"hourly_by_publisher,omitempty"`
	DailyByPub   []dayPublisher   `json:"daily_by_publisher"`
	Top          []publisherShare `json:"top_publishers"`
	Other        *publisherShare  `json:"other_publishers"`
	PriceFormula priceFormula     `json:"price_formula"`
	Notes        []string         `json:"notes"`
	// LargestPoster is the publisher with the most bytes in the window.
	LargestPoster *publisherShare `json:"largest_poster"`
	// Namespaces is how many namespaces the window's settlements used, and
	// NamespacesTotal how many any settlement on record has used.
	Namespaces      int64 `json:"namespaces"`
	NamespacesTotal int64 `json:"namespaces_total"`
	// Publishers is /v1/publishers over the same window, computed in the
	// same pass (computePublishing), so the publisher page's board and its
	// table cannot describe two moments. It is not part of /v1/market
	// (handleMarket drops it); it is kept in the snapshot, and in its file,
	// so a restarted API serves both at once.
	Publishers []publisherRow `json:"publishers,omitempty"`
	// PublishersListed says computePublishing filled Publishers. An empty list
	// is left out of the snapshot file (omitempty), so the list alone cannot
	// tell this build's file for a window with no publisher from an older
	// build's, which never carried one; this can. Not part of /v1/market
	// either.
	PublishersListed bool `json:"publishers_listed,omitempty"`
}

var marketNotes = []string{
	"every figure here is something the chain recorded; none was measured by this observer",
	"a settlement's fee is not in any chain event; it is recomputed from blob_size with the module's own formula (price_formula)",
	"blob_size is the padded blob size the module charges for, not the payload",
	"timeouts count only promises whose timeout somebody submitted; an abandoned promise nobody reports leaves no trace, so this is a floor",
	"fees go to the fee collector and are distributed by stake; the chain records no per-validator share, so none is shown",
	"escrow balances are state reads for publishers already seen in a payment; there is no list-all query",
}

const marketSource = "x/fibre transactions and events (payments table); escrow balances by state query"

// escrowInfo is a publisher's current escrow as the chain holds it.
type escrowInfo struct {
	Found         bool   `json:"found"`
	BalanceUtia   int64  `json:"balance_utia"`
	AvailableUtia int64  `json:"available_utia"`
	Height        int64  `json:"height"`
	UpdatedAt     string `json:"updated_at"`
}

type publisherRow struct {
	Publisher   string   `json:"publisher"`
	Label       string   `json:"label,omitempty"`
	LabelSource string   `json:"label_source,omitempty"`
	Settlements int64    `json:"settlements"`
	Bytes       int64    `json:"bytes"`
	BytesShare  *float64 `json:"bytes_share"`
	FeesUtia    int64    `json:"fees_utia"`
	FeesShare   *float64 `json:"fees_share"`
	PaidPerMiB  *float64 `json:"paid_per_mib_utia"`
	AvgBlob     *float64 `json:"avg_blob_bytes"`
	LargestBlob int64    `json:"largest_blob_bytes"`
	Timeouts    int64    `json:"timeouts"`
	TimedOut    int64    `json:"timed_out_utia"`
	// FirstSeen and LastSeen are the first and last escrow movement of any
	// kind, over the whole history rather than the window.
	FirstSeen string      `json:"first_seen_at"`
	LastSeen  string      `json:"last_seen_at"`
	Escrow    *escrowInfo `json:"escrow"`
	// PendingWithdrawals is the account's withdrawal queue as last read
	// from state (withdrawals.go); null until the queue has been read.
	PendingWithdrawals *pendingSummary `json:"pending_withdrawals"`
}

// ---- label registry ----

// PublisherLabel is one entry of the operator-maintained registry: a name
// for an account, with where the name came from. The registry is a YAML
// file, checked into the deployment, and the source is published with the
// label so a reader knows it is the operator's word and not the chain's.
type PublisherLabel struct {
	Address string `yaml:"address" json:"address"`
	Label   string `yaml:"label" json:"label"`
	Source  string `yaml:"source" json:"source,omitempty"` // e.g. "self-declared", "this observer's test publisher"
	URL     string `yaml:"url" json:"url,omitempty"`
}

type labelFile struct {
	Publishers []PublisherLabel `yaml:"publishers"`
}

// LoadPublisherLabels reads the registry. A missing file is an empty registry.
func LoadPublisherLabels(path string) (map[string]PublisherLabel, error) {
	out := map[string]PublisherLabel{}
	if path == "" {
		return out, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	var f labelFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for i, l := range f.Publishers {
		l.Address = strings.TrimSpace(l.Address)
		if l.Address == "" || strings.TrimSpace(l.Label) == "" {
			return nil, fmt.Errorf("%s: publishers[%d]: address and label are required", path, i)
		}
		if _, _, err := bech32.DecodeAndConvert(l.Address); err != nil {
			return nil, fmt.Errorf("%s: publishers[%d]: %q is not a bech32 address: %v", path, i, l.Address, err)
		}
		out[l.Address] = l
	}
	return out, nil
}

func (s *Server) label(addr string) (string, string) {
	if l, ok := s.labels[addr]; ok {
		return l.Label, l.Source
	}
	return "", ""
}

// ---- queries ----

func share(part, whole int64) *float64 {
	if whole <= 0 {
		return nil
	}
	v := float64(part) / float64(whole)
	return &v
}

func perMiB(fees, bytes int64) *float64 {
	if bytes <= 0 {
		return nil
	}
	v := float64(fees) / (float64(bytes) / MiB)
	return &v
}

func (s *Server) computeMarket(ctx context.Context, win Window) (*marketResponse, error) {
	db := s.st.DB()
	r := &marketResponse{Window: win, Vantage: s.vantage, Source: marketSource, PriceFormula: formula, Notes: marketNotes, RecordThrough: s.recordThrough(ctx)}
	// Both bounds, on every query below. Without the upper one a pinned
	// window answered with the payments that arrived after the pin while
	// the response's own window said otherwise, so ?as_of= on this route
	// was not reproducible and not honest. Unpinned, End is the moment the
	// query was planned, so the bound admits everything the store holds.
	start, end := win.startArg(), win.endArg()

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(amount_utia),0), COALESCE(SUM(blob_size),0), COUNT(DISTINCT publisher)
		FROM payments WHERE kind = 'settlement' AND time >= ? AND time <= ?`, start, end).
		Scan(&r.Settlements, &r.FeesSettledUtia, &r.Bytes, &r.PublishersActive); err != nil {
		return nil, fmt.Errorf("settlements: %w", err)
	}
	r.PaidPerMiBUtia = perMiB(r.FeesSettledUtia, r.Bytes)
	if err := db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT namespace) FROM payments
		WHERE kind = 'settlement' AND namespace <> '' AND time >= ? AND time <= ?`, start, end).Scan(&r.Namespaces); err != nil {
		return nil, fmt.Errorf("namespaces: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT namespace) FROM payments
		WHERE kind = 'settlement' AND namespace <> '' AND time <= ?`, end).Scan(&r.NamespacesTotal); err != nil {
		return nil, fmt.Errorf("namespaces on record: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT COALESCE(pub.blob_version || ':' || pub.commitment, 'promise:' || pay.promise_hash))
		FROM payments pay LEFT JOIN publications pub ON pub.promise_hash = pay.promise_hash
		WHERE pay.kind = 'settlement' AND pay.time >= ? AND pay.time <= ?`, start, end).Scan(&r.Blobs); err != nil {
		return nil, fmt.Errorf("blobs: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(amount_utia),0), COUNT(DISTINCT processor)
		FROM payments WHERE kind = 'timeout' AND time >= ? AND time <= ?`, start, end).
		Scan(&r.Timeouts, &r.TimedOutUtia, &r.TimeoutProcessors); err != nil {
		return nil, fmt.Errorf("timeouts: %w", err)
	}
	r.SettlementRate = rate(r.Settlements, r.Settlements+r.Timeouts)
	for kind, dst := range map[string]*sum{
		"deposit": &r.Deposits, "withdrawal_request": &r.WithdrawalsRequested, "withdrawal_executed": &r.WithdrawalsExecuted,
	} {
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(amount_utia),0) FROM payments WHERE kind = ? AND time >= ? AND time <= ?`, kind, start, end).
			Scan(&dst.Count, &dst.Utia); err != nil {
			return nil, fmt.Errorf("%s: %w", kind, err)
		}
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(balance_utia),0) FROM escrow_accounts WHERE found = 1`).
		Scan(&r.EscrowAccounts, &r.EscrowHeldUtia); err != nil {
		return nil, fmt.Errorf("escrow: %w", err)
	}
	if v, err := s.st.Meta("escrow_module_utia"); err == nil && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			r.EscrowTotalUtia = &n
			if at, _ := s.st.Meta("escrow_module_polled_at"); at != "" {
				r.EscrowTotalAt = &at
			}
		}
	}

	if !win.AsOf {
		wq, err := s.withdrawalQueueSummary(ctx, win)
		if err != nil {
			return nil, fmt.Errorf("withdrawal queue: %w", err)
		}
		r.WithdrawalQueue = wq
	}

	if win.Span > 0 && win.Span <= 25*time.Hour {
		hrows, err := db.QueryContext(ctx, `SELECT substr(time, 1, 13) AS hour,
				COALESCE(SUM(blob_size), 0), COUNT(*), COALESCE(SUM(amount_utia), 0)
			FROM payments WHERE kind = 'settlement' AND time >= ? AND time <= ?
			GROUP BY hour ORDER BY hour`, start, end)
		if err != nil {
			return nil, fmt.Errorf("hourly: %w", err)
		}
		r.Hourly = []hourBucket{}
		for hrows.Next() {
			var h hourBucket
			if err := hrows.Scan(&h.Hour, &h.Bytes, &h.Settlements, &h.FeesUtia); err != nil {
				hrows.Close()
				return nil, err
			}
			r.Hourly = append(r.Hourly, h)
		}
		hrows.Close()
		if err := hrows.Err(); err != nil {
			return nil, err
		}
	}

	// Daily buckets, over settlements and timeouts. The day is the block
	// time's UTC date.
	rows, err := db.QueryContext(ctx, `SELECT substr(time, 1, 10) AS day,
			COALESCE(SUM(CASE WHEN kind = 'settlement' THEN amount_utia END), 0),
			COALESCE(SUM(CASE WHEN kind = 'settlement' THEN blob_size END), 0),
			SUM(CASE WHEN kind = 'settlement' THEN 1 ELSE 0 END),
			SUM(CASE WHEN kind = 'timeout' THEN 1 ELSE 0 END),
			COALESCE(SUM(CASE WHEN kind = 'timeout' THEN amount_utia END), 0)
		FROM payments WHERE kind IN ('settlement','timeout') AND time >= ? AND time <= ?
		GROUP BY day ORDER BY day`, start, end)
	if err != nil {
		return nil, fmt.Errorf("daily: %w", err)
	}
	r.Daily = []dayBucket{}
	for rows.Next() {
		var d dayBucket
		if err := rows.Scan(&d.Day, &d.FeesUtia, &d.Bytes, &d.Settlements, &d.Timeouts, &d.TimedOutUtia); err != nil {
			rows.Close()
			return nil, err
		}
		r.Daily = append(r.Daily, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Top publishers by fees, the rest folded into one bucket so the
	// breakdown always sums to the window.
	prow, err := db.QueryContext(ctx, `SELECT publisher, COUNT(*), COALESCE(SUM(amount_utia),0), COALESCE(SUM(blob_size),0)
		FROM payments WHERE kind = 'settlement' AND time >= ? AND time <= ?
		GROUP BY publisher ORDER BY SUM(amount_utia) DESC, publisher`, start, end)
	if err != nil {
		return nil, fmt.Errorf("top: %w", err)
	}
	r.Top = []publisherShare{}
	var other publisherShare
	var largest *publisherShare
	n := 0
	for prow.Next() {
		var p publisherShare
		if err := prow.Scan(&p.Publisher, &p.Settlements, &p.FeesUtia, &p.Bytes); err != nil {
			prow.Close()
			return nil, err
		}
		p.Label, _ = s.label(p.Publisher)
		p.FeesShare = share(p.FeesUtia, r.FeesSettledUtia)
		p.BytesShare = share(p.Bytes, r.Bytes)
		if largest == nil || p.Bytes > largest.Bytes {
			cp := p
			largest = &cp
		}
		if n < 5 {
			r.Top = append(r.Top, p)
		} else {
			other.Publishers++
			other.FeesUtia += p.FeesUtia
			other.Bytes += p.Bytes
			other.Settlements += p.Settlements
		}
		n++
	}
	prow.Close()
	if err := prow.Err(); err != nil {
		return nil, err
	}
	if other.Publishers > 0 {
		other.FeesShare = share(other.FeesUtia, r.FeesSettledUtia)
		other.BytesShare = share(other.Bytes, r.Bytes)
		r.Other = &other
	}
	r.LargestPoster = largest

	// Per day per top publisher, the rest of each day folded into "other".
	// Identity is fixed by the window's ranking above, so a publisher keeps
	// its slot on every day of the chart.
	top := map[string]bool{}
	for _, p := range r.Top {
		top[p.Publisher] = true
	}
	days, err := s.splitByPublisher(ctx, dayKey, start, end, top)
	if err != nil {
		return nil, fmt.Errorf("daily by publisher: %w", err)
	}
	r.DailyByPub = make([]dayPublisher, len(days))
	for i, d := range days {
		r.DailyByPub[i] = dayPublisher{Day: d.Bucket, Publisher: d.Publisher, Label: d.Label, FeesUtia: d.FeesUtia, Bytes: d.Bytes, Settlements: d.Settlements}
	}
	// The same split per hour where the window is charted by the hour, by
	// the same ranking, so the one-day chart names the same publishers.
	if r.Hourly != nil {
		hours, err := s.splitByPublisher(ctx, hourKey, start, end, top)
		if err != nil {
			return nil, fmt.Errorf("hourly by publisher: %w", err)
		}
		r.HourlyByPub = make([]hourPublisher, len(hours))
		for i, h := range hours {
			r.HourlyByPub[i] = hourPublisher{Hour: h.Bucket, Publisher: h.Publisher, Label: h.Label, FeesUtia: h.FeesUtia, Bytes: h.Bytes, Settlements: h.Settlements}
		}
	}
	return r, nil
}

// The prefixes of a block time (RFC 3339, UTC) that key a UTC day and a UTC
// hour: "2026-09-21" and "2026-09-21T14".
const (
	dayKey  = len("2006-01-02")
	hourKey = len("2006-01-02T15")
)

// publisherSlice is one publisher's share of one time bucket.
type publisherSlice struct {
	Bucket, Publisher, Label     string
	FeesUtia, Bytes, Settlements int64
}

// splitByPublisher is the window's settlements per time bucket (the first
// keyLen characters of the block time) per publisher in top, the rest of each
// bucket folded into one row with no publisher, ordered by bucket, then
// publisher. The daily and the hourly split are both this, so the two charts
// fold and order alike.
func (s *Server) splitByPublisher(ctx context.Context, keyLen int, start, end string, top map[string]bool) ([]publisherSlice, error) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT substr(time, 1, ?) AS bucket, publisher, COUNT(*), COALESCE(SUM(amount_utia),0), COALESCE(SUM(blob_size),0)
		FROM payments WHERE kind = 'settlement' AND time >= ? AND time <= ?
		GROUP BY bucket, publisher ORDER BY bucket, publisher`, keyLen, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []publisherSlice{}
	other := map[string]*publisherSlice{}
	var otherBuckets []string
	for rows.Next() {
		var p publisherSlice
		if err := rows.Scan(&p.Bucket, &p.Publisher, &p.Settlements, &p.FeesUtia, &p.Bytes); err != nil {
			return nil, err
		}
		if top[p.Publisher] {
			p.Label, _ = s.label(p.Publisher)
			out = append(out, p)
			continue
		}
		o := other[p.Bucket]
		if o == nil {
			o = &publisherSlice{Bucket: p.Bucket}
			other[p.Bucket] = o
			otherBuckets = append(otherBuckets, p.Bucket)
		}
		o.Settlements += p.Settlements
		o.FeesUtia += p.FeesUtia
		o.Bytes += p.Bytes
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, b := range otherBuckets {
		out = append(out, *other[b])
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Bucket != out[j].Bucket {
			return out[i].Bucket < out[j].Bucket
		}
		return out[i].Publisher < out[j].Publisher
	})
	return out, nil
}

// marketSnapshotCurrent vets a market snapshot read back from disk: a file
// from before computePublishing carries no publisher list, and serving it
// would answer /v1/publishers with an empty table until the warm-up
// replaced it. Likewise a day's file from before the hours carried fees and
// a publisher split: its hours would chart no fees and no publisher. Any
// settlement in an hour puts that hour in the split, so hours with no split
// are such a file.
func marketSnapshotCurrent(r *marketResponse) bool {
	return r != nil && r.PublishersListed && (len(r.Hourly) == 0 || len(r.HourlyByPub) > 0)
}

// computePublishing is the market snapshot: computeMarket and the publisher
// list over the same window, one right after the other.
//
// /v1/publishers used to be computed per request while /v1/market was served
// from a snapshot, and the publisher page shows both: its board ("largest
// publisher 93.6%") from one and its table from the other, minutes apart
// under a burst of blobs, so the page contradicted itself. Serving both from
// one snapshot, refreshed on the live lane, keeps them one moment.
func (s *Server) computePublishing(ctx context.Context, win Window) (*marketResponse, error) {
	r, err := s.computeMarket(ctx, win)
	if err != nil {
		return nil, err
	}
	rows, err := s.publisherRows(ctx, win, "")
	if err != nil {
		return nil, fmt.Errorf("publishers: %w", err)
	}
	if rows == nil {
		rows = []publisherRow{}
	}
	if err := s.attachPending(ctx, rows); err != nil {
		return nil, err
	}
	r.Publishers, r.PublishersListed = rows, true
	return r, nil
}

// publisherRows lists every publisher with a settlement, timeout, deposit or
// withdrawal in the window (only narrows to one address).
func (s *Server) publisherRows(ctx context.Context, win Window, only string) ([]publisherRow, error) {
	db := s.st.DB()
	start, end := win.startArg(), win.endArg()
	filter, args := "", []any{start}
	if only != "" {
		filter = " AND p.publisher = ?"
		args = append(args, only)
	}
	var totalFees, totalBytes int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(amount_utia),0), COALESCE(SUM(blob_size),0)
		FROM payments WHERE kind = 'settlement' AND time >= ? AND time <= ?`, start, end).Scan(&totalFees, &totalBytes); err != nil {
		return nil, err
	}
	// The window's aggregates are taken over the window's rows only, found
	// through payments_time (or payments_publisher_time for one address),
	// and a publisher is listed when it has at least one of them — the
	// same set the HAVING clause used to select. first_seen and last_seen
	// stay unbounded on purpose, because they are facts about the
	// publisher rather than about the window; they are asked per listed
	// publisher of payments_publisher_time, where MIN and MAX are one seek
	// each.
	//
	// The first cut grouped every payment ever recorded and filtered
	// afterwards, so a 24h view cost the whole history and grew by one row
	// per blob settled, on a route that is not cached.
	rows, err := db.QueryContext(ctx, publisherRowsSQL(filter), append([]any{start, end}, args[1:]...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []publisherRow
	for rows.Next() {
		var p publisherRow
		var inWindow int64
		var found, bal, avail, height sql.NullInt64
		var updated sql.NullString
		if err := rows.Scan(&p.Publisher, &p.Settlements, &p.Bytes, &p.FeesUtia, &p.LargestBlob, &p.Timeouts, &p.TimedOut,
			&p.FirstSeen, &p.LastSeen, &inWindow, &found, &bal, &avail, &height, &updated); err != nil {
			return nil, err
		}
		p.Label, p.LabelSource = s.label(p.Publisher)
		p.BytesShare = share(p.Bytes, totalBytes)
		p.FeesShare = share(p.FeesUtia, totalFees)
		p.PaidPerMiB = perMiB(p.FeesUtia, p.Bytes)
		if p.Settlements > 0 {
			v := float64(p.Bytes) / float64(p.Settlements)
			p.AvgBlob = &v
		}
		if updated.Valid {
			p.Escrow = &escrowInfo{Found: found.Int64 == 1, BalanceUtia: bal.Int64, AvailableUtia: avail.Int64, Height: height.Int64, UpdatedAt: updated.String}
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// paymentRow is one escrow movement as the API shows it.
type paymentRow struct {
	Kind        string `json:"kind"`
	Height      int64  `json:"height"`
	Time        string `json:"time"`
	TxHash      string `json:"tx_hash,omitempty"`
	Publisher   string `json:"publisher"`
	Processor   string `json:"processor,omitempty"`
	PromiseHash string `json:"promise_hash,omitempty"`
	Namespace   string `json:"namespace,omitempty"`
	BlobSize    int64  `json:"blob_size,omitempty"`
	GasUnits    int64  `json:"gas_units,omitempty"`
	AmountUtia  int64  `json:"amount_utia"`
	AvailableAt string `json:"available_at,omitempty"`
}

func (s *Server) paymentRows(ctx context.Context, where string, limit int, args ...any) ([]paymentRow, error) {
	q := `SELECT kind, height, time, tx_hash, publisher, processor, promise_hash, namespace, blob_size, gas_units, amount_utia, COALESCE(available_at, '')
		FROM payments`
	if where != "" {
		q += " WHERE " + where
	}
	q += fmt.Sprintf(" ORDER BY height DESC, tx_index DESC, msg_index DESC LIMIT %d", limit)
	rows, err := s.st.DB().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []paymentRow{}
	for rows.Next() {
		var p paymentRow
		if err := rows.Scan(&p.Kind, &p.Height, &p.Time, &p.TxHash, &p.Publisher, &p.Processor, &p.PromiseHash, &p.Namespace,
			&p.BlobSize, &p.GasUnits, &p.AmountUtia, &p.AvailableAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// blobCharge is what the payments table knows about one promise.
type blobCharge struct {
	FeeUtia   int64  `json:"fee_utia"`
	GasUnits  int64  `json:"gas_units"`
	Publisher string `json:"publisher"`
	// Settled is true when a MsgPayForFibre for this promise is in the
	// payments table; TimedOut when a MsgPaymentPromiseTimeout is. Both can
	// be false for a publication ingested before payments were recorded.
	Settled   bool   `json:"settled"`
	TimedOut  bool   `json:"timed_out"`
	Processor string `json:"processor,omitempty"` // who submitted the timeout
}

// unpaidPublicationsOf lists the publications with no settlement payment on
// record whose promise was signed by the key of addr. They are few (records
// from before payments were kept), and the key is only readable in Go.
func (s *Server) unpaidPublicationsOf(ctx context.Context, addr string) ([]string, error) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT promise_hash, signer_public_key FROM publications pub
		WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.promise_hash = pub.promise_hash AND p.kind = 'settlement')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var h string
		var key sql.NullString
		if err := rows.Scan(&h, &key); err != nil {
			return nil, err
		}
		if p, err := scan.PublisherOf(key.String); err == nil && p == addr {
			out = append(out, h)
		}
	}
	return out, rows.Err()
}

// chargesFor looks up the fee side of a set of promise hashes in one query.
func (s *Server) chargesFor(ctx context.Context, hashes []string) (map[string]*blobCharge, error) {
	out := map[string]*blobCharge{}
	if len(hashes) == 0 {
		return out, nil
	}
	args := make([]any, len(hashes))
	marks := make([]string, len(hashes))
	for i, h := range hashes {
		args[i], marks[i] = h, "?"
	}
	rows, err := s.st.DB().QueryContext(ctx, `SELECT promise_hash, kind, amount_utia, gas_units, publisher, processor
		FROM payments WHERE kind IN ('settlement','timeout') AND promise_hash IN (`+strings.Join(marks, ",")+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var h, kind, pub, proc string
		var amt, gas int64
		if err := rows.Scan(&h, &kind, &amt, &gas, &pub, &proc); err != nil {
			return nil, err
		}
		c := out[h]
		if c == nil {
			c = &blobCharge{}
			out[h] = c
		}
		c.FeeUtia, c.GasUnits, c.Publisher = amt, gas, pub
		switch kind {
		case "settlement":
			c.Settled = true
		case "timeout":
			c.TimedOut = true
			c.Processor = proc
		}
	}
	return out, rows.Err()
}

// timeoutsByAccount counts timeouts submitted per processor over the window,
// keyed by the 20 account bytes in hex so a validator's operator address
// (same bytes, different prefix) can be matched to it.
func (s *Server) timeoutsByAccount(ctx context.Context, win Window) (map[string]int64, error) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT processor, COUNT(*) FROM payments
		WHERE kind = 'timeout' AND time >= ? AND time <= ? AND processor != '' GROUP BY processor`, win.startArg(), win.endArg())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var proc string
		var n int64
		if err := rows.Scan(&proc, &n); err != nil {
			return nil, err
		}
		if k := accountKey(proc); k != "" {
			out[k] += n
		}
	}
	return out, rows.Err()
}

// accountKey is the hex of an address's bytes whatever its bech32 prefix, or
// "" when it does not decode.
func accountKey(bech string) string {
	_, b, err := bech32.DecodeAndConvert(bech)
	if err != nil || len(b) == 0 {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

// ---- handlers ----

func (s *Server) handleMarket(w http.ResponseWriter, r *http.Request) {
	win, err := parseWindow(r, time.Now())
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if win.AsOf {
		// As on /v1/network: computed on demand, rationed, and never
		// stored. The snapshot cache is keyed on the window's name alone,
		// so handing it a pinned window would file an answer about last
		// month under "24h" and serve it to everyone, on disk too.
		if !s.asOf.allow(time.Now()) {
			w.Header().Set("Retry-After", "2")
			writeErr(w, 429, "as_of requests are limited to one every two seconds")
			return
		}
		if !s.asOf.enter() {
			w.Header().Set("Retry-After", "5")
			writeErr(w, 429, "as_of computations already in flight; try again shortly")
			return
		}
		defer s.asOf.leave()
		t0 := time.Now()
		resp, err := s.computeMarket(r.Context(), win)
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		resp.ComputedAt, resp.ComputeMs = t0.UTC().Format(time.RFC3339Nano), time.Since(t0).Milliseconds()
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, 200, resp)
		return
	}
	resp, at, ms, err := s.market.get(r.Context(), s.logf(), win)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	cp := *resp
	cp.ComputedAt, cp.ComputeMs = at.UTC().Format(time.RFC3339), ms
	cp.Publishers, cp.PublishersListed = nil, false // /v1/publishers' half of the snapshot
	writeJSON(w, 200, cp)
}

func (s *Server) handlePublishers(w http.ResponseWriter, r *http.Request) {
	win, err := parseWindow(r, time.Now())
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if !win.AsOf {
		// The market snapshot's publisher list: the same pass as
		// /v1/market for this window, so the two agree (computePublishing).
		// The window is the one the rows were selected with, and
		// computed_at says when.
		snap, at, ms, err := s.market.get(r.Context(), s.logf(), win)
		if err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		rows := snap.Publishers
		if rows == nil {
			rows = []publisherRow{}
		}
		writeJSON(w, 200, map[string]any{
			"window": snap.Window, "publishers": rows, "count": len(rows),
			"source": marketSource, "price_formula": formula, "notes": marketNotes, "vantage": s.vantage,
			"computed_at": at.UTC().Format(time.RFC3339), "compute_ms": ms,
		})
		return
	}
	// A pinned window is computed on demand, unbounded work: the same ration
	// as every other route that computes an aggregate on demand.
	if !s.asOf.allow(time.Now()) {
		w.Header().Set("Retry-After", "2")
		writeErr(w, 429, "as_of requests are limited to one every two seconds")
		return
	}
	if !s.asOf.enter() {
		w.Header().Set("Retry-After", "5")
		writeErr(w, 429, "as_of computations already in flight; try again shortly")
		return
	}
	defer s.asOf.leave()
	w.Header().Set("Cache-Control", "no-store")
	rows, err := s.publisherRows(r.Context(), win, "")
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	if rows == nil {
		rows = []publisherRow{}
	}
	if err := s.attachPending(r.Context(), rows); err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"window": win, "publishers": rows, "count": len(rows),
		"source": marketSource, "price_formula": formula, "notes": marketNotes, "vantage": s.vantage,
	})
}

func (s *Server) handlePublisher(w http.ResponseWriter, r *http.Request) {
	addr := strings.TrimSpace(r.PathValue("addr"))
	if hrp, _, err := bech32.DecodeAndConvert(addr); err != nil || hrp != "celestia" {
		writeErr(w, 400, "publisher must be a celestia1... account address")
		return
	}
	now := time.Now()
	ctx := r.Context()
	win, err := parseWindow(r, now)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if win.AsOf {
		if !s.asOf.allow(now) {
			w.Header().Set("Retry-After", "2")
			writeErr(w, 429, "as_of requests are limited to one every two seconds")
			return
		}
		if !s.asOf.enter() {
			w.Header().Set("Retry-After", "5")
			writeErr(w, 429, "as_of computations already in flight; try again shortly")
			return
		}
		defer s.asOf.leave()
		w.Header().Set("Cache-Control", "no-store")
	}
	rows, err := s.publisherRows(ctx, win, addr)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	if len(rows) == 0 {
		// Not in this window; try the whole history before saying no.
		all := Window{Name: "all", End: now}
		if rows, err = s.publisherRows(ctx, all, addr); err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		if len(rows) == 0 {
			writeErr(w, 404, "no escrow movement recorded for this account")
			return
		}
		// Zero the window figures: the row was built over all time.
		p := rows[0]
		p.Settlements, p.Bytes, p.FeesUtia, p.LargestBlob, p.Timeouts, p.TimedOut = 0, 0, 0, 0, 0, 0
		p.BytesShare, p.FeesShare, p.PaidPerMiB, p.AvgBlob = nil, nil, nil, nil
		rows = []publisherRow{p}
	}
	type span struct {
		Window      Window   `json:"window"`
		Settlements int64    `json:"settlements"`
		Bytes       int64    `json:"bytes"`
		FeesUtia    int64    `json:"fees_utia"`
		Timeouts    int64    `json:"timeouts"`
		PaidPerMiB  *float64 `json:"paid_per_mib_utia"`
	}
	var spans []span
	for _, name := range []string{"24h", "7d", "30d", "all"} {
		sw := Window{Name: name, Span: windows[name], End: now}
		if sw.Span > 0 {
			sw.Start = now.Add(-sw.Span)
		}
		var sp span
		sp.Window = sw
		if err := s.st.DB().QueryRowContext(ctx, `SELECT
				COALESCE(SUM(CASE WHEN kind = 'settlement' THEN 1 ELSE 0 END), 0),
				COALESCE(SUM(CASE WHEN kind = 'settlement' THEN blob_size END), 0),
				COALESCE(SUM(CASE WHEN kind = 'settlement' THEN amount_utia END), 0),
				COALESCE(SUM(CASE WHEN kind = 'timeout' THEN 1 ELSE 0 END), 0)
			FROM payments WHERE publisher = ? AND time >= ?`, addr, sw.startArg()).
			Scan(&sp.Settlements, &sp.Bytes, &sp.FeesUtia, &sp.Timeouts); err != nil {
			s.writeInternal(w, r.URL.Path, err)
			return
		}
		sp.PaidPerMiB = perMiB(sp.FeesUtia, sp.Bytes)
		spans = append(spans, sp)
	}
	payments, err := s.paymentRows(ctx, `publisher = ?`, 100, addr)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	// The blobs this account paid for: by the charge on record, or, for a
	// publication with no payment row, by the key that signed its promise,
	// as each blob row names its publisher. Not by the submitter: anyone can
	// submit a settlement, typically an endorsing validator.
	unpaid, err := s.unpaidPublicationsOf(ctx, addr)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	where, wargs := `promise_hash IN (SELECT promise_hash FROM payments WHERE kind = 'settlement' AND publisher = ?)`, []any{addr}
	if len(unpaid) > 0 {
		where += ` OR promise_hash IN (?` + strings.Repeat(", ?", len(unpaid)-1) + `)`
		for _, h := range unpaid {
			wargs = append(wargs, h)
		}
	}
	blobs, err := s.blobRows(ctx, "("+where+")", 50, wargs...)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	if blobs == nil {
		blobs = []blobRow{}
	}
	blobs, moreBlobs := trim(blobs, 50)
	if err := s.attachPending(ctx, rows); err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	withdrawals, err := s.publisherWithdrawalDetail(ctx, addr)
	if err != nil {
		s.writeInternal(w, r.URL.Path, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"window": win, "publisher": rows[0], "windows": spans, "withdrawals": withdrawals,
		"recent_payments": payments, "recent_blobs": blobs, "recent_blobs_truncated": moreBlobs,
		"source": marketSource, "price_formula": formula, "notes": marketNotes, "vantage": s.vantage,
	})
}

// sortShares orders a breakdown by fees, then bytes, then address.
func sortShares(v []publisherShare) {
	sort.Slice(v, func(i, j int) bool {
		if v[i].FeesUtia != v[j].FeesUtia {
			return v[i].FeesUtia > v[j].FeesUtia
		}
		if v[i].Bytes != v[j].Bytes {
			return v[i].Bytes > v[j].Bytes
		}
		return v[i].Publisher < v[j].Publisher
	})
}

// publisherRowsSQL is publisherRows' query; filter narrows the window's rows
// (to one publisher) and must refer to payments as p.
func publisherRowsSQL(filter string) string {
	return `WITH w AS (
			SELECT publisher,
				SUM(CASE WHEN kind = 'settlement' THEN 1 ELSE 0 END) AS settlements,
				COALESCE(SUM(CASE WHEN kind = 'settlement' THEN blob_size END), 0) AS bytes,
				COALESCE(SUM(CASE WHEN kind = 'settlement' THEN amount_utia END), 0) AS fees,
				COALESCE(MAX(CASE WHEN kind = 'settlement' THEN blob_size END), 0) AS largest,
				SUM(CASE WHEN kind = 'timeout' THEN 1 ELSE 0 END) AS timeouts,
				COALESCE(SUM(CASE WHEN kind = 'timeout' THEN amount_utia END), 0) AS timed_out,
				COUNT(*) AS in_window
			FROM payments p
			WHERE p.time >= ? AND p.time <= ?` + filter + `
			GROUP BY publisher)
		SELECT w.publisher, w.settlements, w.bytes, w.fees, w.largest, w.timeouts, w.timed_out,
			(SELECT MIN(x.time) FROM payments x WHERE x.publisher = w.publisher),
			(SELECT MAX(x.time) FROM payments x WHERE x.publisher = w.publisher),
			w.in_window,
			e.found, e.balance_utia, e.available_utia, e.height, e.updated_at
		FROM w
		LEFT JOIN escrow_accounts e ON e.publisher = w.publisher
		ORDER BY 4 DESC, 3 DESC, w.publisher`
}
