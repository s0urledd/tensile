package probe

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/record"
	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// SampledOutFile is where the prober recorded a publication the load policy
// sampled out, one line per publication, beside measurements.jsonl. Nothing
// is sampled any more; the file is the record of the draws made while
// publications were, read by the collector and sentinel-recompute.
//
// A sampled-out publication used to be written as a NOT_PROBED measurement
// for every assigned validator at every schedule point: eighty validators and
// six points is 480 rows, each carrying the same probability, binding,
// commitment and reason. Every one of those rows said the same single thing
// (this observer drew this publication out of its sample), and at Mocha's
// load they were most of what the record grew by. The decision is now
// written once, with what is needed to reproduce every row it stands for:
// the points it covered and the draw's inputs. The validators come from the
// publication record, which is where the prober took them from.
//
// It is a file of its own rather than a second kind of line in
// measurements.jsonl because every reader of that file (the prober's own
// restart index, the collector, sentinel-recompute, the confirm service, the
// backup and restore checks) takes each line to be one Measurement keyed by
// (vantage, promise, validator, scheduled_at). A different shape inside it
// would have to be taught to all of them, and one that was not taught would
// read a decision as a malformed probe.
const SampledOutFile = "sampling_decisions.jsonl"

// SampledOutSchemaVersion is bumped when the SampledOut JSON shape changes.
const SampledOutSchemaVersion = 1

// SampledOutKind is SampledOut.Kind: the only decision this file records is
// a whole publication drawn out of the sample.
const SampledOutKind = "sampled_out"

// SampledOutReasonPrefix starts the reason the policy gives when a whole
// publication is drawn out of the sample ("budget:p=0.286:<binding>:
// day_commitment=<hex>"). A per-validator cap denial is "budget:<cap>" with
// no p, and stays a row of its own.
const SampledOutReasonPrefix = "budget:p="

// SampledOutPoint is one schedule point a sampled-out publication would have
// been probed at, with the phase its NOT_PROBED rows were stamped with.
type SampledOutPoint struct {
	Label string    `json:"schedule_label"`
	At    time.Time `json:"scheduled_at"`
	Phase Phase     `json:"phase"`
}

// SampledOut is one publication the load policy sampled out: the record of
// the draw that decided it, standing for a NOT_PROBED row per assigned
// validator per point.
type SampledOut struct {
	SchemaVersion int    `json:"schema_version"`
	Kind          string `json:"kind"`
	Vantage       string `json:"vantage"`

	PromiseHash        string    `json:"promise_hash"`
	Commitment         string    `json:"commitment"`
	BlobVersion        uint32    `json:"blob_version"`
	SettlementTime     time.Time `json:"settlement_time"`
	MustServeUntil     time.Time `json:"must_serve_until"`
	ValidatorSetHeight int64     `json:"validator_set_height"`

	// DecidedAt is when the decision was recorded: the started_at of every
	// row it stands for.
	DecidedAt time.Time `json:"decided_at"`
	// Sampling is the draw: the probability the publication was drawn at,
	// the cap that bound it, and the commitment to the day secret. The
	// sampling audit recomputes H(promise_hash || secret) >= p * 2^64 from
	// it once the day's secret is revealed.
	Sampling SamplingDecision `json:"sampling"`
	// Reason is the policy's own explanation, the classification_reason of
	// every row the decision stands for.
	Reason string `json:"reason"`
	// Points are the schedule points the decision covers.
	Points []SampledOutPoint `json:"points"`
	// Validators is how many assigned validators the decision covers: the
	// ones in the publication record with a row count, which is what
	// Expand fans out to. Recorded so a reader can check the record it
	// expands against says the same.
	Validators int `json:"validators"`

	Observer *ObserverInfo `json:"observer,omitempty"`
}

// Key is the decision's natural key: one decision per publication per vantage.
func (d SampledOut) Key() string { return d.Vantage + "|" + d.PromiseHash }

// IsSampledOutRow reports whether m is one of the rows a sampled-out
// publication was recorded with before decisions had a record of their own.
func IsSampledOutRow(m Measurement) bool {
	return m.Classification == ClassNotProbed && strings.HasPrefix(m.ClassificationReason, SampledOutReasonPrefix)
}

// Expand is the decision as the NOT_PROBED rows it stands for: one per
// assigned validator of pub (the publication record, in its order) at every
// point, exactly as the prober wrote them before the decision was recorded
// once. Every figure computed from rows (the obligations, the class tallies,
// the gap counts) is therefore the same whether it reads the decision or the
// rows. pub must be the publication the decision names.
func (d SampledOut) Expand(pub scan.Publication) []Measurement {
	var out []Measurement
	sampling := d.Sampling
	for _, v := range pub.Assignment.Validators {
		if v.RowCount <= 0 {
			continue
		}
		for _, pt := range d.Points {
			out = append(out, Measurement{
				SchemaVersion: MeasurementSchemaVersion, Vantage: d.Vantage,
				PromiseHash: d.PromiseHash, Commitment: d.Commitment,
				BlobVersion: d.BlobVersion, MustServeUntil: d.MustServeUntil,
				ValidatorSetHeight: d.ValidatorSetHeight,
				ValidatorAddress:   v.Address,
				Assigned:           true, Attested: v.Attested, AttestationUnknown: !pub.HasAttestation(),
				AssignedRowCount: v.RowCount,
				ScheduleLabel:    pt.Label, ScheduledAt: pt.At.UTC(),
				StartedAt: d.DecidedAt.UTC(), FinishedAt: d.DecidedAt.UTC(),
				Phase: pt.Phase, Outcome: OutcomeMissed,
				Classification: ClassNotProbed, ClassificationReason: d.Reason,
				Sampling: &sampling,
			})
		}
	}
	return out
}

// LoadSampledOut reads a sampling_decisions.jsonl, its archived segments
// first when it has any. A later line for a key already read is dropped:
// the file is append-only and the first decision is the one the record
// stands on. The decisions are one line per publication, so reading the
// whole record at the prober's start stays small; the archived ones make
// Has true for publications long finished, which is what they are.
func LoadSampledOut(path string) ([]SampledOut, error) {
	f, err := record.OpenAll(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<26)
	var out []SampledOut
	seen := map[string]bool{}
	n := 0
	for sc.Scan() {
		n++
		if len(sc.Bytes()) == 0 {
			continue
		}
		var d SampledOut
		if err := json.Unmarshal(sc.Bytes(), &d); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", path, n, err)
		}
		if d.PromiseHash == "" || seen[d.Key()] {
			continue
		}
		seen[d.Key()] = true
		out = append(out, d)
	}
	return out, sc.Err()
}
