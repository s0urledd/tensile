package assign

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// PinnedCelestiaApp identifies the exact celestia-app tree ParamsV10BlobV0 was
// read from. The assignment-relevant constants live in
// fibre/protocol_params.go and fibre/blob.go. None of them — least of all
// MinRowsPerValidator and LivenessThreshold — is exposed by any node or Fibre
// server RPC, so the only way to confirm a network matches is to diff those
// files at its release tag. See ProtocolParams and Fingerprint.
const (
	PinnedCelestiaAppVersion = "v10"
	// v10.4.0-mocha (1 October 2026), the build Mocha runs. Against
	// v10.2.0-mocha (3b77dc2f) it changes the Fibre server (connection age,
	// storage, upload admission), x/valaddr's genesis export, celestia-core
	// and the cosmos-sdk fork, and not fibre/protocol_params.go,
	// fibre/blob.go, fibre/validator, x/fibre or the shard download; reftest
	// is bit-identical across the two.
	PinnedCelestiaAppCommit = "5187d2fb5eb8bc4b534c74724882943c54253ae9"
	// PinnedCelestiaAppMajor is PinnedCelestiaAppVersion as the number the
	// chain reports in abci_info app_version. A chain above it may assign
	// rows differently from this package.
	PinnedCelestiaAppMajor uint64 = 10
)

// Fraction is a rational number. It matches cometbft/libs/math.Fraction: both
// fields are uint64 and the value is Numerator/Denominator. AssignedRows
// converts each to int64 the same way the reference does.
type Fraction struct {
	Numerator   uint64
	Denominator uint64
}

// ProtocolParams are the assignment inputs that do NOT come from the chain and
// are NOT observable at runtime. They are constants compiled into a specific
// celestia-app build:
//
//   - OriginalRows, TotalRows: fixed per blob version. Blob v0 = 4096 / 16384
//     (fibre/protocol_params.go: Rows = 1<<12, EncodingRatio = 0.25;
//     fibre/blob.go: v0 config). The blob version is on-chain (in the payment
//     promise); this K/N mapping for it is not.
//   - MinRowsPerValidator: the unique-decodability floor,
//     ProtocolParams.MinRowsPerValidator() on DefaultProtocolParams — derived
//     once at startup from a float formula, 148 for v10 defaults. Server and
//     client both hardcode it (`toml:"-"`); operators cannot change it.
//   - LivenessThreshold: DefaultProtocolParams.LivenessThreshold = {1, 3}. Also
//     `toml:"-"`, also unchangeable by operators.
//
// There is no ProtocolParams argument default anywhere in this package. The
// caller must construct or select one and is thereby asserting "I have
// confirmed the network I am observing runs a celestia-app build with these
// values".
type ProtocolParams struct {
	OriginalRows        int
	TotalRows           int
	MinRowsPerValidator int
	LivenessThreshold   Fraction
}

// ParamsV10BlobV0 is the parameter set for blob version 0 as compiled into
// celestia-app at PinnedCelestiaAppCommit.
//
// It is NOT a default and NOT selected implicitly by any function. Pass it to
// Assign explicitly, and only after confirming that the network you observe
// runs a celestia-app whose fibre/protocol_params.go and fibre/blob.go match
// the pin. If you cannot confirm that — for example the network upgraded, or
// you are on a fork — read the values from that build's source and construct
// your own ProtocolParams. A wrong MinRowsPerValidator or LivenessThreshold
// silently produces a different assignment and blames the wrong validator.
var ParamsV10BlobV0 = ProtocolParams{
	OriginalRows:        4096,
	TotalRows:           16384,
	MinRowsPerValidator: 148,
	LivenessThreshold:   Fraction{Numerator: 1, Denominator: 3},
}

// Validate rejects a params value that cannot produce a well-defined
// assignment. It says nothing about whether the values match any particular
// celestia-app build.
func (p ProtocolParams) Validate() error {
	switch {
	case p.OriginalRows <= 0:
		return &ParamsError{"OriginalRows must be positive"}
	case p.TotalRows < p.OriginalRows:
		return &ParamsError{"TotalRows must be >= OriginalRows"}
	case p.MinRowsPerValidator < 0:
		return &ParamsError{"MinRowsPerValidator must be non-negative"}
	case p.MinRowsPerValidator > p.OriginalRows:
		return &ParamsError{"MinRowsPerValidator must be <= OriginalRows"}
	case p.LivenessThreshold.Numerator == 0 || p.LivenessThreshold.Denominator == 0:
		return &ParamsError{"LivenessThreshold parts must be positive"}
	case p.LivenessThreshold.Numerator >= p.LivenessThreshold.Denominator:
		return &ParamsError{"LivenessThreshold must be < 1"}
	case p.LivenessThreshold.Numerator > 1<<62 || p.LivenessThreshold.Denominator > 1<<62:
		return &ParamsError{"LivenessThreshold parts too large"}
	default:
		return nil
	}
}

// Fingerprint is a short stable digest of the four fields. Use it to cross
// check a ProtocolParams obtained from a second source (manual inspection of a
// release, a future node endpoint, a config file) against a pin:
//
//	if got.Fingerprint() != assign.ParamsV10BlobV0.Fingerprint() {
//	    // do not trust an assignment computed with `got`
//	}
func (p ProtocolParams) Fingerprint() string {
	var b []byte
	b = appendInt(b, int64(p.OriginalRows))
	b = appendInt(b, int64(p.TotalRows))
	b = appendInt(b, int64(p.MinRowsPerValidator))
	b = appendInt(b, int64(p.LivenessThreshold.Numerator))
	b = appendInt(b, int64(p.LivenessThreshold.Denominator))
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

func appendInt(b []byte, v int64) []byte {
	return append(b, []byte(fmt.Sprintf("%d;", v))...)
}
