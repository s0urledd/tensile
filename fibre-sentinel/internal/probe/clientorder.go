package probe

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/celestiaorg/celestia-app/v10/fibre/validator"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	cmtmath "github.com/cometbft/cometbft/libs/math"
	core "github.com/cometbft/cometbft/types"

	"github.com/plsgiveup/fibre/fibre-sentinel/internal/scan"
)

// ClientOrder is the order celestia-app's Fibre client asks validators in
// for a blob, and the rows it expects from each: validator.Set.Select over
// the whole set at the promise height (fibre/download.go, downloadBlob),
// called here rather than copied. Addresses are lower-case hex. The order
// is shuffled by stake afresh on every call, as the client's is.
//
// The expected rows are the set's own row count for each validator
// (Set.AssignedRows), which is what fibre-assign computes as its row count;
// a test holds the two together.
func ClientOrder(members []scan.ValSetMember, height int64, pp scan.ProtocolParamsSnapshot) (order []string, expected map[string]int, err error) {
	if pp.OriginalRows <= 0 || pp.LivenessThresholdDen == 0 || pp.LivenessThresholdNum == 0 {
		return nil, nil, fmt.Errorf("protocol params without original rows or a liveness threshold")
	}
	vals := make([]*core.Validator, 0, len(members))
	for _, m := range members {
		if len(m.PubKey) != cmted25519.PubKeySize {
			return nil, nil, fmt.Errorf("validator %x has no ed25519 consensus key", m.Address)
		}
		vals = append(vals, core.NewValidator(cmted25519.PubKey(append([]byte(nil), m.PubKey...)), m.VotingPower))
	}
	var set *core.ValidatorSet
	func() {
		// NewValidatorSet panics on a set it will not build (a duplicate,
		// an overflow); that is a set this observer cannot order, not a
		// reason to stop reading.
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("validator set: %v", r)
			}
		}()
		set = core.NewValidatorSet(vals)
	}()
	if err != nil {
		return nil, nil, err
	}
	if set == nil || len(set.Validators) == 0 {
		return nil, nil, errors.New("empty validator set")
	}
	sel := validator.Set{ValidatorSet: set, Height: uint64(height)}.Select(pp.OriginalRows, pp.MinRowsPerValidator,
		cmtmath.Fraction{Numerator: pp.LivenessThresholdNum, Denominator: pp.LivenessThresholdDen})
	order = make([]string, 0, len(sel))
	expected = make(map[string]int, len(sel))
	for _, s := range sel {
		a := strings.ToLower(hex.EncodeToString(s.Address))
		order = append(order, a)
		expected[a] = s.ExpectedRows
	}
	return order, expected, nil
}
