package assign

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
)

// Address is a 20-byte consensus (Tendermint) validator address:
// the first 20 bytes of the SHA-256 of the ed25519 consensus public key.
type Address [20]byte

func (a Address) String() string { return hex.EncodeToString(a[:]) }

// AddressFromEd25519PubKey derives the consensus Address from a 32-byte ed25519
// consensus public key, matching cometbft crypto/ed25519 PubKey.Address()
// (SHA-256 truncated to 20 bytes).
func AddressFromEd25519PubKey(pub []byte) (Address, error) {
	if len(pub) != 32 {
		return Address{}, fmt.Errorf("fibre assign: ed25519 pubkey must be 32 bytes, got %d", len(pub))
	}
	sum := sha256.Sum256(pub)
	var a Address
	copy(a[:], sum[:20])
	return a, nil
}

// MustAddressFromHex parses a 40-hex-char consensus address. Panics on bad
// input — for tests and constants only.
func MustAddressFromHex(s string) Address {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 20 {
		panic(fmt.Sprintf("fibre assign: bad address hex %q", s))
	}
	var a Address
	copy(a[:], b)
	return a
}

// Validator is one member of the consensus validator set at the blob's promise
// height. Only the address and voting power affect assignment.
type Validator struct {
	// Address is the 20-byte consensus address.
	Address Address
	// VotingPower is the cometbft consensus voting power (staking-reduced),
	// as returned by the /validators RPC or the BlockAPI ValidatorSet endpoint
	// for the promise height.
	VotingPower int64
}

// canonicalOrder returns a copy of vals in the order core.ValidatorSet keeps
// its members: voting power descending, ties broken by address ascending
// (bytes.Compare). This is the order Set.Assign walks. It also reports a
// duplicate address if present. The check is by address over the whole set,
// not between sort neighbours: two entries for one address with different
// voting power are not adjacent once another validator's power falls between
// them.
func canonicalOrder(vals []Validator) ([]Validator, *Address) {
	seen := make(map[Address]struct{}, len(vals))
	for _, v := range vals {
		if _, ok := seen[v.Address]; ok {
			dup := v.Address
			return nil, &dup
		}
		seen[v.Address] = struct{}{}
	}
	out := slices.Clone(vals)
	slices.SortFunc(out, func(a, b Validator) int {
		if a.VotingPower != b.VotingPower {
			if a.VotingPower > b.VotingPower {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.Address[:], b.Address[:])
	})
	return out, nil
}
