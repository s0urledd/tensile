package scan

import (
	"encoding/hex"
	"testing"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
)

// The publisher read back from a stored promise key is the account the
// payments are charged to: the same derivation, from the same key.
func TestPublisherOfIsThePaymentsAccount(t *testing.T) {
	key := secp256k1.GenPrivKey().PubKey().(*secp256k1.PubKey)
	want, err := accountFromPubKey(&fibretypes.PaymentPromise{SignerPublicKey: *key})
	if err != nil {
		t.Fatal(err)
	}
	got, err := PublisherOf(hex.EncodeToString(key.Key))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("PublisherOf = %s, the payments account is %s", got, want)
	}
	for _, bad := range []string{"", "zz", hex.EncodeToString(key.Key[:32])} {
		if _, err := PublisherOf(bad); err == nil {
			t.Errorf("PublisherOf(%q) accepted a key that is not 33 bytes of hex", bad)
		}
	}
}
