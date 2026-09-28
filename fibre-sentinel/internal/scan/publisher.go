package scan

import (
	"encoding/hex"
	"fmt"

	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	"github.com/cosmos/cosmos-sdk/types/bech32"
)

// PublisherOf is the account a promise charges, its escrow owner, from the
// hex key that signed the promise (PromiseFields.SignerPublicKey). The module
// derives the same address to find the escrow (x/fibre msg_server:
// SignerPublicKey.Address()), and so does accountFromPubKey for payments. It
// need not be the account that submitted MsgPayForFibre: anyone can submit
// one, typically an endorsing validator.
func PublisherOf(signerPublicKeyHex string) (string, error) {
	b, err := hex.DecodeString(signerPublicKeyHex)
	if err != nil {
		return "", fmt.Errorf("signer public key: %w", err)
	}
	if len(b) != secp256k1.PubKeySize {
		return "", fmt.Errorf("signer public key: %d bytes, want %d", len(b), secp256k1.PubKeySize)
	}
	return bech32.ConvertAndEncode(accountHRP, (&secp256k1.PubKey{Key: b}).Address())
}
