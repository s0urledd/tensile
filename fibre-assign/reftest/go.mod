// Nested module: NOT part of the shippable fibre-assign package. It exists only
// to run the differential test against celestia-app's real
// fibre/validator.Set.Assign. `cd fibre-assign && go test ./...` does not
// descend here, and consumers of fibre-assign never pull celestia-app.
//
// celestia-app is a normal pinned dependency, fetched from the module proxy at
// the pseudo-version for PinnedCelestiaAppCommit — no local checkout needed.
// Its own replace block is copied verbatim below: Go does not apply a
// dependency's replace directives, so a build of this module has to repeat
// them. Refresh both the pin and the block together.
module github.com/plsgiveup/fibre/fibre-assign/reftest

go 1.26.6

require (
	github.com/celestiaorg/celestia-app/v10 v10.4.0-mocha
	github.com/cometbft/cometbft v1.0.1
	github.com/plsgiveup/fibre/fibre-assign v0.0.0
)

require (
	github.com/celestiaorg/go-square/v3 v3.0.2 // indirect
	github.com/celestiaorg/nmt v0.24.5 // indirect
	github.com/cosmos/gogoproto v1.7.2 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.0 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.6.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/reedsolomon v1.14.2 // indirect
	github.com/oasisprotocol/curve25519-voi v0.0.0-20230904125328-1f23a7beb09a // indirect
	github.com/petermattis/goid v0.0.0-20250813065127-a731cc31b4fe // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sasha-s/go-deadlock v0.3.9 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/exp v0.0.0-20260410095643-746e56fc9e2f // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260825221802-da73d73af1c5 // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

// The parent module, one directory up in the same repo.
replace github.com/plsgiveup/fibre/fibre-assign => ../

// Copied verbatim from celestia-app/go.mod at PinnedCelestiaAppCommit.
replace (
	cosmossdk.io/api => github.com/celestiaorg/cosmos-sdk/api v0.7.7
	cosmossdk.io/log => github.com/celestiaorg/cosmos-sdk/log v1.3.0
	cosmossdk.io/store => github.com/celestiaorg/cosmos-sdk/store v1.1.3-celestia.3
	cosmossdk.io/x/evidence => github.com/celestiaorg/cosmos-sdk/x/evidence v0.1.2-celestia
	cosmossdk.io/x/tx => github.com/celestiaorg/cosmos-sdk/x/tx v0.13.9
	cosmossdk.io/x/upgrade => github.com/celestiaorg/cosmos-sdk/x/upgrade v0.2.0
	github.com/bcp-innovations/hyperlane-cosmos => github.com/celestiaorg/hyperlane-cosmos v1.3.0
	github.com/cometbft/cometbft => github.com/celestiaorg/celestia-core v0.42.3
	github.com/cosmos/cosmos-sdk => github.com/celestiaorg/cosmos-sdk v0.52.12
	github.com/cosmos/ibc-go/v8 => github.com/celestiaorg/ibc-go/v8 v8.7.2
	github.com/cosmos/ledger-cosmos-go => github.com/cosmos/ledger-cosmos-go v0.16.0
	github.com/syndtr/goleveldb => github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
	github.com/tendermint/tendermint => github.com/celestiaorg/celestia-core v1.55.0-tm-v0.34.35
)
