package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/cosmos/cosmos-sdk/types/bech32"
)

// Operator and account addresses, resolved to the consensus address.
//
// Every row this observer keeps is keyed by the 20-byte consensus address,
// because that is what a promise's signature set and x/valaddr name. An
// operator does not think in that address: the one they paste is the
// celestiavaloper1… they see in every explorer, or the celestia1… account they
// sign transactions with. parseAddr used to refuse both with a 400, which was
// correct — the bytes of an operator address are not a consensus address, and
// looking them up as one silently matches nothing — but it left the operator
// with an error instead of their page.
//
// The two are not derivable from each other: the consensus address hashes the
// consensus public key, the operator address the account key. The mapping is
// the staking module's, and the store already has it: validator_identities
// carries both for every validator the collector has read (store.go,
// migration 4). So an operator or account address is resolved by lookup, and
// one the staking set has never named is a 404 that says so, never a 400 that
// calls a real address malformed.

// errAddrFormat is the 400 every route that takes a validator address gives
// for a string that is none of the accepted forms.
const errAddrFormat = "address must be a consensus address (40 hex chars or celestiavalcons1…), an operator address (celestiavaloper1…) or the operator's account address (celestia1…)"

// operatorForm returns the celestiavaloper1… spelling of an operator or
// account address, or ok=false when s is not bech32 or carries any other
// prefix. The account and operator addresses of one validator are the same
// twenty bytes under two prefixes, so both reduce to one lookup key.
func operatorForm(s string) (string, bool) {
	hrp, raw, err := bech32.DecodeAndConvert(strings.TrimSpace(strings.ToLower(s)))
	if err != nil || len(raw) != 20 {
		return "", false
	}
	var valoper string
	switch {
	case strings.HasSuffix(hrp, "valoper"):
		valoper = hrp
	case !strings.Contains(hrp, "val") && !strings.HasSuffix(hrp, "pub"):
		// An account prefix ("celestia"): the operator prefix is the account
		// prefix plus "valoper", the SDK's own convention. Anything with
		// "val" in it that is not valoper (valcons, valconspub) is not an
		// operator and never gets here as one.
		valoper = hrp + "valoper"
	default:
		return "", false
	}
	out, err := bech32.ConvertAndEncode(valoper, raw)
	if err != nil {
		return "", false
	}
	return out, true
}

// addrError is a failed resolution with the status it should be answered
// with: 400 for a string that is no validator address at all, 404 for a real
// operator or account address the staking set has not named.
type addrError struct {
	status int
	msg    string
}

func (e *addrError) Error() string { return e.msg }

// resolveAddr turns any accepted spelling of a validator into its consensus
// address in lower-case hex: the consensus forms directly (parseAddr), the
// operator and account forms through validator_identities.
func (s *Server) resolveAddr(ctx context.Context, raw string) (string, error) {
	if addr, err := parseAddr(raw); err == nil {
		return addr, nil
	}
	op, ok := operatorForm(raw)
	if !ok {
		return "", &addrError{400, errAddrFormat}
	}
	var cons string
	// operator_address is written exactly as the chain prints it, which is
	// lower case, so an equality seeks rather than scanning with lower().
	// Two identities can share one; currentIdentity picks between them.
	err := s.st.DB().QueryRowContext(ctx, `SELECT cons_address FROM validator_identities
		WHERE operator_address = ? ORDER BY `+currentIdentity+` LIMIT 1`, op).Scan(&cons)
	if errors.Is(err, sql.ErrNoRows) {
		return "", &addrError{404, fmt.Sprintf("no validator with operator address %s is in the staking set this observer has read", op)}
	}
	if err != nil {
		return "", err
	}
	return strings.ToLower(cons), nil
}

// currentIdentity orders the identities that share one operator address so
// that the one the address stands for today comes first. validator_identities
// is only ever upserted, so a validator that unbonds fully, is removed from
// the staking set and is created again under the same operator with a new
// consensus key leaves its old consensus address behind under the same
// operator address. The collector refreshes updated_at on every validator
// the staking set lists at each poll, so the newest row is the one the chain
// names now; the consensus address only settles a tie, which one poll
// cannot produce.
const currentIdentity = "updated_at DESC, cons_address DESC"

// operatorAddrs maps each consensus address the staking set names (lower-case
// hex, the key every row here carries) to its operator address. The rows of
// a reading, a blob's readings and the feeds' links name a validator by the
// consensus address they are keyed by; a reader knows it by the
// celestiavaloper1… every explorer shows, so an answer that names validators
// carries that too, looked up here once per answer rather than once per row.
// A validator the collector has not read, or one with no operator address on
// record, is absent, and its rows carry none.
//
// An operator address goes only to the consensus address it resolves to
// (resolveAddr, by currentIdentity). A superseded consensus address is
// absent, so its rows carry no operator address and a link to its page
// falls back to the consensus address: a link by the operator address would
// open the newer validator's page, and the older one's page and history
// could only be reached by typing its hex address.
func (s *Server) operatorAddrs(ctx context.Context) (map[string]string, error) {
	rows, err := s.st.DB().QueryContext(ctx, `SELECT cons_address, operator_address FROM validator_identities
		WHERE operator_address <> '' ORDER BY operator_address, `+currentIdentity)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	named := map[string]bool{}
	for rows.Next() {
		var cons, op string
		if err := rows.Scan(&cons, &op); err != nil {
			return nil, err
		}
		if named[op] {
			continue // superseded: an older consensus key of the same operator
		}
		named[op] = true
		out[strings.ToLower(cons)] = op
	}
	return out, rows.Err()
}

// withOperators sets each reading's operator address from ops.
func withOperators(rows []probeRow, ops map[string]string) {
	for i := range rows {
		rows[i].OperatorAddress = ops[strings.ToLower(rows[i].ValidatorAddress)]
	}
}

// writeAddrErr answers a failed resolveAddr: its own status for an addrError,
// 500 for anything else (a store error is never the caller's fault).
func (s *Server) writeAddrErr(w http.ResponseWriter, path string, err error) {
	var ae *addrError
	if errors.As(err, &ae) {
		writeErr(w, ae.status, ae.msg)
		return
	}
	s.writeInternal(w, path, err)
}
