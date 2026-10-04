package api

import (
	"context"
	"database/sql"
	"errors"
)

// endpointCheck is the newest heartbeat against one validator's Fibre host,
// stage by stage, with the error the handshake ended on. It is what an
// operator setting up a server needs to see — DNS, TCP, TLS or the identity
// in the certificate — and the dashboard is the only place outside their own
// machine that can show it.
type endpointCheck struct {
	At             string `json:"at"`
	Host           string `json:"host"`
	Outcome        string `json:"outcome"`
	DNSOK          bool   `json:"dns_ok"`
	TCPOK          bool   `json:"tcp_ok"`
	TCPMS          int64  `json:"tcp_ms"`
	TLSOK          bool   `json:"tls_ok"`
	TLSMS          int64  `json:"tls_ms"`
	IdentityOK     bool   `json:"identity_ok"`
	IdentityReason string `json:"identity_reason,omitempty"`
	RawError       string `json:"raw_error,omitempty"`
	Vantage        string `json:"vantage"`
}

// lastEndpointCheck is the newest reachability row for addr up to the
// window's end (so ?as_of= shows the check as it stood then), nil when the
// validator was never checked. It is this observer's own row: when another
// vantage's check confirms the endpoint up (validatorRow.ConfirmedFrom), this
// is still what failed from here.
func (s *Server) lastEndpointCheck(ctx context.Context, addr string, win Window) (*endpointCheck, error) {
	var c endpointCheck
	var dns, tcp, tls, id int
	err := s.q(ctx).QueryRowContext(ctx, `SELECT started_at, validator_host, outcome, dns_ok, tcp_ok, tcp_ms, tls_ok, tls_ms,
			identity_ok, identity_reason, raw_error, vantage
		FROM reachability WHERE validator_address = ? AND started_at <= ? AND +vantage = ?
		ORDER BY started_at DESC LIMIT 1`, addr, win.endArg(), s.vantage).
		Scan(&c.At, &c.Host, &c.Outcome, &dns, &tcp, &c.TCPMS, &tls, &c.TLSMS, &id, &c.IdentityReason, &c.RawError, &c.Vantage)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.DNSOK, c.TCPOK, c.TLSOK, c.IdentityOK = dns == 1, tcp == 1, tls == 1, id == 1
	return &c, nil
}
