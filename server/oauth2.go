package server

import "github.com/influxdata/chronograf"

// OAuth2Provider are the common parameters for all providers (RFC 6749)
type OAuth2Provider interface {
	// ID is issued to the registered client by the authorization (RFC 6749 Section 2.2)
	ID() string
	// Secret associated is with the ID (Section 2.2)
	Secret() string
	// Scopes is used by the authorization server to "scope" responses (Section 3.3)
	Scopes() []string
	// Authenticator generates and validates tokens
	Authenticator() chronograf.Authenticator
}
