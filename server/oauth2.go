package server

import (
	"net/http"

	"golang.org/x/oauth2"
)

// OAuth2Provider are the common parameters for all providers (RFC 6749)
type OAuth2Provider interface {
	// ID is issued to the registered client by the authorization (RFC 6749 Section 2.2)
	ID() string
	// Secret associated is with the ID (Section 2.2)
	Secret() string
	// Scopes is used by the authorization server to "scope" responses (Section 3.3)
	Scopes() []string
	// Config is the OAuth2 configuration settings for this provider
	Config() *oauth2.Config
	// PrincipalID with fetch the identifier to be associated with the principal.
	PrincipalID(provider *http.Client) (string, error)
}
