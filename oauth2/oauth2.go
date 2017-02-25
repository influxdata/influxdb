package oauth2

import (
	"context"
	"errors"
	"net/http"
	"time"

	"golang.org/x/oauth2"
)

type principalKey string

func (p principalKey) String() string {
	return string(p)
}

var (
	// PrincipalKey is used to pass principal
	// via context.Context to request-scoped
	// functions.
	PrincipalKey = principalKey("principal")
	// ErrAuthentication means that oauth2 exchange failed
	ErrAuthentication = errors.New("user not authenticated")
	// ErrOrgMembership means that the user is not in the OAuth2 filtered group
	ErrOrgMembership = errors.New("Not a member of the required organization")
)

/* Types */

// Principal is any entity that can be authenticated
type Principal struct {
	Subject string
	Issuer  string
}

/* Interfaces */

// Provider are the common parameters for all providers (RFC 6749)
type Provider interface {
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

	// Name is the name of the Provider
	Name() string
}

// Mux is a collection of handlers responsible for servicing an Oauth2 interaction between a browser and a provider
type Mux interface {
	Login() http.Handler
	Logout() http.Handler
	Callback() http.Handler
}

// Authenticator represents a service for authenticating users.
type Authenticator interface {
	// Authenticate returns User associated with token if successful.
	Authenticate(ctx context.Context, token string) (Principal, error)
	// Token generates a valid token for Principal lasting a duration
	Token(context.Context, Principal, time.Duration) (string, error)
}

// TokenExtractor extracts tokens from http requests
type TokenExtractor interface {
	// Extract will return the token or an error.
	Extract(r *http.Request) (string, error)
}
