package chronograf

import (
	"net/http"
	"time"

	"golang.org/x/net/context"
)

// Principal is any entity that can be authenticated
type Principal string

// PrincipalKey is used to pass principal
// via context.Context to request-scoped
// functions.
const PrincipalKey Principal = "principal"

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
