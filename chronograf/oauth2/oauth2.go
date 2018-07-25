package oauth2

import (
	"context"
	"errors"
	"net/http"
	"time"

	gojwt "github.com/dgrijalva/jwt-go"
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
	Subject      string
	Issuer       string
	Organization string
	Group        string
	ExpiresAt    time.Time
	IssuedAt     time.Time
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
	// Group is a comma delimited list of groups and organizations for a provider
	// TODO: This will break if there are any group names that contain commas.
	//       I think this is okay, but I'm not 100% certain.
	Group(provider *http.Client) (string, error)
}

// Mux is a collection of handlers responsible for servicing an Oauth2 interaction between a browser and a provider
type Mux interface {
	Login() http.Handler
	Logout() http.Handler
	Callback() http.Handler
}

// Authenticator represents a service for authenticating users.
type Authenticator interface {
	// Validate returns Principal associated with authenticated and authorized
	// entity if successful.
	Validate(context.Context, *http.Request) (Principal, error)
	// Authorize will grant privileges to a Principal
	Authorize(context.Context, http.ResponseWriter, Principal) error
	// Extend will extend the lifetime of a already validated Principal
	Extend(context.Context, http.ResponseWriter, Principal) (Principal, error)
	// Expire revokes privileges from a Principal
	Expire(http.ResponseWriter)
}

// Token represents a time-dependent reference (i.e. identifier) that maps back
// to the sensitive data through a tokenization system
type Token string

// Tokenizer substitutes a sensitive data element (Principal) with a
// non-sensitive equivalent, referred to as a token, that has no extrinsic
// or exploitable meaning or value.
type Tokenizer interface {
	// Create issues a token at Principal's IssuedAt that lasts until Principal's ExpireAt
	Create(context.Context, Principal) (Token, error)
	// ValidPrincipal checks if the token has a valid Principal and requires
	// a lifespan duration to ensure it complies with possible server runtime arguments.
	ValidPrincipal(ctx context.Context, token Token, lifespan time.Duration) (Principal, error)
	// ExtendedPrincipal adds the extention to the principal's lifespan.
	ExtendedPrincipal(ctx context.Context, principal Principal, extension time.Duration) (Principal, error)
	// GetClaims returns a map with verified claims
	GetClaims(tokenString string) (gojwt.MapClaims, error)
}
