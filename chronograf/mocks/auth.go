package mocks

import (
	"context"
	"net/http"

	"github.com/influxdata/chronograf/oauth2"
)

// Authenticator implements a OAuth2 authenticator
type Authenticator struct {
	Principal   oauth2.Principal
	ValidateErr error
	ExtendErr   error
	Serialized  string
}

// Validate returns Principal associated with authenticated and authorized
// entity if successful.
func (a *Authenticator) Validate(context.Context, *http.Request) (oauth2.Principal, error) {
	return a.Principal, a.ValidateErr
}

// Extend will extend the lifetime of a already validated Principal
func (a *Authenticator) Extend(ctx context.Context, w http.ResponseWriter, p oauth2.Principal) (oauth2.Principal, error) {
	cookie := http.Cookie{}

	http.SetCookie(w, &cookie)
	return a.Principal, a.ExtendErr
}

// Authorize will grant privileges to a Principal
func (a *Authenticator) Authorize(ctx context.Context, w http.ResponseWriter, p oauth2.Principal) error {
	cookie := http.Cookie{}

	http.SetCookie(w, &cookie)
	return nil
}

// Expire revokes privileges from a Principal
func (a *Authenticator) Expire(http.ResponseWriter) {}

// ValidAuthorization returns the Principal
func (a *Authenticator) ValidAuthorization(ctx context.Context, serializedAuthorization string) (oauth2.Principal, error) {
	return oauth2.Principal{}, nil
}

// Serialize the serialized values stored on the Authenticator
func (a *Authenticator) Serialize(context.Context, oauth2.Principal) (string, error) {
	return a.Serialized, nil
}
