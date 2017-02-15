package oauth2

import (
	"context"
	"net/http"
	"strings"

	"github.com/influxdata/chronograf"
)

// CookieExtractor extracts the token from the value of the Name cookie.
type CookieExtractor struct {
	Name string
}

// Extract returns the value of cookie Name
func (c *CookieExtractor) Extract(r *http.Request) (string, error) {
	cookie, err := r.Cookie(c.Name)
	if err != nil {
		return "", ErrAuthentication
	}
	return cookie.Value, nil
}

// BearerExtractor extracts the token from Authorization: Bearer header.
type BearerExtractor struct{}

// Extract returns the string following Authorization: Bearer
func (b *BearerExtractor) Extract(r *http.Request) (string, error) {
	s := r.Header.Get("Authorization")
	if s == "" {
		return "", ErrAuthentication
	}

	// Check for Bearer token.
	strs := strings.Split(s, " ")

	if len(strs) != 2 || strs[0] != "Bearer" {
		return "", ErrAuthentication
	}
	return strs[1], nil
}

// AuthorizedToken extracts the token and validates; if valid the next handler
// will be run.  The principal will be sent to the next handler via the request's
// Context.  It is up to the next handler to determine if the principal has access.
// On failure, will return http.StatusUnauthorized.
func AuthorizedToken(auth Authenticator, te TokenExtractor, logger chronograf.Logger, next http.Handler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := logger.
			WithField("component", "auth").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		token, err := te.Extract(r)
		if err != nil {
			// Happens when Provider okays authentication, but Token is bad
			log.Info("Unauthenticated user")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// We do not check the validity of the principal.  Those
		// served further down the chain should do so.
		principal, err := auth.Authenticate(r.Context(), token)
		if err != nil {
			log.Error("Invalid token")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Send the principal to the next handler
		ctx := context.WithValue(r.Context(), PrincipalKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
		return
	})
}
