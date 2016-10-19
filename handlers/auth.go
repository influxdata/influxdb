package handlers

import (
	"context"
	"net/http"
	"strings"

	"github.com/influxdata/mrfusion"
)

// CookieExtractor extracts the token from the value of the Name cookie.
type CookieExtractor struct {
	Name string
}

// Extract returns the value of cookie Name
func (c *CookieExtractor) Extract(r *http.Request) (string, error) {
	cookie, err := r.Cookie(c.Name)
	if err != nil {
		return "", mrfusion.ErrAuthentication
	}
	return cookie.Value, nil
}

// BearerExtractor extracts the token from Authorization: Bearer header.
type BearerExtractor struct{}

// Extract returns the string following Authorization: Bearer
func (b *BearerExtractor) Extract(r *http.Request) (string, error) {
	s := r.Header.Get("Authorization")
	if s == "" {
		return "", mrfusion.ErrAuthentication
	}

	// Check for Bearer token.
	strs := strings.Split(s, " ")

	if len(strs) != 2 || strs[0] != "Bearer" {
		return "", mrfusion.ErrAuthentication
	}
	return strs[1], nil
}

// AuthorizedToken extracts the token and validates; if valid the next handler
// will be run.  The principal will be sent to the next handler via the request's
// Context.  It is up to the next handler to determine if the principal has access.
// On failure, will return http.StatusUnauthorized.
func AuthorizedToken(auth mrfusion.Authenticator, te mrfusion.TokenExtractor, logger mrfusion.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := logger.
			WithField("component", "auth").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		token, err := te.Extract(r)
		if err != nil {
			log.Error("Unable to extract token")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// We do not check the validity of the principal.  Those
		// handlers further down the chain should do so.
		principal, err := auth.Authenticate(r.Context(), token)
		if err != nil {
			log.Error("Invalid token")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Send the principal to the next handler
		ctx := context.WithValue(r.Context(), mrfusion.PrincipalKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
		return
	})
}
