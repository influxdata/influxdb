// +build OMIT
package oauth2

import (
	"context"
	"net/http"
)

func AuthorizedToken(auth Authenticator, te TokenExtractor, next http.Handler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := te.Extract(r)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		principal, err := auth.Authenticate(r.Context(), token)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Send the principal to the next handler for further authorization
		ctx := context.WithValue(r.Context(), PrincipalKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
		return
	})
}
