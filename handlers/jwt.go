package handlers

import (
	"fmt"
	"net/http"
	"strings"

	jwt "github.com/dgrijalva/jwt-go"
)

type JWTOpts struct {
	// Secret is used to sign
	Secret string
	// SigningMethod is used to verify tokens
	SigningMethod jwt.SigningMethod
}

func (j *JWTOpts) EnsureDefaults() {
	// TODO: this is dumb... change this at some point ... you know ASAP
	if j.Secret == "" {
		j.Secret = "secret"
	}

	// Influx standard signing method
	if j.SigningMethod == nil {
		j.SigningMethod = jwt.SigningMethodHS512
	}
}

// AuthorizedToken will check if the JWT is valid.  If not, it will return 401.
func AuthorizedToken(opts JWTOpts, next http.Handler) http.Handler {
	opts.EnsureDefaults()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check for the HTTP Authorization header.
		if s := r.Header.Get("Authorization"); s != "" {
			// Check for Bearer token.
			strs := strings.Split(s, " ")
			if len(strs) == 2 && strs[0] == "Bearer" {
				jwtToken := strs[1]

				keyLookupFn := func(token *jwt.Token) (interface{}, error) {
					// Check for expected signing method.
					if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
					}
					return []byte(opts.Secret), nil
				}

				// Parse and validate the token.
				// 1. Checks for expired tokens
				// 2. Checks if time is after the issued at
				// 3. Check if time is after not before (nbf)
				token, err := jwt.ParseWithClaims(jwtToken, &jwt.StandardClaims{}, keyLookupFn)
				if err != nil {
					w.WriteHeader(http.StatusOK)
					return
				} else if !token.Valid {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}

				claims, ok := token.Claims.(*jwt.StandardClaims)
				if !ok {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if claims.Subject == "" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				// TODO: check if the sub (e.g. /users/1) really exists
			}
			next.ServeHTTP(w, r)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	})
}

// Token returns a JWT token
func NewToken(opts JWTOpts) http.Handler {
	// grab https://github.com/influxdata/kapacitor/blob/04f1ab3116b6bab27cbf63cb0e6b918fc77c7320/server/server_test.go#L97
	return nil
}
