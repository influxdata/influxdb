package http

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const tokenScheme = "Token "
const altTokenScheme = "Bearer "

// errors
var (
	ErrAuthHeaderMissing = errors.New("authorization Header is missing")
	ErrAuthBadScheme     = errors.New("authorization Header Scheme is invalid")
)

// GetToken will parse the token from http Authorization Header.
func GetToken(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", ErrAuthHeaderMissing
	}

	if strings.HasPrefix(header, tokenScheme) {
		return header[len(tokenScheme):], nil
	} else if strings.HasPrefix(header, altTokenScheme) {
		return header[len(altTokenScheme):], nil
	}

	return "", ErrAuthBadScheme
}

// SetToken adds the token to the request.
func SetToken(token string, req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("%s%s", tokenScheme, token))
}
