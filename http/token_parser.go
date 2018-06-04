package http

import (
	"errors"
	"net/http"
	"strings"
)

const tokenScheme = "Token "

// errors
var (
	ErrAuthHeaderMissing = errors.New("Authorization Header is missing")
	ErrAuthBadScheme     = errors.New("Authorization Header Scheme has to Token")
)

// ParseAuthHeaderToken will parse the token from http Authorization Header.
func ParseAuthHeaderToken(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", ErrAuthHeaderMissing
	}
	if !strings.HasPrefix(header, tokenScheme) {
		return "", ErrAuthBadScheme
	}
	return header[len(tokenScheme):], nil
}
