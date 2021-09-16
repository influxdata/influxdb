package http

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const (
	tokenScheme  = "Token "
	bearerScheme = "Bearer "
)

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

	if len(header) >= len(tokenScheme) &&
		strings.EqualFold(header[:len(tokenScheme)], tokenScheme) {
		return header[len(tokenScheme):], nil
	} else if len(header) > len(bearerScheme) &&
		strings.EqualFold(header[:len(bearerScheme)], bearerScheme) {
		return header[len(bearerScheme):], nil
	}

	return "", ErrAuthBadScheme
}

// SetToken adds the token to the request.
func SetToken(token string, req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("%s%s", tokenScheme, token))
}
