package oauth2

import (
	"context"
	"net/http"
	"time"
)

const (
	// DefaultCookieName is the name of the stored cookie
	DefaultCookieName = "session"
)

var _ Authenticator = &cookie{}

// cookie represents the location and expiration time of new cookies.
type cookie struct {
	Name     string
	Duration time.Duration
	Now      func() time.Time
	Tokens   Tokenizer
}

// NewCookieJWT creates an Authenticator that uses cookies for auth
func NewCookieJWT(secret string, duration time.Duration) Authenticator {
	return &cookie{
		Name:     DefaultCookieName,
		Duration: duration,
		Now:      time.Now,
		Tokens: &JWT{
			Secret: secret,
			Now:    time.Now,
		},
	}
}

// Validate returns Principal of the Cookie if the Token is valid.
func (c *cookie) Validate(ctx context.Context, r *http.Request) (Principal, error) {
	cookie, err := r.Cookie(c.Name)
	if err != nil {
		return Principal{}, ErrAuthentication
	}
	return c.Tokens.ValidPrincipal(ctx, Token(cookie.Value), c.Duration)
}

// Authorize will create cookies containing token information.  It'll create
// a token with cookie.Duration of life to be stored as the cookie's value.
func (c *cookie) Authorize(ctx context.Context, w http.ResponseWriter, p Principal) error {
	token, err := c.Tokens.Create(ctx, p, c.Duration)
	if err != nil {
		return err
	}
	// Cookie has a Token baked into it
	cookie := http.Cookie{
		Name:     DefaultCookieName,
		Value:    string(token),
		HttpOnly: true,
		Path:     "/",
	}

	// Only set a cookie to be persistent (endure beyond the browser session)
	// if auth duration is greater than zero
	if c.Duration > 0 {
		cookie.Expires = c.Now().UTC().Add(c.Duration)
	}

	http.SetCookie(w, &cookie)
	return nil
}

// Expire returns a cookie that will expire an existing cookie
func (c *cookie) Expire(w http.ResponseWriter) {
	cookie := http.Cookie{
		Name:     DefaultCookieName,
		Value:    "none",
		HttpOnly: true,
		Path:     "/",
		Expires:  c.Now().UTC().Add(-1 * time.Hour), // to expire cookie set the time in the past
	}
	http.SetCookie(w, &cookie)
}
