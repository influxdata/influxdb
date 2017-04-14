package oauth2

import (
	"context"
	"net/http"
	"time"
)

const (
	// DefaultCookieName is the name of the stored cookie
	DefaultCookieName = "session"
	// DefaultInactivityDuration is the duration a token is valid without any new activity
	DefaultInactivityDuration = 5 * time.Minute
)

var _ Authenticator = &cookie{}

// cookie represents the location and expiration time of new cookies.
type cookie struct {
	Name       string        // Name is the name of the cookie stored on the browser
	Lifespan   time.Duration // Lifespan is the expiration date of the cookie. 0 means session cookie
	Inactivity time.Duration // Inactivity is the length of time a token is valid if there is no activity
	Now        func() time.Time
	Tokens     Tokenizer
}

// NewCookieJWT creates an Authenticator that uses cookies for auth
func NewCookieJWT(secret string, lifespan time.Duration) Authenticator {
	return &cookie{
		Name:       DefaultCookieName,
		Lifespan:   lifespan,
		Inactivity: DefaultInactivityDuration,
		Now:        time.Now,
		Tokens: &JWT{
			Secret: secret,
			Now:    time.Now,
		},
	}
}

// Validate returns Principal of the Cookie if the Token is valid.
func (c *cookie) Validate(ctx context.Context, w http.ResponseWriter, r *http.Request) (Principal, error) {
	cookie, err := r.Cookie(c.Name)
	if err != nil {
		return Principal{}, ErrAuthentication
	}

	p, err := c.Tokens.ValidPrincipal(ctx, Token(cookie.Value), c.Lifespan)
	if err != nil {
		return Principal{}, ErrAuthentication
	}

	// Refresh the token by extending its life another Inactivity duration
	p, err = c.Tokens.ExtendPrincipal(ctx, p, c.Inactivity)
	if err != nil {
		return Principal{}, ErrAuthentication
	}

	token, err := c.Tokens.Create(ctx, p)
	if err != nil {
		return Principal{}, ErrAuthentication
	}

	// Cookie lifespan can be indirectly figured out by taking the token's
	// issued at time and adding the lifespan setting  The token's issued at
	// time happens to correspond to the cookie's original issued at time.
	exp := p.IssuedAt.Add(c.Lifespan)
	// Once the token has been extended, write it out as a new cookie.
	c.setCookie(w, string(token), exp)

	return p, nil
}

// Authorize will create cookies containing token information.  It'll create
// a token with cookie.Duration of life to be stored as the cookie's value.
func (c *cookie) Authorize(ctx context.Context, w http.ResponseWriter, p Principal) error {
	// Principal will be issued at Now() and will expire
	// c.Inactivity into the future
	now := c.Now().UTC()
	p.IssuedAt = now
	p.ExpiresAt = now.Add(c.Inactivity)

	token, err := c.Tokens.Create(ctx, p)
	if err != nil {
		return err
	}

	// The time when the cookie expires
	exp := now.Add(c.Lifespan)
	c.setCookie(w, string(token), exp)

	return nil
}

// setCookie creates a cookie with value expiring at exp and writes it as a cookie into the response
func (c *cookie) setCookie(w http.ResponseWriter, value string, exp time.Time) {
	// Cookie has a Token baked into it
	cookie := http.Cookie{
		Name:     DefaultCookieName,
		Value:    value,
		HttpOnly: true,
		Path:     "/",
	}

	// Only set a cookie to be persistent (endure beyond the browser session)
	// if auth duration is greater than zero
	if c.Lifespan > 0 || exp.Before(c.Now().UTC()) {
		cookie.Expires = exp
	}

	http.SetCookie(w, &cookie)
}

// Expire returns a cookie that will expire an existing cookie
func (c *cookie) Expire(w http.ResponseWriter) {
	// to expire cookie set the time in the past
	c.setCookie(w, "none", c.Now().UTC().Add(-1*time.Hour))
}
