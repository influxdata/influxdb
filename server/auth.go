package server

import (
	"context"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2"

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
		return "", chronograf.ErrAuthentication
	}
	return cookie.Value, nil
}

// BearerExtractor extracts the token from Authorization: Bearer header.
type BearerExtractor struct{}

// Extract returns the string following Authorization: Bearer
func (b *BearerExtractor) Extract(r *http.Request) (string, error) {
	s := r.Header.Get("Authorization")
	if s == "" {
		return "", chronograf.ErrAuthentication
	}

	// Check for Bearer token.
	strs := strings.Split(s, " ")

	if len(strs) != 2 || strs[0] != "Bearer" {
		return "", chronograf.ErrAuthentication
	}
	return strs[1], nil
}

// AuthorizedToken extracts the token and validates; if valid the next handler
// will be run.  The principal will be sent to the next handler via the request's
// Context.  It is up to the next handler to determine if the principal has access.
// On failure, will return http.StatusUnauthorized.
func AuthorizedToken(auth chronograf.Authenticator, te chronograf.TokenExtractor, logger chronograf.Logger, next http.Handler) http.HandlerFunc {
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
		// server further down the chain should do so.
		principal, err := auth.Authenticate(r.Context(), token)
		if err != nil {
			log.Error("Invalid token")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Send the principal to the next handler
		ctx := context.WithValue(r.Context(), chronograf.PrincipalKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
		return
	})
}

// Login returns a handler that redirects to the providers OAuth login.
// Uses JWT with a random string as the state validation method.
// JWTs are used because they can be validated without storing state.
func Login(provider OAuth2Provider, auth chronograf.Authenticator, logger chronograf.Logger) http.HandlerFunc {
	conf := provider.Config()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// We are creating a token with an encoded random string to prevent CSRF attacks
		// This token will be validated during the OAuth callback.
		// We'll give our users 10 minutes from this point to type in their github password.
		// If the callback is not received within 10 minutes, then authorization will fail.
		csrf := randomString(32) // 32 is not important... just long
		state, err := auth.Token(r.Context(), chronograf.Principal(csrf), 10*time.Minute)
		// This is likely an internal server error
		if err != nil {
			logger.
				WithField("component", "auth").
				WithField("remote_addr", r.RemoteAddr).
				WithField("method", r.Method).
				WithField("url", r.URL).
				Error("Internal authentication error: ", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		url := conf.AuthCodeURL(state, oauth2.AccessTypeOnline)
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
	})
}

// Logout handler will expire our authentication cookie and redirect to the successURL
func Logout(cookie Cookie, successURL string, now func() time.Time) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deleteCookie := http.Cookie{
			Name:     cookie.Name,
			Value:    "none",
			Expires:  now().UTC().Add(-1 * time.Hour),
			HttpOnly: true,
			Path:     "/",
		}
		http.SetCookie(w, &deleteCookie)
		http.Redirect(w, r, successURL, http.StatusTemporaryRedirect)
	})
}

// CallbackOpts are the options for the Callback handler
type CallbackOpts struct {
	Provider   OAuth2Provider
	Auth       chronograf.Authenticator
	Cookie     Cookie
	Logger     chronograf.Logger
	SuccessURL string           // SuccessURL is redirect location after successful authorization
	FailureURL string           // FailureURL is redirect location after authorization failure
	Now        func() time.Time // Now returns the current time
}

// Callback is used by OAuth2 provider after authorization is granted.  If
// granted, Callback will set a cookie with a month-long expiration.  The
// value of the cookie is a JWT because the JWT can be validated without
// the need for saving state. The JWT contains the principal's identifier (e.g.
// email address).
func Callback(opts CallbackOpts) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := opts.Logger.
			WithField("component", "auth").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		state := r.FormValue("state")
		// Check if the OAuth state token is valid to prevent CSRF
		_, err := opts.Auth.Authenticate(r.Context(), state)
		if err != nil {
			log.Error("Invalid OAuth state received: ", err.Error())
			http.Redirect(w, r, opts.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		// Exchange the code back with the provider to the the token
		conf := opts.Provider.Config()
		code := r.FormValue("code")
		token, err := conf.Exchange(r.Context(), code)
		if err != nil {
			log.Error("Unable to exchange code for token ", err.Error())
			http.Redirect(w, r, opts.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		// Using the token get the principal identifier from the provider
		oauthClient := conf.Client(r.Context(), token)
		id, err := opts.Provider.PrincipalID(oauthClient)
		if err != nil {
			log.Error("Unable to get principal identifier ", err.Error())
			http.Redirect(w, r, opts.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		// We create an auth token that will be used by all other endpoints to validate the principal has a claim
		authToken, err := opts.Auth.Token(r.Context(), chronograf.Principal(id), opts.Cookie.Duration)
		if err != nil {
			log.Error("Unable to create cookie auth token ", err.Error())
			http.Redirect(w, r, opts.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		expireCookie := opts.Now().UTC().Add(opts.Cookie.Duration)
		cookie := http.Cookie{
			Name:     opts.Cookie.Name,
			Value:    authToken,
			Expires:  expireCookie,
			HttpOnly: true,
			Path:     "/",
		}
		log.Info("User ", id, " is authenticated")
		http.SetCookie(w, &cookie)
		http.Redirect(w, r, opts.SuccessURL, http.StatusTemporaryRedirect)
	})
}
