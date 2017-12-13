package oauth2

import (
	"net/http"
	"path"
	"time"

	"github.com/influxdata/chronograf"
	"golang.org/x/oauth2"
)

// Check to ensure AuthMux is an oauth2.Mux
var _ Mux = &AuthMux{}

// TenMinutes is the default length of time to get a response back from the OAuth provider
const TenMinutes = 10 * time.Minute

// NewAuthMux constructs a Mux handler that checks a cookie against the authenticator
func NewAuthMux(p Provider, a Authenticator, t Tokenizer, basepath string, l chronograf.Logger) *AuthMux {
	return &AuthMux{
		Provider:   p,
		Auth:       a,
		Tokens:     t,
		SuccessURL: path.Join(basepath, "/"),
		FailureURL: path.Join(basepath, "/login"),
		Now:        DefaultNowTime,
		Logger:     l,
	}
}

// AuthMux services an Oauth2 interaction with a provider and browser and
// stores the resultant token in the user's browser as a cookie. The benefit of
// this is that the cookie's authenticity can be verified independently by any
// Chronograf instance as long as the Authenticator has no external
// dependencies (e.g. on a Database).
type AuthMux struct {
	Provider   Provider          // Provider is the OAuth2 service
	Auth       Authenticator     // Auth is used to Authorize after successful OAuth2 callback and Expire on Logout
	Tokens     Tokenizer         // Tokens is used to create and validate OAuth2 "state"
	Logger     chronograf.Logger // Logger is used to give some more information about the OAuth2 process
	SuccessURL string            // SuccessURL is redirect location after successful authorization
	FailureURL string            // FailureURL is redirect location after authorization failure
	Now        func() time.Time  // Now returns the current time (for testing)
}

// Login uses a Cookie with a random string as the state validation method.  JWTs are
// a good choice here for encoding because they can be validated without
// storing state. Login returns a handler that redirects to the providers OAuth login.
func (j *AuthMux) Login() http.Handler {
	conf := j.Provider.Config()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// We are creating a token with an encoded random string to prevent CSRF attacks
		// This token will be validated during the OAuth callback.
		// We'll give our users 10 minutes from this point to type in their
		// oauth2 provider's password.
		// If the callback is not received within 10 minutes, then authorization will fail.
		csrf := randomString(32) // 32 is not important... just long
		now := j.Now()

		// This token will be valid for 10 minutes.  Any chronograf server will
		// be able to validate this token.
		p := Principal{
			Subject:   csrf,
			IssuedAt:  now,
			ExpiresAt: now.Add(TenMinutes),
		}
		token, err := j.Tokens.Create(r.Context(), p)

		// This is likely an internal server error
		if err != nil {
			j.Logger.
				WithField("component", "auth").
				WithField("remote_addr", r.RemoteAddr).
				WithField("method", r.Method).
				WithField("url", r.URL).
				Error("Internal authentication error: ", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		url := conf.AuthCodeURL(string(token), oauth2.AccessTypeOnline)
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
	})
}

// Callback is used by OAuth2 provider after authorization is granted.  If
// granted, Callback will set a cookie with a month-long expiration.  It is
// recommended that the value of the cookie be encoded as a JWT because the JWT
// can be validated without the need for saving state. The JWT contains the
// principal's identifier (e.g.  email address).
func (j *AuthMux) Callback() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := j.Logger.
			WithField("component", "auth").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		state := r.FormValue("state")
		// Check if the OAuth state token is valid to prevent CSRF
		// The state variable we set is actually a token.  We'll check
		// if the token is valid.  We don't need to know anything
		// about the contents of the principal only that it hasn't expired.
		if _, err := j.Tokens.ValidPrincipal(r.Context(), Token(state), TenMinutes); err != nil {
			log.Error("Invalid OAuth state received: ", err.Error())
			http.Redirect(w, r, j.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		// Exchange the code back with the provider to the the token
		conf := j.Provider.Config()
		code := r.FormValue("code")
		token, err := conf.Exchange(r.Context(), code)
		if err != nil {
			log.Error("Unable to exchange code for token ", err.Error())
			http.Redirect(w, r, j.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		// if we received an extra id_token, inspect it
		var id string
		if tokenString, ok := token.Extra("id_token").(string); ok {
			log.Debug("token provides extra id_token")
			if provider, ok := j.Provider.(ExtendedProvider); ok {
				log.Debug("provider implements PrincipalIDFromClaims()")
				claims, err := j.Tokens.GetClaims(tokenString)
				if err != nil {
					log.Error("parsing extra id_token failed:", err)
					http.Redirect(w, r, j.FailureURL, http.StatusTemporaryRedirect)
					return
				}
				log.Debug("found claims: ", claims)
				if id, err = provider.PrincipalIDFromClaims(claims); err != nil {
					log.Error("claim not found:", err)
					http.Redirect(w, r, j.FailureURL, http.StatusTemporaryRedirect)
					return
				}
			} else {
				log.Debug("provider does not implement PrincipalIDFromClaims()")
			}
		}

		// otherwise perform an additional lookup
		if id == "" {
			// Using the token get the principal identifier from the provider
			oauthClient := conf.Client(r.Context(), token)
			id, err = j.Provider.PrincipalID(oauthClient)
			if err != nil {
				log.Error("Unable to get principal identifier ", err.Error())
				http.Redirect(w, r, j.FailureURL, http.StatusTemporaryRedirect)
				return
			}
		}

		p := Principal{
			Subject: id,
			Issuer:  j.Provider.Name(),
		}
		ctx := r.Context()
		err = j.Auth.Authorize(ctx, w, p)
		if err != nil {
			log.Error("Unable to get add session to response ", err.Error())
			http.Redirect(w, r, j.FailureURL, http.StatusTemporaryRedirect)
			return
		}
		log.Info("User ", id, " is authenticated")
		http.Redirect(w, r, j.SuccessURL, http.StatusTemporaryRedirect)
	})
}

// Logout handler will expire our authentication cookie and redirect to the successURL
func (j *AuthMux) Logout() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		j.Auth.Expire(w)
		http.Redirect(w, r, j.SuccessURL, http.StatusTemporaryRedirect)
	})
}
