package oauth2

import (
	"net/http"
	"time"

	"github.com/influxdata/chronograf"
	"golang.org/x/oauth2"
)

const (
	// DefaultCookieName is the name of the stored cookie
	DefaultCookieName = "session"
	// DefaultCookieDuration is the length of time the cookie is valid
	DefaultCookieDuration = time.Hour * 24 * 30
)

// Cookie represents the location and expiration time of new cookies.
type cookie struct {
	Name     string
	Duration time.Duration
}

// Check to ensure CookieMux is an oauth2.Mux
var _ Mux = &CookieMux{}

// NewCookieMux constructs a Mux handler that checks a cookie against the authenticator
func NewCookieMux(p Provider, a Authenticator, l chronograf.Logger) *CookieMux {
	return &CookieMux{
		Provider:   p,
		Auth:       a,
		Logger:     l,
		SuccessURL: "/",
		FailureURL: "/login",
		Now:        time.Now,

		cookie: cookie{
			Name:     DefaultCookieName,
			Duration: DefaultCookieDuration,
		},
	}
}

// CookieMux services an Oauth2 interaction with a provider and browser and
// stores the resultant token in the user's browser as a cookie. The benefit of
// this is that the cookie's authenticity can be verified independently by any
// Chronograf instance as long as the Authenticator has no external
// dependencies (e.g. on a Database).
type CookieMux struct {
	Provider   Provider
	Auth       Authenticator
	cookie     cookie
	Logger     chronograf.Logger
	SuccessURL string           // SuccessURL is redirect location after successful authorization
	FailureURL string           // FailureURL is redirect location after authorization failure
	Now        func() time.Time // Now returns the current time
}

// Login uses a Cookie with a random string as the state validation method.  JWTs are
// a good choice here for encoding because they can be validated without
// storing state.
func (j *CookieMux) Login() http.Handler {
	conf := j.Provider.Config()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// We are creating a token with an encoded random string to prevent CSRF attacks
		// This token will be validated during the OAuth callback.
		// We'll give our users 10 minutes from this point to type in their github password.
		// If the callback is not received within 10 minutes, then authorization will fail.
		csrf := randomString(32) // 32 is not important... just long
		p := Principal{
			Subject: csrf,
		}
		state, err := j.Auth.Token(r.Context(), p, 10*time.Minute)
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
		url := conf.AuthCodeURL(state, oauth2.AccessTypeOnline)
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
	})
}

// Callback is used by OAuth2 provider after authorization is granted.  If
// granted, Callback will set a cookie with a month-long expiration.  It is
// recommended that the value of the cookie be encoded as a JWT because the JWT
// can be validated without the need for saving state. The JWT contains the
// principal's identifier (e.g.  email address).
func (j *CookieMux) Callback() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := j.Logger.
			WithField("component", "auth").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		state := r.FormValue("state")
		// Check if the OAuth state token is valid to prevent CSRF
		_, err := j.Auth.Authenticate(r.Context(), state)
		if err != nil {
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

		// Using the token get the principal identifier from the provider
		oauthClient := conf.Client(r.Context(), token)
		id, err := j.Provider.PrincipalID(oauthClient)
		if err != nil {
			log.Error("Unable to get principal identifier ", err.Error())
			http.Redirect(w, r, j.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		p := Principal{
			Subject: id,
			Issuer:  j.Provider.Name(),
		}
		// We create an auth token that will be used by all other endpoints to validate the principal has a claim
		authToken, err := j.Auth.Token(r.Context(), p, j.cookie.Duration)
		if err != nil {
			log.Error("Unable to create cookie auth token ", err.Error())
			http.Redirect(w, r, j.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		expireCookie := j.Now().UTC().Add(j.cookie.Duration)
		cookie := http.Cookie{
			Name:     j.cookie.Name,
			Value:    authToken,
			Expires:  expireCookie,
			HttpOnly: true,
			Path:     "/",
		}
		log.Info("User ", id, " is authenticated")
		http.SetCookie(w, &cookie)
		http.Redirect(w, r, j.SuccessURL, http.StatusTemporaryRedirect)
	})
} // Login returns a handler that redirects to the providers OAuth login.

// Logout handler will expire our authentication cookie and redirect to the successURL
func (j *CookieMux) Logout() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deleteCookie := http.Cookie{
			Name:     j.cookie.Name,
			Value:    "none",
			Expires:  j.Now().UTC().Add(-1 * time.Hour),
			HttpOnly: true,
			Path:     "/",
		}
		http.SetCookie(w, &deleteCookie)
		http.Redirect(w, r, j.SuccessURL, http.StatusTemporaryRedirect)
	})
}
