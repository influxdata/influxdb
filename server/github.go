package server

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/google/go-github/github"
	"github.com/influxdata/chronograf"
	"golang.org/x/oauth2"
	ogh "golang.org/x/oauth2/github"
)

const (
	// DefaultCookieName is the name of the stored cookie
	DefaultCookieName = "session"
	// DefaultCookieDuration is the length of time the cookie is valid
	DefaultCookieDuration = time.Hour * 24 * 30
)

// Cookie represents the location and expiration time of new cookies.
type Cookie struct {
	Name     string
	Duration time.Duration
}

// NewCookie creates a Cookie with DefaultCookieName and DefaultCookieDuration
func NewCookie() Cookie {
	return Cookie{
		Name:     DefaultCookieName,
		Duration: DefaultCookieDuration,
	}
}

// Github provides OAuth Login and Callback server. Callback will set
// an authentication cookie.  This cookie's value is a JWT containing
// the user's primary Github email address.
type Github struct {
	Cookie        Cookie
	Authenticator chronograf.Authenticator
	ClientID      string
	ClientSecret  string
	Scopes        []string
	SuccessURL    string   // SuccessURL is redirect location after successful authorization
	FailureURL    string   // FailureURL is redirect location after authorization failure
	Orgs          []string // Optional github organization checking
	Now           func() time.Time
	Logger        chronograf.Logger
}

// NewGithub constructs a Github with default cookie behavior and scopes.
func NewGithub(clientID, clientSecret, successURL, failureURL string, orgs []string, auth chronograf.Authenticator, log chronograf.Logger) Github {
	scopes := []string{"user:email"}
	if len(orgs) > 0 {
		scopes = append(scopes, "read:org")
	}
	return Github{
		ClientID:      clientID,
		ClientSecret:  clientSecret,
		Cookie:        NewCookie(),
		Scopes:        scopes,
		Orgs:          orgs,
		SuccessURL:    successURL,
		FailureURL:    failureURL,
		Authenticator: auth,
		Now:           time.Now,
		Logger:        log,
	}
}

func (g *Github) config() *oauth2.Config {
	return &oauth2.Config{
		ClientID:     g.ClientID,
		ClientSecret: g.ClientSecret,
		Scopes:       g.Scopes,
		Endpoint:     ogh.Endpoint,
	}
}

// Login returns a handler that redirects to Github's OAuth login.
// Uses JWT with a random string as the state validation method.
// JWTs are used because they can be validated without storing
// state.
func (g *Github) Login() http.HandlerFunc {
	conf := g.config()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// We are creating a token with an encoded random string to prevent CSRF attacks
		// This token will be validated during the OAuth callback.
		// We'll give our users 10 minutes from this point to type in their github password.
		// If the callback is not received within 10 minutes, then authorization will fail.
		csrf := randomString(32) // 32 is not important... just long
		state, err := g.Authenticator.Token(r.Context(), chronograf.Principal(csrf), 10*time.Minute)
		// This is likely an internal server error
		if err != nil {
			g.Logger.
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

// Logout will expire our authentication cookie and redirect to the SuccessURL
func (g *Github) Logout() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deleteCookie := http.Cookie{
			Name:     g.Cookie.Name,
			Value:    "none",
			Expires:  g.Now().UTC().Add(-1 * time.Hour),
			HttpOnly: true,
			Path:     "/",
		}
		http.SetCookie(w, &deleteCookie)
		http.Redirect(w, r, g.SuccessURL, http.StatusTemporaryRedirect)
	})
}

// Callback used by github callback after authorization is granted.  If
// granted, Callback will set a cookie with a month-long expiration.  The
// value of the cookie is a JWT because the JWT can be validated without
// the need for saving state. The JWT contains the Github user's primary
// email address.
func (g *Github) Callback() http.HandlerFunc {
	conf := g.config()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := g.Logger.
			WithField("component", "auth").
			WithField("remote_addr", r.RemoteAddr).
			WithField("method", r.Method).
			WithField("url", r.URL)

		state := r.FormValue("state")
		// Check if the OAuth state token is valid to prevent CSRF
		_, err := g.Authenticator.Authenticate(r.Context(), state)
		if err != nil {
			log.Error("Invalid OAuth state received: ", err.Error())
			http.Redirect(w, r, g.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		code := r.FormValue("code")
		token, err := conf.Exchange(r.Context(), code)
		if err != nil {
			log.Error("Unable to exchange code for token ", err.Error())
			http.Redirect(w, r, g.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		oauthClient := conf.Client(r.Context(), token)
		client := github.NewClient(oauthClient)
		// If we need to restrict to a set of organizations, we first get the org
		// and filter.
		if len(g.Orgs) > 0 {
			orgs, err := getOrganizations(client, log)
			if err != nil {
				http.Redirect(w, r, g.FailureURL, http.StatusTemporaryRedirect)
				return
			}
			// Not a member, so, deny permission
			if ok := isMember(g.Orgs, orgs); !ok {
				log.Error("Not a member of required github organization")
				http.Redirect(w, r, g.FailureURL, http.StatusTemporaryRedirect)
				return
			}
		}

		email, err := getPrimaryEmail(client, log)
		if err != nil {
			http.Redirect(w, r, g.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		// We create an auth token that will be used by all other endpoints to validate the principal has a claim
		authToken, err := g.Authenticator.Token(r.Context(), chronograf.Principal(email), g.Cookie.Duration)
		if err != nil {
			log.Error("Unable to create cookie auth token ", err.Error())
			http.Redirect(w, r, g.FailureURL, http.StatusTemporaryRedirect)
			return
		}

		expireCookie := time.Now().UTC().Add(g.Cookie.Duration)
		cookie := http.Cookie{
			Name:     g.Cookie.Name,
			Value:    authToken,
			Expires:  expireCookie,
			HttpOnly: true,
			Path:     "/",
		}
		log.Info("User ", email, " is authenticated")
		http.SetCookie(w, &cookie)
		http.Redirect(w, r, g.SuccessURL, http.StatusTemporaryRedirect)
	})
}

func randomString(length int) string {
	k := make([]byte, length)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(k)
}

func logResponseError(log chronograf.Logger, resp *github.Response, err error) {
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		log.Error("OAuth access to email address forbidden ", err.Error())
	default:
		log.Error("Unable to retrieve Github email ", err.Error())
	}
}

// isMember makes sure that the user is in one of the required organizations
func isMember(requiredOrgs []string, userOrgs []*github.Organization) bool {
	for _, requiredOrg := range requiredOrgs {
		for _, userOrg := range userOrgs {
			if userOrg.Login != nil && *userOrg.Login == requiredOrg {
				return true
			}
		}
	}
	return false
}

// getOrganizations gets all organization for the currently authenticated user
func getOrganizations(client *github.Client, log chronograf.Logger) ([]*github.Organization, error) {
	// Get all pages of results
	var allOrgs []*github.Organization
	for {
		opt := &github.ListOptions{
			PerPage: 10,
		}
		// Get the organizations for the current authenticated user.
		orgs, resp, err := client.Organizations.List("", opt)
		if err != nil {
			logResponseError(log, resp, err)
			return nil, err
		}
		allOrgs = append(allOrgs, orgs...)
		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}
	return allOrgs, nil
}

// getPrimaryEmail gets the primary email account for the authenticated user.
func getPrimaryEmail(client *github.Client, log chronograf.Logger) (string, error) {
	emails, resp, err := client.Users.ListEmails(nil)
	if err != nil {
		logResponseError(log, resp, err)
		return "", err
	}

	email, err := primaryEmail(emails)
	if err != nil {
		log.Error("Unable to retrieve primary Github email ", err.Error())
		return "", err
	}
	return email, nil
}

func primaryEmail(emails []*github.UserEmail) (string, error) {
	for _, m := range emails {
		if m != nil && m.Primary != nil && m.Verified != nil && m.Email != nil {
			return *m.Email, nil
		}
	}
	return "", errors.New("No primary email address")
}
