package oauth2

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/influxdata/platform/chronograf"
)

var _ Provider = &Auth0{}

type Auth0 struct {
	Generic
	Organizations map[string]bool // the set of allowed organizations users may belong to
}

func (a *Auth0) PrincipalID(provider *http.Client) (string, error) {
	type Account struct {
		Email        string `json:"email"`
		Organization string `json:"organization"`
	}

	resp, err := provider.Get(a.Generic.APIURL)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	act := Account{}
	if err = json.NewDecoder(resp.Body).Decode(&act); err != nil {
		return "", err
	}

	// check for organization membership if required
	if len(a.Organizations) > 0 && !a.Organizations[act.Organization] {
		a.Logger.
			WithField("org", act.Organization).
			Error(ErrOrgMembership)

		return "", ErrOrgMembership
	}
	return act.Email, nil
}

func (a *Auth0) Group(provider *http.Client) (string, error) {
	type Account struct {
		Email        string `json:"email"`
		Organization string `json:"organization"`
	}

	resp, err := provider.Get(a.Generic.APIURL)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()
	act := Account{}
	if err = json.NewDecoder(resp.Body).Decode(&act); err != nil {
		return "", err
	}

	return act.Organization, nil
}

func NewAuth0(auth0Domain, clientID, clientSecret, redirectURL string, organizations []string, logger chronograf.Logger) (Auth0, error) {
	domain, err := url.Parse(auth0Domain)
	if err != nil {
		return Auth0{}, err
	}

	domain.Scheme = "https"

	domain.Path = "/authorize"
	authURL := domain.String()

	domain.Path = "/oauth/token"
	tokenURL := domain.String()

	domain.Path = "/userinfo"
	apiURL := domain.String()

	a0 := Auth0{
		Generic: Generic{
			PageName: "auth0",

			ClientID:     clientID,
			ClientSecret: clientSecret,

			RequiredScopes: []string{"openid", "email"},

			RedirectURL: redirectURL,
			AuthURL:     authURL,
			TokenURL:    tokenURL,
			APIURL:      apiURL,

			Logger: logger,
		},
		Organizations: make(map[string]bool, len(organizations)),
	}

	for _, org := range organizations {
		a0.Organizations[org] = true
	}
	return a0, nil
}
