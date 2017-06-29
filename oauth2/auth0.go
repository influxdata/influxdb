package oauth2

import (
	"net/url"

	"github.com/influxdata/chronograf"
)

type Auth0 struct {
	Generic
}

func NewAuth0(auth0Domain, clientID, clientSecret, redirectURL string, logger chronograf.Logger) (Auth0, error) {
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

	return Auth0{
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
	}, nil
}
