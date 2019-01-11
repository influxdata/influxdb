package oauth2

import (
	"fmt"
	"net/http"

	"github.com/influxdata/influxdb/chronograf"
	"golang.org/x/oauth2"
	goauth2 "google.golang.org/api/oauth2/v2"
)

// GoogleEndpoint is Google's OAuth 2.0 endpoint.
// Copied here to remove tons of package dependencies
var GoogleEndpoint = oauth2.Endpoint{
	AuthURL:  "https://accounts.google.com/o/oauth2/auth",
	TokenURL: "https://accounts.google.com/o/oauth2/token",
}
var _ Provider = &Google{}

// Google is an oauth2 provider supporting google.
type Google struct {
	ClientID     string
	ClientSecret string
	RedirectURL  string
	Domains      []string // Optional google email domain checking
	Logger       chronograf.Logger
}

// Name is the name of the provider
func (g *Google) Name() string {
	return "google"
}

// ID returns the google application client id
func (g *Google) ID() string {
	return g.ClientID
}

// Secret returns the google application client secret
func (g *Google) Secret() string {
	return g.ClientSecret
}

// Scopes for google is only the email address
// Documentation is here: https://developers.google.com/+/web/api/rest/oauth#email
func (g *Google) Scopes() []string {
	return []string{
		goauth2.UserinfoEmailScope,
		goauth2.UserinfoProfileScope,
	}
}

// Config is the Google OAuth2 exchange information and endpoints
func (g *Google) Config() *oauth2.Config {
	return &oauth2.Config{
		ClientID:     g.ID(),
		ClientSecret: g.Secret(),
		Scopes:       g.Scopes(),
		Endpoint:     GoogleEndpoint,
		RedirectURL:  g.RedirectURL,
	}
}

// PrincipalID returns the google email address of the user.
func (g *Google) PrincipalID(provider *http.Client) (string, error) {
	srv, err := goauth2.New(provider)
	if err != nil {
		g.Logger.Error("Unable to communicate with Google ", err.Error())
		return "", err
	}
	info, err := srv.Userinfo.Get().Do()
	if err != nil {
		g.Logger.Error("Unable to retrieve Google email ", err.Error())
		return "", err
	}
	// No domain filtering required, so, the user is autenticated.
	if len(g.Domains) == 0 {
		return info.Email, nil
	}

	// Check if the account domain is acceptable
	for _, requiredDomain := range g.Domains {
		if info.Hd == requiredDomain {
			return info.Email, nil
		}
	}
	g.Logger.Error("Domain '", info.Hd, "' is not a member of required Google domain(s): ", g.Domains)
	return "", fmt.Errorf("not in required domain")
}

// Group returns the string of domain a user belongs to in Google
func (g *Google) Group(provider *http.Client) (string, error) {
	srv, err := goauth2.New(provider)
	if err != nil {
		g.Logger.Error("Unable to communicate with Google ", err.Error())
		return "", err
	}
	info, err := srv.Userinfo.Get().Do()
	if err != nil {
		g.Logger.Error("Unable to retrieve Google email ", err.Error())
		return "", err
	}

	return info.Hd, nil
}
