package oauth2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	gojwt "github.com/dgrijalva/jwt-go"
	"github.com/influxdata/chronograf"
	"golang.org/x/oauth2"
)

// ExtendedProvider extendts the base Provider interface with optional methods
type ExtendedProvider interface {
	Provider
	// get PrincipalID from id_token
	PrincipalIDFromClaims(claims gojwt.MapClaims) (string, error)
	GroupFromClaims(claims gojwt.MapClaims) (string, error)
}

var _ ExtendedProvider = &Generic{}

// Generic provides OAuth Login and Callback server and is modeled
// after the Github OAuth2 provider. Callback will set an authentication
// cookie.  This cookie's value is a JWT containing the user's primary
// email address.
type Generic struct {
	PageName       string // Name displayed on the login page
	ClientID       string
	ClientSecret   string
	RequiredScopes []string
	Domains        []string // Optional email domain checking
	RedirectURL    string
	AuthURL        string
	TokenURL       string
	APIURL         string // APIURL returns OpenID Userinfo
	APIKey         string // APIKey is the JSON key to lookup email address in APIURL response
	Logger         chronograf.Logger
}

// Name is the name of the provider
func (g *Generic) Name() string {
	if g.PageName == "" {
		return "generic"
	}
	return g.PageName
}

// ID returns the generic application client id
func (g *Generic) ID() string {
	return g.ClientID
}

// Secret returns the generic application client secret
func (g *Generic) Secret() string {
	return g.ClientSecret
}

// Scopes for generic provider required of the client.
func (g *Generic) Scopes() []string {
	return g.RequiredScopes
}

// Config is the Generic OAuth2 exchange information and endpoints
func (g *Generic) Config() *oauth2.Config {
	return &oauth2.Config{
		ClientID:     g.ID(),
		ClientSecret: g.Secret(),
		Scopes:       g.Scopes(),
		RedirectURL:  g.RedirectURL,
		Endpoint: oauth2.Endpoint{
			AuthURL:  g.AuthURL,
			TokenURL: g.TokenURL,
		},
	}
}

// PrincipalID returns the email address of the user.
func (g *Generic) PrincipalID(provider *http.Client) (string, error) {
	res := map[string]interface{}{}

	r, err := provider.Get(g.APIURL)
	if err != nil {
		return "", err
	}

	defer r.Body.Close()
	if err = json.NewDecoder(r.Body).Decode(&res); err != nil {
		return "", err
	}

	email := ""
	value := res[g.APIKey]
	if e, ok := value.(string); ok {
		email = e
	}

	// If we did not receive an email address, try to lookup the email
	// in a similar way as github
	if email == "" {
		email, err = g.getPrimaryEmail(provider)
		if err != nil {
			return "", err
		}
	}

	// If we need to restrict to a set of domains, we first get the org
	// and filter.
	if len(g.Domains) > 0 {
		// If not in the domain deny permission
		if ok := ofDomain(g.Domains, email); !ok {
			msg := "Not a member of required domain"
			g.Logger.Error(msg)
			return "", fmt.Errorf(msg)
		}
	}

	return email, nil
}

// Group returns the domain that a user belongs to in the
// the generic OAuth.
func (g *Generic) Group(provider *http.Client) (string, error) {
	res := struct {
		Email string `json:"email"`
	}{}

	r, err := provider.Get(g.APIURL)
	if err != nil {
		return "", err
	}

	defer r.Body.Close()
	if err = json.NewDecoder(r.Body).Decode(&res); err != nil {
		return "", err
	}

	email := strings.Split(res.Email, "@")
	if len(email) != 2 {
		return "", fmt.Errorf("malformed email address, expected %q to contain @ symbol", res.Email)
	}

	return email[1], nil
}

// UserEmail represents user's email address
type UserEmail struct {
	Email    *string `json:"email,omitempty"`
	Primary  *bool   `json:"primary,omitempty"`
	Verified *bool   `json:"verified,omitempty"`
}

// getPrimaryEmail gets the private email account for the authenticated user.
func (g *Generic) getPrimaryEmail(client *http.Client) (string, error) {
	emailsEndpoint := g.APIURL + "/emails"
	r, err := client.Get(emailsEndpoint)
	if err != nil {
		return "", err
	}
	defer r.Body.Close()

	emails := []*UserEmail{}
	if err = json.NewDecoder(r.Body).Decode(&emails); err != nil {
		return "", err
	}

	email, err := g.primaryEmail(emails)
	if err != nil {
		g.Logger.Error("Unable to retrieve primary email ", err.Error())
		return "", err
	}
	return email, nil
}

func (g *Generic) primaryEmail(emails []*UserEmail) (string, error) {
	for _, m := range emails {
		if m != nil && m.Primary != nil && m.Verified != nil && m.Email != nil {
			return *m.Email, nil
		}
	}
	return "", errors.New("No primary email address")
}

// ofDomain makes sure that the email is in one of the required domains
func ofDomain(requiredDomains []string, email string) bool {
	for _, domain := range requiredDomains {
		emailDomain := fmt.Sprintf("@%s", domain)
		if strings.HasSuffix(email, emailDomain) {
			return true
		}
	}
	return false
}

// PrincipalIDFromClaims verifies an optional id_token and extracts email address of the user
func (g *Generic) PrincipalIDFromClaims(claims gojwt.MapClaims) (string, error) {
	if id, ok := claims[g.APIKey].(string); ok {
		return id, nil
	}
	return "", fmt.Errorf("no claim for %s", g.APIKey)
}

// GroupFromClaims verifies an optional id_token, extracts the email address of the user and splits off the domain part
func (g *Generic) GroupFromClaims(claims gojwt.MapClaims) (string, error) {
	if id, ok := claims[g.APIKey].(string); ok {
		email := strings.Split(id, "@")
		if len(email) != 2 {
			g.Logger.Error("malformed email address, expected %q to contain @ symbol", id)
			return "DEFAULT", nil
		}

		return email[1], nil
	}

	return "", fmt.Errorf("no claim for %s", g.APIKey)
}
