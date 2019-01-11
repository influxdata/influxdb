package oauth2

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/influxdb/chronograf"
	"golang.org/x/oauth2"
	hrk "golang.org/x/oauth2/heroku"
)

// Ensure that Heroku is an oauth2.Provider
var _ Provider = &Heroku{}

const (
	// HerokuAccountRoute is required for interacting with Heroku API
	HerokuAccountRoute string = "https://api.heroku.com/account"
)

// Heroku is an OAuth2 Provider allowing users to authenticate with Heroku to
// gain access to Chronograf
type Heroku struct {
	// OAuth2 Secrets
	ClientID     string
	ClientSecret string

	Organizations []string // set of organizations permitted to access the protected resource. Empty means "all"

	Logger chronograf.Logger
}

// Config returns the OAuth2 exchange information and endpoints
func (h *Heroku) Config() *oauth2.Config {
	return &oauth2.Config{
		ClientID:     h.ID(),
		ClientSecret: h.Secret(),
		Scopes:       h.Scopes(),
		Endpoint:     hrk.Endpoint,
	}
}

// ID returns the Heroku application client ID
func (h *Heroku) ID() string {
	return h.ClientID
}

// Name returns the name of this provider (heroku)
func (h *Heroku) Name() string {
	return "heroku"
}

// PrincipalID returns the Heroku email address of the user.
func (h *Heroku) PrincipalID(provider *http.Client) (string, error) {
	type DefaultOrg struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	type Account struct {
		Email               string     `json:"email"`
		DefaultOrganization DefaultOrg `json:"default_organization"`
	}

	req, err := http.NewRequest("GET", HerokuAccountRoute, nil)
	if err != nil {
		return "", err
	}

	// Requests fail to Heroku unless this Accept header is set.
	req.Header.Set("Accept", "application/vnd.heroku+json; version=3")
	resp, err := provider.Do(req)
	if resp.StatusCode/100 != 2 {
		err := fmt.Errorf(
			"unable to GET user data from %s. Status: %s",
			HerokuAccountRoute,
			resp.Status,
		)
		h.Logger.Error("", err)
		return "", err
	}
	if err != nil {
		h.Logger.Error("Unable to communicate with Heroku. err:", err)
		return "", err
	}
	defer resp.Body.Close()
	d := json.NewDecoder(resp.Body)

	var account Account
	if err := d.Decode(&account); err != nil {
		h.Logger.Error("Unable to decode response from Heroku. err:", err)
		return "", err
	}

	// check if member of org
	if len(h.Organizations) > 0 {
		for _, org := range h.Organizations {
			if account.DefaultOrganization.Name == org {
				return account.Email, nil
			}
		}
		h.Logger.Error(ErrOrgMembership)
		return "", ErrOrgMembership
	}
	return account.Email, nil
}

// Group returns the Heroku organization that user belongs to.
func (h *Heroku) Group(provider *http.Client) (string, error) {
	type DefaultOrg struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	type Account struct {
		Email               string     `json:"email"`
		DefaultOrganization DefaultOrg `json:"default_organization"`
	}

	resp, err := provider.Get(HerokuAccountRoute)
	if err != nil {
		h.Logger.Error("Unable to communicate with Heroku. err:", err)
		return "", err
	}
	defer resp.Body.Close()
	d := json.NewDecoder(resp.Body)

	var account Account
	if err := d.Decode(&account); err != nil {
		h.Logger.Error("Unable to decode response from Heroku. err:", err)
		return "", err
	}

	return account.DefaultOrganization.Name, nil
}

// Scopes for heroku is "identity" which grants access to user account
// information. This will grant us access to the user's email address which is
// used as the Principal's identifier.
func (h *Heroku) Scopes() []string {
	return []string{"identity"}
}

// Secret returns the Heroku application client secret
func (h *Heroku) Secret() string {
	return h.ClientSecret
}
