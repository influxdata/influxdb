package oauth2

import (
	"encoding/json"
	"net/http"

	"github.com/influxdata/chronograf"

	"golang.org/x/oauth2"
	hrk "golang.org/x/oauth2/heroku"
)

// Ensure that Heroku is an oauth2.Provider
var _ Provider = &Heroku{}

const (
	// Routes required for interacting with Heroku API
	HEROKU_ACCOUNT_ROUTE string = "https://api.heroku.com/account"
)

// Heroku is an OAuth2 Provider allowing users to authenticate with Heroku to
// gain access to Chronograf
type Heroku struct {
	// OAuth2 Secrets
	ClientID     string
	ClientSecret string

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
	resp, err := provider.Get(HEROKU_ACCOUNT_ROUTE)
	if err != nil {
		h.Logger.Error("Unable to communicate with Heroku. err:", err)
		return "", err
	}
	defer resp.Body.Close()
	d := json.NewDecoder(resp.Body)
	var account struct {
		Email string `json:"email"`
	}
	if err := d.Decode(&account); err != nil {
		h.Logger.Error("Unable to decode response from Heroku. err:", err)
		return "", err
	}
	return account.Email, nil
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
