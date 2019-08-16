package endpoint

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/influxdata/influxdb"
)

var _ influxdb.NotificationEndpoint = &WebHook{}

const (
	webhookTokenSuffix    = "-token"
	webhookUsernameSuffix = "-username"
	webhookPasswordSuffix = "-password"
)

// WebHook is the notification endpoint config of webhook.
type WebHook struct {
	Base
	// Path is the API path of WebHook
	URL string `json:"url"`
	// Token is the bearer token for authorization
	Token           influxdb.SecretField `json:"token,omitempty"`
	Username        influxdb.SecretField `json:"username,omitempty"`
	Password        influxdb.SecretField `json:"password,omitempty"`
	AuthMethod      string               `json:"authmethod"`
	Method          string               `json:"method"`
	ContentTemplate string               `json:"contentTemplate"`
}

// BackfillSecretKeys fill back fill the secret field key during the unmarshalling
// if value of that secret field is not nil.
func (s *WebHook) BackfillSecretKeys() {
	if s.Token.Key == "" && s.Token.Value != nil {
		s.Token.Key = s.ID.String() + webhookTokenSuffix
	}
	if s.Username.Key == "" && s.Username.Value != nil {
		s.Username.Key = s.ID.String() + webhookUsernameSuffix
	}
	if s.Password.Key == "" && s.Password.Value != nil {
		s.Password.Key = s.ID.String() + webhookPasswordSuffix
	}
}

// SecretFields return available secret fields.
func (s WebHook) SecretFields() []influxdb.SecretField {
	arr := make([]influxdb.SecretField, 0)
	if s.Token.Key != "" {
		arr = append(arr, s.Token)
	}
	if s.Username.Key != "" {
		arr = append(arr, s.Username)
	}
	if s.Password.Key != "" {
		arr = append(arr, s.Password)
	}
	return arr
}

var goodWebHookAuthMethod = map[string]bool{
	"none":   true,
	"basic":  true,
	"bearer": true,
}

var goodHTTPMethod = map[string]bool{
	http.MethodGet:  true,
	http.MethodPost: true,
	http.MethodPut:  true,
}

// Valid returns error if some configuration is invalid
func (s WebHook) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.URL == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "webhook endpoint URL is empty",
		}
	}
	if _, err := url.Parse(s.URL); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("webhook endpoint URL is invalid: %s", err.Error()),
		}
	}
	if !goodHTTPMethod[s.Method] {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid webhook http method",
		}
	}
	if !goodWebHookAuthMethod[s.AuthMethod] {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid webhook auth method",
		}
	}
	if s.AuthMethod == "basic" &&
		(s.Username.Key != s.ID.String()+webhookUsernameSuffix ||
			s.Password.Key != s.ID.String()+webhookPasswordSuffix) {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid webhook username/password for basic auth",
		}
	}
	if s.AuthMethod == "bearer" && s.Token.Key != webhookTokenSuffix {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid webhook token for bearer auth",
		}
	}

	return nil
}

type webhookAlias WebHook

// MarshalJSON implement json.Marshaler interface.
func (s WebHook) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			webhookAlias
			Type string `json:"type"`
		}{
			webhookAlias: webhookAlias(s),
			Type:         s.Type(),
		})
}

// Type returns the type.
func (s WebHook) Type() string {
	return WebhookType
}

// ParseResponse will parse the http response from webhook.
func (s WebHook) ParseResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return &influxdb.Error{
			Msg: string(body),
		}
	}
	return nil
}
