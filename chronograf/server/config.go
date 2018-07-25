package server

import (
	"encoding/json"
	"net/http"

	"github.com/influxdata/platform/chronograf"
)

type configLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
	Auth string `json:"auth"` // Auth link to the auth config endpoint
}

type configResponse struct {
	Links configLinks `json:"links"`
	chronograf.Config
}

func newConfigResponse(config chronograf.Config) *configResponse {
	return &configResponse{
		Links: configLinks{
			Self: "/chronograf/v1/config",
			Auth: "/chronograf/v1/config/auth",
		},
		Config: config,
	}
}

type authConfigResponse struct {
	Links selfLinks `json:"links"`
	chronograf.AuthConfig
}

func newAuthConfigResponse(config chronograf.Config) *authConfigResponse {
	return &authConfigResponse{
		Links: selfLinks{
			Self: "/chronograf/v1/config/auth",
		},
		AuthConfig: config.Auth,
	}
}

// Config retrieves the global application configuration
func (s *Service) Config(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	config, err := s.Store.Config(ctx).Get(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	if config == nil {
		Error(w, http.StatusBadRequest, "Configuration object was nil", s.Logger)
		return
	}
	res := newConfigResponse(*config)

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// AuthConfig retrieves the auth section of the global application configuration
func (s *Service) AuthConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	config, err := s.Store.Config(ctx).Get(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	if config == nil {
		Error(w, http.StatusBadRequest, "Configuration object was nil", s.Logger)
		return
	}

	res := newAuthConfigResponse(*config)

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// ReplaceAuthConfig replaces the auth section of the global application configuration
func (s *Service) ReplaceAuthConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var authConfig chronograf.AuthConfig
	if err := json.NewDecoder(r.Body).Decode(&authConfig); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	config, err := s.Store.Config(ctx).Get(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	if config == nil {
		Error(w, http.StatusBadRequest, "Configuration object was nil", s.Logger)
		return
	}
	config.Auth = authConfig

	res := newAuthConfigResponse(*config)
	if err := s.Store.Config(ctx).Update(ctx, config); err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}
