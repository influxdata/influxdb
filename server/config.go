package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
)

type configResponse struct {
	Links selfLinks `json:"links"`
	chronograf.Config
}

func newConfigResponse(config chronograf.Config) *configResponse {
	return &configResponse{
		Links: selfLinks{
			Self: "/chronograf/v1/config",
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

// ConfigSection retrieves the section of the global application configuration
func (s *Service) ConfigSection(w http.ResponseWriter, r *http.Request) {
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

	section := httprouter.GetParamFromContext(ctx, "section")
	var res interface{}
	switch section {
	case "auth":
		res = newAuthConfigResponse(*config)
	default:
		Error(w, http.StatusBadRequest, fmt.Sprintf("received unknown section %q", section), s.Logger)
		return
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// ReplaceConfigSection replaces a section of the global application configuration
func (s *Service) ReplaceConfigSection(w http.ResponseWriter, r *http.Request) {
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

	section := httprouter.GetParamFromContext(ctx, "section")
	var res interface{}
	switch section {
	case "auth":
		var authConfig chronograf.AuthConfig
		if err := json.NewDecoder(r.Body).Decode(&authConfig); err != nil {
			invalidJSON(w, s.Logger)
			return
		}
		config.Auth = authConfig
		res = newAuthConfigResponse(*config)
	default:
		Error(w, http.StatusBadRequest, fmt.Sprintf("received unknown section %q", section), s.Logger)
		return
	}

	if err := s.Store.Config(ctx).Update(ctx, config); err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}
