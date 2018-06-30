package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"

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

type logViewerUIResponse struct {
	Links selfLinks `json:"links"`
	chronograf.LogViewerUIConfig
}

func newLogViewerUIConfigResponse(config chronograf.Config) *logViewerUIResponse {
	return &logViewerUIResponse{
		Links: selfLinks{
			Self: "/chronograf/v1/config/logViewer",
		},
		LogViewerUIConfig: config.LogViewerUI,
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

	_, section := path.Split(r.URL.String())

	var res interface{}
	switch section {
	case "auth":
		res = newAuthConfigResponse(*config)
	case "logViewer":
		res = newLogViewerUIConfigResponse(*config)
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
	case "logViewer":
		var logViewerUIConfig chronograf.LogViewerUIConfig
		if err := json.NewDecoder(r.Body).Decode(&logViewerUIConfig); err != nil {
			invalidJSON(w, s.Logger)
			return
		}
		config.LogViewerUI = logViewerUIConfig
		res = newLogViewerUIConfigResponse(*config)
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
