package server

import (
	"encoding/json"
	"fmt"
	"net/http"

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

type logViewerResponse struct {
	Links selfLinks `json:"links"`
	chronograf.LogViewerConfig
}

func newLogViewerConfigResponse(config chronograf.Config) *logViewerResponse {
	return &logViewerResponse{
		Links: selfLinks{
			Self: "/chronograf/v1/config/logviewer",
		},
		LogViewerConfig: config.LogViewer,
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

// LogViewerConfig retrieves the log viewer UI section of the global application configuration
func (s *Service) LogViewerConfig(w http.ResponseWriter, r *http.Request) {
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

	res := newLogViewerConfigResponse(*config)

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// ReplaceLogViewerConfig replaces the log viewer UI section of the global application configuration
func (s *Service) ReplaceLogViewerConfig(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var logViewerConfig chronograf.LogViewerConfig
	if err := json.NewDecoder(r.Body).Decode(&logViewerConfig); err != nil {
		invalidJSON(w, s.Logger)
		return
	}
	if err := validLogViewerConfig(logViewerConfig); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
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
	config.LogViewer = logViewerConfig

	res := newLogViewerConfigResponse(*config)
	if err := s.Store.Config(ctx).Update(ctx, config); err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

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

// validLogViewerConfig ensures that the request body log viewer UI config is valid
// to be valid, it must: not be empty, have at least one column, not have multiple
// columns with the same name or position value, each column must have a visbility
// of either "visible" or "hidden" and if a column is of type severity, it must have
// at least one severity format of type icon, text, or both
func validLogViewerConfig(cfg chronograf.LogViewerConfig) error {
	if len(cfg.Columns) == 0 {
		return fmt.Errorf("Invalid log viewer config: must have at least 1 column")
	}

	nameMatcher := map[string]bool{}
	positionMatcher := map[int32]bool{}

	for _, clm := range cfg.Columns {
		iconCount := 0
		textCount := 0
		visibility := 0

		// check that each column has a unique value for the name and position properties
		if _, ok := nameMatcher[clm.Name]; ok {
			return fmt.Errorf("Invalid log viewer config: Duplicate column name %s", clm.Name)
		}
		nameMatcher[clm.Name] = true
		if _, ok := positionMatcher[clm.Position]; ok {
			return fmt.Errorf("Invalid log viewer config: Multiple columns with same position value")
		}
		positionMatcher[clm.Position] = true

		for _, e := range clm.Encodings {
			if e.Type == "visibility" {
				visibility++
				if !(e.Value == "visible" || e.Value == "hidden") {
					return fmt.Errorf("Invalid log viewer config: invalid visibility in column %s", clm.Name)
				}
			}

			if clm.Name == "severity" {
				if e.Value == "icon" {
					iconCount++
				} else if e.Value == "text" {
					textCount++
				}
			}
		}

		if visibility != 1 {
			return fmt.Errorf("Invalid log viewer config: missing visibility encoding in column %s", clm.Name)
		}

		if clm.Name == "severity" {
			if iconCount+textCount == 0 || iconCount > 1 || textCount > 1 {
				return fmt.Errorf("Invalid log viewer config: invalid number of severity format encodings in column %s", clm.Name)
			}
		}
	}

	return nil
}
