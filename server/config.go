package server

import (
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
