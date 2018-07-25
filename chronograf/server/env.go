package server

import (
	"net/http"

	"github.com/influxdata/platform/chronograf"
)

type envResponse struct {
	Links                  selfLinks `json:"links"`
	TelegrafSystemInterval string    `json:"telegrafSystemInterval"`
}

func newEnvResponse(env chronograf.Environment) *envResponse {
	return &envResponse{
		Links: selfLinks{
			Self: "/chronograf/v1/env",
		},
		TelegrafSystemInterval: env.TelegrafSystemInterval.String(),
	}
}

// Environment retrieves the global application configuration
func (s *Service) Environment(w http.ResponseWriter, r *http.Request) {
	res := newEnvResponse(s.Env)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}
