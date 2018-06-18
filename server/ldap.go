package server

import (
	"net/http"
	"net/url"

	"github.com/influxdata/chronograf/enterprise"
	"github.com/influxdata/chronograf/influx"
)

type LDAPEnabledResponse struct {
	Enabled bool `json:"enabled"`
}

func (s *Service) LDAPEnabled(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	src, err := s.Store.Sources(ctx).Get(ctx, id)
	if err != nil || src.MetaURL == "" {
		notFound(w, id, s.Logger)
		return
	}

	authorizer := influx.DefaultAuthorization(&src)
	metaURL, err := url.Parse(src.MetaURL)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}

	client := enterprise.NewMetaClient(metaURL, src.InsecureSkipVerify, authorizer)
	res := LDAPEnabledResponse{Enabled: true}
	config, err := client.GetLDAPConfig(ctx)
	if err != nil {
		res.Enabled = false
	}

	res.Enabled = config.Enabled

	encodeJSON(w, http.StatusOK, res, s.Logger)
}
