package server

import (
	"encoding/json"
	"net/http"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/influx/queries"
)

type QueryRequest struct {
	ID    string `json:"id"`
	Query string `json:"query"`
}

type QueriesRequest struct {
	Queries []QueryRequest `json:"queries"`
}

type QueryResponse struct {
	ID          string                   `json:"id"`
	Query       string                   `json:"query"`
	QueryConfig chronograf.QueryConfig   `json:"queryConfig"`
	QueryAST    *queries.SelectStatement `json:"queryAST,omitempty"`
}

type QueriesResponse struct {
	Queries []QueryResponse `json:"queries"`
}

// Queries parses InfluxQL and returns the JSON
func (s *Service) Queries(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	if _, err = s.SourcesStore.Get(ctx, srcID); err != nil {
		notFound(w, srcID, s.Logger)
		return
	}

	var req QueriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	res := QueriesResponse{
		Queries: make([]QueryResponse, len(req.Queries)),
	}

	for i, q := range req.Queries {
		qr := QueryResponse{
			ID:          q.ID,
			Query:       q.Query,
			QueryConfig: ToQueryConfig(q.Query),
		}
		if stmt, err := queries.ParseSelect(q.Query); err == nil {
			qr.QueryAST = stmt
		}
		qr.QueryConfig.ID = q.ID
		res.Queries[i] = qr
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}
