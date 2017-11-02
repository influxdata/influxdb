package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/net/context"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/influx"
	"github.com/influxdata/chronograf/influx/queries"
)

type QueryRequest struct {
	ID           string                  `json:"id"`
	Query        string                  `json:"query"`
	TemplateVars chronograf.TemplateVars `json:"tempVars,omitempty"`
}

type QueriesRequest struct {
	Queries []QueryRequest `json:"queries"`
}

type QueryResponse struct {
	ID             string                   `json:"id"`
	Query          string                   `json:"query"`
	QueryConfig    chronograf.QueryConfig   `json:"queryConfig"`
	QueryAST       *queries.SelectStatement `json:"queryAST,omitempty"`
	QueryTemplated *string                  `json:"queryTemplated,omitempty"`
	TemplateVars   chronograf.TemplateVars  `json:"tempVars,omitempty"`
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
	src, err := s.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
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
			ID:    q.ID,
			Query: q.Query,
		}

		query := q.Query
		if len(q.TemplateVars) > 0 {
			query = influx.TemplateReplace(query, q.TemplateVars)
			qr.QueryTemplated = &query
		}

		qc := ToQueryConfig(query)
		if err := s.DefaultRP(ctx, &qc, &src); err != nil {
			Error(w, http.StatusBadRequest, err.Error(), s.Logger)
			return
		}
		qr.QueryConfig = qc

		if stmt, err := queries.ParseSelect(query); err == nil {
			qr.QueryAST = stmt
		}

		if len(q.TemplateVars) > 0 {
			qr.TemplateVars = q.TemplateVars
			qr.QueryConfig.RawText = &qr.Query
		}

		qr.QueryConfig.ID = q.ID
		res.Queries[i] = qr
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// DefaultRP will add the default retention policy to the QC if one has not been specified
func (s *Service) DefaultRP(ctx context.Context, qc *chronograf.QueryConfig, src *chronograf.Source) error {
	// Only need to find the default RP IFF the qc's rp is empty
	if qc.RetentionPolicy != "" {
		return nil
	}

	// For queries without databases, measurements, or fields we will not
	// be able to find an RP
	if qc.Database == "" || qc.Measurement == "" || len(qc.Fields) == 0 {
		return nil
	}

	db := s.Databases
	if err := db.Connect(ctx, src); err != nil {
		return fmt.Errorf("Unable to connect to source: %v", err)
	}

	rps, err := db.AllRP(ctx, qc.Database)
	if err != nil {
		return fmt.Errorf("Unable to load RPs from DB %s: %v", qc.Database, err)
	}

	for _, rp := range rps {
		if rp.Default {
			qc.RetentionPolicy = rp.Name
			return nil
		}
	}

	return nil
}
