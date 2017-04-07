package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/chronograf/influx/queries"
)

type QueryRequest struct {
	Query string `json:"query"`
}

// Queries parses InfluxQL and returns the JSON
func (s *Service) Queries(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	stmt, err := queries.ParseSelect(req.Query)
	if err != nil {
		Error(w, http.StatusBadRequest, fmt.Sprintf("Error parsing query: %v", err), s.Logger)
		return
	}

	encodeJSON(w, http.StatusOK, stmt, s.Logger)
}
