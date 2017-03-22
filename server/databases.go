package server

import (
	"net/http"
)

type dbLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
	RPs  string `json:"rps"`  // URL for retention policies for this database
}

type dbResponse struct {
	Name          string  `json:"name"`                  // a unique string identifier for the database
	Duration      string  `json:"duration,omitempty"`    // the duration (when creating a default retention policy)
	Replication   int32   `json:"replication,omitempty"` // the replication factor (when creating a default retention policy)
	ShardDuration string  `json:shardDuration,omitempty` // the shard duration (when creating a default retention policy)
	Links         dbLinks `json:links`                   // Links are URI locations related to the database
}

type dbsResponse struct {
  Databases []dbResponse `json:"databases"`
}

// Databases queries the list of all databases for a source
func (h *Service) Databases(w http.ResponseWriter, r *http.Request) {
  ctx := r.Context()
  srcID, ts, err := h.sourcesSeries(ctx, w, r)
  if err != nil {
    return
  }

  databases, err := ts.AllDB(ctx)
  if err != nil {
    Error(w, http.StatusBadRequest, err.Error(), h.Logger)
    return
  }

  dbs := make([]dbResponse, len(databases))
  for i, d := range databases {

  }

	res := dbsResponse{
		Databases: dbs,
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}
