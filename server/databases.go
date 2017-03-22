package server

import (
	"fmt"
	"net/http"

	"github.com/influxdata/chronograf"
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

// type influxResponse struct {
// 	Results interface{} `json:"results"` // results from influx
// }

// func (h *Service) sourcesSeries(ctx context.Context, w http.ResponseWriter, r *http.Request) (int, chronograf.TimeSeries, error) {
// 	srcID, err := paramID("id", r)
// 	if err != nil {
// 		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
// 		return 0, nil, err
// 	}
//
// 	src, err := h.SourcesStore.Get(ctx, srcID)
// 	if err != nil {
// 		notFound(w, srcID, h.Logger)
// 		return 0, nil, err
// 	}
//
// 	ts, err := h.TimeSeries(src)
// 	if err != nil {
// 		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
// 		Error(w, http.StatusBadRequest, msg, h.Logger)
// 		return 0, nil, err
// 	}
//
// 	if err = ts.Connect(ctx, &src); err != nil {
// 		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
// 		Error(w, http.StatusBadRequest, msg, h.Logger)
// 		return 0, nil, err
// 	}
// 	return srcID, ts, nil
// }

// Databases queries the list of all databases for a source
func (h *Service) Databases(w http.ResponseWriter, r *http.Request) {
  ctx := r.Context()
  srcID, ts, err := h.sourcesSeries(ctx, w, r)
  if err != nil {
    return
  }

  store := ts.Databases(ctx)
  databases, err := store.All(ctx)
  if err != nil {
    Error(w, http.StatusBadRequest, err.Error(), h.Logger)
    return
  }

  dbs := make([]dbResponse, len(databases))
  for i, d := range databases {

  }

  // res = append(res, database{Name: response})

	res := dbsResponse{
		Databases: dbs,
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}
