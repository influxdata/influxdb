package server

import (
	"fmt"
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
	ShardDuration string  `json:"shardDuration,omitempty"` // the shard duration (when creating a default retention policy)
	Links         dbLinks `json:"links"`                   // Links are URI locations related to the database
}

type dbsResponse struct {
  Databases []dbResponse `json:"databases"`
}

// Databases queries the list of all databases for a source
func (h *Service) GetDatabases(w http.ResponseWriter, r *http.Request) {
  ctx := r.Context()

  srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.SourcesStore.Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

  db := h.Databases

	if err = db.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

  databases, err := db.AllDB(ctx)
  if err != nil {
    Error(w, http.StatusBadRequest, err.Error(), h.Logger)
    return
  }

  dbs := make([]dbResponse, len(databases))
  for i, d := range databases {
	  dbs[i] = dbResponse{
      Name: d.Name,
    }
  }

	res := dbsResponse{
		Databases: dbs,
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}

func (h *Service) NewDatabase(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.SourcesStore.Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	db := h.Databases

	if err = db.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

  // need to pass database object from POST
	database, err := db.CreateDB(ctx, )
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	res := dbResponse{Name: database.Name}
	encodeJSON(w, http.StatusOK, res, h.Logger)
}
