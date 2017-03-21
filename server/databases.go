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

type database struct {
	Name          string  `json:"name"`                  // a unique string identifier for the database
	Duration      string  `json:"duration,omitempty"`    // the duration (when creating a default retention policy)
	Replication   int32   `json:"replication,omitempty"` // the replication factor (when creating a default retention policy)
	ShardDuration string  `json:shardDuration,omitempty` // the shard duration (when creating a default retention policy)
	Links         dbLinks `json:links`                   // Links are URI locations related to the database
}

type postInfluxResponze struct {
	Results interface{} `json:"results"` // results from influx
}

// Databases queries the list of all databases for a source
func (h *Service) Databases(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	// res := []database{}

	// move this influxdb communication code somewhere else after it's working
	// START
	ctx := r.Context()
	src, err := h.SourcesStore.Get(ctx, id)
	if err != nil {
		notFound(w, id, h.Logger)
		return
	}

	ts, err := h.TimeSeries(src)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	if err = ts.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", id, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	req := chronograf.Query{Command: "SHOW DATABASES"}

	response, err := ts.Query(ctx, req)
	if err != nil {
		if err == chronograf.ErrUpstreamTimeout {
			msg := "Timeout waiting for Influx response"
			Error(w, http.StatusRequestTimeout, msg, h.Logger)
			return
		}
		// TODO: Here I want to return the error code from influx.
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	res := postInfluxResponze{
		Results: response,
	}

	//fmt.Printf("%+v\n", foo)
	// END

	encodeJSON(w, http.StatusOK, res, h.Logger)
}
