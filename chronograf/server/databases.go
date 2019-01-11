package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/influxdata/influxdb/chronograf"
	"github.com/julienschmidt/httprouter"
)

const (
	limitQuery  = "limit"
	offsetQuery = "offset"
)

type dbLinks struct {
	Self         string `json:"self"`              // Self link mapping to this resource
	RPs          string `json:"retentionPolicies"` // URL for retention policies for this database
	Measurements string `json:"measurements"`      // URL for measurements for this database
}

type dbResponse struct {
	Name          string       `json:"name"`                    // a unique string identifier for the database
	Duration      string       `json:"duration,omitempty"`      // the duration (when creating a default retention policy)
	Replication   int32        `json:"replication,omitempty"`   // the replication factor (when creating a default retention policy)
	ShardDuration string       `json:"shardDuration,omitempty"` // the shard duration (when creating a default retention policy)
	RPs           []rpResponse `json:"retentionPolicies"`       // RPs are the retention policies for a database
	Links         dbLinks      `json:"links"`                   // Links are URI locations related to the database
}

// newDBResponse creates the response for the /databases endpoint
func newDBResponse(srcID int, db string, rps []rpResponse) dbResponse {
	base := "/chronograf/v1/sources"
	return dbResponse{
		Name: db,
		RPs:  rps,
		Links: dbLinks{
			Self:         fmt.Sprintf("%s/%d/dbs/%s", base, srcID, db),
			RPs:          fmt.Sprintf("%s/%d/dbs/%s/rps", base, srcID, db),
			Measurements: fmt.Sprintf("%s/%d/dbs/%s/measurements?limit=100&offset=0", base, srcID, db),
		},
	}
}

type dbsResponse struct {
	Databases []dbResponse `json:"databases"`
}

type rpLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type rpResponse struct {
	Name          string  `json:"name"`          // a unique string identifier for the retention policy
	Duration      string  `json:"duration"`      // the duration
	Replication   int32   `json:"replication"`   // the replication factor
	ShardDuration string  `json:"shardDuration"` // the shard duration
	Default       bool    `json:"isDefault"`     // whether the RP should be the default
	Links         rpLinks `json:"links"`         // Links are URI locations related to the database
}

// WithLinks adds links to an rpResponse in place
func (r *rpResponse) WithLinks(srcID int, db string) {
	base := "/chronograf/v1/sources"
	r.Links = rpLinks{
		Self: fmt.Sprintf("%s/%d/dbs/%s/rps/%s", base, srcID, db, r.Name),
	}
}

type measurementLinks struct {
	Self  string `json:"self"`
	First string `json:"first"`
	Next  string `json:"next,omitempty"`
	Prev  string `json:"prev,omitempty"`
}

func newMeasurementLinks(src int, db string, limit, offset int) measurementLinks {
	base := "/chronograf/v1/sources"
	res := measurementLinks{
		Self:  fmt.Sprintf("%s/%d/dbs/%s/measurements?limit=%d&offset=%d", base, src, db, limit, offset),
		First: fmt.Sprintf("%s/%d/dbs/%s/measurements?limit=%d&offset=0", base, src, db, limit),
		Next:  fmt.Sprintf("%s/%d/dbs/%s/measurements?limit=%d&offset=%d", base, src, db, limit, offset+limit),
	}
	if offset-limit > 0 {
		res.Prev = fmt.Sprintf("%s/%d/dbs/%s/measurements?limit=%d&offset=%d", base, src, db, limit, offset-limit)
	}

	return res
}

type measurementsResponse struct {
	Measurements []chronograf.Measurement `json:"measurements"` // names of all measurements
	Links        measurementLinks         `json:"links"`        // Links are the URI locations for measurements pages
}

// GetDatabases queries the list of all databases for a source
func (h *Service) GetDatabases(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	dbsvc := h.Databases
	if err = dbsvc.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	databases, err := dbsvc.AllDB(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	dbs := make([]dbResponse, len(databases))
	for i, d := range databases {
		rps, err := h.allRPs(ctx, dbsvc, srcID, d.Name)
		if err != nil {
			Error(w, http.StatusBadRequest, err.Error(), h.Logger)
			return
		}
		dbs[i] = newDBResponse(srcID, d.Name, rps)
	}

	res := dbsResponse{
		Databases: dbs,
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// NewDatabase creates a new database within the datastore
func (h *Service) NewDatabase(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	dbsvc := h.Databases

	if err = dbsvc.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	postedDB := &chronograf.Database{}
	if err := json.NewDecoder(r.Body).Decode(postedDB); err != nil {
		invalidJSON(w, h.Logger)
		return
	}

	if err := ValidDatabaseRequest(postedDB); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	database, err := dbsvc.CreateDB(ctx, postedDB)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	rps, err := h.allRPs(ctx, dbsvc, srcID, database.Name)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}
	res := newDBResponse(srcID, database.Name, rps)
	encodeJSON(w, http.StatusCreated, res, h.Logger)
}

// DropDatabase removes a database from a data source
func (h *Service) DropDatabase(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	dbsvc := h.Databases

	if err = dbsvc.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	db := httprouter.ParamsFromContext(ctx).ByName("db")

	dropErr := dbsvc.DropDB(ctx, db)
	if dropErr != nil {
		Error(w, http.StatusBadRequest, dropErr.Error(), h.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// RetentionPolicies lists retention policies within a database
func (h *Service) RetentionPolicies(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	dbsvc := h.Databases
	if err = dbsvc.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	db := httprouter.ParamsFromContext(ctx).ByName("db")
	res, err := h.allRPs(ctx, dbsvc, srcID, db)
	if err != nil {
		msg := fmt.Sprintf("unable to connect get RPs %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

func (h *Service) allRPs(ctx context.Context, dbsvc chronograf.Databases, srcID int, db string) ([]rpResponse, error) {
	allRP, err := dbsvc.AllRP(ctx, db)
	if err != nil {
		return nil, err
	}

	rps := make([]rpResponse, len(allRP))
	for i, rp := range allRP {
		rp := rpResponse{
			Name:          rp.Name,
			Duration:      rp.Duration,
			Replication:   rp.Replication,
			ShardDuration: rp.ShardDuration,
			Default:       rp.Default,
		}
		rp.WithLinks(srcID, db)
		rps[i] = rp
	}
	return rps, nil
}

// NewRetentionPolicy creates a new retention policy for a database
func (h *Service) NewRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	dbsvc := h.Databases
	if err = dbsvc.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	postedRP := &chronograf.RetentionPolicy{}
	if err := json.NewDecoder(r.Body).Decode(postedRP); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := ValidRetentionPolicyRequest(postedRP); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	db := httprouter.ParamsFromContext(ctx).ByName("db")
	rp, err := dbsvc.CreateRP(ctx, db, postedRP)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}
	res := rpResponse{
		Name:          rp.Name,
		Duration:      rp.Duration,
		Replication:   rp.Replication,
		ShardDuration: rp.ShardDuration,
		Default:       rp.Default,
	}
	res.WithLinks(srcID, db)
	encodeJSON(w, http.StatusCreated, res, h.Logger)
}

// UpdateRetentionPolicy modifies an existing retention policy for a database
func (h *Service) UpdateRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	dbsvc := h.Databases
	if err = dbsvc.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	postedRP := &chronograf.RetentionPolicy{}
	if err := json.NewDecoder(r.Body).Decode(postedRP); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := ValidRetentionPolicyRequest(postedRP); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	params := httprouter.ParamsFromContext(ctx)
	db := params.ByName("db")
	rp := params.ByName("rp")
	p, err := dbsvc.UpdateRP(ctx, db, rp, postedRP)

	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	res := rpResponse{
		Name:          p.Name,
		Duration:      p.Duration,
		Replication:   p.Replication,
		ShardDuration: p.ShardDuration,
		Default:       p.Default,
	}
	res.WithLinks(srcID, db)
	encodeJSON(w, http.StatusCreated, res, h.Logger)
}

// DropRetentionPolicy removes a retention policy from a database
func (s *Service) DropRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	src, err := s.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, s.Logger)
		return
	}

	dbsvc := s.Databases
	if err = dbsvc.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	params := httprouter.ParamsFromContext(ctx)
	db := params.ByName("db")
	rp := params.ByName("rp")
	dropErr := dbsvc.DropRP(ctx, db, rp)
	if dropErr != nil {
		Error(w, http.StatusBadRequest, dropErr.Error(), s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Measurements lists measurements within a database
func (h *Service) Measurements(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	limit, offset, err := validMeasurementQuery(r.URL.Query())
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	src, err := h.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	dbsvc := h.Databases
	if err = dbsvc.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	db := httprouter.ParamsFromContext(ctx).ByName("db")
	measurements, err := dbsvc.GetMeasurements(ctx, db, limit, offset)
	if err != nil {
		msg := fmt.Sprintf("Unable to get measurements %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	res := measurementsResponse{
		Measurements: measurements,
		Links:        newMeasurementLinks(srcID, db, limit, offset),
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}

func validMeasurementQuery(query url.Values) (limit, offset int, err error) {
	limitParam := query.Get(limitQuery)
	if limitParam == "" {
		limit = 100
	} else {
		limit, err = strconv.Atoi(limitParam)
		if err != nil {
			return
		}
		if limit <= 0 {
			limit = 100
		}
	}

	offsetParam := query.Get(offsetQuery)
	if offsetParam == "" {
		offset = 0
	} else {
		offset, err = strconv.Atoi(offsetParam)
		if err != nil {
			return
		}
		if offset < 0 {
			offset = 0
		}
	}

	return
}

// ValidDatabaseRequest checks if the database posted is valid
func ValidDatabaseRequest(d *chronograf.Database) error {
	if len(d.Name) == 0 {
		return fmt.Errorf("name is required")
	}
	return nil
}

// ValidRetentionPolicyRequest checks if a retention policy is valid on POST
func ValidRetentionPolicyRequest(rp *chronograf.RetentionPolicy) error {
	if len(rp.Name) == 0 {
		return fmt.Errorf("name is required")
	}
	if len(rp.Duration) == 0 {
		return fmt.Errorf("duration is required")
	}
	if rp.Replication == 0 {
		return fmt.Errorf("replication factor is invalid")
	}
	return nil
}
