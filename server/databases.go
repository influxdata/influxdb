package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
)

type dbLinks struct {
	Self string `json:"self"`              // Self link mapping to this resource
	RPs  string `json:"retentionPolicies"` // URL for retention policies for this database
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
func newDBResponse(srcID int, name string, rps []rpResponse) dbResponse {
	base := "/chronograf/v1/sources"
	return dbResponse{
		Name: name,
		RPs:  rps,
		Links: dbLinks{
			Self: fmt.Sprintf("%s/%d/dbs/%s", base, srcID, name),
			RPs:  fmt.Sprintf("%s/%d/dbs/%s/rps", base, srcID, name),
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
func (r *rpResponse) WithLinks(srcID int, dbName string) {
	base := "/chronograf/v1/sources"
	r.Links = rpLinks{
		Self: fmt.Sprintf("%s/%d/dbs/%s/rps/%s", base, srcID, dbName, r.Name),
	}
}

type rpsResponse struct {
	RetentionPolicies []rpResponse `json:"retentionPolicies"`
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
		rps, err := h.allRPs(ctx, db, srcID, d.Name)
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

	db := h.Databases

	if err = db.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
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

	database, err := db.CreateDB(ctx, postedDB)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), h.Logger)
		return
	}

	rps, err := h.allRPs(ctx, db, srcID, database.Name)
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

	db := h.Databases

	if err = db.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	dbID := httprouter.GetParamFromContext(ctx, "dbid")

	dropErr := db.DropDB(ctx, dbID)
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

	db := h.Databases
	if err = db.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}

	dbID := httprouter.GetParamFromContext(ctx, "dbid")
	res, err := h.allRPs(ctx, db, srcID, dbID)
	if err != nil {
		msg := fmt.Sprintf("Unable to connect get RPs %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, h.Logger)
		return
	}
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

func (h *Service) allRPs(ctx context.Context, db chronograf.Databases, srcID int, dbID string) ([]rpResponse, error) {
	allRP, err := db.AllRP(ctx, dbID)
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
		rp.WithLinks(srcID, dbID)
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

	db := h.Databases
	if err = db.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
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

	dbID := httprouter.GetParamFromContext(ctx, "dbid")
	rp, err := db.CreateRP(ctx, dbID, postedRP)
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
	res.WithLinks(srcID, dbID)
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

	db := h.Databases
	if err = db.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
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

	dbID := httprouter.GetParamFromContext(ctx, "dbid")
	rpID := httprouter.GetParamFromContext(ctx, "rpid")
	rp, err := db.UpdateRP(ctx, dbID, rpID, postedRP)

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
	res.WithLinks(srcID, dbID)
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

	db := s.Databases
	if err = db.Connect(ctx, &src); err != nil {
		msg := fmt.Sprintf("Unable to connect to source %d: %v", srcID, err)
		Error(w, http.StatusBadRequest, msg, s.Logger)
		return
	}

	dbID := httprouter.GetParamFromContext(ctx, "dbid")
	rpID := httprouter.GetParamFromContext(ctx, "rpid")
	dropErr := db.DropRP(ctx, dbID, rpID)
	if dropErr != nil {
		Error(w, http.StatusBadRequest, dropErr.Error(), s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
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
