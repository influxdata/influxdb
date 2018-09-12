// Package influxql implements the transpiler for executing influxql queries in the 2.0 query engine.
package influxql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/functions"
	"github.com/influxdata/influxql"
	"github.com/influxdata/platform"
)

// Transpiler converts InfluxQL queries into a query spec.
type Transpiler struct {
	Config         *Config
	dbrpMappingSvc platform.DBRPMappingService
}

func NewTranspiler(dbrpMappingSvc platform.DBRPMappingService) *Transpiler {
	return NewTranspilerWithConfig(dbrpMappingSvc, Config{})
}

func NewTranspilerWithConfig(dbrpMappingSvc platform.DBRPMappingService, cfg Config) *Transpiler {
	return &Transpiler{
		Config:         &cfg,
		dbrpMappingSvc: dbrpMappingSvc,
	}
}

func (t *Transpiler) Transpile(ctx context.Context, txt string) (*flux.Spec, error) {
	// Parse the text of the query.
	q, err := influxql.ParseQuery(txt)
	if err != nil {
		return nil, err
	}

	transpiler := newTranspilerState(t.dbrpMappingSvc, t.Config)
	for i, s := range q.Statements {
		if err := transpiler.Transpile(ctx, i, s); err != nil {
			return nil, err
		}
	}
	return transpiler.spec, nil
}

type transpilerState struct {
	id             int
	stmt           *influxql.SelectStatement
	config         Config
	spec           *flux.Spec
	nextID         map[string]int
	dbrpMappingSvc platform.DBRPMappingService
}

func newTranspilerState(dbrpMappingSvc platform.DBRPMappingService, config *Config) *transpilerState {
	state := &transpilerState{
		spec:           &flux.Spec{},
		nextID:         make(map[string]int),
		dbrpMappingSvc: dbrpMappingSvc,
	}
	if config != nil {
		state.config = *config
	}
	if state.config.NowFn == nil {
		state.config.NowFn = time.Now
	}

	// Stamp the current time using the now function from the config or the default.
	state.spec.Now = state.config.NowFn()
	return state
}

func (t *transpilerState) Transpile(ctx context.Context, id int, s influxql.Statement) error {
	switch stmt := s.(type) {
	case *influxql.SelectStatement:
		if err := t.transpileSelect(ctx, id, stmt); err != nil {
			return err
		}
	case *influxql.ShowTagValuesStatement:
		if err := t.transpileShowTagValues(ctx, id, stmt); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown statement type %T", s)
	}
	return nil
}

func (t *transpilerState) transpileShowTagValues(ctx context.Context, id int, stmt *influxql.ShowTagValuesStatement) error {

	return nil
}

func (t *transpilerState) transpileSelect(ctx context.Context, id int, stmt *influxql.SelectStatement) error {
	// Clone the select statement and omit the time from the list of column names.
	t.stmt = stmt.Clone()
	t.stmt.OmitTime = true
	t.id = id

	groups, err := identifyGroups(t.stmt)
	if err != nil {
		return err
	} else if len(groups) == 0 {
		return errors.New("at least 1 non-time field must be queried")
	}

	cursors := make([]cursor, 0, len(groups))
	for _, gr := range groups {
		cur, err := gr.createCursor(t)
		if err != nil {
			return err
		}
		cursors = append(cursors, cur)
	}

	// Join the cursors together on the measurement name.
	// TODO(jsternberg): This needs to join on all remaining group keys.
	cur := Join(t, cursors, []string{"_time", "_measurement"})

	// Map each of the fields into another cursor. This evaluates any lingering expressions.
	cur, err = t.mapFields(cur)
	if err != nil {
		return err
	}

	// Yield the cursor from the last cursor to a stream with the name of the statement id.
	// TODO(jsternberg): Include the statement id in the transpiler state when we create
	// the state so we can yield to something other than zero.
	t.op("yield", &functions.YieldOpSpec{Name: strconv.Itoa(t.id)}, cur.ID())
	return nil
}

func (t *transpilerState) mapType(ref *influxql.VarRef) influxql.DataType {
	// TODO(jsternberg): Actually evaluate the type against the schema.
	return influxql.Tag
}

func (t *transpilerState) from(m *influxql.Measurement) (flux.OperationID, error) {
	db, rp := m.Database, m.RetentionPolicy
	if db == "" {
		if t.config.DefaultDatabase == "" {
			return "", errors.New("database is required")
		}
		db = t.config.DefaultDatabase
	}
	if rp == "" {
		if t.config.DefaultRetentionPolicy != "" {
			rp = t.config.DefaultRetentionPolicy
		}
	}

	var filter platform.DBRPMappingFilter
	filter.Cluster = &t.config.Cluster
	if db != "" {
		filter.Database = &db
	}
	if rp != "" {
		filter.RetentionPolicy = &rp
	}
	defaultRP := rp == ""
	filter.Default = &defaultRP
	mapping, err := t.dbrpMappingSvc.Find(context.TODO(), filter)
	if err != nil {
		return "", err
	}

	spec := &functions.FromOpSpec{
		BucketID: mapping.BucketID,
	}
	return t.op("from", spec), nil
}

func (t *transpilerState) op(name string, spec flux.OperationSpec, parents ...flux.OperationID) flux.OperationID {
	op := flux.Operation{
		ID:   flux.OperationID(fmt.Sprintf("%s%d", name, t.nextID[name])),
		Spec: spec,
	}
	t.spec.Operations = append(t.spec.Operations, &op)
	for _, pid := range parents {
		t.spec.Edges = append(t.spec.Edges, flux.Edge{
			Parent: pid,
			Child:  op.ID,
		})
	}
	t.nextID[name]++
	return op.ID
}
