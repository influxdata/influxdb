package engine

import (
	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

// TODO: Explain how JoinEngine work
type JoinEngine struct {
	query       *parser.SelectQuery
	next        Processor
	name        string // the output table name
	tableIdx    map[string]int
	tablesState []joinEngineState
	pts         int
}

// Create and return a new JoinEngine given the shards that will be
// processed and the query
func NewJoinEngine(shards []uint32, query *parser.SelectQuery, next Processor) Processor {
	tableNames := query.GetFromClause().Names
	name := query.GetFromClause().GetString()
	log4go.Debug("NewJoinEngine: shards=%v, query=%s, next=%s, tableNames=%v, name=%s",
		shards, query.GetQueryString(), next.Name(), tableNames, name)

	joinEngine := &JoinEngine{
		next:        next,
		name:        name,
		tablesState: make([]joinEngineState, len(tableNames)),
		tableIdx:    make(map[string]int, len(tableNames)),
		query:       query,
		pts:         0,
	}

	for i, tn := range tableNames {
		alias := tn.GetAlias()
		joinEngine.tablesState[i] = joinEngineState{}
		joinEngine.tableIdx[alias] = i
	}

	mergeEngine := NewCommonMergeEngine(shards, false, query.Ascending, joinEngine)
	return mergeEngine
}

func (je *JoinEngine) Name() string {
	return "JoinEngine"
}

func (je *JoinEngine) Close() error {
	return je.next.Close()
}

func (je *JoinEngine) Yield(s *protocol.Series) (bool, error) {
	log4go.Fine("JoinEngine.Yield(): %s", s)
	idx := je.tableIdx[s.GetName()]
	state := &je.tablesState[idx]
	// If the state for this table didn't contain a point already,
	// increment the number of tables ready to emit a point by
	// incrementing `pts`
	if state.lastPoint == nil {
		je.pts++
	}
	state.lastPoint = s.Points[len(s.Points)-1]
	// update the fields for this table. the fields shouldn't change
	// after the first point, so we only need to set them once
	if state.lastFields == nil {
		for _, f := range s.Fields {
			state.lastFields = append(state.lastFields, s.GetName()+"."+f)
		}
	}

	log4go.Fine("JoinEngine: pts = %d", je.pts)
	// if the number of tables ready to emit a point isn't equal to the
	// total number of tables being joined, then return
	if je.pts != len(je.tablesState) {
		return true, nil
	}

	// we arbitrarily use the timestamp of the first table's point as
	// the timestamp of the resulting point. may be we should use the
	// smalles (or largest) timestamp.
	ts := je.tablesState[0].lastPoint.Timestamp
	newSeries := &protocol.Series{
		Name:   &je.name,
		Fields: je.fields(),
		Points: []*protocol.Point{
			{
				Timestamp: ts,
				Values:    je.values(),
			},
		},
	}

	// filter the point. the user may have a where clause with the join,
	// e.g. `select * from join(foo1, foo2) where foo1.val > 10`. we
	// can't evaluate the where clause until after join happens
	filteredSeries, err := Filter(je.query, newSeries)
	if err != nil {
		return false, err
	}

	if len(filteredSeries.Points) > 0 {
		return je.next.Yield(newSeries)
	}
	return true, nil
}

func (self *JoinEngine) Next() Processor {
	return self.next
}

// private

type joinEngineState struct {
	lastFields []string
	lastPoint  *protocol.Point
}

// Returns the field names from all tables appended together
func (je *JoinEngine) fields() []string {
	fs := []string{}
	for _, s := range je.tablesState {
		fs = append(fs, s.lastFields...)
	}
	return fs
}

// Returns the field values from all tables appended together
func (je *JoinEngine) values() []*protocol.FieldValue {
	vs := make([]*protocol.FieldValue, 0)
	for i := range je.tablesState {
		// Take the address of the slice element, since we set lastPoint
		// to nil
		s := &je.tablesState[i]
		vs = append(vs, s.lastPoint.Values...)
		s.lastPoint = nil
	}
	je.pts = 0
	return vs
}
