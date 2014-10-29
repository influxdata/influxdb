package engine

import (
	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type JoinEngineState struct {
	lastFields []string
	lastPoint  *protocol.Point
}

type JoinEngine struct {
	query      *parser.SelectQuery
	next       Processor
	name       string // the output table name
	tableIdx   map[string]int
	tableState []JoinEngineState
	pts        int
}

func NewJoinEngine(shards []uint32, query *parser.SelectQuery, next Processor) Processor {
	tableNames := query.GetFromClause().Names
	name := query.GetFromClause().GetString()
	log4go.Debug("NewJoinEngine: shards=%v, query=%s, next=%s, tableNames=%v, name=%s",
		shards, query.GetQueryString(), next.Name(), tableNames, name)

	joinEngine := &JoinEngine{
		next:       next,
		name:       name,
		tableState: make([]JoinEngineState, len(tableNames)),
		tableIdx:   make(map[string]int, len(tableNames)),
		query:      query,
		pts:        0,
	}

	for i, tn := range tableNames {
		alias := tn.GetAlias()
		joinEngine.tableState[i] = JoinEngineState{}
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
	state := &je.tableState[idx]
	if state.lastPoint == nil {
		je.pts++
	}
	state.lastPoint = s.Points[len(s.Points)-1]
	if state.lastFields == nil {
		for _, f := range s.Fields {
			state.lastFields = append(state.lastFields, s.GetName()+"."+f)
		}
	}

	log4go.Fine("JoinEngine: pts = %d", je.pts)
	if je.pts != len(je.tableState) {
		return true, nil
	}

	ts := je.tableState[0].lastPoint.Timestamp
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

	// reset the number of points available for
	filteredSeries, err := Filter(je.query, newSeries)
	if err != nil {
		return false, err
	}

	if len(filteredSeries.Points) > 0 {
		return je.next.Yield(newSeries)
	}
	return true, nil
}

func (je *JoinEngine) fields() []string {
	fs := []string{}
	for _, s := range je.tableState {
		fs = append(fs, s.lastFields...)
	}
	return fs
}

func (je *JoinEngine) values() []*protocol.FieldValue {
	vs := make([]*protocol.FieldValue, 0)
	for i := range je.tableState {
		s := &je.tableState[i]
		vs = append(vs, s.lastPoint.Values...)
		s.lastPoint = nil
	}
	je.pts = 0
	return vs
}

func (self *JoinEngine) Next() Processor {
	return self.next
}
