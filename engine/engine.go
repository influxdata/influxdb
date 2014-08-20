package engine

import (
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

var (
	TRUE = true
)

type SeriesState struct {
	started       bool
	trie          *Trie
	pointsRange   *PointRange
	lastTimestamp int64
}

const (
	POINT_BATCH_SIZE = 64
)

func NewQueryEngine(next Processor, query *parser.SelectQuery) (Processor, error) {
	limit := query.Limit

	var engine Processor = NewPassthroughEngineWithLimit(next, 1, limit)

	var err error
	if query.HasAggregates() {
		engine, err = NewAggregatorEngine(query, engine)
	} else if containsArithmeticOperators(query) {
		engine, err = NewArithmeticEngine(query, engine)
	}

	fromClause := query.GetFromClause()
	if fromClause.Type == parser.FromClauseMerge {
		engine = NewMergeEngine(fromClause.Names[0].Name.Name, fromClause.Names[1].Name.Name, query.Ascending, engine)
	} else if fromClause.Type == parser.FromClauseInnerJoin {
		engine = NewJoinEngine(query, engine)
	}

	if err != nil {
		return nil, err
	}
	return engine, nil
}

func containsArithmeticOperators(query *parser.SelectQuery) bool {
	for _, column := range query.GetColumnNames() {
		if column.Type == parser.ValueExpression {
			return true
		}
	}
	return false
}

func crossProduct(values [][][]*protocol.FieldValue) [][]*protocol.FieldValue {
	if len(values) == 0 {
		return [][]*protocol.FieldValue{{}}
	}

	_returnedValues := crossProduct(values[:len(values)-1])
	returnValues := [][]*protocol.FieldValue{}
	for _, v := range values[len(values)-1] {
		for _, values := range _returnedValues {
			returnValues = append(returnValues, append(values, v...))
		}
	}
	return returnValues
}
