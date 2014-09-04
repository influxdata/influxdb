package engine

import "github.com/influxdb/influxdb/parser"

func NewQueryEngine(next Processor, query *parser.SelectQuery) (Processor, error) {
	limit := query.Limit

	var engine Processor = NewPassthroughEngineWithLimit(next, 1, limit)

	var err error
	if query.HasAggregates() {
		engine, err = NewAggregatorEngine(query, engine)
	} else if query.ContainsArithmeticOperators() {
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
