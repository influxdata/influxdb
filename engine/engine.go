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

	switch fromClause.Type {
	case parser.FromClauseInnerJoin:
		engine = NewJoinEngine(query, engine)
	case parser.FromClauseMerge:
		tables := make([]string, len(fromClause.Names))
		for i, name := range fromClause.Names {
			tables[i] = name.Name.Name
		}
		engine = NewMergeEngine(tables, query.Ascending, engine)
	case parser.FromClauseMergeFun:
		panic("QueryEngine cannot be called with merge function")
	}

	if err != nil {
		return nil, err
	}
	return engine, nil
}
