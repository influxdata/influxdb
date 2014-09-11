package engine

import (
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type JoinEngine struct {
	query                    *parser.SelectQuery
	next                     Processor
	table1, table2           string
	name                     string // the output table name
	lastPoint1, lastPoint2   *protocol.Point
	lastFields1, lastFields2 []string
}

func NewJoinEngine(shards []uint32, query *parser.SelectQuery, next Processor) Processor {
	table1 := query.GetFromClause().Names[0].GetAlias()
	table2 := query.GetFromClause().Names[1].GetAlias()
	name := table1 + "_join_" + table2

	joinEngine := &JoinEngine{
		next:   next,
		name:   name,
		table1: table1,
		table2: table2,
		query:  query,
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
	if *s.Name == je.table1 {
		je.lastPoint1 = s.Points[len(s.Points)-1]
		if je.lastFields1 == nil {
			for _, f := range s.Fields {
				je.lastFields1 = append(je.lastFields1, je.table1+"."+f)
			}
		}
	}

	if *s.Name == je.table2 {
		je.lastPoint2 = s.Points[len(s.Points)-1]
		if je.lastFields2 == nil {
			for _, f := range s.Fields {
				je.lastFields2 = append(je.lastFields2, je.table2+"."+f)
			}
		}
	}

	if je.lastPoint1 == nil || je.lastPoint2 == nil {
		return true, nil
	}

	newSeries := &protocol.Series{
		Name:   &je.name,
		Fields: append(je.lastFields1, je.lastFields2...),
		Points: []*protocol.Point{
			{
				Values:    append(je.lastPoint1.Values, je.lastPoint2.Values...),
				Timestamp: je.lastPoint2.Timestamp,
			},
		},
	}

	je.lastPoint1 = nil
	je.lastPoint2 = nil

	filteredSeries, err := Filter(je.query, newSeries)
	if err != nil {
		return false, err
	}

	if len(filteredSeries.Points) > 0 {
		return je.next.Yield(newSeries)
	}
	return true, nil
}
