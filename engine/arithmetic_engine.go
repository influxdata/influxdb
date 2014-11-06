package engine

import (
	"strconv"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type ArithmeticEngine struct {
	next  Processor
	names map[string]*parser.Value
}

func NewArithmeticEngine(query *parser.SelectQuery, next Processor) (*ArithmeticEngine, error) {

	names := map[string]*parser.Value{}
	for idx, v := range query.GetColumnNames() {
		switch v.Type {
		case parser.ValueSimpleName:
			names[v.Name] = v
		case parser.ValueFunctionCall:
			names[v.Name] = v
		case parser.ValueExpression:
			if v.Alias != "" {
				names[v.Alias] = v
			} else {
				names["expr"+strconv.Itoa(idx)] = v
			}
		}
	}

	return &ArithmeticEngine{
		next:  next,
		names: names,
	}, nil
}

func (ae *ArithmeticEngine) Yield(s *protocol.Series) (bool, error) {
	if len(s.Points) == 0 {
		return ae.next.Yield(s)
	}

	newSeries := &protocol.Series{
		Name: s.Name,
	}

	// create the new column names
	for name := range ae.names {
		newSeries.Fields = append(newSeries.Fields, name)
	}

	for _, point := range s.Points {
		newPoint := &protocol.Point{
			Timestamp:      point.Timestamp,
			SequenceNumber: point.SequenceNumber,
		}
		for _, field := range newSeries.Fields {
			value := ae.names[field]
			v, err := GetValue(value, s.Fields, point)
			if err != nil {
				log.Error("Error in arithmetic computation: %s", err)
				return false, err
			}
			newPoint.Values = append(newPoint.Values, v)
		}
		newSeries.Points = append(newSeries.Points, newPoint)
	}

	return ae.next.Yield(newSeries)
}

func (self *ArithmeticEngine) Close() error {
	return self.next.Close()
}

func (self *ArithmeticEngine) Name() string {
	return "Arithmetic Engine"
}

func (self *ArithmeticEngine) Next() Processor {
	return self.next
}
