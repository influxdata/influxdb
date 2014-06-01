package engine

import (
	"common"
	"protocol"
	"parser"
	"strconv"
	"sort"
	"errors"
	"fmt"

	log "code.google.com/p/log4go"
)

type HavingEngine struct {
	query               *parser.SelectQuery
	responseChan        chan *protocol.Response
	response            *protocol.Response
	maxPointsInResponse int
	limiter             *Limiter
	responseType        *protocol.Response_Type

	// query statistics
	runStartTime  float64
	runEndTime    float64
	pointsRead    int64
	pointsWritten int64
	shardId       int
	shardLocal    bool

	// having
	aggregator string
	name string
	limit int64

}

type conditionState struct {
	Value *parser.Value
}

func checkCondition(condition *parser.WhereCondition, state *conditionState) error {
	bool, ok:= condition.GetBoolExpression()
	log.Debug("Name: %+v, %+v", condition, bool)
	if ok {
		if bool.Type == parser.ValueFunctionCall {
			if bool.Name != "top" && bool.Name != "bottom" {
				return errors.New(fmt.Sprintf("%s aggregate function does not supported", bool.Name))
			} else {
				if state.Value == nil {
					state.Value = bool
					return nil
				} else {
					return errors.New(fmt.Sprintf("having clause can't call aggregate function multiple times."))
				}
			}
		} else if bool.Type == parser.ValueExpression {
			if bool.Elems[0].Type == parser.ValueFunctionCall || bool.Elems[1].Type == parser.ValueFunctionCall {
				return errors.New(fmt.Sprintf("having clause doesn't support calling function in expression"))
			}
		}
	} else {
		left, ok := condition.GetLeftWhereCondition()
		if ok {
			err := checkCondition(left, state)
			if err != nil {
				return err
			}
		}

		if condition.Right != nil {
			return checkCondition(condition.Right, state)
		}
	}

	return nil
}



func NewHavingEngine(query *parser.SelectQuery, responseChan chan *protocol.Response, maxPointsInResponse int) (*HavingEngine, error) {
	return NewHavingEngineWithLimit(query, responseChan, maxPointsInResponse, 0)
}

func NewHavingEngineWithLimit(query *parser.SelectQuery, responseChan chan *protocol.Response, maxPointsInResponse, limit int) (*HavingEngine, error) {
	HavingEngine := &HavingEngine{
		query: query,
		responseChan:        responseChan,
		maxPointsInResponse: maxPointsInResponse,
		limiter:             NewLimiter(limit),
		responseType:        &queryResponse,
		runStartTime:        0,
		runEndTime:          0,
		pointsRead:          0,
		pointsWritten:       0,
		shardId:             0,
		shardLocal:          false, //that really doesn't matter if it is not EXPLAIN query
	}


	having := query.GetGroupByClause().GetCondition()
	state := &conditionState{}
	err := checkCondition(having, state)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("wrong conditions: %s", err))
	}

	if state.Value != nil {
		b := state.Value
		if b.Name != "top" && b.Name != "bottom" {
			return nil, errors.New(fmt.Sprintf("wrong conditions: %s function not supproted", b.Name))
		}
		if (len(b.Elems) != 2) {
			return nil, errors.New(fmt.Sprintf("wrong conditions: %s requires exact 2 parameters", b.Name))
		}

		name := b.Elems[0].Name
		l, err := strconv.ParseInt(b.Elems[1].Name, 10, 64)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("wrong conditions: %s", err))
		}
		HavingEngine.aggregator = b.Name
		HavingEngine.name = name
		HavingEngine.limit = l
	}

	return HavingEngine, nil
}

func (self *HavingEngine) YieldPoint(seriesName *string, columnNames []string, point *protocol.Point) bool {
	series := &protocol.Series{Name: seriesName, Points: []*protocol.Point{point}, Fields: columnNames}
	return self.YieldSeries(series)
}

func Filter2(query *parser.SelectQuery, condition *parser.WhereCondition, series *protocol.Series) (*protocol.Series, error) {
	if condition == nil {
		return series, nil
	}

	columns := map[string]struct{}{}
	for _, cs := range query.GetResultColumns() {
		for _, c := range cs {
			columns[c] = struct{}{}
		}
	}

	points := series.Points
	series.Points = nil
	for _, point := range points {
		ok, err := matches(condition , series.Fields, point)

		if err != nil {
			return nil, err
		}

		if ok {
			filterColumns(columns, series.Fields, point)
			series.Points = append(series.Points, point)
		}
	}

	if _, ok := columns["*"]; !ok {
		newFields := []string{}
		for _, f := range series.Fields {
			if _, ok := columns[f]; !ok {
				continue
			}

			newFields = append(newFields, f)
		}
		series.Fields = newFields
	}
	return series, nil
}

func (self *HavingEngine) YieldSeries(seriesIncoming *protocol.Series) bool {
	log.Debug("HavingEngine YieldSeries %d", len(seriesIncoming.Points))

	if *seriesIncoming.Name == "explain query" {
		self.responseType = &explainQueryResponse
		log.Debug("Response Changed!")
	} else {
		self.responseType = &queryResponse
	}


	index := 0
	if self.name != "" {
		// find index
		ok := false
		for offset, n := range seriesIncoming.GetFields() {
			if n == self.name {
				index = offset
				ok = true
				break
			}
		}

		if !ok {
			log.Debug("damepo not ok!")
			return false
		}
	}

	seriesIncoming , err := Filter2(self.query, self.query.GetGroupByClause().GetCondition(), seriesIncoming)
	if err != nil {
		log.Debug("Error: %+v", err)
	}

	if self.response == nil {
		self.response = &protocol.Response{
			Type:   self.responseType,
			Series: seriesIncoming,
		}
	} else {
		self.response.Series = common.MergeSeries(self.response.Series, seriesIncoming)
	}

	//	// TODO: Currently, having clause only supports top and bottom.
	if self.aggregator == "top" {
		log.Debug("TOP")
		sort.Sort(ByPointColumnDesc{self.response.Series.Points, index})
	} else if self.aggregator == "bottom" {
		log.Debug("BOTTOM")
		sort.Sort(ByPointColumnAsc{self.response.Series.Points, index})
	} else {
		log.Debug("THROUGH:")
		//return false
	}

	if self.limit > 0 && int64(len(self.response.Series.Points)) > self.limit {
		self.response.Series.Points = self.response.Series.Points[0:self.limit]
	}

	return true
}

func (self *HavingEngine) Close() {
	if self.response != nil && self.response.Series != nil && self.response.Series.Name != nil {
		log.Debug("HAVING WRITING:")
		self.responseChan <- self.response
	}
	response := &protocol.Response{Type: &endStreamResponse}
	self.responseChan <- response
}

func (self *HavingEngine) SetShardInfo(shardId int, shardLocal bool) {
	//EXPLAIN doens't really work with this query (yet ?)
}

func (self *HavingEngine) GetName() string {
	return "HavingEngine"
}
