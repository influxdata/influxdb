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

var having_aggregators []string = []string{
	"top",
	"bottom",
}

type HavingEngine struct {
	query               *parser.SelectQuery
	responseChan        chan *protocol.Response
	response            *protocol.Response
	maxPointsInResponse int
	responseType        *protocol.Response_Type

	// having
	hasAggregator bool
	aggregatorName string
	name string
	aggregatorLimit int64
	Limit int

	// query statistics
	runStartTime  float64
	runEndTime    float64
	pointsRead    int64
	pointsWritten int64
	shardId       int
	shardLocal    bool
}

type conditionState struct {
	Value *parser.Value
}

func checkConditionAndCollectAggregateValue(condition *parser.WhereCondition, state *conditionState) error {
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
			err := checkConditionAndCollectAggregateValue(left, state)
			if err != nil {
				return err
			}
		}

		if condition.Right != nil {
			return checkConditionAndCollectAggregateValue(condition.Right, state)
		}
	}

	return nil
}

func HavingFilter(query *parser.SelectQuery, condition *parser.WhereCondition, series *protocol.Series) (*protocol.Series, error) {
	if condition == nil {
		return series, nil
	}

	columns := map[string]struct{}{}
	for _, cs := range series.Fields {
		columns[cs] = struct{}{}
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

	return series, nil
}

func NewHavingEngine(query *parser.SelectQuery, responseChan chan *protocol.Response, maxPointsInResponse int) (*HavingEngine, error) {
	return NewHavingEngineWithLimit(query, responseChan, maxPointsInResponse, 0)
}

func NewHavingEngineWithLimit(query *parser.SelectQuery, responseChan chan *protocol.Response, maxPointsInResponse, limit int) (*HavingEngine, error) {
	HavingEngine := &HavingEngine{
		query: query,
		responseChan:        responseChan,
		maxPointsInResponse: maxPointsInResponse,
		responseType:        &queryResponse,
		runStartTime:        0,
		runEndTime:          0,
		pointsRead:          0,
		pointsWritten:       0,
		shardId:             0,
		shardLocal:          false, //that really doesn't matter if it is not EXPLAIN query
		Limit:               limit,
	}

	having := query.GetGroupByClause().GetCondition()
	state := &conditionState{}
	err := checkConditionAndCollectAggregateValue(having, state)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("wrong conditions: %s", err))
	}

	if state.Value != nil {
		ok := false
		for _, name := range having_aggregators {
			if state.Value.Name == name {
				ok = true
			}
		}

		if !ok{
			return nil, errors.New(fmt.Sprintf("wrong conditions: %s function not supproted", state.Value.Name))
		}

		if (len(state.Value.Elems) != 2) {
			return nil, errors.New(fmt.Sprintf("wrong conditions: %s requires exact 2 parameters", state.Value.Name))
		}

		name := state.Value.Elems[0].Name
		l, err := strconv.ParseInt(state.Value.Elems[1].Name, 10, 64)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("wrong conditions: %s", err))
		}

		HavingEngine.aggregatorName = state.Value.Name
		HavingEngine.name = name
		HavingEngine.aggregatorLimit = l
		HavingEngine.hasAggregator = true
	}

	return HavingEngine, nil
}

func (self *HavingEngine) YieldPoint(seriesName *string, columnNames []string, point *protocol.Point) bool {
	series := &protocol.Series{Name: seriesName, Points: []*protocol.Point{point}, Fields: columnNames}
	return self.YieldSeries(series)
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

	seriesIncoming , err := HavingFilter(self.query, self.query.GetGroupByClause().GetCondition(), seriesIncoming)
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

	if self.hasAggregator {
		if self.aggregatorName == "top" {
			sort.Sort(ByPointColumnDesc{self.response.Series.Points, index})
		} else if self.aggregatorName == "bottom" {
			sort.Sort(ByPointColumnAsc{self.response.Series.Points, index})
		} else {
			panic("never get here")
		}

		if self.aggregatorLimit > 0 && int64(len(self.response.Series.Points)) > self.aggregatorLimit {
			self.response.Series.Points = self.response.Series.Points[0:self.aggregatorLimit]
		}
	}

	return true
}

func (self *HavingEngine) Close() {
	log.Debug("LIMIT: %+v", self.response.Series.Points)
	if self.Limit > 0 && len(self.response.Series.Points) > self.Limit {
		self.response.Series.Points = self.response.Series.Points[0:self.Limit]
	}

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
