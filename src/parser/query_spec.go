package parser

import (
	"common"
	"time"
)

type QuerySpec struct {
	query                       *Query
	database                    string
	isRegex                     bool
	names                       []string
	user                        common.User
	startTime                   time.Time
	endTime                     time.Time
	seriesValuesAndColumns      map[*Value][]string
	RunAgainstAllServersInShard bool
	groupByInterval             *time.Duration
	groupByColumnCount          int
}

func NewQuerySpec(user common.User, database string, query *Query) *QuerySpec {
	return &QuerySpec{user: user, query: query, database: database}
}

func (self *QuerySpec) GetStartTime() time.Time {
	if self.query.SelectQuery != nil {
		return self.query.SelectQuery.GetStartTime()
	} else if self.query.DeleteQuery != nil {
		return self.query.DeleteQuery.GetStartTime()
	}
	return time.Now()
}

func (self *QuerySpec) GetEndTime() time.Time {
	if self.query.SelectQuery != nil {
		return self.query.SelectQuery.GetEndTime()
	} else if self.query.DeleteQuery != nil {
		return self.query.DeleteQuery.GetEndTime()
	}
	return time.Now()
}

func (self *QuerySpec) Database() string {
	return self.database
}

func (self *QuerySpec) User() common.User {
	return self.user
}

func (self *QuerySpec) DeleteQuery() *DeleteQuery {
	return self.query.DeleteQuery
}

func (self *QuerySpec) TableNames() []string {
	if self.names != nil {
		return self.names
	}
	if self.query.SelectQuery == nil {
		self.names = []string{}
		return self.names
	}

	namesAndColumns := self.query.SelectQuery.GetReferencedColumns()

	names := make([]string, 0, len(namesAndColumns))
	for name, _ := range namesAndColumns {
		if _, isRegex := name.GetCompiledRegex(); isRegex {
			self.isRegex = true
		} else {
			names = append(names, name.Name)
		}
	}
	return names
}

func (self *QuerySpec) SeriesValuesAndColumns() map[*Value][]string {
	if self.seriesValuesAndColumns != nil {
		return self.seriesValuesAndColumns
	}
	if self.query.SelectQuery != nil {
		self.seriesValuesAndColumns = self.query.SelectQuery.GetReferencedColumns()
	} else {
		self.seriesValuesAndColumns = make(map[*Value][]string)
	}
	return self.seriesValuesAndColumns
}

func (self *QuerySpec) GetGroupByInterval() *time.Duration {
	if self.query.SelectQuery == nil {
		return nil
	}
	if self.groupByInterval == nil {
		self.groupByInterval, _ = self.query.SelectQuery.GetGroupByClause().GetGroupByTime()
	}
	return self.groupByInterval
}

func (self *QuerySpec) GetGroupByColumnCount() int {
	if self.query.SelectQuery == nil {
		return 0
	}
	if self.groupByColumnCount == 0 {
		self.groupByColumnCount = len(self.query.SelectQuery.GetGroupByClause().Elems) - 1
	}
	return self.groupByColumnCount
}

func (self *QuerySpec) IsRegex() bool {
	self.TableNames()
	return self.isRegex
}

func (self *QuerySpec) HasReadAccess(name string) bool {
	return self.user.HasReadAccess(name)
}

func (self *QuerySpec) IsSinglePointQuery() bool {
	if self.query.SelectQuery != nil {
		return self.query.SelectQuery.IsSinglePointQuery()
	}
	return false
}

func (self *QuerySpec) IsExplainQuery() bool {
	if self.query.SelectQuery != nil {
		return self.query.SelectQuery.IsExplainQuery()
	}
	return false
}

func (self *QuerySpec) SelectQuery() *SelectQuery {
	return self.query.SelectQuery
}

func (self *QuerySpec) ShouldQueryShortTermAndLongTerm() (shouldQueryShortTerm bool, shouldQueryLongTerm bool) {
	for val, _ := range self.SeriesValuesAndColumns() {
		_, isRegex := val.GetCompiledRegex()
		if isRegex {
			return true, true
		}
		firstChar := val.Name[0]
		if firstChar < 97 {
			shouldQueryLongTerm = true
		} else {
			shouldQueryShortTerm = true
		}
	}
	return
}

func (self *QuerySpec) IsListSeriesQuery() bool {
	return self.query.IsListSeriesQuery()
}

func (self *QuerySpec) IsDeleteFromSeriesQuery() bool {
	return self.query.DeleteQuery != nil
}

func (self *QuerySpec) GetQueryString() string {
	return self.query.GetQueryString()
}

func (self *QuerySpec) GetQueryStringWithTimeCondition() string {
	if self.query.SelectQuery != nil {
		return self.query.SelectQuery.GetQueryStringWithTimeCondition()
	} else if self.query.DeleteQuery != nil {
		return self.query.DeleteQuery.GetQueryStringWithTimeCondition()
	}
	return self.query.GetQueryString()
}

func (self *QuerySpec) IsDropSeriesQuery() bool {
	return self.query.DropSeriesQuery != nil
}

func (self *QuerySpec) Query() *Query {
	return self.query
}

func (self *QuerySpec) IsAscending() bool {
	if self.query.SelectQuery != nil {
		return self.query.SelectQuery.Ascending
	}
	return false
}

func (self *QuerySpec) ReadsFromMultipleSeries() bool {
	return len(self.SeriesValuesAndColumns()) > 1
}

func (self *QuerySpec) IsDestructiveQuery() bool {
	return self.query.DeleteQuery != nil || self.query.DropQuery != nil || self.query.DropSeriesQuery != nil
}

func (self *QuerySpec) HasAggregates() bool {
	return self.SelectQuery() != nil && self.SelectQuery().HasAggregates()
}
