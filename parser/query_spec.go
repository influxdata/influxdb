package parser

import (
	"regexp"
	"time"

	"github.com/influxdb/influxdb/common"
)

type QuerySpec struct {
	query                       *Query
	database                    string
	isRegex                     bool
	regex                       *regexp.Regexp
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

func (self *QuerySpec) AllShardsQuery() bool {
	return self.IsDropSeriesQuery()
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
	names, _ := self.TableNamesAndRegex()
	return names
}

func (self *QuerySpec) TableNamesAndRegex() ([]string, *regexp.Regexp) {
	if self.names != nil {
		return self.names, self.regex
	}
	if self.query.SelectQuery == nil {
		if self.query.DeleteQuery != nil {
			self.names = make([]string, 0)
			for _, n := range self.query.DeleteQuery.GetFromClause().Names {
				self.names = append(self.names, n.Name.Name)
			}
		} else {
			self.names = []string{}
		}
		return self.names, nil
	}

	namesAndColumns := self.query.SelectQuery.GetReferencedColumns()

	names := make([]string, 0, len(namesAndColumns))
	for name := range namesAndColumns {
		if r, isRegex := name.GetCompiledRegex(); isRegex {
			self.regex = r
			self.isRegex = true
		} else {
			names = append(names, name.Name)
		}
	}
	self.names = names
	return names, self.regex
}

func (self *QuerySpec) SeriesValuesAndColumns() map[*Value][]string {
	if self.seriesValuesAndColumns != nil {
		return self.seriesValuesAndColumns
	}
	if self.query.SelectQuery != nil {
		self.seriesValuesAndColumns = self.query.SelectQuery.GetReferencedColumns()
	} else if self.query.DeleteQuery != nil {
		self.seriesValuesAndColumns = make(map[*Value][]string)
		for _, name := range self.query.DeleteQuery.GetFromClause().Names {
			self.seriesValuesAndColumns[name.Name] = nil
		}
	} else if self.query.DropSeriesQuery != nil {
		self.seriesValuesAndColumns = make(map[*Value][]string)
		value := &Value{Name: self.query.DropSeriesQuery.GetTableName()}
		self.seriesValuesAndColumns[value] = nil
	}
	return self.seriesValuesAndColumns
}

func (self *QuerySpec) GetFromClause() *FromClause {
	if q := self.query.SelectQuery; q != nil {
		return q.GetFromClause()
	}
	return nil
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
	return self.query.GetQueryStringWithTimeCondition()
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
