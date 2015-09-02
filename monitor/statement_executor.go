package monitor

import (
	"fmt"

	"github.com/influxdb/influxdb/influxql"
)

// StatementExecutor translates InfluxQL queries to Monitor methods.
type StatementExecutor struct {
	Monitor interface {
		Statistics() ([]*statistic, error)
	}
}

// ExecuteStatement executes monitor-related query statements.
func (s *StatementExecutor) ExecuteStatement(stmt influxql.Statement) *influxql.Result {
	switch stmt := stmt.(type) {
	case *influxql.ShowStatsStatement:
		return s.executeShowStatistics()
	case *influxql.ShowDiagnosticsStatement:
		return s.executeShowDiagnostics()
	default:
		panic(fmt.Sprintf("unsupported statement type: %T", stmt))
	}
}

func (s *StatementExecutor) executeShowStatistics() *influxql.Result {
	stats, _ := s.Monitor.Statistics()
	rows := make([]*influxql.Row, len(stats))

	for n, stat := range stats {
		row := &influxql.Row{Name: stat.Name, Tags: stat.Tags}

		values := make([]interface{}, 0, len(stat.Values))
		for _, k := range stat.valueNames() {
			row.Columns = append(row.Columns, k)
			values = append(values, stat.Values[k])
		}
		row.Values = [][]interface{}{values}
		rows[n] = row
	}
	return &influxql.Result{Series: rows}
}

func (s *StatementExecutor) executeShowDiagnostics() *influxql.Result {
	return nil
}
