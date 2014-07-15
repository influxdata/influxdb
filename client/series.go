package influxdb

import "github.com/influxdb/influxdb/common"

type Series common.SerializedSeries

func (s *Series) GetName() string {
	return ((*common.SerializedSeries)(s)).GetName()
}

func (s *Series) GetColumns() []string {
	return ((*common.SerializedSeries)(s)).GetColumns()
}

func (s *Series) GetPoints() [][]interface{} {
	return ((*common.SerializedSeries)(s)).GetPoints()
}
