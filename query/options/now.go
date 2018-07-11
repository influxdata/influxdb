package options

import (
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/functions"
)

func init() {
	query.RegisterBuiltInOption("now", functions.SystemTime())
}
