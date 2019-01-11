package tsdb

import (
	"github.com/influxdata/influxdb/query"
)

// EOF represents a "not found" key returned by a Cursor.
const EOF = query.ZeroTime
