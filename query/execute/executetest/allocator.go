package executetest

import (
	"math"

	"github.com/influxdata/ifql/query/execute"
)

var UnlimitedAllocator = &execute.Allocator{
	Limit: math.MaxInt64,
}
