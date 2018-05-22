package executetest

import (
	"math"

	"github.com/influxdata/platform/query/execute"
)

var UnlimitedAllocator = &execute.Allocator{
	Limit: math.MaxInt64,
}
