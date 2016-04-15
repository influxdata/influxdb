package tsm1

import (
	"fmt"

	"github.com/influxdata/influxdb/influxql"
)

func newLimitIterator(input influxql.Iterator, opt influxql.IteratorOptions) influxql.Iterator {
	switch input := input.(type) {
	case influxql.FloatIterator:
		return newFloatLimitIterator(input, opt)
	case influxql.IntegerIterator:
		return newIntegerLimitIterator(input, opt)
	case influxql.StringIterator:
		return newStringLimitIterator(input, opt)
	case influxql.BooleanIterator:
		return newBooleanLimitIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported limit iterator type: %T", input))
	}
}
