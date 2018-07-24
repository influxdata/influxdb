package functions

import "github.com/influxdata/platform/query"

func init() {
	query.RegisterBuiltIn("increase", increaseBuiltIn)
}

var increaseBuiltIn = `
// Increase returns the total non-negative difference between values in a table. 
// A main usage case is tracking changes in counter values which may wrap over time when they hit 
// a threshold or are reset. In the case of a wrap/reset,
// we can assume that the absolute delta between two points will be at least their non-negative difference.
increase = (table=<-, columns=["_value"]) => 
	table
		|> difference(nonNegative: true, columns:columns)
		|> cumulativeSum()
`
