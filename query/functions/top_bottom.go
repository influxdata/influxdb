package functions

import (
	"github.com/influxdata/ifql/query"
)

func init() {
	query.RegisterBuiltIn("top-bottom", topBottomBuiltIn)
	// TODO(nathanielc): Provide an implementation of top/bottom transformation that can use a more efficient sort based on the limit.
	// This transformation should be used when ever the planner sees a sort |> limit pair of procedures.
}

var topBottomBuiltIn = `
// _sortLimit is a helper function, which sorts and limits a table.
_sortLimit = (n, desc, cols=["_value"], table=<-) =>
	table
		|> sort(cols:cols, desc:desc)
		|> limit(n:n)

// top sorts a table by cols and keeps only the top n records.
top = (n, cols=["_value"], table=<-) => _sortLimit(table:table, n:n, cols:cols, desc:true)

// top sorts a table by cols and keeps only the bottom n records.
bottom = (n, cols=["_value"], table=<-) => _sortLimit(table:table, n:n, cols:cols, desc:false)

// _highestOrLowest is a helper function, which reduces all groups into a single group by specific tags and a reducer function,
// then it selects the highest or lowest records based on the cols and the _sortLimit function.
// The default reducer assumes no reducing needs to be performed.
_highestOrLowest = (n, _sortLimit, reducer=(t=<-) => t, cols=["_value"], by=[], table=<-) =>
	table
		|> group(by:by)
		|> reducer()
		|> group(keep:by)
		|> _sortLimit(n:n, cols:cols)

// highestMax returns the top N records from all groups using the maximum of each group.
highestMax = (n, cols=["_value"], by=[], table=<-) =>
	_highestOrLowest(
		table:table,
		n:n,
		cols:cols,
		by:by,
		// TODO(nathanielc): Once max/min support selecting based on multiple columns change this to pass all columns.
		reducer: (t=<-) => max(table:t, column:cols[0]),
		_sortLimit: top,
	)

// highestAverage returns the top N records from all groups using the average of each group.
highestAverage = (n, cols=["_value"], by=[], table=<-) =>
	_highestOrLowest(
		table: table,
		n:n,
		cols:cols,
		by:by,
		reducer: (t=<-) => mean(table:t, ),
		_sortLimit: top,
	)

// highestCurrent returns the top N records from all groups using the last value of each group.
highestCurrent = (n, cols=["_value"], by=[], table=<-) =>
	_highestOrLowest(
		table: table,
		n:n,
		cols:cols,
		by:by,
		reducer: (t=<-) => last(table:t, column:cols[0]),
		_sortLimit: top,
	)

// lowestMin returns the bottom N records from all groups using the minimum of each group.
lowestMin = (n, cols=["_value"], by=[], table=<-) =>
	_highestOrLowest(
		table: table,
		n:n,
		cols:cols,
		by:by,
		// TODO(nathanielc): Once max/min support selecting based on multiple columns change this to pass all columns.
		reducer: (t=<-) => min(table:t, column:cols[0]),
		_sortLimit: bottom,
	)

// lowestAverage returns the bottom N records from all groups using the average of each group.
lowestAverage = (n, cols=["_value"], by=[], table=<-) =>
	_highestOrLowest(
		table: table,
		n:n,
		cols:cols,
		by:by,
		reducer: (t=<-) => mean(table:t),
		_sortLimit: bottom,
	)

// lowestCurrent returns the bottom N records from all groups using the last value of each group.
lowestCurrent = (n, cols=["_value"], by=[], table=<-) =>
	_highestOrLowest(
		table: table,
		n:n,
		cols:cols,
		by:by,
		reducer: (t=<-) => last(table:t, column:cols[0]),
		_sortLimit: bottom,
	)
`
