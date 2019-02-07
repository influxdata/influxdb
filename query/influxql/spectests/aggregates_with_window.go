package spectests

import "fmt"

func init() {
	RegisterFixture(
		AggregateTest(func(name string) (stmt, want string) {
			return fmt.Sprintf(`SELECT %s(value) FROM db0..cpu WHERE time >= now() - 10m GROUP BY time(1m)`, name),
				`package main

` + fmt.Sprintf(`from(bucketID: "%s"`, bucketID.String()) + `)
	|> range(start: 2010-09-15T08:50:00Z, stop: 2010-09-15T09:00:00Z)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	|> group(columns: ["_measurement", "_start"], mode: "by")
	|> window(every: 1m)
	|> ` + name + `()
	|> duplicate(column: "_start", as: "_time")
	|> window(every: inf)
	|> map(fn: (r) => ({_time: r._time, ` + name + `: r._value}))
	|> yield(name: "0")
`
		}),
	)
}
