package spectests

import "fmt"

func init() {
	RegisterFixture(
		AggregateTest(func(name string) (stmt, want string) {
			return fmt.Sprintf(`SELECT %s(value) FROM db0..cpu WHERE time >= now() - 10m GROUP BY time(1m)`, name),
				`package main

` + fmt.Sprintf(`from(bucketID: "%s"`, bucketID.String()) + `) |> range(start: 2010-09-15T08:50:00Z, stop: 2010-09-15T09:00:00Z) |> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	|> group(columns: ["_measurement", "_start", "_stop", "_field"], mode: "by")
	|> keep(columns: ["_measurement", "_start", "_stop", "_field", "_time", "_value"])
	|> window(every: 1m)
	|> ` + name + `()
	|> map(fn: (r) => ({r with _time: r._start}))
	|> window(every: inf)
	|> rename(columns: {_value: "` + name + `"})
	|> yield(name: "0")
`
		}),
	)
}
