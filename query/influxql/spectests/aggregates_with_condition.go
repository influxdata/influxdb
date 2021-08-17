package spectests

import "fmt"

func init() {
	RegisterFixture(
		AggregateTest(func(name string) (stmt, want string) {
			return fmt.Sprintf(`SELECT %s(value) FROM db0..cpu WHERE host = 'server01'`, name),
				`package main

` + fmt.Sprintf(`from(bucketID: "%s"`, bucketID.String()) + `) |> range(start: 1677-09-21T00:12:43.145224194Z, stop: 2262-04-11T23:47:16.854775806Z) |> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	|> filter(fn: (r) => r["host"] == "server01")
	|> group(columns: ["_measurement", "_start", "_stop", "_field"], mode: "by")
	|> keep(columns: ["_measurement", "_start", "_stop", "_field", "_time", "_value"])
	|> ` + name + `()
	|> map(fn: (r) => ({r with _time: 1970-01-01T00:00:00Z}))
	|> rename(columns: {_value: "` + name + `"})
	|> yield(name: "0")
`
		}),
	)
}
