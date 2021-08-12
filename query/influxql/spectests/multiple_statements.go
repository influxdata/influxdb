package spectests

func init() {
	RegisterFixture(
		NewFixture(
			`SELECT mean(value) FROM db0..cpu; SELECT max(value) FROM db0..cpu`,
			`package main

from(bucketID: "") |> range(start: 1677-09-21T00:12:43.145224194Z, stop: 2262-04-11T23:47:16.854775806Z) |> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	|> group(columns: ["_measurement", "_start", "_stop", "_field"], mode: "by")
	|> keep(columns: ["_measurement", "_start", "_stop", "_field", "_time", "_value"])
	|> mean()
	|> map(fn: (r) => ({r with _time: 1970-01-01T00:00:00Z}))
	|> rename(columns: {_value: "mean"})
	|> yield(name: "0")
from(bucketID: "") |> range(start: 1677-09-21T00:12:43.145224194Z, stop: 2262-04-11T23:47:16.854775806Z) |> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	|> group(columns: ["_measurement", "_start", "_stop", "_field"], mode: "by")
	|> keep(columns: ["_measurement", "_start", "_stop", "_field", "_time", "_value"])
	|> max()
	|> rename(columns: {_value: "max"})
	|> yield(name: "1")
`,
		),
	)
}
