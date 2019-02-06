package spectests

func init() {
	RegisterFixture(
		NewFixture(
			`SELECT mean(value) FROM db0..cpu; SELECT max(value) FROM db0..cpu`,
			`package main

from(bucketID: "")
	|> range(start: 1677-09-21T00:12:43.145224194Z, stop: 2262-04-11T23:47:16.854775806Z)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	|> group(columns: ["_measurement", "_start"], mode: "by")
	|> mean()
	|> duplicate(column: "_start", as: "_time")
	|> map(fn: (r) => ({_time: r._time, mean: r._value}))
	|> yield(name: "0")
from(bucketID: "")
	|> range(start: 1677-09-21T00:12:43.145224194Z, stop: 2262-04-11T23:47:16.854775806Z)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	|> group(columns: ["_measurement", "_start"], mode: "by")
	|> max()
	|> map(fn: (r) => ({_time: r._time, max: r._value}))
	|> yield(name: "1")
`,
		),
	)
}
