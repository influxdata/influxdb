package spectests

func init() {
	RegisterFixture(
		NewFixture(
			`SELECT value FROM db0..cpu WHERE host = 'server01'`,
			`package main

from(bucketID: "")
	|> range(start: 1677-09-21T00:12:43.145224194Z, stop: 2262-04-11T23:47:16.854775806Z)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	|> filter(fn: (r) => r["host"] == "server01")
	|> group(columns: ["_measurement", "_start"], mode: "by")
	|> map(fn: (r) => ({_time: r._time, value: r._value}))
	|> yield(name: "0")
`,
		),
	)
}
