package spectests

func init() {
	RegisterFixture(
		NewFixture(
			`SHOW TAG VALUES ON "db0" FROM "cpu", "mem", "gpu" WITH KEY = "host"`,
			`package main

from(bucketID: "") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "cpu" or (r._measurement == "mem" or r._measurement == "gpu"))
	|> keyValues(keyColumns: ["host"])
	|> group(columns: ["_measurement", "_key"], mode: "by")
	|> distinct()
	|> group(columns: ["_measurement"], mode: "by")
	|> rename(columns: {_key: "key", _value: "value"})
	|> yield(name: "0")
`,
		),
	)
}
