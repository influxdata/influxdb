package spectests

func init() {
	RegisterFixture(
		NewFixture(
			`SHOW TAG VALUES ON "db0" WITH KEY = "host"`,
			`package main

from(bucketID: "") |> range(start: -1h) |> keyValues(keyColumns: ["host"])
	|> group(columns: ["_measurement", "_key"], mode: "by")
	|> distinct()
	|> group(columns: ["_measurement"], mode: "by")
	|> rename(columns: {_key: "key", _value: "value"})
	|> yield(name: "0")
`,
		),
	)
}
