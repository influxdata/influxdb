package spectests

func init() {
	RegisterFixture(
		NewFixture(
			`SHOW DATABASES`,
			`package main

import v1 "influxdata/influxdb/v1"

v1.databases() |> rename(columns: {databaseName: "name"}) |> keep(columns: ["name"])
	|> yield(name: "0")
`,
		),
	)
}
