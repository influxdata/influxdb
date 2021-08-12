package spectests

func init() {
	RegisterFixture(
		NewFixture(
			`SHOW RETENTION POLICIES ON telegraf`,
			`package main

import v1 "influxdata/influxdb/v1"

v1.databases() |> filter(fn: (r) => r.databaseName == "telegraf") |> rename(columns: {retentionPolicy: "name", retentionPeriod: "duration"})
	|> set(key: "shardGroupDuration", value: "0")
	|> set(key: "replicaN", value: "2")
	|> keep(columns: ["name", "duration", "shardGroupDuration", "replicaN", "default"])
	|> yield(name: "0")
`,
		),
	)
}
