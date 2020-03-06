package spectests

func init() {
	// TODO(ethan): https://github.com/influxdata/flux/issues/2594
	//	RegisterFixture(
	//		NewFixture(
	//			`SELECT mean(value), max(value) FROM db0..cpu`,
	//			`package main
	//
	//t0 = from(bucketID: "")
	//	|> range(start: 1677-09-21T00:12:43.145224194Z, stop: 2262-04-11T23:47:16.854775806Z)
	//	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	//	|> group(columns: ["_measurement", "_start"], mode: "by")
	//	|> mean()
	//	|> duplicate(column: "_start", as: "_time")
	//t1 = from(bucketID: "")
	//	|> range(start: 1677-09-21T00:12:43.145224194Z, stop: 2262-04-11T23:47:16.854775806Z)
	//	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "value")
	//	|> group(columns: ["_measurement", "_start"], mode: "by")
	//	|> max()
	//	|> drop(columns: ["_time"])
	//	|> duplicate(column: "_start", as: "_time")
	//join(tables: {t0: t0, t1: t1}, on: ["_time", "_measurement"])
	//	|> map(fn: (r) => ({_time: r._time, mean: r["t0__value"], max: r["t1__value"]}), mergeKey: true)
	//	|> yield(name: "0")
	//`,
	//		),
	//	)
}
