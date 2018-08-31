from(bucket: "test")
	|> range(start: 2018-05-22T19:53:26Z)
	|> keep(columns: ["_time", "_value", "_field"])
