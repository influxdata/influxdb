from(bucket: "test")
    |> range(start:2018-05-22T19:53:26Z)
	|> filter(fn: (r) => r._value == null)
