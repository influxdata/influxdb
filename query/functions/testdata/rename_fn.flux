from(db: "test")
	|> range(start:2018-05-22T19:53:26Z)
	|> rename(fn: (col) => col)
	|> drop(fn: (col) => col == "_start" or col == "_stop")
