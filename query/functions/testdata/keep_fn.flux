from(db: "test")
	|> range(start: 2018-05-22T19:53:26Z)
	|> keep(fn: (col) => col == "_field" or col == "_value")
    |> keep(fn: (col) =>  {return col == "_value"})
