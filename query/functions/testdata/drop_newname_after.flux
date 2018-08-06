from(db: "test")
	|> range(start: 2018-05-22T19:53:26Z)
	|> rename(columns:{old:"new"})
	|> drop(columns: ["new"])
