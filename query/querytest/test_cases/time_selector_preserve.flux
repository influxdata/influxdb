from(db:"test")
	|> range(start:-5m)
	|> top(n:3)