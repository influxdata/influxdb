package spectests

func init() {
	RegisterFixture(
		NewFixture(
			`SHOW DATABASES`,
			`package main

databases()
	|> rename(columns: {databaseName: "name"})
	|> keep(columns: ["name"])
	|> yield(name: "0")
`,
		),
	)
}
