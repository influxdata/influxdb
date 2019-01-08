inData = "

"
outData = "err: error calling function "filter": name "null" does not exist in scope
"

t_null_as_value = (table=<-) =>
  table
    |> range(start:2018-05-22T19:53:26Z)
	|> filter(fn: (r) => r._value == null)

testingTest(name: "null_as_value",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_null_as_value)