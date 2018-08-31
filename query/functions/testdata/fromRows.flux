fromRows(bucket:"testdb")
  |> range(start: 2018-05-22T19:53:26Z, stop: 2018-05-22T19:54:17Z)
  |> yield(name:"0")