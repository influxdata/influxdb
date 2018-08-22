from(bucket:"testdb")
  |> range(start: 2018-05-22T19:53:26Z)
  |> pivot(rowKey: ["_time"], colKey: ["_field"], valueCol: "_value")
  |> yield(name:"0")