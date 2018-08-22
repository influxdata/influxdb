from(bucket: "test")
    |> range(start:2018-05-22T19:53:00Z)
    |> window(start:2018-05-22T19:53:30Z,every: 1m)