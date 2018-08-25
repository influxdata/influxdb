from(db: "test")
    |> range(start:2018-05-22T19:53:30Z, stop: 2018-05-22T19:59:00Z)
    |> window(every:1m)
