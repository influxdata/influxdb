from(db: "test")
    |> range(start:-5m)
    |> filter(fn: (r) => r._field == "no_exist")
    |> difference()


