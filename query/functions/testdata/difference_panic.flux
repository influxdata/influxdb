from(db: "test")
    |> range(start:-5m)
    |> filter(fn: (r) => r._field == "usage_guest")
    |> difference()