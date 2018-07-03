from(db: "test")
    |> range(start:-5m)
    |> sort(cols:["device"])