from(db:"test")
    |> range(start:-5m)
    |> group(by: ["_value"])