left = from(bucket: "test")
    |> range(start:2018-05-22T19:53:00Z, stop:2018-05-22T19:55:00Z)
    |> drop(columns: ["_start", "_stop"])
    |> filter(fn: (r) => r.user == "user1")
    |> group(by: ["user"])

right = from(bucket: "test")
    |> range(start:2018-05-22T19:53:00Z, stop:2018-05-22T19:55:00Z)
    |> drop(columns: ["_start", "_stop"])
    |> filter(fn: (r) => r.user == "user2")
    |> group(by: ["_measurement"])

join(tables: {left:left, right:right}, on: ["_time", "_measurement"])
    |> rename(columns: {
        left__value: "left_value",
        right__value: "right_value"
        })