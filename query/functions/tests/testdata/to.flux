option task = {
    name: "test",
    every: 10m,
}

from(bucket: "test")
    |> range(start:2018-05-22T19:53:26Z)
    |> window(every: task.every)
    |> group(by: ["_field", "host"])
    |> sum()
    |> to(bucket: "test", tagColumns:["host", "_field"])
