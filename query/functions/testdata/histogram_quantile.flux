from(db:"testdb")
    |> range(start: 2018-05-22T19:53:00Z)
    |> histogramQuantile(quantile:0.90,upperBoundColumn:"le")
