import "internal/gen"

gen.tables(n: 50000, tags: [{name: "_measurement", cardinality: 1}, {name: "_field", cardinality: 1}, {name: "t0", cardinality: 10}, {name: "t1", cardinality: 5}])
  |> to(bucket: "preview-test")
