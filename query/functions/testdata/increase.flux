increase = (table=<-) => 
    difference(table: table, nonNegative: true)
    |> cumulativeSum()
  
from(db: "test")
    |> range(start:-5m)
    |> increase()

