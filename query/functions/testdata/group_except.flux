from(db:"test")
    |> range(start:-5m)
    |> group(except:["_measurement"])
    |> max() 
