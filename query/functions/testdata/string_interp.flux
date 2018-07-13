n = 1
fieldSelect = "field{n}"
from(db:"test")
    |> range(start:-5m)
    |> filter(fn: (r) => r._field == fieldSelect)