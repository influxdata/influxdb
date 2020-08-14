For more detailed and up to date information check out the [GitHub Respository](https://github.com/influxdata/influxdb-client-python)

##### Install Package

```
pip install influxdb-client
```

##### Initialize the Client

```
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate a Token from the "Tokens Tab" in the UI
token = "<%= token %>"
org = "<%= org %>"
bucket = "<%= bucket %>"

client = InfluxDBClient(url="<%= server %>", token=token)
```

##### Write Data

Option 1: Use InfluxDB Line Protocol to write data

```
write_api = client.write_api(write_options=SYNCHRONOUS)

data = "mem,host=host1 used_percent=23.43234543"
write_api.write(bucket, org, data)
```

Option 2: Use a Data Point to write data

```
point = Point("mem")\\
  .tag("host", "host1")\\
  .field("used_percent", 23.43234543)\\
  .time(datetime.utcnow(), WritePrecision.NS)

write_api.write(bucket, org, point)
```

Option 3: Use a Batch Sequence to write data

```
sequence = ["mem,host=host1 used_percent=23.43234543",
            "mem,host=host1 available_percent=15.856523"]
write_api.write(bucket, org, sequence)
```

##### Execute a Flux query

```
query = f'from(bucket: \\"{bucket}\\") |> range(start: -1h)'
tables = client.query_api().query(query, org=org)
```
