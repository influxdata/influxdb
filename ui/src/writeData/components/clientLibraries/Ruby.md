For more detailed and up to date information check out the [GitHub Respository](https://github.com/influxdata/influxdb-client-ruby)

##### Install the Gem

```
gem install influxdb-client
```

##### Initialize the Client

```
require 'influxdb-client'

# You can generate a Token from the "Tokens Tab" in the UI
token = '<%= token %>'
org = '<%= org %>'
bucket = '<%= bucket %>'

client = InfluxDB2::Client.new('<%= server %>', token,
  precision: InfluxDB2::WritePrecision::NANOSECOND)
```

##### Write Data

Option 1: Use InfluxDB Line Protocol to write data

```
write_api = client.create_write_api

data = 'mem,host=host1 used_percent=23.43234543'
write_api.write(data: data, bucket: bucket, org: org)
```

Option 2: Use a Data Point to write data

```
point = InfluxDB2::Point.new(name: 'mem')
  .add_tag('host', 'host1')
  .add_field('used_percent', 23.43234543)
  .time(Time.now.utc, InfluxDB2::WritePrecision::NANOSECOND)

write_api.write(data: point, bucket: bucket, org: org)
```

Option 3: Use a Hash to write data

```
hash = {name: 'h2o',
  tags: {host: 'aws', region: 'us'},
  fields: {level: 5, saturation: '99%'},
  time: Time.now.utc}

write_api.write(data: hash, bucket: bucket, org: org)
```

Option 4: Use a Batch Sequence to write data

```
point = InfluxDB2::Point.new(name: 'mem')
  .add_tag('host', 'host1')
  .add_field('used_percent', 23.43234543)
  .time(Time.now.utc, InfluxDB2::WritePrecision::NANOSECOND)

hash = {name: 'h2o',
  tags: {host: 'aws', region: 'us'},
  fields: {level: 5, saturation: '99%'},
  time: Time.now.utc}

data = 'mem,host=host1 used_percent=23.23234543'

write_api.write(data: [point, hash, data], bucket: bucket, org: org)
```

##### Execute a Flux query

```
query = "from(bucket: \\"#{bucket}\\") |> range(start: -1h)"
tables = client.create_query_api.query(query: query, org: org)
```
