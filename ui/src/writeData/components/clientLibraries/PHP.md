For more detailed and up to date information check out the [GitHub Respository](https://github.com/influxdata/influxdb-client-php)

##### Install via Composer

```
composer require influxdata/influxdb-client-php
```

##### Initialize the Client

```
use InfluxDB2\\Client;
use InfluxDB2\\Model\\WritePrecision;
use InfluxDB2\\Point;

# You can generate a Token from the "Tokens Tab" in the UI
$token = '<%= token %>';
$org = '<%= org %>';
$bucket = '<%= bucket %>';

$client = new Client([
    "url" => "<%= server %>",
    "token" => $token,
]);
```

##### Write Data

Option 1: Use InfluxDB Line Protocol to write data

```
$writeApi = $client->createWriteApi();

$data = "mem,host=host1 used_percent=23.43234543";

$writeApi->write($data, WritePrecision::S, $bucket, $org);
```

Option 2: Use a Data Point to write data

```
$point = Point::measurement('mem')
  ->addTag('host', 'host1')
  ->addField('used_percent', 23.43234543)
  ->time(microtime(true));

$writeApi->write($point, WritePrecision::S, $bucket, $org);
```

Option 3: Use an Array structure to write data

```
$dataArray = ['name' => 'cpu',
  'tags' => ['host' => 'server_nl', 'region' => 'us'],
  'fields' => ['internal' => 5, 'external' => 6],
  'time' => microtime(true)];

$writeApi->write($dataArray, WritePrecision::S, $bucket, $org);
```

##### Execute a Flux query

```
$query = "from(bucket: \\"{$bucket}\\") |> range(start: -1h)";
$tables = $client->createQueryApi()->query($query, $org);
```
