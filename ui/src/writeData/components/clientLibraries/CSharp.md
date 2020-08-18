For more detailed and up to date information check out the [GitHub Respository](https://github.com/influxdata/influxdb-client-csharp)

##### Install Package

Library Manager

```
Install-Package InfluxDB.Client
```

.NET CLI

```
dotnet add package InfluxDB.Client
```

Package Reference

```
<PackageReference Include="InfluxDB.Client" />
```

Initialize the Client

```
using System;
using System.Threading.Tasks;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core;
using InfluxDB.Client.Writes;

namespace Examples
{
  public class Examples
  {
    public static async Task Main(string[] args)
    {
      // You can generate a Token from the "Tokens Tab" in the UI
      const string token = "<%= token %>";
      const string bucket = "<%= bucket %>";
      const string org = "<%= org %>";

      var client = InfluxDBClientFactory.Create("<%= server %>", token.ToCharArray());
    }
  }
}
```

##### Write Data

Option 1: Use InfluxDB Line Protocol to write data

```
const string data = "mem,host=host1 used_percent=23.43234543";
using (var writeApi = client.GetWriteApi())
{
  writeApi.WriteRecord(bucket, org, WritePrecision.Ns, data);
}
```

Option 2: Use a Data Point to write data

```
var point = PointData
  .Measurement("mem")
  .Tag("host", "host1")
  .Field("used_percent", 23.43234543)
  .Timestamp(DateTime.UtcNow, WritePrecision.Ns);

using (var writeApi = client.GetWriteApi())
{
  writeApi.WritePoint(bucket, org, point);
}
```

Option 3: Use POCO and corresponding Class to write data

```
var mem = new Mem { Host = "host1", UsedPercent = 23.43234543, Time = DateTime.UtcNow };

using (var writeApi = client.GetWriteApi())
{
  writeApi.WriteMeasurement(bucket, org, WritePrecision.Ns, mem);
}
```

```
// Public class
[Measurement("mem")]
private class Mem
{
  [Column("host", IsTag = true)] public string Host { get; set; }
  [Column("used_percent")] public double? UsedPercent { get; set; }
  [Column(IsTimestamp = true)] public DateTime Time { get; set; }
}
```

##### Execute a Flux query

```
var query = $"from(bucket: \\"{bucket}\\") |> range(start: -1h)";
var tables = await client.GetQueryApi().QueryAsync(query, org)
```
