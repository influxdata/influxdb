For more detailed and up to date information check out the [GitHub Respository](https://github.com/influxdata/influxdb-client-java)

##### Add Dependency

Build with Maven

```
<dependency>
  <groupId>com.influxdb</groupId>
  <artifactId>influxdb-client-java</artifactId>
  <version>1.8.0</version>
</dependency>
```

Build with Gradle

```
dependencies {
  compile "com.influxdb:influxdb-client-java:1.8.0"
}
```

##### Initialize the Client

```
package example;

import java.time.Instant;
import java.util.List;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxTable;

public class InfluxDB2Example {
  public static void main(final String[] args) {

    // You can generate a Token from the "Tokens Tab" in the UI
    String token = "<%= token %>";
    String bucket = "<%= bucket %>";
    String org = "<%= org %>";

    InfluxDBClient client = InfluxDBClientFactory.create("<%= server %>", token.toCharArray());
  }
}
```

##### Write Data

Option 1: Use InfluxDB Line Protocol to write data

```
String data = "mem,host=host1 used_percent=23.43234543";
try (WriteApi writeApi = client.getWriteApi()) {
  writeApi.writeRecord(bucket, org, WritePrecision.NS, data);
}
```

Option 2: Use a Data Point to write data

```
Point point = Point
  .measurement("mem")
  .addTag("host", "host1")
  .addField("used_percent", 23.43234543)
  .time(Instant.now(), WritePrecision.NS);

try (WriteApi writeApi = client.getWriteApi()) {
  writeApi.writePoint(bucket, org, point);
}
```

Option 3: Use POJO and corresponding class to write data

```
Mem mem = new Mem();
mem.host = "host1";
mem.used_percent = 23.43234543;
mem.time = Instant.now();

try (WriteApi writeApi = client.getWriteApi()) {
  writeApi.writeMeasurement(bucket, org, WritePrecision.NS, mem);
}
```

```
@Measurement(name = "mem")
public static class Mem {
  @Column(tag = true)
  String host;
  @Column
  Double used_percent;
  @Column(timestamp = true)
  Instant time;
}
```

##### Execute a Flux query

```
String query = String.format("from(bucket: \\"%s\\") |> range(start: -1h)", bucket);
List<FluxTable> tables = client.getQueryApi().query(query, org);
```
