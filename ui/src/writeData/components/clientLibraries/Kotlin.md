For more detailed and up to date information check out the [GitHub Repository](https://github.com/influxdata/influxdb-client-java/tree/master/client-kotlin)

##### Add Dependency

Build with Maven

```
<dependency>
  <groupId>com.influxdb</groupId>
  <artifactId>influxdb-client-kotlin</artifactId>
  <version>2.2.0</version>
</dependency>
```

Build with Gradle

```
dependencies {
  compile "com.influxdb:influxdb-client-kotlin:2.2.0"
}
```

##### Initialize the Client

```
package example

import com.influxdb.client.kotlin.InfluxDBClientKotlinFactory
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.filter
import kotlinx.coroutines.channels.take
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {

    // You can generate a Token from the "Tokens Tab" in the UI
    val token = "<%= token %>"
    val org = "<%= org %>"
    val bucket = "<%= bucket %>"

    val client = InfluxDBClientKotlinFactory.create("<%= server %>", token.toCharArray(), org, bucket)
}
```

##### Write Data

Option 1: Use InfluxDB Line Protocol to write data

```
val writeApi = client.getWriteKotlinApi()

val data = "mem,host=host1 used_percent=23.43234543"
writeApi.writeRecord(data, WritePrecision.NS)
```

Option 2: Use a Data Point to write data

```
val writeApi = client.getWriteKotlinApi()

val point = Point
    .measurement("mem")
    .addTag("host", "host1")
    .addField("used_percent", 23.43234543)
    .time(Instant.now(), WritePrecision.NS)
writeApi.writePoint(point)
```

Option 3: Use POJO and corresponding Data class to write data

```
val writeApi = client.getWriteKotlinApi()

val temperature = Temperature("south", 62.0, Instant.now())
writeApi.writeMeasurement(temperature, WritePrecision.NS)
```

```
@Measurement(name = "temperature")
data class Temperature(
    @Column(tag = true) val location: String,
    @Column val value: Double,
    @Column(timestamp = true) val time: Instant
)
```

##### Execute a Flux query

```
val query = ("from(bucket: \\"$bucket\\")"
  + " |> range(start: -1d)"
  + " |> filter(fn: (r) => (r[\\"_measurement\\"] == \\"cpu\\" and r[\\"_field\\"] == \\"usage_system\\"))")

// Result is returned as a stream
val results = client.getQueryKotlinApi().query(query)

// Example of additional result stream processing on client side
results
  // filter on client side using \`filter\` built-in operator
  .filter { "cpu0" == it.getValueByKey("cpu") }
  // take first 20 records
  .take(20)
  // print results
  .consumeEach { println("Measurement: $\{it.measurement}, value: $\{it.value}") }

client.close()
```
