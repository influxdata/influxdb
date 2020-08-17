For more detailed and up to date information check out the [GitHub Respository](https://github.com/influxdata/influxdb-client-java/tree/master/client-kotlin)

##### Add Dependency

Build with Maven

```
<dependency>
  <groupId>com.influxdb</groupId>
  <artifactId>influxdb-client-kotlin</artifactId>
  <version>1.8.0</version>
</dependency>
```

Build with Gradle

```
dependencies {
  compile "com.influxdb:influxdb-client-kotlin:1.8.0"
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

    val client = InfluxDBClientKotlinFactory.create("<%= server %>", token.toCharArray(), org)
}
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
