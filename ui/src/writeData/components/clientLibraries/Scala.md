For more detailed and up to date information check out the [GitHub Respository](https://github.com/influxdata/influxdb-client-java/tree/master/client-scala)

##### Add Dependency

Build with sbt

```
libraryDependencies += "com.influxdb" % "influxdb-client-scala" % "1.8.0"
```

Build with Maven

```
<dependency>
  <groupId>com.influxdb</groupId>
  <artifactId>influxdb-client-scala</artifactId>
  <version>1.8.0</version>
</dependency>
```

Build with Gradle

```
dependencies {
  compile "com.influxdb:influxdb-client-scala:1.8.0"
}
```

##### Initialize the Client

```
package example

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.influxdb.client.scala.InfluxDBClientScalaFactory
import com.influxdb.query.FluxRecord

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object InfluxDB2ScalaExample {

  implicit val system: ActorSystem = ActorSystem("it-tests")

  def main(args: Array[String]): Unit = {

    // You can generate a Token from the "Tokens Tab" in the UI
    val token = "<%= token %>"
    val org = "<%= org %>"
    val bucket = "<%= bucket %>"

    val client = InfluxDBClientScalaFactory.create("<%= server %>", token.toCharArray, org)
  }
}
```

##### Execute a Flux query

```
val query = (s"""from(bucket: "$bucket")"""
  + " |> range(start: -1d)"
  + " |> filter(fn: (r) => (r[\\"_measurement\\"] == \\"cpu\\" and r[\\"_field\\"] == \\"usage_system\\"))")

// Result is returned as a stream
val results = client.getQueryScalaApi().query(query)

// Example of additional result stream processing on client side
val sink = results
  // filter on client side using \`filter\` built-in operator
  .filter(it => "cpu0" == it.getValueByKey("cpu"))
  // take first 20 records
  .take(20)
  // print results
  .runWith(Sink.foreach[FluxRecord](it => println(s"Measurement: $\{it.getMeasurement}, value: $\{it.getValue}")
  ))

// wait to finish
Await.result(sink, Duration.Inf)

client.close()
system.terminate()
```
