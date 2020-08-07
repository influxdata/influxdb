export interface ClientLibrary {
  id: string
  name: string
  url: string
}

export const clientArduinoLibrary = {
  id: 'arduino',
  name: 'Arduino',
  url: 'https://github.com/tobiasschuerg/InfluxDB-Client-for-Arduino',
  installingLibraryManagerCodeSnippet: `1. Open the Arduino IDE and click to the "Sketch" menu and then Include Library > Manage Libraries.
2. Type 'influxdb' in the search box
3. Install the 'InfluxDBClient for Arduino' library`,
  installingManualCodeSnippet: `1. cd <arduino-sketch-location>/library.
2. git clone <%= url %>
3. Restart the Arduino IDE`,
  initializeClientCodeSnippet: `#if defined(ESP32)
#include <WiFiMulti.h>
WiFiMulti wifiMulti;
#define DEVICE "ESP32"
#elif defined(ESP8266)
#include <ESP8266WiFiMulti.h>
ESP8266WiFiMulti wifiMulti;
#define DEVICE "ESP8266"
#endif

#include <InfluxDbClient.h>
#include <InfluxDbCloud.h>

// WiFi AP SSID
#define WIFI_SSID "SSID"
// WiFi password
#define WIFI_PASSWORD "PASSWORD"
// InfluxDB v2 server url, e.g. https://eu-central-1-1.aws.cloud2.influxdata.com (Use: InfluxDB UI -> Load Data -> Client Libraries)
#define INFLUXDB_URL "<%= server %>"
// InfluxDB v2 server or cloud API authentication token (Use: InfluxDB UI -> Data -> Tokens -> <select token>)
#define INFLUXDB_TOKEN "<%= token %>"
// InfluxDB v2 organization id (Use: InfluxDB UI -> User -> About -> Common Ids )
#define INFLUXDB_ORG "<%= org %>"
// InfluxDB v2 bucket name (Use: InfluxDB UI ->  Data -> Buckets)
#define INFLUXDB_BUCKET "<%= bucket %>"

// Set timezone string according to https://www.gnu.org/software/libc/manual/html_node/TZ-Variable.html
// Examples:
//  Pacific Time: "PST8PDT"
//  Eastern: "EST5EDT"
//  Japanesse: "JST-9"
//  Central Europe: "CET-1CEST,M3.5.0,M10.5.0/3"
#define TZ_INFO "CET-1CEST,M3.5.0,M10.5.0/3"

// InfluxDB client instance with preconfigured InfluxCloud certificate
InfluxDBClient client(INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET, INFLUXDB_TOKEN, InfluxDbCloud2CACert);

// Data point
Point sensor("wifi_status");

void setup() {
  Serial.begin(115200);

  // Setup wifi
  WiFi.mode(WIFI_STA);
  wifiMulti.addAP(WIFI_SSID, WIFI_PASSWORD);

  Serial.print("Connecting to wifi");
  while (wifiMulti.run() != WL_CONNECTED) {
    Serial.print(".");
    delay(100);
  }
  Serial.println();

  // Add tags
  sensor.addTag("device", DEVICE);
  sensor.addTag("SSID", WiFi.SSID());

  // Accurate time is necessary for certificate validation and writing in batches
  // For the fastest time sync find NTP servers in your area: https://www.pool.ntp.org/zone/
  // Syncing progress and the time will be printed to Serial.
  timeSync(TZ_INFO, "pool.ntp.org", "time.nis.gov");

  // Check server connection
  if (client.validateConnection()) {
    Serial.print("Connected to InfluxDB: ");
    Serial.println(client.getServerUrl());
  } else {
    Serial.print("InfluxDB connection failed: ");
    Serial.println(client.getLastErrorMessage());
  }
}`,
  writingDataPointCodeSnippet: `void loop() {
  // Clear fields for reusing the point. Tags will remain untouched
  sensor.clearFields();
  
  // Store measured value into point
  // Report RSSI of currently connected network
  sensor.addField("rssi", WiFi.RSSI());
  
  // Print what are we exactly writing
  Serial.print("Writing: ");
  Serial.println(sensor.toLineProtocol());
  
  // If no Wifi signal, try to reconnect it
  if ((WiFi.RSSI() == 0) && (wifiMulti.run() != WL_CONNECTED)) {
    Serial.println("Wifi connection lost");
  }
  
  // Write point
  if (!client.writePoint(sensor)) {
    Serial.print("InfluxDB write failed: ");
    Serial.println(client.getLastErrorMessage());
  }

  //Wait 10s
  Serial.println("Wait 10s");
  delay(10000);
}`,
  executeQueryCodeSnippet: `void loop() {
  // Construct a Flux query
  // Query will find the worst RSSI for last hour for each connected WiFi network with this device
  String query = "from(bucket: \\"" INFLUXDB_BUCKET "\\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \\"wifi_status\\" and r._field == \\"rssi\\"";
  query += " and r.device == \\""  DEVICE  "\\")";
  query += "|> min()";

  // Print ouput header
  Serial.print("==== ");
  Serial.print(selectorFunction);
  Serial.println(" ====");

  // Print composed query
  Serial.print("Querying with: ");
  Serial.println(query);

  // Send query to the server and get result
  FluxQueryResult result = client.query(query);

  // Iterate over rows. Even there is just one row, next() must be called at least once.
  while (result.next()) {
    // Get converted value for flux result column 'SSID'
    String ssid = result.getValueByName("SSID").getString();
    Serial.print("SSID '");
    Serial.print(ssid);

    Serial.print("' with RSSI ");
    // Get converted value for flux result column '_value' where there is RSSI value
    long value = result.getValueByName("_value").getLong();
    Serial.print(value);

    // Get converted value for the _time column
    FluxDateTime time = result.getValueByName("_time").getDateTime();

    // Format date-time for printing
    // Format string according to http://www.cplusplus.com/reference/ctime/strftime/
    String timeStr = time.format("%F %T");

    Serial.print(" at ");
    Serial.print(timeStr);
    
    Serial.println();
  }

  // Check if there was an error
  if(result.getError() != "") {
    Serial.print("Query result error: ");
    Serial.println(result.getError());
  }

  // Close the result
  result.close();
}
`,
}

export const clientCSharpLibrary = {
  id: 'csharp',
  name: 'C#',
  url: 'https://github.com/influxdata/influxdb-client-csharp',
  installingPackageManagerCodeSnippet: `Install-Package InfluxDB.Client`,
  installingPackageDotNetCLICodeSnippet: `dotnet add package InfluxDB.Client`,
  packageReferenceCodeSnippet: `<PackageReference Include="InfluxDB.Client" />`,
  initializeClientCodeSnippet: `using System;
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
}`,
  executeQueryCodeSnippet: `var query = $"from(bucket: \\"{bucket}\\") |> range(start: -1h)";
var tables = await client.GetQueryApi().QueryAsync(query, org)`,
  writingDataLineProtocolCodeSnippet: `const string data = "mem,host=host1 used_percent=23.43234543";
using (var writeApi = client.GetWriteApi())
{
  writeApi.WriteRecord(bucket, org, WritePrecision.Ns, data);
}`,
  writingDataPointCodeSnippet: `var point = PointData
  .Measurement("mem")
  .Tag("host", "host1")
  .Field("used_percent", 23.43234543)
  .Timestamp(DateTime.UtcNow, WritePrecision.Ns);

using (var writeApi = client.GetWriteApi())
{
  writeApi.WritePoint(bucket, org, point);
}`,
  writingDataPocoCodeSnippet: `var mem = new Mem { Host = "host1", UsedPercent = 23.43234543, Time = DateTime.UtcNow };

using (var writeApi = client.GetWriteApi())
{
  writeApi.WriteMeasurement(bucket, org, WritePrecision.Ns, mem);
}`,
  pocoClassCodeSnippet: `// Public class
[Measurement("mem")]
private class Mem
{
  [Column("host", IsTag = true)] public string Host { get; set; }
  [Column("used_percent")] public double? UsedPercent { get; set; }
  [Column(IsTimestamp = true)] public DateTime Time { get; set; }
}`,
}

export const clientGoLibrary = {
  id: 'go',
  name: 'GO',
  url: 'https://github.com/influxdata/influxdb-client-go',
  initializeClientCodeSnippet: `package main

import (
  "context"
  "fmt"
  "github.com/influxdata/influxdb-client-go"
  "time"
)

func main() {
  // You can generate a Token from the "Tokens Tab" in the UI
  const token = "<%= token %>"
  const bucket = "<%= bucket %>"
  const org = "<%= org %>"

  client := influxdb2.NewClient("<%= server %>", token)
  // always close client at the end
  defer client.Close()
}`,
  writingDataPointCodeSnippet: `// create point using full params constructor
p := influxdb2.NewPoint("stat",
  map[string]string{"unit": "temperature"},
  map[string]interface{}{"avg": 24.5, "max": 45},
  time.Now())
// write point asynchronously
writeApi.WritePoint(p)
// create point using fluent style
p = influxdb2.NewPointWithMeasurement("stat").
  AddTag("unit", "temperature").
  AddField("avg", 23.2).
  AddField("max", 45).
  SetTime(time.Now())
// write point asynchronously
writeApi.WritePoint(p)
// Flush writes
writeApi.Flush()`,
  writingDataLineProtocolCodeSnippet: `// get non-blocking write client
writeApi := client.WriteApi(org, bucket)

// write line protocol
writeApi.WriteRecord(fmt.Sprintf("stat,unit=temperature avg=%f,max=%f", 23.5, 45.0))
writeApi.WriteRecord(fmt.Sprintf("stat,unit=temperature avg=%f,max=%f", 22.5, 45.0))
// Flush writes
writeApi.Flush()`,
  executeQueryCodeSnippet: `query := fmt.Sprintf("from(bucket:\\"%v\\")|> range(start: -1h) |> filter(fn: (r) => r._measurement == \\"stat\\")", bucket)
// Get query client
queryApi := client.QueryApi(org)
// get QueryTableResult
result, err := queryApi.Query(context.Background(), query)
if err == nil {
  // Iterate over query response
  for result.Next() {
    // Notice when group key has changed
    if result.TableChanged() {
      fmt.Printf("table: %s\\n", result.TableMetadata().String())
    }
    // Access data
    fmt.Printf("value: %v\\n", result.Record().Value())
  }
  // check for an error
  if result.Err() != nil {
    fmt.Printf("query parsing error: %\\n", result.Err().Error())
  }
} else {
  panic(err)
}`,
}

export const clientJavaLibrary = {
  id: 'java',
  name: 'Java',
  url: 'https://github.com/influxdata/influxdb-client-java',
  buildWithMavenCodeSnippet: `<dependency>
  <groupId>com.influxdb</groupId>
  <artifactId>influxdb-client-java</artifactId>
  <version>1.8.0</version>
</dependency>`,
  buildWithGradleCodeSnippet: `dependencies {
  compile "com.influxdb:influxdb-client-java:1.8.0"
}`,
  initializeClientCodeSnippet: `package example;

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
}`,
  executeQueryCodeSnippet: `String query = String.format("from(bucket: \\"%s\\") |> range(start: -1h)", bucket);
List<FluxTable> tables = client.getQueryApi().query(query, org);`,
  writingDataLineProtocolCodeSnippet: `String data = "mem,host=host1 used_percent=23.43234543";
try (WriteApi writeApi = client.getWriteApi()) {
  writeApi.writeRecord(bucket, org, WritePrecision.NS, data);
}`,
  writingDataPointCodeSnippet: `Point point = Point
  .measurement("mem")
  .addTag("host", "host1")
  .addField("used_percent", 23.43234543)
  .time(Instant.now(), WritePrecision.NS);

try (WriteApi writeApi = client.getWriteApi()) {
  writeApi.writePoint(bucket, org, point);
}`,
  writingDataPojoCodeSnippet: `Mem mem = new Mem();
mem.host = "host1";
mem.used_percent = 23.43234543;
mem.time = Instant.now();

try (WriteApi writeApi = client.getWriteApi()) {
  writeApi.writeMeasurement(bucket, org, WritePrecision.NS, mem);
}`,
  pojoClassCodeSnippet: `@Measurement(name = "mem")
public static class Mem {
  @Column(tag = true)
  String host;
  @Column
  Double used_percent;
  @Column(timestamp = true)
  Instant time;
}`,
}

export const clientJSLibrary = {
  id: 'javascript-node',
  name: 'JavaScript/Node.js',
  url: 'https://github.com/influxdata/influxdb-client-js',
  initializeNPMCodeSnippet: `npm i @influxdata/influxdb-client`,
  initializeClientCodeSnippet: `const {InfluxDB} = require('@influxdata/influxdb-client')

// You can generate a Token from the "Tokens Tab" in the UI
const token = '<%= token %>'
const org = '<%= org %>'
const bucket = '<%= bucket %>'

const client = new InfluxDB({url: '<%= server %>', token: token})`,
  executeQueryCodeSnippet: `const queryApi = client.getQueryApi(org)

const query = \`from(bucket: \"\${bucket}\") |> range(start: -1h)\`
queryApi.queryRows(query, {
  next(row, tableMeta) {
    const o = tableMeta.toObject(row)
    console.log(
      \`\${o._time} \${o._measurement} in \'\${o.location}\' (\${o.example}): \${o._field}=\${o._value}\`
    )
  },
  error(error) {
    console.error(error)
    console.log('\\nFinished ERROR')
  },
  complete() {
    console.log('\\nFinished SUCCESS')
  },
})`,
  writingDataLineProtocolCodeSnippet: `const {Point} = require('@influxdata/influxdb-client')
const writeApi = client.getWriteApi(org, bucket)
writeApi.useDefaultTags({host: 'host1'})

const point = new Point('mem')
  .floatField('used_percent', 23.43234543)
writeApi.writePoint(point)
writeApi
    .close()
    .then(() => {
        console.log('FINISHED')
    })
    .catch(e => {
        console.error(e)
        console.log('\\nFinished ERROR')
    })`,
}

export const clientPythonLibrary = {
  id: 'python',
  name: 'Python',
  url: 'https://github.com/influxdata/influxdb-client-python',
  initializePackageCodeSnippet: `pip install influxdb-client`,
  initializeClientCodeSnippet: `from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate a Token from the "Tokens Tab" in the UI
token = "<%= token %>"
org = "<%= org %>"
bucket = "<%= bucket %>"

client = InfluxDBClient(url="<%= server %>", token=token)`,
  executeQueryCodeSnippet: `query = f'from(bucket: \\"{bucket}\\") |> range(start: -1h)'
tables = client.query_api().query(query, org=org)`,
  writingDataLineProtocolCodeSnippet: `write_api = client.write_api(write_options=SYNCHRONOUS)

data = "mem,host=host1 used_percent=23.43234543"
write_api.write(bucket, org, data)`,
  writingDataPointCodeSnippet: `point = Point("mem")\\
  .tag("host", "host1")\\
  .field("used_percent", 23.43234543)\\
  .time(datetime.utcnow(), WritePrecision.NS)

write_api.write(bucket, org, point)`,
  writingDataBatchCodeSnippet: `sequence = ["mem,host=host1 used_percent=23.43234543",
            "mem,host=host1 available_percent=15.856523"]
write_api.write(bucket, org, sequence)`,
}

export const clientRubyLibrary = {
  id: 'ruby',
  name: 'Ruby',
  url: 'https://github.com/influxdata/influxdb-client-ruby',
  initializeGemCodeSnippet: `gem install influxdb-client`,
  initializeClientCodeSnippet: `require 'influxdb-client'

# You can generate a Token from the "Tokens Tab" in the UI
token = '<%= token %>'
org = '<%= org %>'
bucket = '<%= bucket %>'

client = InfluxDB2::Client.new('<%= server %>', token,
  precision: InfluxDB2::WritePrecision::NANOSECOND)`,
  executeQueryCodeSnippet: `query = "from(bucket: \\"#{bucket}\\") |> range(start: -1h)"
tables = client.create_query_api.query(query: query, org: org)`,
  writingDataLineProtocolCodeSnippet: `write_api = client.create_write_api

data = 'mem,host=host1 used_percent=23.43234543'
write_api.write(data: data, bucket: bucket, org: org)`,
  writingDataPointCodeSnippet: `point = InfluxDB2::Point.new(name: 'mem')
  .add_tag('host', 'host1')
  .add_field('used_percent', 23.43234543)
  .time(Time.now.utc, InfluxDB2::WritePrecision::NANOSECOND)

write_api.write(data: point, bucket: bucket, org: org)`,
  writingDataHashCodeSnippet: `hash = {name: 'h2o',
  tags: {host: 'aws', region: 'us'},
  fields: {level: 5, saturation: '99%'},
  time: Time.now.utc}

write_api.write(data: hash, bucket: bucket, org: org)`,
  writingDataBatchCodeSnippet: `point = InfluxDB2::Point.new(name: 'mem')
  .add_tag('host', 'host1')
  .add_field('used_percent', 23.43234543)
  .time(Time.now.utc, InfluxDB2::WritePrecision::NANOSECOND)

hash = {name: 'h2o',
  tags: {host: 'aws', region: 'us'},
  fields: {level: 5, saturation: '99%'},
  time: Time.now.utc}

data = 'mem,host=host1 used_percent=23.23234543'

write_api.write(data: [point, hash, data], bucket: bucket, org: org)`,
}

export const clientPHPLibrary = {
  id: 'php',
  name: 'PHP',
  url: 'https://github.com/influxdata/influxdb-client-php',
  initializeComposerCodeSnippet: `composer require influxdata/influxdb-client-php`,
  initializeClientCodeSnippet: `use InfluxDB2\\Client;
use InfluxDB2\\Model\\WritePrecision;
use InfluxDB2\\Point;

# You can generate a Token from the "Tokens Tab" in the UI
$token = '<%= token %>';
$org = '<%= org %>';
$bucket = '<%= bucket %>';

$client = new Client([
    "url" => "<%= server %>",
    "token" => $token,
]);`,
  executeQueryCodeSnippet: `$query = "from(bucket: \\"{$bucket}\\") |> range(start: -1h)";
$tables = $client->createQueryApi()->query($query, $org);`,
  writingDataLineProtocolCodeSnippet: `$writeApi = $client->createWriteApi();

$data = "mem,host=host1 used_percent=23.43234543";

$writeApi->write($data, WritePrecision::S, $bucket, $org);`,
  writingDataPointCodeSnippet: `$point = Point::measurement('mem')
  ->addTag('host', 'host1')
  ->addField('used_percent', 23.43234543)
  ->time(microtime(true));

$writeApi->write($point, WritePrecision::S, $bucket, $org);`,
  writingDataArrayCodeSnippet: `$dataArray = ['name' => 'cpu',
  'tags' => ['host' => 'server_nl', 'region' => 'us'],
  'fields' => ['internal' => 5, 'external' => 6],
  'time' => microtime(true)];

$writeApi->write($dataArray, WritePrecision::S, $bucket, $org);`,
}

export const clientKotlinLibrary = {
  id: 'kotlin',
  name: 'Kotlin',
  url:
    'https://github.com/influxdata/influxdb-client-java/tree/master/client-kotlin',
  buildWithMavenCodeSnippet: `<dependency>
  <groupId>com.influxdb</groupId>
  <artifactId>influxdb-client-kotlin</artifactId>
  <version>1.8.0</version>
</dependency>`,
  buildWithGradleCodeSnippet: `dependencies {
  compile "com.influxdb:influxdb-client-kotlin:1.8.0"
}`,
  initializeClientCodeSnippet: `package example

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
}`,
  executeQueryCodeSnippet: `val query = ("from(bucket: \\"$bucket\\")"
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
  
client.close()`,
}

export const clientScalaLibrary = {
  id: 'scala',
  name: 'Scala',
  url:
    'https://github.com/influxdata/influxdb-client-java/tree/master/client-scala',
  buildWithSBTCodeSnippet: `libraryDependencies += "com.influxdb" % "influxdb-client-scala" % "1.8.0"`,
  buildWithMavenCodeSnippet: `<dependency>
  <groupId>com.influxdb</groupId>
  <artifactId>influxdb-client-scala</artifactId>
  <version>1.8.0</version>
</dependency>`,
  buildWithGradleCodeSnippet: `dependencies {
  compile "com.influxdb:influxdb-client-scala:1.8.0"
}`,
  initializeClientCodeSnippet: `package example

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
}`,
  executeQueryCodeSnippet: `val query = (s"""from(bucket: "$bucket")"""
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
system.terminate()`,
}

export const clientLibraries: ClientLibrary[] = [
  clientArduinoLibrary,
  clientCSharpLibrary,
  clientGoLibrary,
  clientJavaLibrary,
  clientJSLibrary,
  clientKotlinLibrary,
  clientPHPLibrary,
  clientPythonLibrary,
  clientRubyLibrary,
  clientScalaLibrary,
]
