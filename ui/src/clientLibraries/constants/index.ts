import {SFC} from 'react'
import CSharpLogo from '../graphics/CSharpLogo'
import GoLogo from '../graphics/GoLogo'
import JavaLogo from '../graphics/JavaLogo'
import JSLogo from '../graphics/JSLogo'
import PHPLogo from '../graphics/PHPLogo'
import PythonLogo from '../graphics/PythonLogo'
import RubyLogo from '../graphics/RubyLogo'

export interface ClientLibrary {
  id: string
  name: string
  url: string
  image: SFC
}

export const clientCSharpLibrary = {
  id: 'csharp',
  name: 'C#',
  url: 'https://github.com/influxdata/influxdb-client-csharp',
  image: CSharpLogo,
  installingPackageManagerCodeSnippet: `Install-Package InfluxDB.Client`,
  installingPackageDotNetCLICodeSnippet: `dotnet add package InfluxDB.Client`,
  packageReferenceCodeSnippet: `<PackageReference Include="InfluxDB.Client" />`,
  initializeClientCodeSnippet: `using InfluxDB.Client;
namespace Examples
{
  public class Examples
  {
    public static void Main(string[] args)
    {
      // You can generate a Token from the "Tokens Tab" in the UI
      var client = InfluxDBClientFactory.Create("<%= server %>", "<%= token %>".ToCharArray());
    }
  }
}`,
  executeQueryCodeSnippet: `const string query = "from(bucket: \\"<%= bucket %>\\") |> range(start: -1h)";
var tables = await client.GetQueryApi().QueryAsync(query, "<%= org %>");`,
  writingDataLineProtocolCodeSnippet: `const string data = "mem,host=host1 used_percent=23.43234543 1556896326";
using (var writeApi = client.GetWriteApi())
{
  writeApi.WriteRecord("<%= bucket %>", "<%= org %>", WritePrecision.Ns, data);
}`,
  writingDataPointCodeSnippet: `var point = PointData
  .Measurement("mem")
  .Tag("host", "host1")
  .Field("used_percent", 23.43234543)
  .Timestamp(1556896326L, WritePrecision.Ns);

using (var writeApi = client.GetWriteApi())
{
  writeApi.WritePoint("<%= bucket %>", "<%= org %>", point);
}`,
  writingDataPocoCodeSnippet: `var mem = new Mem { Host = "host1", UsedPercent = 23.43234543, Time = DateTime.UtcNow };

using (var writeApi = client.GetWriteApi())
{
  writeApi.WriteMeasurement("<%= bucket %>", "<%= org %>", WritePrecision.Ns, mem);
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
  image: GoLogo,
  initializeClientCodeSnippet: `// You can generate a Token from the "Tokens Tab" in the UI
influx, err := influxdb.New(<%= server %>, <%= token %>, influxdb.WithHTTPClient(myHTTPClient))
if err != nil {
  panic(err) // error handling here; normally we wouldn't use fmt but it works for the example
}
// Add your app code here
influx.Close() // closes the client.  After this the client is useless.`,
  writeDataCodeSnippet: `// we use client.NewRowMetric for the example because it's easy, but if you need extra performance
// it is fine to manually build the []client.Metric{}.
myMetrics := []influxdb.Metric{
  influxdb.NewRowMetric(
    map[string]interface{}{"memory": 1000, "cpu": 0.93},
    "system-metrics",
    map[string]string{"hostname": "hal9000"},
    time.Date(2018, 3, 4, 5, 6, 7, 8, time.UTC)),
  influxdb.NewRowMetric(
    map[string]interface{}{"memory": 1000, "cpu": 0.93},
    "system-metrics",
    map[string]string{"hostname": "hal9000"},
    time.Date(2018, 3, 4, 5, 6, 7, 9, time.UTC)),
}

// The actual write..., this method can be called concurrently.
if _, err := influx.Write(context.Background(), "<%= bucket %>", "<%= org %>", myMetrics...)
if err != nil {
  log.Fatal(err) // as above use your own error handling here.
}`,
}

export const clientJavaLibrary = {
  id: 'java',
  name: 'Java',
  url: 'https://github.com/influxdata/influxdb-client-java',
  image: JavaLogo,
  buildWithMavenCodeSnippet: `<dependency>
  <groupId>com.influxdb</groupId>
  <artifactId>influxdb-client-java</artifactId>
  <version>1.5.0</version>
</dependency>`,
  buildWithGradleCodeSnippet: `dependencies {
  compile "com.influxdb:influxdb-client-java:1.5.0"
}`,
  initializeClientCodeSnippet: `package example;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;

public class InfluxDB2Example {
  public static void main(final String[] args) {
    // You can generate a Token from the "Tokens Tab" in the UI
    InfluxDBClient client = InfluxDBClientFactory.create("<%= server %>", "<%= token %>".toCharArray());
  }
}`,
  executeQueryCodeSnippet: `String query = "from(bucket: \\"<%= bucket %>\\") |> range(start: -1h)";
List<FluxTable> tables = client.getQueryApi().query(query, "<%= org %>");`,
  writingDataLineProtocolCodeSnippet: `String data = "mem,host=host1 used_percent=23.43234543 1556896326";
try (WriteApi writeApi = client.getWriteApi()) {
  writeApi.writeRecord("<%= bucket %>", "<%= org %>", WritePrecision.NS, data);
}`,
  writingDataPointCodeSnippet: `Point point = Point
  .measurement("mem")
  .addTag("host", "host1")
  .addField("used_percent", 23.43234543)
  .time(1556896326L, WritePrecision.NS);

try (WriteApi writeApi = client.getWriteApi()) {
  writeApi.writePoint("<%= bucket %>", "<%= org %>", point);
}`,
  writingDataPojoCodeSnippet: `Mem mem = new Mem();
mem.host = "host1";
mem.used_percent = 23.43234543;
mem.time = Instant.now();

try (WriteApi writeApi = client.getWriteApi()) {
  writeApi.writeMeasurement("<%= bucket %>", "<%= org %>", WritePrecision.NS, mem);
}`,
  pojoClassCodeSnippet: `@Measurement(name = "mem")
public class Mem {
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
  image: JSLogo,
  initializeNPMCodeSnippet: `npm i @influxdata/influxdb-client`,
  initializeClientCodeSnippet: `const {InfluxDB} = require('@influxdata/influxdb-client')
// You can generate a Token from the "Tokens Tab" in the UI
const client = new InfluxDB({url: '<%= server %>', token: '<%= token %>'})`,
  executeQueryCodeSnippet: `const queryApi = client.getQueryApi('<%= org %>')

const query = 'from(bucket: "my_bucket") |> range(start: -1h)'
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
  writingDataLineProtocolCodeSnippet: `const writeApi = client.getWriteApi('<%= org %>', '<%= bucket %>')
  
const data = 'mem,host=host1 used_percent=23.43234543 1556896326' // Line protocol string
writeApi.writeRecord(data)

writeApi.close()
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
  image: PythonLogo,
  initializePackageCodeSnippet: `pip install influxdb-client`,
  initializeClientCodeSnippet: `import influxdb_client
from influxdb_client import InfluxDBClient

## You can generate a Token from the "Tokens Tab" in the UI
client = InfluxDBClient(url="<%= server %>", token="<%= token %>")`,
  executeQueryCodeSnippet: `query = 'from(bucket: "<%= bucket %>") |> range(start: -1h)'
tables = client.query_api().query(query, org="<%= org %>")`,
  writingDataLineProtocolCodeSnippet: `write_api = client.write_api()

data = "mem,host=host1 used_percent=23.43234543 1556896326"
write_api.write("<%= bucket %>", "<%= org %>", data)`,
  writingDataPointCodeSnippet: `point = Point("mem")\\
  .tag("host", "host1")\\
  .field("used_percent", 23.43234543)\\
  .time(1556896326, WritePrecision.NS)

write_api.write("<%= bucket %>", "<%= org %>", point)`,
  writingDataBatchCodeSnippet: `sequence = ["mem,host=host1 used_percent=23.43234543 1556896326",
            "mem,host=host1 available_percent=15.856523 1556896326"]
write_api.write("<%= bucket %>", "<%= org %>", sequence)`,
}

export const clientRubyLibrary = {
  id: 'ruby',
  name: 'Ruby',
  url: 'https://github.com/influxdata/influxdb-client-ruby',
  image: RubyLogo,
  initializeGemCodeSnippet: `gem install influxdb-client -v 1.0.0.beta`,
  initializeClientCodeSnippet: `## You can generate a Token from the "Tokens Tab" in the UI
client = InfluxDB2::Client.new('<%= server %>', '<%= token %>')`,
  executeQueryCodeSnippet: `query = 'from(bucket: "<%= bucket %>") |> range(start: -1h)'
tables = client.create_query_api.query(query: query, org: '<%= org %>')`,
  writingDataLineProtocolCodeSnippet: `data = 'mem,host=host1 used_percent=23.43234543 1556896326'
write_client.write(data: data, bucket: '<%= bucket %>', org: '<%= org %>')`,
  writingDataPointCodeSnippet: `point = InfluxDB2::Point.new(name: 'mem')
  .add_tag('host', 'host1')
  .add_field('used_percent', 23.43234543)
  .time(1_556_896_326, WritePrecision.NS)

write_client.write(data: point, bucket: '<%= bucket %>', org: '<%= org %>')`,
  writingDataHashCodeSnippet: `hash = { name: 'h2o',
  tags: { host: 'aws', region: 'us' },
  fields: { level: 5, saturation: '99%' },
  time: 123 }

write_client.write(data: hash, bucket: '<%= bucket %>', org: '<%= org %>')`,
  writingDataBatchCodeSnippet: `point = InfluxDB2::Point.new(name: 'mem')
  .add_tag('host', 'host1')
  .add_field('used_percent', 23.43234543)
  .time(1_556_896_326, WritePrecision.NS)
 
hash = { name: 'h2o',
  tags: { host: 'aws', region: 'us' },
  fields: { level: 5, saturation: '99%' },
  time: 123 }
  
data = 'mem,host=host1 used_percent=23.43234543 1556896326'   
            
write_client.write(data: [point, hash, data], bucket: '<%= bucket %>', org: '<%= org %>')`,
}

export const clientPHPLibrary = {
  id: 'php',
  name: 'PHP',
  url: 'https://github.com/influxdata/influxdb-client-php',
  image: PHPLogo,
  initializeComposerCodeSnippet: `composer require influxdata/influxdb-client-php`,
  initializeClientCodeSnippet: `## You can generate a Token from the "Tokens Tab" in the UI
$client = new InfluxDB2\\Client([
  "url" => "<%= server %>",
  "token" => "<%= token %>",
]);`,
  executeQueryCodeSnippet: `$query = 'from(bucket: "<%= bucket %>") |> range(start: -1h)';
$tables = $client->createQueryApi()->query($query, '<%= org %>');`,
  writingDataLineProtocolCodeSnippet: `$writeApi = $client->createWriteApi();
  
$data = "mem,host=host1 used_percent=23.43234543 1556896326";

$writeApi->write($data, \\InfluxDB2\\Model\\WritePrecision::S, '<%= bucket %>', '<%= org %>');`,
  writingDataPointCodeSnippet: `$point = \\InfluxDB2\\Point::measurement('mem')
  ->addTag('host', 'host1')
  ->addField('used_percent', 23.43234543)
  ->time(1556896326);

$writeApi->write($point, \\InfluxDB2\\Model\\WritePrecision::S, '<%= bucket %>', '<%= org %>');`,
  writingDataArrayCodeSnippet: `$dataArray = ['name' => 'cpu',
  'tags' => ['host' => 'server_nl', 'region' => 'us'],
  'fields' => ['internal' => 5, 'external' => 6],
  'time' => microtime(true)];

$writeApi->write($dataArray, \\InfluxDB2\\Model\\WritePrecision::S, '<%= bucket %>', '<%= org %>');`,
}

export const clientLibraries: ClientLibrary[] = [
  clientCSharpLibrary,
  clientGoLibrary,
  clientJavaLibrary,
  clientJSLibrary,
  clientPHPLibrary,
  clientPythonLibrary,
  clientRubyLibrary,
]
