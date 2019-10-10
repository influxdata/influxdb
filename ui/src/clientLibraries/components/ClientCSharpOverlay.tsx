// Libraries
import React, {FunctionComponent} from 'react'
// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
// Constants
import {clientCSharpLibrary} from 'src/clientLibraries/constants'

const ClientCSharpOverlay: FunctionComponent<{}> = () => {
  const {name, url} = clientCSharpLibrary

  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <br />
      <h5>Installing package</h5>
      <p>
        <b>Package Manager</b>
      </p>
      <pre>
        <code>Install-Package InfluxDB.Client</code>
      </pre>
      <p>
        <b>.NET CLI</b>
      </p>
      <pre>
        <code>dotnet add package InfluxDB.Client</code>
      </pre>
      <p>
        <b>Package Reference</b>
      </p>
      <pre>
        <code>&lt;PackageReference Include="InfluxDB.Client"/&gt;</code>
      </pre>
      <h5>Initializing the Client</h5>
      <pre>
        <code>
          using InfluxDB.Client;
          <br />
          <br />
          namespace Examples
          <br />
          &#123;
          <br />
          &nbsp;&nbsp;public class Examples
          <br />
          &nbsp;&nbsp;&#123;
          <br />
          &nbsp;&nbsp;&nbsp;&nbsp;public static void Main(string[] args)
          <br />
          &nbsp;&nbsp;&nbsp;&nbsp;&#123;
          <br />
          &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;var client =
          InfluxDBClientFactory.Create("basepath", "token".ToCharArray());
          <br />
          &nbsp;&nbsp;&nbsp;&nbsp;&#125;
          <br />
          &nbsp;&nbsp;&#125;
          <br />
          &#125;
          <br />
        </code>
      </pre>
      <h5>Using the client to execute a query</h5>
      <pre>
        <code>
          const string query = "from(bucket: \"my_bucket\") |> range(start:
          -1h)";
          <br />
          <br />
          var tables = await client.GetQueryApi().QueryAsync(query,
          "someorgid");
        </code>
      </pre>
      <h5>Writing Data</h5>
      <p>
        Data could be written using InfluxDB Line Protocol, Data Point or POCO
      </p>
      <p>
        <b>InfluxDB Line Protocol</b>
      </p>
      <pre>
        <code>
          const string data = "mem,host=host1 used_percent=23.43234543
          1556896326";
          <br />
          <br />
          using (var writeApi = client.GetWriteApi())
          <br />
          &#123;
          <br />
          &nbsp;&nbsp;writeApi.WriteRecord("bucketID", "orgID",
          WritePrecision.Ns, data);
          <br />
          &#125;
          <br />
        </code>
      </pre>
      <p>
        <b>Data Point</b>
      </p>
      <pre>
        <code>
          var point = PointData
          <br />
          &nbsp;&nbsp;.Measurement("mem")
          <br />
          &nbsp;&nbsp;.Tag("host", "host1")
          <br />
          &nbsp;&nbsp;.Field("used_percent", 23.43234543)
          <br />
          &nbsp;&nbsp;.Timestamp(1556896326L, WritePrecision.Ns);
          <br />
          <br />
          using (var writeApi = client.GetWriteApi())
          <br />
          &#123;
          <br />
          &nbsp;&nbsp;writeApi.WritePoint("bucketID", "orgID", point);
          <br />
          &#125;
          <br />
        </code>
      </pre>
      <p>
        <b>POCO</b>
      </p>
      <pre>
        <code>
          var mem = new Mem &#123;Host = "host1", UsedPercent = 23.43234543,
          Time = DateTime.UtcNow&#125;;
          <br />
          <br />
          using (var writeApi = client.GetWriteApi())
          <br />
          &#123;
          <br />
          &nbsp;&nbsp;writeApi.WriteMeasurement("bucketID", "orgID",
          WritePrecision.Ns, mem);
          <br />
          &#125;
          <br />
        </code>
      </pre>
      <pre>
        <code>
          [Measurement("mem")]
          <br />
          private class Mem
          <br />
          &#123;
          <br />
          &nbsp;&nbsp;[Column("host", IsTag = true)] public string Host &#123;
          get; set; &#125;
          <br />
          &nbsp;&nbsp;[Column("used_percent")] public double? UsedPercent &#123;
          get; set; &#125;
          <br />
          &nbsp;&nbsp;[Column(IsTimestamp = true)] public DateTime Time &#123;
          get; set; &#125;
          <br />
          &#125;
        </code>
      </pre>
    </ClientLibraryOverlay>
  )
}

export default ClientCSharpOverlay
