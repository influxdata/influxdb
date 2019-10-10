// Libraries
import React, {FunctionComponent} from 'react'
// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
// Constants
import {clientJavaLibrary} from 'src/clientLibraries/constants'

const ClientJavaOverlay: FunctionComponent<{}> = () => {
  const {name, url} = clientJavaLibrary

  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <br />
      <h5>Adding dependency</h5>
      <p>
        <b>Build with Maven</b>
      </p>
      <pre>
        <code>
          &lt;dependency&gt;
          <br />
          &nbsp;&nbsp;&lt;groupId&gt;com.influxdb&lt;/groupId&gt;
          <br />
          &nbsp;&nbsp;&lt;artifactId&gt;influxdb-client-java&lt;/artifactId&gt;
          <br />
          &nbsp;&nbsp;&lt;version&gt;1.1.0&lt;/version&gt;
          <br />
          &lt;/dependency&gt;
        </code>
      </pre>
      <p>
        <b>Build with Gradle</b>
      </p>
      <pre>
        <code>
          dependencies &#123;
          <br />
          &nbsp;&nbsp;compile "com.influxdb:influxdb-client-java:1.1.0"
          <br />
          &#125;
        </code>
      </pre>
      <h5>Initializing the Client</h5>
      <pre>
        <code>
          package example;
          <br />
          <br />
          import com.influxdb.client.InfluxDBClient;
          <br />
          import com.influxdb.client.InfluxDBClientFactory;
          <br />
          <br />
          public class InfluxDB2Example &#123;
          <br />
          <br />
          &nbsp;&nbsp;public static void main(final String[] args) &#123;
          <br />
          <br />
          &nbsp;&nbsp;&nbsp;&nbsp;InfluxDBClient client =
          InfluxDBClientFactory.create("basepath", "token".toCharArray());
          <br />
          <br />
          &nbsp;&nbsp;&#125;
          <br />
          &#125;
        </code>
      </pre>
      <h5>Using the client to execute a query</h5>
      <pre>
        <code>
          String query = "from(bucket: \"my_bucket\") |> range(start: -1h)";
          <br />
          <br />
          List&lt;FluxTable&gt; tables = client.getQueryApi().query(query,
          "someorgid");
        </code>
      </pre>
      <h5>Writing Data</h5>
      <p>
        Data could be written using InfluxDB Line Protocol, Data Point or POJO
      </p>
      <p>
        <b>InfluxDB Line Protocol</b>
      </p>
      <pre>
        <code>
          String data = "mem,host=host1 used_percent=23.43234543 1556896326";
          <br />
          <br />
          try (WriteApi writeApi = client.getWriteApi()) &#123;
          <br />
          &nbsp;&nbsp;writeApi.writeRecord("bucketID", "orgID",
          WritePrecision.NS, data);
          <br />
          &#125;
        </code>
      </pre>
      <p>
        <b>Data Point</b>
      </p>
      <pre>
        <code>
          Point point = Point
          <br />
          &nbsp;&nbsp;.measurement("mem")
          <br />
          &nbsp;&nbsp;.addTag("host", "host1")
          <br />
          &nbsp;&nbsp;.addField("used_percent", 23.43234543)
          <br />
          &nbsp;&nbsp;.time(1556896326L, WritePrecision.NS);
          <br />
          <br />
          try (WriteApi writeApi = client.getWriteApi()) &#123;
          <br />
          &nbsp;&nbsp;writeApi.writePoint("bucketID", "orgID", point);
          <br />
          &#125;
        </code>
      </pre>
      <p>
        <b>POJO</b>
      </p>
      <pre>
        <code>
          Mem mem = new Mem();
          <br />
          mem.host = "host1";
          <br />
          mem.used_percent = 23.43234543;
          <br />
          mem.time = Instant.now();
          <br />
          <br />
          try (WriteApi writeApi = client.getWriteApi()) &#123;
          <br />
          &nbsp;&nbsp;writeApi.writeMeasurement("bucketID", "orgID",
          WritePrecision.NS, mem);
          <br />
          &#125;
        </code>
      </pre>
      <pre>
        <code>
          @Measurement(name = "mem")
          <br />
          public class Mem &#123;
          <br />
          &nbsp;&nbsp;@Column(tag = true)
          <br />
          &nbsp;&nbsp;String host;
          <br />
          <br />
          &nbsp;&nbsp;@Column
          <br />
          &nbsp;&nbsp;Double used_percent;
          <br />
          <br />
          &nbsp;&nbsp;@Column(timestamp = true)
          <br />
          &nbsp;&nbsp;Instant time;
          <br />
          &#125;
        </code>
      </pre>
    </ClientLibraryOverlay>
  )
}

export default ClientJavaOverlay
