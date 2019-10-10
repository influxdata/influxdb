// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'

// Constants
import {clientPythonLibrary} from 'src/clientLibraries/constants'

const ClientPythonOverlay: FunctionComponent<{}> = () => {
  const {name, url} = clientPythonLibrary

  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <br />
      <h5>Installing Package</h5>
      <pre>
        <code>pip install influxdb-client</code>
      </pre>
      <h5>Initializing the Client</h5>
      <pre>
        <code>
          import influxdb_client
          <br />
          from influxdb_client import InfluxDBClient
          <br />
          <br />
          client = InfluxDBClient(url="basepath", token="token")
          <br />
        </code>
      </pre>
      <h5>Using the client to execute a query</h5>
      <pre>
        <code>
          query = 'from(bucket: "my_bucket") |> range(start: -1h)'
          <br />
          <br />
          tables = client.query_api().query(query, org="someorgid")
        </code>
      </pre>
      <h5>Writing Data</h5>
      <p>
        Data could be written using InfluxDB Line Protocol,Data Point or
        Sequence
      </p>
      <p>
        <b>InfluxDB Line Protocol</b>
      </p>
      <pre>
        <code>
          data = "mem,host=host1 used_percent=23.43234543 1556896326"
          <br />
          <br />
          write_client.write("bucketID", "orgID", data)
        </code>
      </pre>
      <p>
        <b>Data Point</b>
      </p>
      <pre>
        <code>
          point = Point("mem").tag("host", "host1").field("used_percent",
          23.43234543).time(1556896326, WritePrecision.NS)
          <br />
          <br />
          write_client.write("bucketID", "orgID", point)
        </code>
      </pre>
      <p>
        <b>Sequence</b>
      </p>
      <pre>
        <code>
          sequence = ["mem,host=host1 used_percent=23.43234543 1556896326",
          <br />
          &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"mem,host=host1
          available_percent=15.856523 1556896326"]
          <br />
          <br />
          write_client.write("bucketID", "orgID", sequence)
        </code>
      </pre>
    </ClientLibraryOverlay>
  )
}

export default ClientPythonOverlay
