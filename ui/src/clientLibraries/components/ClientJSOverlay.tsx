// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'

// Constants
import {clientJSLibrary} from 'src/clientLibraries/constants'

const ClientJSOverlay: FunctionComponent<{}> = () => {
  const {name, url} = clientJSLibrary

  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <br />
      <h5>Initializing the Client</h5>
      <pre>
        <code>import Client from '@influxdata/influx'</code>
        <br />
        <code>const client = new Client('basepath', 'token')</code>
      </pre>
      <h5>Using the client to execute a query</h5>
      <pre>
        <code>
          const query = 'from(bucket: "my_bucket") |> range(start: -1h)'
        </code>
        <br />
        <code>
          const &#123;promise, cancel&#125; =
          client.queries.execute('someorgid', query)
        </code>
        <br />
        <code>const csv = await promise</code>
        <br />
        <code>cancel() // Cancels request</code>
      </pre>
      <h5>Writing Data</h5>
      <p>Data should be written using InfluxDB Line Protocol</p>
      <pre>
        <code>const data = '' // Line protocal string</code>
        <code>
          const response = await client.write.create('orgID', 'bucketID', data)
        </code>
      </pre>
    </ClientLibraryOverlay>
  )
}

export default ClientJSOverlay
