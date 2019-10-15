// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'

// Constants
import {clientGoLibrary} from 'src/clientLibraries/constants'
import CodeSnippet from 'src/shared/components/CodeSnippet'

const ClientGoOverlay: FunctionComponent<{}> = () => {
  const {name, url} = clientGoLibrary
  const initializeClientCodeSnippet = `
    influx, err := influxdb.New(myHTTPInfluxAddress, myToken, influxdb.WithHTTPClient(myHTTPClient))
	  if err != nil {
		  panic(err) // error handling here; normally we wouldn't use fmt but it works for the example
    }
    // Add your app code here
    influx.Close() // closes the client.  After this the client is useless.
  `
  const writeDataCodeSnippet = `
    // we use client.NewRowMetric for the example because it's easy, but if you need extra performance
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
    if _, err := influx.Write(context.Background(), "my-awesome-bucket", "my-very-awesome-org", myMetrics...)
    if err != nil {
      log.Fatal(err) // as above use your own error handling here.
    }
  `
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
      <CodeSnippet copyText={initializeClientCodeSnippet} label="Code" />
      <h5>Writing Data</h5>
      <CodeSnippet copyText={writeDataCodeSnippet} label="Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientGoOverlay
