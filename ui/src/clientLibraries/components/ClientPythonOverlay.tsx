// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import CodeSnippet from 'src/shared/components/CodeSnippet'

// Constants
import {clientPythonLibrary} from 'src/clientLibraries/constants'

const ClientPythonOverlay: FunctionComponent<{}> = () => {
  const {
    name,
    url,
    initializePackageCodeSnippet,
    initializeClientCodeSnippet,
    executeQueryCodeSnippet,
    writingDataLineProtocolCodeSnippet,
    writingDataPointCodeSnippet,
    writingDataBatchCodeSnippet,
  } = clientPythonLibrary

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
      <CodeSnippet copyText={initializePackageCodeSnippet} label="Code" />
      <h5>Initializing the Client</h5>
      <CodeSnippet copyText={initializeClientCodeSnippet} label="Code" />
      <h5>Writing Data</h5>
      <p>Option 1: Example for writing data using InfluxDB Line Protocol</p>
      <CodeSnippet copyText={writingDataLineProtocolCodeSnippet} label="Code" />
      <p>Option 2: Example for writing data using a Data Point</p>
      <CodeSnippet copyText={writingDataPointCodeSnippet} label="Code" />
      <p>Option 3: Example for writing data using a Batch Sequence</p>
      <CodeSnippet copyText={writingDataBatchCodeSnippet} label="Code" />
      <h5>Example for executing a Flux query</h5>
      <CodeSnippet copyText={executeQueryCodeSnippet} label="Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientPythonOverlay
