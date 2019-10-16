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
      <h5>Install Package</h5>
      <CodeSnippet copyText={initializePackageCodeSnippet} label="Code" />
      <h5>Initialize the Client</h5>
      <CodeSnippet copyText={initializeClientCodeSnippet} label="Python Code" />
      <h5>Write Data</h5>
      <p>Option 1: Use InfluxDB Line Protocol to write data</p>
      <CodeSnippet
        copyText={writingDataLineProtocolCodeSnippet}
        label="Python Code"
      />
      <p>Option 2: Use a Data Point to write data</p>
      <CodeSnippet copyText={writingDataPointCodeSnippet} label="Python Code" />
      <p>Option 3: Use a Batch Sequence to write data</p>
      <CodeSnippet copyText={writingDataBatchCodeSnippet} label="Python Code" />
      <h5>Execute a Flux query</h5>
      <CodeSnippet copyText={executeQueryCodeSnippet} label="Python Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientPythonOverlay
