// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'

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
      <TemplatedCodeSnippet
        template={initializePackageCodeSnippet}
        label="Code"
      />
      <h5>Initialize the Client</h5>
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="Python Code"
        defaults={{
          server: 'serverUrl',
          token: 'token',
        }}
      />
      <h5>Write Data</h5>
      <p>Option 1: Use InfluxDB Line Protocol to write data</p>
      <TemplatedCodeSnippet
        template={writingDataLineProtocolCodeSnippet}
        label="Python Code"
        defaults={{
          bucket: 'bucketID',
          org: 'orgID',
        }}
      />
      <p>Option 2: Use a Data Point to write data</p>
      <TemplatedCodeSnippet
        template={writingDataPointCodeSnippet}
        label="Python Code"
        defaults={{
          bucket: 'bucketID',
          org: 'orgID',
        }}
      />
      <p>Option 3: Use a Batch Sequence to write data</p>
      <TemplatedCodeSnippet
        template={writingDataBatchCodeSnippet}
        label="Python Code"
        defaults={{
          bucket: 'bucketID',
          org: 'orgID',
        }}
      />
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="Python Code"
        defaults={{
          bucket: 'my_bucket',
          org: 'orgID',
        }}
      />
    </ClientLibraryOverlay>
  )
}

export default ClientPythonOverlay
