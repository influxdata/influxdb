// Libraries
import React, {FunctionComponent} from 'react'
// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
// Constants
import {clientCSharpLibrary} from 'src/clientLibraries/constants'
import CodeSnippet from 'src/shared/components/CodeSnippet'

const ClientCSharpOverlay: FunctionComponent<{}> = () => {
  const {
    name,
    url,
    installingPackageManagerCodeSnippet,
    installingPackageDotNetCLICodeSnippet,
    packageReferenceCodeSnippet,
    initializeClientCodeSnippet,
    executeQueryCodeSnippet,
    writingDataDataPointCodeSnippet,
    writingDataLineProtocolCodeSnippet,
    writingDataPocoCodeSnippet,
    pocoClassCodeSnippet,
  } = clientCSharpLibrary
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
      <CodeSnippet
        copyText={installingPackageManagerCodeSnippet}
        label="Code"
      />
      <p>
        <b>.NET CLI</b>
      </p>
      <CodeSnippet
        copyText={installingPackageDotNetCLICodeSnippet}
        label="Code"
      />
      <p>
        <b>Package Reference</b>
      </p>
      <CodeSnippet copyText={packageReferenceCodeSnippet} label="Code" />
      <h5>Initializing the Client</h5>
      <CodeSnippet copyText={initializeClientCodeSnippet} label="Code" />
      <h5>Using the client to execute a query</h5>
      <CodeSnippet copyText={executeQueryCodeSnippet} label="Code" />
      <h5>Writing Data</h5>
      <p>
        <b>InfluxDB Line Protocol</b>
      </p>
      <CodeSnippet copyText={writingDataLineProtocolCodeSnippet} label="Code" />
      <p>
        <b>Data Point</b>
      </p>
      <CodeSnippet copyText={writingDataDataPointCodeSnippet} label="Code" />
      <p>
        <b>POCO</b>
      </p>
      <CodeSnippet copyText={writingDataPocoCodeSnippet} label="Code" />
      <p>
        <b>POCO Class</b>
      </p>
      <CodeSnippet copyText={pocoClassCodeSnippet} label="Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientCSharpOverlay
