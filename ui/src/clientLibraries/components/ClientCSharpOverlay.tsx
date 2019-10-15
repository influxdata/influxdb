// Libraries
import React, {FunctionComponent} from 'react'
// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import CodeSnippet from 'src/shared/components/CodeSnippet'
// Constants
import {clientCSharpLibrary} from 'src/clientLibraries/constants'

const ClientCSharpOverlay: FunctionComponent<{}> = () => {
  const {
    name,
    url,
    installingPackageManagerCodeSnippet,
    installingPackageDotNetCLICodeSnippet,
    packageReferenceCodeSnippet,
    initializeClientCodeSnippet,
    executeQueryCodeSnippet,
    writingDataPointCodeSnippet: writingDataDataPointCodeSnippet,
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
      <h5>Installing Package</h5>
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
      <CodeSnippet copyText={initializeClientCodeSnippet} label="C# Code" />
      <h5>Writing Data</h5>
      <p>
        <b>Option 1: Example for writing using InfluxDB Line Protocol</b>
      </p>
      <CodeSnippet
        copyText={writingDataLineProtocolCodeSnippet}
        label="C# Code"
      />
      <p>
        <b>Option 2: Example for writing using a Data Point</b>
      </p>
      <CodeSnippet copyText={writingDataDataPointCodeSnippet} label="C# Code" />
      <p>
        <b>
          Option 3: Example for writing using a POCO and corresponding Class
        </b>
      </p>
      <CodeSnippet copyText={writingDataPocoCodeSnippet} label="C# Code" />
      <CodeSnippet copyText={pocoClassCodeSnippet} label="C# Code" />
      <h5>Example for executing a Flux query</h5>
      <CodeSnippet copyText={executeQueryCodeSnippet} label="C# Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientCSharpOverlay
