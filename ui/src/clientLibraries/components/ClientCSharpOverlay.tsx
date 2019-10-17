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
      <h5>Install Package</h5>
      <p>Package Manager</p>
      <CodeSnippet
        copyText={installingPackageManagerCodeSnippet}
        label="Code"
      />
      <p>.NET CLI</p>
      <CodeSnippet
        copyText={installingPackageDotNetCLICodeSnippet}
        label="Code"
      />
      <p>Package Reference</p>
      <CodeSnippet copyText={packageReferenceCodeSnippet} label="Code" />
      <h5>Initialize the Client</h5>
      <CodeSnippet copyText={initializeClientCodeSnippet} label="C# Code" />
      <h5>Write Data</h5>
      <p>Option 1: Use InfluxDB Line Protocol to write data</p>
      <CodeSnippet
        copyText={writingDataLineProtocolCodeSnippet}
        label="C# Code"
      />
      <p>Option 2: Use a Data Point to write data</p>
      <CodeSnippet copyText={writingDataDataPointCodeSnippet} label="C# Code" />
      <p>Option 3: Use POCO and corresponding Class to write data</p>
      <CodeSnippet copyText={writingDataPocoCodeSnippet} label="C# Code" />
      <CodeSnippet copyText={pocoClassCodeSnippet} label="C# Code" />
      <h5>Execute a Flux query</h5>
      <CodeSnippet copyText={executeQueryCodeSnippet} label="C# Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientCSharpOverlay
