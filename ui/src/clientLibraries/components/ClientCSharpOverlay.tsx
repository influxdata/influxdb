// Libraries
import React, {FunctionComponent} from 'react'
// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'
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
      <TemplatedCodeSnippet
        template={installingPackageManagerCodeSnippet}
        label="Code"
      />
      <p>.NET CLI</p>
      <TemplatedCodeSnippet
        template={installingPackageDotNetCLICodeSnippet}
        label="Code"
      />
      <p>Package Reference</p>
      <TemplatedCodeSnippet
        template={packageReferenceCodeSnippet}
        label="Code"
      />
      <h5>Initialize the Client</h5>
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="C# Code"
        defaults={{
          server: 'basepath',
          token: 'token',
        }}
      />
      <h5>Write Data</h5>
      <p>Option 1: Use InfluxDB Line Protocol to write data</p>
      <TemplatedCodeSnippet
        template={writingDataLineProtocolCodeSnippet}
        label="C# Code"
        defaults={{
          org: 'orgID',
          bucket: 'bucketID',
        }}
      />
      <p>Option 2: Use a Data Point to write data</p>
      <TemplatedCodeSnippet
        template={writingDataDataPointCodeSnippet}
        label="C# Code"
        defaults={{
          org: 'orgID',
          bucket: 'bucketID',
        }}
      />
      <p>Option 3: Use POCO and corresponding Class to write data</p>
      <TemplatedCodeSnippet
        template={writingDataPocoCodeSnippet}
        label="C# Code"
        defaults={{
          org: 'orgID',
          bucket: 'bucketID',
        }}
      />
      <TemplatedCodeSnippet template={pocoClassCodeSnippet} label="C# Code" />
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="C# Code"
        defaults={{
          org: 'orgID',
          bucket: 'bucketID',
        }}
      />
    </ClientLibraryOverlay>
  )
}

export default ClientCSharpOverlay
