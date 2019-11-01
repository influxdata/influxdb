// Libraries
import React, {FunctionComponent} from 'react'
// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'
// Constants
import {clientJavaLibrary} from 'src/clientLibraries/constants'

const ClientJavaOverlay: FunctionComponent<{}> = () => {
  const {
    name,
    url,
    buildWithMavenCodeSnippet,
    buildWithGradleCodeSnippet,
    initializeClientCodeSnippet,
    executeQueryCodeSnippet,
    writingDataLineProtocolCodeSnippet,
    writingDataPointCodeSnippet,
    writingDataPojoCodeSnippet,
    pojoClassCodeSnippet,
  } = clientJavaLibrary

  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <h5>Add Dependency</h5>
      <p>Build with Maven</p>
      <TemplatedCodeSnippet template={buildWithMavenCodeSnippet} label="Code" />
      <p>Build with Gradle</p>
      <TemplatedCodeSnippet
        template={buildWithGradleCodeSnippet}
        label="Code"
      />
      <h5>Initialize the Client</h5>
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="Java Code"
        defaults={{
          server: 'serverUrl',
          token: 'token',
        }}
      />
      <h5>Write Data</h5>
      <p>Option 1: Use InfluxDB Line Protocol to write data</p>
      <TemplatedCodeSnippet
        template={writingDataLineProtocolCodeSnippet}
        label="Java Code"
        defaults={{
          bucket: 'bucketID',
          org: 'orgID',
        }}
      />
      <p>Option 2: Use a Data Point to write data</p>
      <TemplatedCodeSnippet
        template={writingDataPointCodeSnippet}
        label="Java Code"
        defaults={{
          bucket: 'bucketID',
          org: 'orgID',
        }}
      />
      <p>Option 3: Use POJO and corresponding class to write data</p>
      <TemplatedCodeSnippet
        template={writingDataPojoCodeSnippet}
        label="Java Code"
        defaults={{
          bucket: 'bucketID',
          org: 'orgID',
        }}
      />
      <TemplatedCodeSnippet template={pojoClassCodeSnippet} label="Java Code" />
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="Java Code"
        defaults={{
          bucket: 'my_bucket',
          org: 'myorgid',
        }}
      />
    </ClientLibraryOverlay>
  )
}

export default ClientJavaOverlay
