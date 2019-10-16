// Libraries
import React, {FunctionComponent} from 'react'
// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import CodeSnippet from 'src/shared/components/CodeSnippet'
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
      <CodeSnippet copyText={buildWithMavenCodeSnippet} label="Code" />
      <p>Build with Gradle</p>
      <CodeSnippet copyText={buildWithGradleCodeSnippet} label="Code" />
      <h5>Initialize the Client</h5>
      <CodeSnippet copyText={initializeClientCodeSnippet} label="Java Code" />
      <h5>Write Data</h5>
      <p>Option 1: Use InfluxDB Line Protocol to write data</p>
      <CodeSnippet
        copyText={writingDataLineProtocolCodeSnippet}
        label="Java Code"
      />
      <p>Option 2: Use a Data Point to write data</p>
      <CodeSnippet copyText={writingDataPointCodeSnippet} label="Java Code" />
      <p>Option 3: Use POJO and corresponding class to write data</p>
      <CodeSnippet copyText={writingDataPojoCodeSnippet} label="Java Code" />
      <CodeSnippet copyText={pojoClassCodeSnippet} label="Java Code" />
      <h5>Execute a Flux query</h5>
      <CodeSnippet copyText={executeQueryCodeSnippet} label="Java Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientJavaOverlay
