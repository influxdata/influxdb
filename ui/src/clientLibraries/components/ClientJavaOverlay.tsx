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
      <br />
      <h5>Adding Dependency</h5>
      <p>
        <b>Build with Maven</b>
      </p>
      <CodeSnippet copyText={buildWithMavenCodeSnippet} label="Code" />
      <p>
        <b>Build with Gradle</b>
      </p>
      <CodeSnippet copyText={buildWithGradleCodeSnippet} label="Code" />
      <h5>Initializing the Client</h5>
      <CodeSnippet copyText={initializeClientCodeSnippet} label="Java Code" />
      <h5>Writing Data</h5>
      <p>Option 1: Example for writing using InfluxDB Line Protocol</p>
      <CodeSnippet
        copyText={writingDataLineProtocolCodeSnippet}
        label="Java Code"
      />
      <p>Option 2: Example for writing using a Data Point</p>
      <CodeSnippet copyText={writingDataPointCodeSnippet} label="Java Code" />
      <p>Option 3: Example for writing using a POJO and corresponding class</p>
      <CodeSnippet copyText={writingDataPojoCodeSnippet} label="Java Code" />
      <CodeSnippet copyText={pojoClassCodeSnippet} label="Java Code" />
      <h5>Example for executing a Flux query</h5>
      <CodeSnippet copyText={executeQueryCodeSnippet} label="Java Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientJavaOverlay
