// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import CodeSnippet from 'src/shared/components/CodeSnippet'

// Constants
import {clientJSLibrary} from 'src/clientLibraries/constants'

const ClientJSOverlay: FunctionComponent<{}> = () => {
  const {
    name,
    url,
    initializeClientCodeSnippet,
    executeQueryCodeSnippet,
    writingDataLineProtocolCodeSnippet,
  } = clientJSLibrary

  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <br />
      <h5>Initialize the Client</h5>
      <CodeSnippet
        copyText={initializeClientCodeSnippet}
        label="JavaScript Code"
      />
      <h5>Write Data</h5>
      <CodeSnippet
        copyText={writingDataLineProtocolCodeSnippet}
        label="JavaScript Code"
      />
      <h5>Execute a Flux query</h5>
      <CodeSnippet copyText={executeQueryCodeSnippet} label="JavaScript Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientJSOverlay
