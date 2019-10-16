// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import CodeSnippet from 'src/shared/components/CodeSnippet'

// Constants
import {clientGoLibrary} from 'src/clientLibraries/constants'

const ClientGoOverlay: FunctionComponent<{}> = () => {
  const {
    name,
    url,
    initializeClientCodeSnippet,
    writeDataCodeSnippet,
  } = clientGoLibrary
  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <h5>Initialize the Client</h5>
      <CodeSnippet copyText={initializeClientCodeSnippet} label="Go Code" />
      <h5>Write Data</h5>
      <CodeSnippet copyText={writeDataCodeSnippet} label="Go Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientGoOverlay
