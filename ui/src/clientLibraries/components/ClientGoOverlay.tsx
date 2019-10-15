// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'

// Constants
import {clientGoLibrary} from 'src/clientLibraries/constants'
import CodeSnippet from 'src/shared/components/CodeSnippet'

const ClientGoOverlay: FunctionComponent<{}> = () => {
  const {
    name,
    url, 
    initializeClientCodeSnippet, 
    writeDataCodeSnippet
  } = clientGoLibrary
  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <br />
      <h5>Initializing the Client</h5>
      <CodeSnippet copyText={initializeClientCodeSnippet} label="Code" />
      <h5>Writing Data</h5>
      <CodeSnippet copyText={writeDataCodeSnippet} label="Code" />
    </ClientLibraryOverlay>
  )
}

export default ClientGoOverlay
