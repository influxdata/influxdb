// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'

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
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="Go Code"
        defaults={{
          token: 'myToken',
          server: 'myHTTPInfluxAddress',
        }}
      />
      <h5>Write Data</h5>
      <TemplatedCodeSnippet
        template={writeDataCodeSnippet}
        label="Go Code"
        defaults={{
          bucket: 'my-awesome-bucket',
          org: 'my-very-awesome-org',
        }}
      />
    </ClientLibraryOverlay>
  )
}

export default ClientGoOverlay
