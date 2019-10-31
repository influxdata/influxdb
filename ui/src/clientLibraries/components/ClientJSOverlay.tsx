// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'

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
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="JavaScript Code"
        defaults={{
          server: 'server',
          token: 'token',
        }}
      />
      <h5>Write Data</h5>
      <TemplatedCodeSnippet
        template={writingDataLineProtocolCodeSnippet}
        label="JavaScript Code"
        defaults={{
          org: 'orgID',
          bucket: 'bucketID',
        }}
      />
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="JavaScript Code"
        defaults={{
          org: 'orgID',
        }}
      />
    </ClientLibraryOverlay>
  )
}

export default ClientJSOverlay
