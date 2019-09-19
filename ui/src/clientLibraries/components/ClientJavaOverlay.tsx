// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'

// Constants
import {clientJavaLibrary} from 'src/clientLibraries/constants'

const ClientJavaOverlay: FunctionComponent<{}> = () => {
  const {name, url} = clientJavaLibrary

  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
    </ClientLibraryOverlay>
  )
}

export default ClientJavaOverlay
