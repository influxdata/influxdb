// Libraries
import React, {FunctionComponent} from 'react'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'

// Constants
import {clientGoLibrary} from 'src/clientLibraries/constants'

interface Props {
  onDismiss: () => void
}

const ClientGoOverlay: FunctionComponent<Props> = ({onDismiss}) => {
  const {name, url} = clientGoLibrary

  return (
    <ClientLibraryOverlay
      title={`${name} Client Library`}
      onDismiss={onDismiss}
    >
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
    </ClientLibraryOverlay>
  )
}

export default ClientGoOverlay
