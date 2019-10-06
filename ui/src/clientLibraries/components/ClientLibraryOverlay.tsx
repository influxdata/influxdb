// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Overlay} from '@influxdata/clockface'

interface Props {
  title: string
  onDismiss: () => void
}

const ClientLibraryOverlay: FunctionComponent<Props> = ({
  title,
  children,
  onDismiss,
}) => {
  return (
    <Overlay visible={true}>
      <Overlay.Container maxWidth={980}>
        <Overlay.Header title={title} onDismiss={onDismiss} />
        <Overlay.Body className="client-library-overlay">
          {children}
        </Overlay.Body>
      </Overlay.Container>
    </Overlay>
  )
}

export default ClientLibraryOverlay
