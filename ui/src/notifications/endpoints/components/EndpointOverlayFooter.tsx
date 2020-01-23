// Libraries
import React, {useState, FC} from 'react'

// Components
import {
  Button,
  ComponentColor,
  ComponentStatus,
  Overlay,
} from '@influxdata/clockface'

// Hooks
import {useEndpointState} from './EndpointOverlayProvider'

// Types
import {NotificationEndpoint, RemoteDataState} from 'src/types'

interface Props {
  saveButtonText: string
  onSave: (endpoint: NotificationEndpoint) => void
  onCancel: () => void
  onSetErrorMessage: (error: string) => void
}

const EndpointOverlayFooter: FC<Props> = ({
  saveButtonText,
  onSave,
  onCancel,
  onSetErrorMessage,
}) => {
  const endpoint = useEndpointState()

  const [saveStatus, setSaveStatus] = useState(RemoteDataState.NotStarted)

  const handleSave = () => {
    if (saveStatus === RemoteDataState.Loading) {
      return
    }

    try {
      setSaveStatus(RemoteDataState.Loading)
      onSetErrorMessage(null)

      onSave(endpoint)
    } catch (e) {
      setSaveStatus(RemoteDataState.Error)
      onSetErrorMessage(e.message)
    }
  }

  const buttonStatus =
    saveStatus === RemoteDataState.Loading
      ? ComponentStatus.Loading
      : ComponentStatus.Default

  return (
    <Overlay.Footer>
      <Button
        testID="endpoint-cancel--button"
        onClick={onCancel}
        text="Cancel"
      />
      <Button
        testID="endpoint-save--button"
        onClick={handleSave}
        text={saveButtonText}
        status={buttonStatus}
        color={ComponentColor.Primary}
      />
    </Overlay.Footer>
  )
}

export default EndpointOverlayFooter
