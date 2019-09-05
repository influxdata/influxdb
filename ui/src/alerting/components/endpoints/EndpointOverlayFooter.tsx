// Libraries
import React, {useState, FC} from 'react'

// Components
import {Button, ComponentColor, ComponentStatus} from '@influxdata/clockface'

// Hooks
import {useEndpointState} from './EndpointOverlayProvider'

// Types
import {NotificationEndpoint, RemoteDataState} from 'src/types'

interface Props {
  saveButtonText: string
  onSave: (endpoint: NotificationEndpoint) => Promise<void>
  onCancel: () => void
}

const EndpointOverlayFooter: FC<Props> = ({
  saveButtonText,
  onSave,
  onCancel,
}) => {
  const endpoint = useEndpointState()

  const [saveStatus, setSaveStatus] = useState(RemoteDataState.NotStarted)
  const [errorMessage, setErrorMessage] = useState<string>(null)

  const handleSave = async () => {
    if (saveStatus === RemoteDataState.Loading) {
      return
    }

    try {
      setSaveStatus(RemoteDataState.Loading)
      setErrorMessage(null)

      await onSave(endpoint)
    } catch (e) {
      setSaveStatus(RemoteDataState.Error)
      setErrorMessage(e.message)
    }
  }

  const buttonStatus =
    saveStatus === RemoteDataState.Loading
      ? ComponentStatus.Loading
      : ComponentStatus.Default

  return (
    <>
      {errorMessage && (
        <div className="endpoint-overlay-footer--error">{errorMessage}</div>
      )}
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
    </>
  )
}

export default EndpointOverlayFooter
