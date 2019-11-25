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

// API
import {testExistingEndpoint} from 'src/alerting/actions/notifications/endpoints'

// Types
import {NotificationEndpoint, RemoteDataState} from 'src/types'

export interface Message {
  text: string
  remoteState: RemoteDataState
}
interface OwnProps {
  saveButtonText: string
  onSave: (endpoint: NotificationEndpoint) => Promise<void>
  onTest: (endpoint: NotificationEndpoint) => Promise<void>
  onCancel: () => void
  onSetMessage: (message: Message) => void
}

type Props = OwnProps

const EndpointOverlayFooter: FC<Props> = ({
  saveButtonText,
  onSave,
  onCancel,
  onSetMessage,
}) => {
  const endpoint = useEndpointState()
  const {pathname} = window.location

  const [saveStatus, setSaveStatus] = useState(RemoteDataState.NotStarted)
  const [testStatus, setTestStatus] = useState(RemoteDataState.NotStarted)

  const handleSave = async () => {
    if (saveStatus === RemoteDataState.Loading) {
      return
    }

    try {
      setSaveStatus(RemoteDataState.Loading)
      onSetMessage(null)

      await onSave(endpoint)
    } catch (e) {
      setSaveStatus(RemoteDataState.Error)
      onSetMessage({remoteState: RemoteDataState.Error, text: e.message})
    }
  }

  const handleTest = async () => {
    if (testStatus === RemoteDataState.Loading) {
      return
    }

    try {
      setTestStatus(RemoteDataState.Loading)
      onSetMessage(null)
      await testExistingEndpoint(endpoint)
      setTestStatus(RemoteDataState.Done)
      onSetMessage({
        text: `Test sent to ${endpoint.type}`,
        remoteState: RemoteDataState.Done,
      })
    } catch (e) {
      onSetMessage({text: e.message, remoteState: RemoteDataState.Error})
      setTestStatus(RemoteDataState.Error)
    }
  }

  const buttonStatus =
    saveStatus === RemoteDataState.Loading
      ? ComponentStatus.Loading
      : ComponentStatus.Default

  const testButtonStatus =
    testStatus === RemoteDataState.Loading
      ? ComponentStatus.Loading
      : ComponentStatus.Default

  return (
    <Overlay.Footer>
      <Button
        testID="endpoint-cancel--button"
        onClick={onCancel}
        text="Cancel"
      />
      {pathname.includes('/edit') && (
        <Button
          testID="endpoint-test--button"
          onClick={handleTest}
          text="Test Endpoint"
          color={ComponentColor.Secondary}
          status={testButtonStatus}
        />
      )}
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
