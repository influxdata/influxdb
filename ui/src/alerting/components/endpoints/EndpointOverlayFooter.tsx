// Libraries
import React, {useState, FC} from 'react'

// Components
import {
  Form,
  Grid,
  Button,
  Columns,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

// Hooks
import {useEndpointState} from './EndpointOverlayProvider'

// Types
import {NotificationEndpoint, RemoteDataState} from 'src/types'

interface Props {
  saveButtonText: string
  onSave: (endpoint: NotificationEndpoint) => Promise<void>
}

const EndpointOverlayFooter: FC<Props> = ({saveButtonText, onSave}) => {
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
      <Grid.Row>
        <Grid.Column widthXS={Columns.Twelve}>
          {errorMessage && (
            <div className="endpoint-overlay-footer--error">{errorMessage}</div>
          )}
          <Form.Footer className="endpoint-overlay-footer">
            <Button
              testID="endpoint-save--button"
              onClick={handleSave}
              text={saveButtonText}
              status={buttonStatus}
              color={ComponentColor.Primary}
            />
          </Form.Footer>
        </Grid.Column>
      </Grid.Row>
    </>
  )
}

export default EndpointOverlayFooter
