// Libraries
import React, {FC, ChangeEvent, useState} from 'react'

// Components
import {
  Grid,
  Form,
  Input,
  TextArea,
  Overlay,
  Columns,
  Alert,
  ComponentColor,
  IconFont,
} from '@influxdata/clockface'
import EndpointOptions from 'src/alerting/components/endpoints/EndpointOptions'
import EndpointTypeDropdown from 'src/alerting/components/endpoints/EndpointTypeDropdown'
import EndpointOverlayFooter, {
  Message,
} from 'src/alerting/components/endpoints/EndpointOverlayFooter'

// Hooks
import {useEndpointReducer} from './EndpointOverlayProvider'

// Types
import {NotificationEndpointType, NotificationEndpoint} from 'src/types'

interface Props {
  saveButtonText: string
  onSave: (endpoint: NotificationEndpoint) => Promise<void>
  onTest: (endpoint: NotificationEndpoint) => Promise<void>
  onCancel: () => void
}

const EndpointOverlayContents: FC<Props> = ({
  onSave,
  onTest,
  saveButtonText,
  onCancel,
}) => {
  const [endpoint, dispatch] = useEndpointReducer()
  const [message, setMessage] = useState<Message>(null)

  const handleChange = (
    e: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const {name, value} = e.target
    dispatch({
      type: 'UPDATE_ENDPOINT',
      endpoint: {...endpoint, [name]: value},
    })
  }

  const handleChangeParameter = (key: string) => (value: string) => {
    dispatch({
      type: 'UPDATE_ENDPOINT',
      endpoint: {...endpoint, [key]: value},
    })
  }

  const handleSelectType = (type: NotificationEndpointType) => {
    dispatch({
      type: 'UPDATE_ENDPOINT_TYPE',
      endpoint: {...endpoint, type},
    })
  }

  return (
    <Form>
      <Overlay.Body>
        <Grid>
          <Grid.Row>
            <Grid.Column widthSM={Columns.Six}>
              <Form.Element label="Destination">
                <EndpointTypeDropdown
                  onSelectType={handleSelectType}
                  selectedType={endpoint.type}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthSM={Columns.Six}>
              <Form.Element label="Name">
                <Input
                  testID="endpoint-name--input"
                  placeholder="Name this endpoint"
                  value={endpoint.name}
                  name="name"
                  onChange={handleChange}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthSM={Columns.Twelve}>
              <Form.Element label="Description">
                <TextArea
                  rows={1}
                  className="endpoint-description--textarea"
                  testID="endpoint-description--textarea"
                  name="description"
                  value={endpoint.description}
                  onChange={handleChange}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthSM={Columns.Twelve}>
              <EndpointOptions
                endpoint={endpoint}
                onChange={handleChange}
                onChangeParameter={handleChangeParameter}
              />
            </Grid.Column>
            <Grid.Column
              style={{
                display: 'flex',
                justifyContent: 'center',
                minHeight: '43px',
              }}
            >
              {message && (
                <Alert
                  color={
                    message.remoteState === 'Done'
                      ? ComponentColor.Success
                      : ComponentColor.Danger
                  }
                  icon={
                    message.remoteState === 'Done'
                      ? IconFont.Checkmark
                      : IconFont.AlertTriangle
                  }
                  style={{width: 'auto', marginTop: '8px'}}
                >
                  {message.text}
                </Alert>
              )}
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Overlay.Body>
      <EndpointOverlayFooter
        onSave={onSave}
        onTest={onTest}
        onCancel={onCancel}
        saveButtonText={saveButtonText}
        onSetMessage={setMessage}
      />
    </Form>
  )
}

export default EndpointOverlayContents
