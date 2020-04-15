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
import EndpointOptions from 'src/notifications/endpoints/components/EndpointOptions'
import EndpointTypeDropdown from 'src/notifications/endpoints/components/EndpointTypeDropdown'
import EndpointOverlayFooter from 'src/notifications/endpoints/components/EndpointOverlayFooter'

// Hooks
import {useEndpointReducer} from './EndpointOverlayProvider'

// Types
import {NotificationEndpointType, NotificationEndpoint} from 'src/types'

interface Props {
  onSave: (endpoint: NotificationEndpoint) => void
  onCancel: () => void
  saveButtonText: string
}

const EndpointOverlayContents: FC<Props> = ({
  onSave,
  saveButtonText,
  onCancel,
}) => {
  const [endpoint, dispatch] = useEndpointReducer()
  const [errorMessage, setErrorMessage] = useState<string>(null)

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
      endpoint: {...endpoint, type} as NotificationEndpoint,
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
              {errorMessage && (
                <Alert
                  color={ComponentColor.Danger}
                  icon={IconFont.AlertTriangle}
                  style={{width: 'auto', marginTop: '8px'}}
                >
                  {errorMessage}
                </Alert>
              )}
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Overlay.Body>
      <EndpointOverlayFooter
        onSave={onSave}
        onCancel={onCancel}
        saveButtonText={saveButtonText}
        onSetErrorMessage={setErrorMessage}
      />
    </Form>
  )
}

export default EndpointOverlayContents
