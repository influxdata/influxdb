// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Grid, Form, Panel, Input, TextArea} from '@influxdata/clockface'
import EndpointOptions from 'src/alerting/components/endpoints/EndpointOptions'
import EndpointTypeDropdown from 'src/alerting/components/endpoints/EndpointTypeDropdown'
import EndpointOverlayFooter from 'src/alerting/components/endpoints/EndpointOverlayFooter'

// Hooks
import {useEndpointReducer} from './EndpointOverlayProvider'

// Types
import {NotificationEndpointType, NotificationEndpoint} from 'src/types'

interface Props {
  onSave: (endpoint: NotificationEndpoint) => Promise<void>
  saveButtonText: string
}

const EndpointOverlayContents: FC<Props> = ({onSave, saveButtonText}) => {
  const [endpoint, dispatch] = useEndpointReducer()
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
      type: 'UPDATE_ENDPOINT',
      endpoint: {...endpoint, type},
    })
  }

  return (
    <Grid>
      <Form>
        <Grid.Row>
          <Grid.Column>
            <Panel>
              <Panel.Body>
                <Form.Element label="Name">
                  <Input
                    testID="endpoint-name--input"
                    placeholder="Name this endpoint"
                    value={endpoint.name}
                    name="name"
                    onChange={handleChange}
                  />
                </Form.Element>
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
                <Form.Element label="Destination">
                  <EndpointTypeDropdown
                    onSelectType={handleSelectType}
                    selectedType={endpoint.type}
                  />
                </Form.Element>
                <EndpointOptions
                  endpoint={endpoint}
                  onChange={handleChange}
                  onChangeParameter={handleChangeParameter}
                />
                <EndpointOverlayFooter
                  onSave={onSave}
                  saveButtonText={saveButtonText}
                />
              </Panel.Body>
            </Panel>
          </Grid.Column>
        </Grid.Row>
      </Form>
    </Grid>
  )
}

export default EndpointOverlayContents
