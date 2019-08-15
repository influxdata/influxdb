// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Grid, Form, Panel, Input, TextArea} from '@influxdata/clockface'
import EndpointTypeDropdown from 'src/alerting/components/endpoints/EndpointTypeDropdown'

// Hooks
import {useEndpointReducer} from './EndpointOverlayProvider'

// Types
import {NotificationEndpointType} from 'src/types'

const EndpointOverlayContents: FC = () => {
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
                    testID="endpoint-description--textarea"
                    name="description"
                    placeholder="Optional"
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
              </Panel.Body>
            </Panel>
          </Grid.Column>
        </Grid.Row>
      </Form>
    </Grid>
  )
}

export default EndpointOverlayContents
