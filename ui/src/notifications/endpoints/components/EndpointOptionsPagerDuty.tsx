// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Input, FormElement, InputType, Panel, Grid} from '@influxdata/clockface'

interface Props {
  clientURL: string
  routingKey: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const EndpointOptionsPagerDuty: FC<Props> = ({
  clientURL,
  routingKey,
  onChange,
}) => {
  return (
    <Panel>
      <Panel.Header>
        <h4>Pagerduty Options</h4>
      </Panel.Header>
      <Panel.Body>
        <Grid>
          <Grid.Row>
            <Grid.Column>
              <FormElement label="Client URL">
                <Input
                  name="clientURL"
                  value={clientURL}
                  testID="pagerduty-url"
                  onChange={onChange}
                />
              </FormElement>
            </Grid.Column>
            <Grid.Column>
              <FormElement label="Routing Key">
                <Input
                  name="routingKey"
                  value={routingKey}
                  testID="pagerduty-routing-key"
                  onChange={onChange}
                  type={InputType.Password}
                />
              </FormElement>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Panel.Body>
    </Panel>
  )
}

export default EndpointOptionsPagerDuty
