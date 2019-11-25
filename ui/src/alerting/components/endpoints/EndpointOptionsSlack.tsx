// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Input, FormElement, Panel, Grid, Columns} from '@influxdata/clockface'

interface Props {
  url: string
  testChannel: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const EndpointOptionsSlack: FC<Props> = ({url, testChannel, onChange}) => {
  const {pathname} = window.location
  return (
    <Panel>
      <Panel.Header>
        <Panel.Title>Slack Options</Panel.Title>
      </Panel.Header>
      <Panel.Body>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <FormElement label="Incoming Webhook URL">
                <Input
                  name="url"
                  value={url}
                  testID="slack-url"
                  onChange={onChange}
                />
              </FormElement>
              {pathname.includes('/edit') && (
                <FormElement label="Test Channel">
                  <Input
                    name="testChannel"
                    value={testChannel}
                    testID="testChannel--input"
                    onChange={onChange}
                  />
                </FormElement>
              )}
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Panel.Body>
    </Panel>
  )
}

export default EndpointOptionsSlack
