// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Input, FormElement, Panel, Grid, Columns} from '@influxdata/clockface'

interface Props {
  url: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const EndpointOptionsSlack: FC<Props> = ({url, onChange}) => {
  return (
    <Panel>
      <Panel.Header>
        <h4>Slack Options</h4>
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
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Panel.Body>
    </Panel>
  )
}

export default EndpointOptionsSlack
