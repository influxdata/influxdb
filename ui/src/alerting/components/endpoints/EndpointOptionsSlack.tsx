// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {
  Input,
  FormElement,
  InputType,
  Panel,
  Grid,
  Columns,
} from '@influxdata/clockface'

interface Props {
  url: string
  token: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const EndpointOptionsSlack: FC<Props> = ({url, token, onChange}) => {
  return (
    <Panel>
      <Panel.Header>
        <Panel.Title>Slack Options</Panel.Title>
      </Panel.Header>
      <Panel.Body>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <FormElement label="URL">
                <Input
                  name="url"
                  value={url}
                  testID="slack-url"
                  onChange={onChange}
                />
              </FormElement>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <FormElement label="Token">
                <Input
                  name="token"
                  value={token}
                  testID="slack-token"
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

export default EndpointOptionsSlack
