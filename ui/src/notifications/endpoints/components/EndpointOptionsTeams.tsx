// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {
  Input,
  FormElement,
  Panel,
  Grid,
  Columns,
  InputType,
} from '@influxdata/clockface'

interface Props {
  url: string
  secretURLSuffix: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const EndpointOptionsTeams: FC<Props> = ({url, secretURLSuffix, onChange}) => {
  return (
    <Panel>
      <Panel.Header>
        <h4>Teams Options</h4>
      </Panel.Header>
      <Panel.Body>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <FormElement label="Incoming Webhook URL">
                <Input
                  name="url"
                  value={url}
                  testID="teams-url"
                  onChange={onChange}
                />
              </FormElement>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <FormElement label="Webhook URL Secret Suffix">
                <Input
                  name="secretURLSuffix"
                  value={secretURLSuffix}
                  testID="teams-secretURLSuffix"
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

export default EndpointOptionsTeams
