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
import MethodTypeDropdown from 'src/alerting/components/endpoints/MethodTypeDropdown'
import AuthMethodTypeDropdown from 'src/alerting/components/endpoints/AuthMethodTypeDropdown'

// Types
import {HTTPNotificationEndpoint} from 'src/types'

interface Props {
  url: string
  token?: string
  username?: string
  password?: string
  method?: HTTPNotificationEndpoint['method']
  authMethod?: HTTPNotificationEndpoint['authMethod']
  contentTemplate: string
  onChange: (e: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => void
  onChangeParameter: (key: string) => (value: string) => void
}

const EndpointOptionsHTTP: FC<Props> = ({
  url,
  onChange,
  token,
  username,
  password,
  method,
  authMethod,
  onChangeParameter,
}) => {
  return (
    <Panel>
      <Panel.Header>
        <Panel.Title>HTTP Options</Panel.Title>
      </Panel.Header>
      <Panel.Body>
        <Grid>
          <Grid.Row>
            <Grid.Column widthSM={Columns.Six}>
              <FormElement label="HTTP Method">
                <MethodTypeDropdown
                  onSelectType={onChangeParameter('method')}
                  selectedType={method}
                />
              </FormElement>
            </Grid.Column>
            <Grid.Column widthSM={Columns.Six}>
              <FormElement label="Auth Method">
                <AuthMethodTypeDropdown
                  onSelectType={onChangeParameter('authMethod')}
                  selectedType={authMethod}
                />
              </FormElement>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <FormElement label="URL">
                <Input
                  name="url"
                  value={url}
                  onChange={onChange}
                  required={true}
                />
              </FormElement>
            </Grid.Column>

            {authMethod === 'bearer' && (
              <Grid.Column widthXS={Columns.Twelve}>
                <FormElement label="Token">
                  <Input
                    name="token"
                    value={token}
                    onChange={onChange}
                    type={InputType.Password}
                  />
                </FormElement>
              </Grid.Column>
            )}
            {authMethod === 'basic' && (
              <>
                <Grid.Column widthSM={Columns.Six}>
                  <FormElement label="Username">
                    <Input
                      name="username"
                      value={username}
                      onChange={onChange}
                      type={
                        username && username.includes('secret: ')
                          ? InputType.Password
                          : InputType.Text
                      }
                    />
                  </FormElement>
                </Grid.Column>
                <Grid.Column widthSM={Columns.Six}>
                  <FormElement label="Password">
                    <Input
                      name="password"
                      value={password}
                      type={InputType.Password}
                      onChange={onChange}
                    />
                  </FormElement>
                </Grid.Column>
              </>
            )}
          </Grid.Row>
        </Grid>
      </Panel.Body>
    </Panel>
  )
}

export default EndpointOptionsHTTP
