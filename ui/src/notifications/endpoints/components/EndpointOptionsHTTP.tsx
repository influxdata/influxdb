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
import MethodTypeDropdown from 'src/notifications/endpoints/components/MethodTypeDropdown'
import AuthMethodTypeDropdown from 'src/notifications/endpoints/components/AuthMethodTypeDropdown'

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
        <h4>HTTP Options</h4>
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
                  testID="http-url"
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
                    testID="http-bearer-token"
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
                      testID="http-username"
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
                      testID="http-password"
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
