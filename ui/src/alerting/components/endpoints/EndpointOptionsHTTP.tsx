// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Input, FormElement, InputType, TextArea} from '@influxdata/clockface'
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
  contentTemplate,
  method,
  authMethod,
  onChangeParameter,
}) => {
  return (
    <>
      <FormElement label="URL">
        <Input name="url" value={url} onChange={onChange} required={true} />
      </FormElement>
      <FormElement label="HTTP method">
        <MethodTypeDropdown
          onSelectType={onChangeParameter('method')}
          selectedType={method}
        />
      </FormElement>
      <FormElement label="auth method">
        <AuthMethodTypeDropdown
          onSelectType={onChangeParameter('authMethod')}
          selectedType={authMethod}
        />
      </FormElement>
      {authMethod === 'bearer' && (
        <FormElement label="Token">
          <Input
            name="token"
            value={token}
            onChange={onChange}
            type={InputType.Password}
          />
        </FormElement>
      )}
      {authMethod === 'basic' && (
        <>
          <FormElement label="username">
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
          <FormElement label="password">
            <Input
              name="password"
              value={password}
              type={InputType.Password}
              onChange={onChange}
            />
          </FormElement>
        </>
      )}
      <FormElement label="Content Template">
        <TextArea
          rows={2}
          className="endpoint-description--textarea"
          name="contentTemplate"
          value={contentTemplate}
          onChange={onChange}
        />
      </FormElement>
    </>
  )
}

export default EndpointOptionsHTTP
