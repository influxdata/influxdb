// Libraries
import React, {FC} from 'react'

// Components
import {Input, FormElement, InputType} from '@influxdata/clockface'

// Types
import {WebhookNotificationEndpoint} from 'src/types'

interface Props {
  url: string
  token?: string
  username?: string
  password?: string
  method?: WebhookNotificationEndpoint['method']
  authmethod?: WebhookNotificationEndpoint['authmethod']
  contentTemplate: string
}

const EndpointOptionsWebhook: FC<Props> = ({
  url,
  token,
  username,
  password,
  contentTemplate,
}) => {
  return (
    <>
      <FormElement label="URL">
        <Input name="url" value={url} />
      </FormElement>
      <FormElement label="Token">
        <Input name="token" value={token} />
      </FormElement>
      <FormElement label="username">
        <Input name="username" value={username} />
      </FormElement>
      <FormElement label="password">
        <Input name="password" value={password} type={InputType.Password} />
      </FormElement>
      {/** add dropdowns for method and authmethod */}
      <FormElement label="Content Template">
        <Input name="contentTemplate" value={contentTemplate} />
      </FormElement>
    </>
  )
}

export default EndpointOptionsWebhook
