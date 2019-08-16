// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import EndpointOptionsSlack from './EndpointOptionsSlack'
import EndpointOptionsPagerDuty from './EndpointOptionsPagerDuty'
import EndpointOptionsWebhook from './EndpointOptionsWebhook'

// Types
import {NotificationEndpoint, WebhookNotificationEndpoint} from 'src/types'

interface Props {
  endpoint: NotificationEndpoint
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const EndpointOptions: FC<Props> = ({endpoint, onChange}) => {
  switch (endpoint.type) {
    case 'slack': {
      const {url, token} = endpoint
      return (
        <EndpointOptionsSlack url={url} token={token} onChange={onChange} />
      )
    }
    case 'pagerduty': {
      const {url, token} = endpoint
      return (
        <EndpointOptionsPagerDuty url={url} token={token} onChange={onChange} />
      )
    }
    case 'webhook': {
      // TODO(watts): add webhook type to the `Destination` dropdown
      // when webhooks are implemented in the backend.
      const {
        url,
        token,
        username,
        password,
        method,
        authmethod,
        contentTemplate,
      } = endpoint as WebhookNotificationEndpoint
      return (
        <EndpointOptionsWebhook
          url={url}
          token={token}
          username={username}
          password={password}
          method={method}
          authmethod={authmethod}
          contentTemplate={contentTemplate}
        />
      )
    }

    default:
      throw new Error(
        `Unknown endpoint type ${endpoint.type} for: ${JSON.stringify(
          endpoint,
          null,
          2
        )}`
      )
  }
}

export default EndpointOptions
