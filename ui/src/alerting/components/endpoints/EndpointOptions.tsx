// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import EndpointOptionsSlack from './EndpointOptionsSlack'
import EndpointOptionsPagerDuty from './EndpointOptionsPagerDuty'
import EndpointOptionsHTTP from './EndpointOptionsHTTP'

// Types
import {
  NotificationEndpoint,
  SlackNotificationEndpoint,
  PagerDutyNotificationEndpoint,
  HTTPNotificationEndpoint,
} from 'src/types'

interface Props {
  endpoint: NotificationEndpoint
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const EndpointOptions: FC<Props> = ({endpoint, onChange}) => {
  switch (endpoint.type) {
    case 'slack': {
      const {url, token} = endpoint as SlackNotificationEndpoint
      return (
        <EndpointOptionsSlack url={url} token={token} onChange={onChange} />
      )
    }
    case 'pagerduty': {
      const {url, routingKey} = endpoint as PagerDutyNotificationEndpoint
      return (
        <EndpointOptionsPagerDuty
          url={url}
          routingKey={routingKey}
          onChange={onChange}
        />
      )
    }
    case 'http': {
      // TODO(watts): add webhook type to the `Destination` dropdown
      // when webhooks are implemented in the backend.
      const {
        url,
        token,
        username,
        password,
        method,
        authMethod,
        contentTemplate,
      } = endpoint as HTTPNotificationEndpoint
      return (
        <EndpointOptionsHTTP
          url={url}
          token={token}
          username={username}
          password={password}
          method={method}
          authMethod={authMethod}
          contentTemplate={contentTemplate}
        />
      )
    }

    default:
      throw new Error(
        `Unknown endpoint type for endpoint: ${JSON.stringify(
          endpoint,
          null,
          2
        )}`
      )
  }
}

export default EndpointOptions
