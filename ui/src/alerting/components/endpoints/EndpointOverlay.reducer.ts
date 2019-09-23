import {omit} from 'lodash'

// Types
import {NotificationEndpoint} from 'src/types'
import {DEFAULT_ENDPOINT_URLS} from 'src/alerting/constants'
import {NotificationEndpointBase} from 'src/client'

export type Action =
  | {type: 'UPDATE_ENDPOINT'; endpoint: NotificationEndpoint}
  | {type: 'UPDATE_ENDPOINT_TYPE'; endpoint: NotificationEndpoint}
  | {type: 'DELETE_ENDPOINT'; endpointID: string}

export type EndpointState = NotificationEndpoint

export const reducer = (
  state: EndpointState,
  action: Action
): EndpointState => {
  switch (action.type) {
    case 'UPDATE_ENDPOINT': {
      const {endpoint} = action
      return {...state, ...endpoint}
    }
    case 'UPDATE_ENDPOINT_TYPE': {
      const {endpoint} = action
      if (state.type != endpoint.type) {
        switch (endpoint.type) {
          case 'pagerduty':
            const pgBaseProps: NotificationEndpointBase = omit(endpoint, [
              'url',
              'token',
              'username',
              'password',
              'method',
              'authMethod',
              'contentTemplate',
              'headers',
            ])
            return {
              ...pgBaseProps,
              type: 'pagerduty',
              clientURL: `${location.origin}/orgs/${
                pgBaseProps.orgID
              }/alert-history`,
            }
          case 'http':
            const httpBaseProps: NotificationEndpointBase = omit(endpoint, [
              'clientURL',
              'routingKey',
            ])
            return {
              ...httpBaseProps,
              type: 'http',
              method: 'POST',
              authMethod: 'none',
              url: DEFAULT_ENDPOINT_URLS.http,
            }
          case 'slack':
            const slackBaseProps: NotificationEndpointBase = omit(endpoint, [
              'clientURL',
              'routingKey',
              'username',
              'password',
              'method',
              'authMethod',
              'contentTemplate',
              'headers',
            ])
            return {
              ...slackBaseProps,
              type: 'slack',
              url: DEFAULT_ENDPOINT_URLS.slack,
            }
        }
      }

      return {...state, ...endpoint}
    }
    case 'DELETE_ENDPOINT': {
      return state
    }

    default:
      const neverAction: never = action

      throw new Error(
        `Unhandled action "${
          (neverAction as any).type
        }" in EndpointsOverlay.reducer.ts`
      )
  }
}
