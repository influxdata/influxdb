import {omit} from 'lodash'

// Types
import {NotificationEndpoint, NotificationEndpointBase} from 'src/types'
import {DEFAULT_ENDPOINT_URLS} from 'src/alerting/constants'

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
        const baseProps: NotificationEndpointBase = omit(endpoint, [
          'url',
          'token',
          'username',
          'password',
          'method',
          'authMethod',
          'contentTemplate',
          'headers',
          'clientURL',
          'routingKey',
        ])

        switch (endpoint.type) {
          case 'pagerduty':
            return {
              ...baseProps,
              type: 'pagerduty',
              clientURL: `${location.origin}/orgs/${baseProps.orgID}/alert-history`,
              routingKey: '',
            }
          case 'http':
            return {
              ...baseProps,
              type: 'http',
              method: 'POST',
              authMethod: 'none',
              url: DEFAULT_ENDPOINT_URLS.http,
            }
          case 'slack':
            return {
              ...baseProps,
              type: 'slack',
              url: DEFAULT_ENDPOINT_URLS.slack,
              token: '',
            }
          case 'telegram':
            return {
              ...baseProps,
              type: 'telegram',
              token: '',
              channel: '',
            }
        }
      }
      return state
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
