// Types
import {NotificationEndpoint} from 'src/types'
import {DEFAULT_ENDPOINT_URLS} from 'src/alerting/constants'

export type Action =
  | {type: 'UPDATE_ENDPOINT'; endpoint: NotificationEndpoint}
  | {type: 'UPDATE_ENDPOINT_TYPE'; endpoint: NotificationEndpoint}
  | {type: 'DELETE_ENDPOINT'; endpointID: string}

export type EndpointState = NotificationEndpoint

export const reducer = (state: EndpointState, action: Action) => {
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
            return {
              ...state,
              ...endpoint,
              clientURL: `${location.origin}/orgs/${
                endpoint.orgID
              }/alert-history`,
            }
          default:
            return {
              ...state,
              ...endpoint,
              url: DEFAULT_ENDPOINT_URLS[endpoint.type],
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
