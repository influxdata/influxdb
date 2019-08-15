// Types
import {NotificationEndpoint} from 'src/types'

export type Action =
  | {type: 'UPDATE_ENDPOINT'; endpoint: NotificationEndpoint}
  | {type: 'DELETE_ENDPOINT'; endpointID: string}

export type EndpointState = NotificationEndpoint

export const reducer = (state: EndpointState, action: Action) => {
  switch (action.type) {
    case 'UPDATE_ENDPOINT': {
      const {endpoint} = action
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
