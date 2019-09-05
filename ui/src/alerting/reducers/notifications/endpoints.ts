// Libraries
import produce from 'immer'

// Types
import {NotificationEndpoint} from 'src/types'
import {RemoteDataState} from '@influxdata/clockface'
import {Action} from 'src/alerting/actions/notifications/endpoints'

export interface NotificationEndpointsState {
  status: RemoteDataState
  list: NotificationEndpoint[]
}

const initialState = {
  status: RemoteDataState.NotStarted,
  list: [],
}

type State = NotificationEndpointsState

export default (
  state: State = initialState,
  action: Action
): NotificationEndpointsState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_ALL_ENDPOINTS': {
        const {status, endpoints} = action

        if (endpoints) {
          draftState.list = endpoints
        }

        draftState.status = status

        return
      }

      case 'SET_ENDPOINT': {
        const {endpoint} = action
        const index = state.list.findIndex(ep => ep.id === endpoint.id)

        if (index === -1) {
          draftState.list.push(endpoint)
          return
        }

        draftState.list[index] = endpoint
        return
      }

      case 'REMOVE_ENDPOINT': {
        const {endpointID} = action

        draftState.list = state.list.filter(ep => ep.id !== endpointID)
        return
      }
      case 'ADD_LABEL_TO_ENDPOINT': {
        draftState.list = draftState.list.map(e => {
          if (e.id === action.endpointID) {
            e.labels = [...e.labels, action.label]
          }
          return e
        })
        return
      }
      case 'REMOVE_LABEL_FROM_ENDPOINT': {
        draftState.list = draftState.list.map(e => {
          if (e.id === action.endpointID) {
            e.labels = e.labels.filter(label => label.id !== action.label.id)
          }
          return e
        })
        return
      }
    }
  })
