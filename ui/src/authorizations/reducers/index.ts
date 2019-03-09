// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState} from 'src/types'
import {Action} from 'src/authorizations/actions'
import {Authorization} from '@influxdata/influx'

const initialState = (): AuthorizationsState => ({
  status: RemoteDataState.NotStarted,
  list: [],
})

export interface AuthorizationsState {
  status: RemoteDataState
  list: Authorization[]
}

export const authorizationsReducer = (
  state: AuthorizationsState = initialState(),
  action: Action
): AuthorizationsState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_AUTHS': {
        const {status, list} = action.payload

        draftState.status = status

        if (list) {
          draftState.list = list
        }

        return
      }

      case 'ADD_AUTH': {
        const {authorization} = action.payload

        draftState.list.push(authorization)

        return
      }

      case 'EDIT_AUTH': {
        const {authorization} = action.payload
        const {list} = draftState

        draftState.list = list.map(l => {
          if (l.id === authorization.id) {
            return authorization
          }

          return l
        })

        return
      }

      case 'REMOVE_AUTH': {
        const {id} = action.payload
        const {list} = draftState
        const deleted = list.filter(l => {
          return l.id !== id
        })

        draftState.list = deleted
        return
      }
    }
  })
