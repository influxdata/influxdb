// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState} from 'src/types'
import {Action} from 'src/members/actions'
import {Member} from 'src/types'
import {User} from '@influxdata/influx'

export interface UsersMap {
  [userID: string]: User
}

const initialState = (): MembersState => ({
  status: RemoteDataState.NotStarted,
  list: [],
  users: {status: RemoteDataState.NotStarted, item: {}},
})

export interface MembersState {
  status: RemoteDataState
  list: Member[]
  users: {status: RemoteDataState; item: UsersMap}
}

export const membersReducer = (
  state: MembersState = initialState(),
  action: Action
): MembersState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_MEMBERS': {
        const {status, list} = action.payload

        draftState.status = status

        if (list) {
          draftState.list = list
        }

        return
      }

      case 'ADD_MEMBER': {
        const {member} = action.payload

        draftState.list.push(member)

        return
      }

      case 'REMOVE_MEMBER': {
        const {id} = action.payload
        const {list} = draftState

        const deleted = list.filter(l => {
          return l.id !== id
        })

        draftState.list = deleted
        return
      }

      case 'SET_USERS': {
        const {status, list} = action.payload

        draftState.users.status = status

        if (list) {
          draftState.users.item = list
        }

        return
      }
    }
  })
