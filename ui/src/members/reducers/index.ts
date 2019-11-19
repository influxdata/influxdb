// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState} from 'src/types'
import {Action} from 'src/members/actions'
import {Member} from 'src/types'

const initialState = (): MembersState => ({
  status: RemoteDataState.NotStarted,
  list: [],
})

export interface MembersState {
  status: RemoteDataState
  list: Member[]
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
    }
  })
