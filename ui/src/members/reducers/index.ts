// Libraries
import {get} from 'lodash'
import {combineReducers} from 'redux'

// Types
import {RemoteDataState, ResourceState} from 'src/types'
import {Action, SET_MEMBERS} from 'src/members/actions'

export type MembersState = ResourceState['members']

const byID = (state: MembersState['byID'] = {}, action: Action) => {
  switch (action.type) {
    case SET_MEMBERS: {
      const {normalized} = action

      if (get(normalized, 'entities.members')) {
        state = normalized.entities.members
      }

      return state
    }
    default:
      return state
  }
}

const allIDs = (state: MembersState['allIDs'] = [], action: Action) => {
  switch (action.type) {
    case SET_MEMBERS: {
      const {normalized} = action

      if (get(normalized, 'result')) {
        state = normalized.result
      }

      return state
    }
    default:
      return state
  }
}

const status = (
  state: MembersState['status'] = RemoteDataState.NotStarted,
  action: Action
) => {
  switch (action.type) {
    case SET_MEMBERS: {
      const {status} = action

      return status
    }
    default:
      return state
  }
}

export default combineReducers<MembersState>({
  byID,
  allIDs,
  status,
})
