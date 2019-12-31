// Libraries
import {get, omit} from 'lodash'
import {combineReducers} from 'redux'

// Types
import {RemoteDataState, ResourceState} from 'src/types'
import {
  Action,
  SET_MEMBERS,
  ADD_MEMBER,
  REMOVE_MEMBER,
} from 'src/members/actions'

export type MembersState = ResourceState['members']

const byID = (state: MembersState['byID'] = {}, action: Action) => {
  switch (action.type) {
    case ADD_MEMBER:
    case SET_MEMBERS: {
      const {schema} = action

      if (!get(schema, 'entities.members')) {
        return state
      }

      return {...state, ...schema.entities.members}
    }

    case REMOVE_MEMBER: {
      const {id} = action

      return omit(state, id)
    }

    default:
      return state
  }
}

const allIDs = (state: MembersState['allIDs'] = [], action: Action) => {
  switch (action.type) {
    case ADD_MEMBER:
    case SET_MEMBERS: {
      const {schema} = action

      if (!get(schema, 'result')) {
        return state
      }

      return [...state, ...schema.result]
    }

    case REMOVE_MEMBER: {
      return state.filter(id => id !== action.id)
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
