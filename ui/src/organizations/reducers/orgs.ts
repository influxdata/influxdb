// Libraries
import {get, omit} from 'lodash'

// Types
import {
  SET_ORGS,
  SET_ORG,
  ADD_ORG,
  REMOVE_ORG,
  Action,
  EDIT_ORG,
} from 'src/organizations/actions/creators'
import {RemoteDataState, ResourceState} from 'src/types'
import {combineReducers} from 'redux'

type OrgsState = ResourceState['orgs']

const org = (state: OrgsState['org'] = null, action: Action) => {
  switch (action.type) {
    case SET_ORG: {
      const {org} = action

      if (!org) {
        return state
      }

      return org
    }

    default:
      return state
  }
}

const byID = (state: OrgsState['byID'] = {}, action: Action) => {
  switch (action.type) {
    case SET_ORGS: {
      const {schema} = action

      if (!get(schema, 'entities.orgs')) {
        return state
      }

      return schema.entities.orgs
    }

    case ADD_ORG: {
      const {schema} = action

      if (!get(schema, 'entities.orgs')) {
        return state
      }

      const org = schema.entities[schema.result]
      return {...state, [action.schema.result]: org}
    }

    case REMOVE_ORG: {
      const {id} = action

      return omit(state, id)
    }

    case EDIT_ORG: {
      const {schema} = action
      const org = schema.entities[schema.result]
      return {...state, [action.schema.result]: org}
    }

    default:
      return state
  }
}

const allIDs = (state: OrgsState['allIDs'] = [], action: Action) => {
  switch (action.type) {
    case ADD_ORG: {
      const {schema} = action
      if (!get(schema, 'result')) {
        return state
      }

      return [...state, schema.result]
    }

    case SET_ORGS: {
      const {schema} = action
      if (!get(schema, 'result')) {
        return state
      }

      return [...schema.result]
    }

    case REMOVE_ORG: {
      return state.filter(id => id !== action.id)
    }

    default:
      return state
  }
}

const status = (
  state: OrgsState['status'] = RemoteDataState.NotStarted,
  action: Action
) => {
  switch (action.type) {
    case SET_ORGS: {
      const {status} = action
      return status
    }

    default:
      return state
  }
}

export default combineReducers({
  byID,
  allIDs,
  status,
  org,
})
