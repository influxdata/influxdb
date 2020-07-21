import {Actions, SET_ME} from 'src/shared/actions/me'
import {RemoteDataState} from 'src/types'

export interface MeLinks {
  self: string
  log: string
}

export interface MeState {
  id: string
  name: string
  links: MeLinks
}

export interface MeReducerState {
  status: RemoteDataState
  resource: MeState
}

const defaultState: MeReducerState = {
  status: RemoteDataState.NotStarted,
  resource: {
    id: '',
    name: '',
    links: {
      self: '',
      log: '',
    },
  },
}

export default (state = defaultState, action: Actions): MeReducerState => {
  switch (action.type) {
    case SET_ME:
      return {
        ...state,
        status: action.payload.status,
        resource: {
          ...state.resource,
          ...action.payload.me,
        },
      }
    default:
      return state
  }
}
