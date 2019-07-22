// Types
import {RemoteDataState, Check} from 'src/types'
import {Action} from 'src/alerting/actions/checks'

export interface ChecksState {
  status: RemoteDataState
  list: Check[]
  current: {status: RemoteDataState; check: Check}
}

export const defaultChecksState: ChecksState = {
  status: RemoteDataState.NotStarted,
  list: [],
  current: {status: RemoteDataState.NotStarted, check: null},
}

export default (
  state: ChecksState = defaultChecksState,
  action: Action
): ChecksState => {
  switch (action.type) {
    case 'SET_CHECKS_STATUS':
      return {
        ...state,
        status: action.payload.status,
      }
    case 'SET_ALL_CHECKS':
      return {
        ...state,
        list: action.payload.checks,
        status: RemoteDataState.Done,
      }
    case 'SET_CHECK_STATUS':
      return {
        ...state,
        current: {...state.current, status: action.payload.status},
      }
    case 'SET_CHECK':
      return {
        ...state,
        current: {status: action.payload.status, check: action.payload.check},
      }

    default:
      return state
  }
}
