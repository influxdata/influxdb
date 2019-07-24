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
    case 'SET_ALL_CHECKS':
      if (action.payload.checks) {
        return {
          ...state,
          list: action.payload.checks,
          status: action.payload.status,
        }
      }
      return {
        ...state,
        status: action.payload.status,
      }
    case 'SET_CHECK':
      const newCheck = action.payload.check
      const checkIndex = state.list.findIndex(c => c.id == newCheck.id)

      let updatedList = state.list
      if (checkIndex == -1) {
        updatedList = [...updatedList, newCheck]
      } else {
        updatedList[checkIndex] = newCheck
      }

      return {
        ...state,
        list: updatedList,
      }
    case 'SET_CURRENT_CHECK':
      if (action.payload.check) {
        return {
          ...state,
          current: {check: action.payload.check, status: action.payload.status},
        }
      }
      return {
        ...state,
        current: {...state.current, status: action.payload.status},
      }
    case 'REMOVE_CHECK':
      const list = state.list.filter(c => c.id != action.payload.checkID)
      return {
        ...state,
        list,
      }
    default:
      return state
  }
}
