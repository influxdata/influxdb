// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState, Check} from 'src/types'
import {Action} from 'src/alerting/actions/checks'

export interface ChecksState {
  status: RemoteDataState
  list: Check[]
  current: {status: RemoteDataState; check: Partial<Check>}
}

export const defaultChecksState: ChecksState = {
  status: RemoteDataState.NotStarted,
  list: [],
  current: {status: RemoteDataState.NotStarted, check: null},
}

export default (
  state: ChecksState = defaultChecksState,
  action: Action
): ChecksState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_ALL_CHECKS':
        const {status, checks} = action.payload
        draftState.status = status
        if (checks) {
          draftState.list = checks
        }
        return

      case 'SET_CHECK':
        const newCheck = action.payload.check
        const checkIndex = state.list.findIndex(c => c.id == newCheck.id)

        if (checkIndex == -1) {
          draftState.list.push(newCheck)
        } else {
          draftState.list[checkIndex] = newCheck
        }
        return

      case 'REMOVE_CHECK':
        const {checkID} = action.payload
        draftState.list = draftState.list.filter(c => c.id != checkID)
        return

      case 'SET_CURRENT_CHECK':
        draftState.current.status = action.payload.status
        if (action.payload.check) {
          draftState.current.check = action.payload.check
        }
        return
      case 'UPDATE_CURRENT_CHECK':
        draftState.current.check = {
          ...draftState.current.check,
          ...action.payload.checkUpdate,
        } as Check

        return
    }
  })
