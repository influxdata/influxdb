// Libraries
import {produce} from 'immer'
import {omit} from 'lodash'

// Types
import {RemoteDataState, Check, ThresholdCheck, DeadmanCheck} from 'src/types'
import {Action} from 'src/alerting/actions/checks'
import {DEFAULT_THRESHOLD_CHECK, DEFAULT_DEADMAN_CHECK} from '../constants'

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
        draftState.current.check = action.payload.check
        return

      case 'SET_CURRENT_CHECK_STATUS':
        draftState.current.status = action.payload.status
        return

      case 'UPDATE_CURRENT_CHECK':
        draftState.current.check = {
          ...draftState.current.check,
          ...action.payload.checkUpdate,
        } as Check

        return
      case 'CHANGE_CURRENT_CHECK_TYPE':
        const exCheck = draftState.current.check
        if (action.payload.type == 'deadman') {
          draftState.current.check = {
            ...DEFAULT_DEADMAN_CHECK,
            ...omit(exCheck, 'thresholds'),
          } as DeadmanCheck
        }
        if (action.payload.type == 'threshold') {
          draftState.current.check = {
            ...DEFAULT_THRESHOLD_CHECK,
            ...omit(exCheck, ['timeSince', 'reportZero', 'level']),
          } as ThresholdCheck
        }
        return
    }
  })
