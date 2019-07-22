// Libraries
import {client} from 'src/utils/api'
import {Dispatch} from 'react'

// Constants
import * as copy from 'src/shared/copy/notifications'

//Actions
import {
  notify,
  Action as NotificationAction,
} from 'src/shared/actions/notifications'

// Types
import {RemoteDataState} from '@influxdata/clockface'
import {Check, GetState} from 'src/types'

export type Action =
  | ReturnType<typeof setAllChecks>
  | ReturnType<typeof setChecksStatus>
  | ReturnType<typeof setCheck>
  | ReturnType<typeof setCheckStatus>

const setAllChecks = (status: RemoteDataState, checks?: Check[]) => ({
  type: 'SET_ALL_CHECKS' as 'SET_ALL_CHECKS',
  payload: {status, checks},
})

const setChecksStatus = (status: RemoteDataState) => ({
  type: 'SET_CHECKS_STATUS' as 'SET_CHECKS_STATUS',
  payload: {status},
})

const setCheck = (status: RemoteDataState, check?: Check) => ({
  type: 'SET_CHECK' as 'SET_CHECK',
  payload: {status, check},
})

const setCheckStatus = (status: RemoteDataState) => ({
  type: 'SET_CHECK_STATUS' as 'SET_CHECK_STATUS',
  payload: {status},
})

export const getChecks = () => async (
  dispatch: Dispatch<Action | NotificationAction>,
  getState: GetState
) => {
  try {
    dispatch(setChecksStatus(RemoteDataState.Loading))
    const {
      orgs: {
        org: {id: orgID},
      },
    } = getState()

    const checks = await client.checks.getAll(orgID)

    dispatch(setAllChecks(RemoteDataState.Done, checks))
  } catch (e) {
    console.error(e)
    dispatch(setChecksStatus(RemoteDataState.Error))
    dispatch(notify(copy.getChecksFailed(e.message)))
  }
}

export const getCheck = (checkID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    dispatch(setCheckStatus(RemoteDataState.Loading))

    const check = await client.checks.get(checkID)

    dispatch(setCheck(RemoteDataState.Done, check))
  } catch (e) {
    console.error(e)
    dispatch(setCheckStatus(RemoteDataState.Error))
    dispatch(notify(copy.getCheckFailed(e.message)))
  }
}
