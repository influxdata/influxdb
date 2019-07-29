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
  | ReturnType<typeof setCheck>
  | ReturnType<typeof removeCheck>
  | ReturnType<typeof setCurrentCheck>
  | ReturnType<typeof updateCurrentCheck>

export const setAllChecks = (status: RemoteDataState, checks?: Check[]) => ({
  type: 'SET_ALL_CHECKS' as 'SET_ALL_CHECKS',
  payload: {status, checks},
})

export const setCheck = (check: Check) => ({
  type: 'SET_CHECK' as 'SET_CHECK',
  payload: {check},
})

export const removeCheck = (checkID: string) => ({
  type: 'REMOVE_CHECK' as 'REMOVE_CHECK',
  payload: {checkID},
})

export const setCurrentCheck = (
  status: RemoteDataState,
  check?: Partial<Check>
) => ({
  type: 'SET_CURRENT_CHECK' as 'SET_CURRENT_CHECK',
  payload: {status, check},
})

export const updateCurrentCheck = (checkUpdate: Partial<Check>) => ({
  type: 'UPDATE_CURRENT_CHECK' as 'UPDATE_CURRENT_CHECK',
  payload: {status, checkUpdate},
})

export const getChecks = () => async (
  dispatch: Dispatch<Action | NotificationAction>,
  getState: GetState
) => {
  try {
    dispatch(setAllChecks(RemoteDataState.Loading))
    const {
      orgs: {
        org: {id: orgID},
      },
    } = getState()

    const checks = await client.checks.getAll(orgID)

    dispatch(setAllChecks(RemoteDataState.Done, checks))
  } catch (e) {
    console.error(e)
    dispatch(setAllChecks(RemoteDataState.Error))
    dispatch(notify(copy.getChecksFailed(e.message)))
  }
}

export const getCurrentCheck = (checkID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    dispatch(setCurrentCheck(RemoteDataState.Loading))

    const check = await client.checks.get(checkID)

    dispatch(setCurrentCheck(RemoteDataState.Done, check))
  } catch (e) {
    console.error(e)
    dispatch(setCurrentCheck(RemoteDataState.Error))
    dispatch(notify(copy.getCheckFailed(e.message)))
  }
}

export const createCheck = (check: Check) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    client.checks.create(check)
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.createCheckFailed(e.message)))
  }
}

export const updateCheck = (check: Partial<Check>) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const updatedCheck = await client.checks.update(check.id, check)
    dispatch(setCheck(updatedCheck))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.updateCheckFailed(e.message)))
  }
}

export const deleteCheck = (checkID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    await client.checks.delete(checkID)
    dispatch(removeCheck(checkID))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.deleteCheckFailed(e.message)))
  }
}
