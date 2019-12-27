// Libraries
import {Dispatch} from 'react'
import {push} from 'react-router-redux'
import {get} from 'lodash'

// Constants
import * as copy from 'src/shared/copy/notifications'

// APIs
import * as api from 'src/client'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {incrementCloneName} from 'src/utils/naming'

//Actions
import {
  notify,
  Action as NotificationAction,
} from 'src/shared/actions/notifications'
import {
  Action as TimeMachineAction,
  setActiveTimeMachine,
  updateTimeMachineCheck,
  setCheckStatus,
  setTimeMachineCheck,
} from 'src/timeMachine/actions'
import {executeQueries} from 'src/timeMachine/actions/queries'
import {checkChecksLimits} from 'src/cloud/actions/limits'

// Types
import {
  Check,
  GetState,
  RemoteDataState,
  CheckViewProperties,
  Label,
  PostCheck,
} from 'src/types'
import {createView} from 'src/shared/utils/view'

export type Action =
  | ReturnType<typeof setAllChecks>
  | ReturnType<typeof setCheck>
  | ReturnType<typeof removeCheck>
  | ReturnType<typeof addLabelToCheck>
  | ReturnType<typeof removeLabelFromCheck>

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

export const addLabelToCheck = (checkID: string, label: Label) => ({
  type: 'ADD_LABEL_TO_CHECK' as 'ADD_LABEL_TO_CHECK',
  payload: {checkID, label},
})

export const removeLabelFromCheck = (checkID: string, label: Label) => ({
  type: 'REMOVE_LABEL_FROM_CHECK' as 'REMOVE_LABEL_FROM_CHECK',
  payload: {checkID, label},
})

export const getChecks = () => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkChecksLimits>
  >,
  getState: GetState
) => {
  try {
    dispatch(setAllChecks(RemoteDataState.Loading))
    const {
      orgs: {
        org: {id: orgID},
      },
    } = getState()

    const resp = await api.getChecks({query: {orgID}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(setAllChecks(RemoteDataState.Done, resp.data.checks))
    dispatch(checkChecksLimits())
  } catch (e) {
    console.error(e)
    dispatch(setAllChecks(RemoteDataState.Error))
    dispatch(notify(copy.getChecksFailed(e.message)))
  }
}

export const getCheckForTimeMachine = (checkID: string) => async (
  dispatch: Dispatch<TimeMachineAction | NotificationAction>,
  getState: GetState
) => {
  const {
    orgs: {org},
  } = getState()
  try {
    dispatch(setCheckStatus(RemoteDataState.Loading))

    const resp = await api.getCheck({checkID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const check = resp.data

    const view = createView<CheckViewProperties>(check.type)
    // todo: when check has own view get view here until then:
    view.properties.queries = [check.query]

    dispatch(
      setActiveTimeMachine('alerting', {
        view,
        activeTab: 'alerting',
        alerting: {check, checkStatus: RemoteDataState.Done},
      })
    )
  } catch (e) {
    console.error(e)
    dispatch(push(`/orgs/${org.id}/alerting`))
    dispatch(setCheckStatus(RemoteDataState.Error))
    dispatch(notify(copy.getCheckFailed(e.message)))
  }
}

export const saveCheckFromTimeMachine = () => async (
  dispatch: Dispatch<any>,
  getState: GetState
) => {
  try {
    const state = getState()
    const {
      orgs: {
        org: {id: orgID},
      },
    } = state

    const {
      draftQueries,
      alerting: {check},
    } = getActiveTimeMachine(state)

    const labels = get(check, 'labels', []) as Label[]

    const checkWithOrg = {
      ...check,
      query: draftQueries[0],
      orgID,
      labels: labels.map(l => l.id),
    } as PostCheck

    const resp = check.id
      ? await updateCheckFromTimeMachine(checkWithOrg)
      : await api.postCheck({data: checkWithOrg})

    if (resp.status === 200 || resp.status === 201) {
      dispatch(setCheck(resp.data))
      dispatch(checkChecksLimits())

      dispatch(push(`/orgs/${orgID}/alerting`))
      dispatch(setTimeMachineCheck(RemoteDataState.NotStarted, null))
    } else {
      throw new Error(resp.data.message)
    }
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.createCheckFailed(e.message)))
  }
}

export const updateCheck = (check: Partial<Check>) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  const resp = await api.putCheck({checkID: check.id, data: check as Check})
  if (resp.status === 200) {
    dispatch(setCheck(resp.data))
  } else {
    throw new Error(resp.data.message)
  }
  dispatch(setCheck(resp.data))
}

const updateCheckFromTimeMachine = async (check: Check) => {
  // todo: refactor after https://github.com/influxdata/influxdb/issues/16317
  const getCheckResponse = await api.getCheck({checkID: check.id})

  if (getCheckResponse.status !== 200) {
    throw new Error(getCheckResponse.data.message)
  }

  return api.putCheck({
    checkID: check.id,
    data: {...check, ownerID: getCheckResponse.data.ownerID},
  })
}

export const deleteCheck = (checkID: string) => async (
  dispatch: Dispatch<any>
) => {
  try {
    const resp = await api.deleteCheck({checkID})

    if (resp.status === 204) {
      dispatch(removeCheck(checkID))
    } else {
      throw new Error(resp.data.message)
    }

    dispatch(removeCheck(checkID))
    dispatch(checkChecksLimits())
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.deleteCheckFailed(e.message)))
  }
}

export const addCheckLabel = (checkID: string, label: Label) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.postChecksLabel({checkID, data: {labelID: label.id}})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(addLabelToCheck(checkID, label))
  } catch (e) {
    console.error(e)
  }
}

export const deleteCheckLabel = (checkID: string, label: Label) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.deleteChecksLabel({
      checkID,
      labelID: label.id,
    })

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeLabelFromCheck(checkID, label))
  } catch (e) {
    console.error(e)
  }
}

export const cloneCheck = (check: Check) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkChecksLimits>
  >,
  getState: GetState
): Promise<void> => {
  try {
    const {
      checks: {list},
    } = getState()

    const allCheckNames = list.map(c => c.name)

    const clonedName = incrementCloneName(allCheckNames, check.name)
    const labels = get(check, 'labels', []) as Label[]
    const data = {
      ...check,
      name: clonedName,
      labels: labels.map(l => l.id),
    } as PostCheck
    const resp = await api.postCheck({data})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(setCheck(resp.data))
    dispatch(checkChecksLimits())
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.createCheckFailed(error.message)))
  }
}

export const selectCheckEvery = (every: string) => dispatch => {
  dispatch(updateTimeMachineCheck({every}))
  dispatch(executeQueries())
}
