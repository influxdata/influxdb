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

// Actions
import {
  notify,
  Action as NotificationAction,
} from 'src/shared/actions/notifications'
import {
  Action as TimeMachineAction,
  setActiveTimeMachine,
} from 'src/timeMachine/actions'
import {
  Action as AlertBuilderAction,
  setAlertBuilderCheck,
  setAlertBuilderCheckStatus,
  resetAlertBuilder,
} from 'src/alerting/actions/alertBuilder'
import {checkChecksLimits} from 'src/cloud/actions/limits'

// Types
import {
  Check,
  GetState,
  RemoteDataState,
  CheckViewProperties,
  Label,
  PostCheck,
  CheckPatch,
  ThresholdCheck,
  DeadmanCheck,
} from 'src/types'

// Utils
import {createView} from 'src/shared/utils/view'
import {getOrg} from 'src/organizations/selectors'

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
    const {id: orgID} = getOrg(getState())

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
  dispatch: Dispatch<
    TimeMachineAction | NotificationAction | AlertBuilderAction
  >,
  getState: GetState
) => {
  const org = getOrg(getState())
  try {
    dispatch(setAlertBuilderCheckStatus(RemoteDataState.Loading))

    const resp = await api.getCheck({checkID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const check = resp.data

    const view = createView<CheckViewProperties>(check.type)

    view.properties.queries = [check.query]

    dispatch(
      setActiveTimeMachine('alerting', {
        view,
        activeTab: check.type === 'custom' ? 'customCheckQuery' : 'alerting',
      })
    )
    dispatch(setAlertBuilderCheck(check))
  } catch (e) {
    console.error(e)
    dispatch(push(`/orgs/${org.id}/alerting`))
    dispatch(setAlertBuilderCheckStatus(RemoteDataState.Error))
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
      alertBuilder: {
        type,
        id,
        status,
        name,
        every,
        offset,
        tags,
        statusMessageTemplate,
        timeSince,
        reportZero,
        staleTime,
        level,
        thresholds,
      },
    } = state
    const {id: orgID} = getOrg(state)

    const {draftQueries} = getActiveTimeMachine(state)

    let check = {
      id,
      type,
      status,
      name,
      query: draftQueries[0],
      orgID,
    } as Check

    if (check.type === 'threshold') {
      check = {
        ...check,
        every,
        offset,
        statusMessageTemplate,
        tags,
        thresholds,
      } as ThresholdCheck
    } else if (check.type === 'deadman') {
      check = {
        ...check,
        every,
        level,
        offset,
        reportZero,
        staleTime,
        statusMessageTemplate,
        tags,
        timeSince,
      } as DeadmanCheck
    }

    const resp = check.id
      ? await updateCheckFromTimeMachine(check)
      : await api.postCheck({data: check})

    if (resp.status === 200 || resp.status === 201) {
      dispatch(setCheck(resp.data))
      dispatch(checkChecksLimits())

      dispatch(push(`/orgs/${orgID}/alerting`))
      dispatch(resetAlertBuilder())
    } else {
      throw new Error(resp.data.message)
    }
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.createCheckFailed(e.message)))
  }
}

export const updateCheckDisplayProperties = (
  checkID: string,
  update: CheckPatch
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  const resp = await api.patchCheck({checkID, data: update})

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
