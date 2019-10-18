// Libraries
import {Dispatch} from 'react'
import {push} from 'react-router-redux'

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

  const checkWithOrg = {...check, query: draftQueries[0], orgID} as Check

  const resp = check.id
    ? await api.patchCheck({checkID: check.id, data: checkWithOrg})
    : await api.postCheck({data: checkWithOrg})

  if (resp.status === 201 || resp.status === 200) {
    dispatch(setCheck(resp.data))
    dispatch(checkChecksLimits())
  } else {
    throw new Error(resp.data.message)
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

    const resp = await api.postCheck({data: {...check, name: clonedName}})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(setCheck(resp.data))
    dispatch(checkChecksLimits())

    // add labels
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.createCheckFailed(error.message)))
  }
}

export const selectCheckEvery = (every: string) => dispatch => {
  dispatch(updateTimeMachineCheck({every}))
  dispatch(executeQueries())
}
