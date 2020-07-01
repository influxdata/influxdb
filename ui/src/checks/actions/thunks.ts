// Libraries
import {Dispatch} from 'react'
import {push, RouterAction} from 'connected-react-router'
import {normalize} from 'normalizr'

// Constants
import * as copy from 'src/shared/copy/notifications'

// APIs
import * as api from 'src/client'

// Schemas
import {checkSchema, arrayOfChecks} from 'src/schemas/checks'

// Utils
import {incrementCloneName} from 'src/utils/naming'
import {reportError} from 'src/shared/utils/errors'
import {createView} from 'src/views/helpers'
import {getOrg} from 'src/organizations/selectors'
import {toPostCheck, builderToPostCheck} from 'src/checks/utils'
import {getAll, getStatus} from 'src/resources/selectors'
import {getErrorMessage} from 'src/utils/api'

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
import {
  Action,
  setChecks,
  setCheck,
  removeCheck,
  removeLabelFromCheck,
} from 'src/checks/actions/creators'
import {checkChecksLimits} from 'src/cloud/actions/limits'
import {setLabelOnResource} from 'src/labels/actions/creators'

// Types
import {
  Check,
  GetState,
  RemoteDataState,
  CheckViewProperties,
  Label,
  LabelEntities,
  CheckPatch,
  CheckEntities,
  ResourceType,
} from 'src/types'
import {labelSchema} from 'src/schemas/labels'

import {LIMIT} from 'src/resources/constants'

export const getChecks = () => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkChecksLimits>
  >,
  getState: GetState
) => {
  try {
    const state = getState()
    if (getStatus(state, ResourceType.Checks) === RemoteDataState.NotStarted) {
      dispatch(setChecks(RemoteDataState.Loading))
    }
    const {id: orgID} = getOrg(state)

    // bump the limit up to the max. see idpe 6592
    // TODO: https://github.com/influxdata/influxdb/issues/17541
    const resp = await api.getChecks({query: {orgID, limit: LIMIT}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const checks = normalize<Check, CheckEntities, string[]>(
      resp.data.checks,
      arrayOfChecks
    )

    dispatch(setChecks(RemoteDataState.Done, checks))
    dispatch(checkChecksLimits())
  } catch (e) {
    console.error(e)
    dispatch(setChecks(RemoteDataState.Error))
    dispatch(notify(copy.getChecksFailed(e.message)))
  }
}

export const getCheckForTimeMachine = (checkID: string) => async (
  dispatch: Dispatch<
    TimeMachineAction | NotificationAction | AlertBuilderAction | RouterAction
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

    const normCheck = normalize<Check, CheckEntities, string>(
      resp.data,
      checkSchema
    )

    const builderCheck = normCheck.entities.checks[normCheck.result]

    dispatch(setAlertBuilderCheck(builderCheck))
  } catch (error) {
    console.error(error)
    dispatch(push(`/orgs/${org.id}/alerting`))
    dispatch(setAlertBuilderCheckStatus(RemoteDataState.Error))
    dispatch(notify(copy.getCheckFailed(error.message)))
  }
}

type SendToTimeMachineAction =
  | ReturnType<typeof checkChecksLimits>
  | ReturnType<typeof push>
  | ReturnType<typeof resetAlertBuilder>
  | NotificationAction

export const createCheckFromTimeMachine = () => async (
  dispatch: Dispatch<Action | SendToTimeMachineAction | RouterAction>,
  getState: GetState
): Promise<void> => {
  const rename = 'Please rename the check before saving'
  try {
    const state = getState()
    const check = builderToPostCheck(state)
    const resp = await api.postCheck({data: check})
    if (resp.status !== 201) {
      if (resp.data.code.includes('conflict')) {
        throw new Error(`A check named ${check.name} already exists. ${rename}`)
      }
      throw new Error(resp.data.message)
    }

    const normCheck = normalize<Check, CheckEntities, string>(
      resp.data,
      checkSchema
    )

    dispatch(setCheck(resp.data.id, RemoteDataState.Done, normCheck))
    dispatch(checkChecksLimits())

    dispatch(push(`/orgs/${check.orgID}/alerting`))
    dispatch(resetAlertBuilder())
  } catch (error) {
    console.error(error)
    const message = getErrorMessage(error)
    dispatch(notify(copy.createCheckFailed(message)))
    if (!message.includes(rename)) {
      reportError(error, {
        context: {state: getState()},
        name: 'saveCheckFromTimeMachine function',
      })
    }
  }
}

export const updateCheckFromTimeMachine = () => async (
  dispatch: Dispatch<Action | SendToTimeMachineAction | RouterAction>,
  getState: GetState
) => {
  const state = getState()
  const check = builderToPostCheck(state)
  // todo: refactor after https://github.com/influxdata/influxdb/issues/16317
  try {
    const getCheckResponse = await api.getCheck({checkID: check.id})

    if (getCheckResponse.status !== 200) {
      throw new Error(getCheckResponse.data.message)
    }

    const resp = await api.putCheck({
      checkID: check.id,
      data: {...check, ownerID: getCheckResponse.data.ownerID},
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const normCheck = normalize<Check, CheckEntities, string>(
      resp.data,
      checkSchema
    )

    dispatch(setCheck(resp.data.id, RemoteDataState.Done, normCheck))
    dispatch(checkChecksLimits())

    dispatch(push(`/orgs/${check.orgID}/alerting`))
    dispatch(resetAlertBuilder())
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.updateCheckFailed(error.message)))
    reportError(error, {
      context: {state: getState()},
      name: 'saveCheckFromTimeMachine function',
    })
  }
}

export const updateCheckDisplayProperties = (
  checkID: string,
  update: CheckPatch
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  try {
    const resp = await api.patchCheck({checkID, data: update})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const check = normalize<Check, CheckEntities, string>(
      resp.data,
      checkSchema
    )

    dispatch(setCheck(checkID, RemoteDataState.Done, check))
  } catch (error) {
    console.error(error)
  }
}

export const deleteCheck = (checkID: string) => async (
  dispatch: Dispatch<any>
) => {
  try {
    const resp = await api.deleteCheck({checkID})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeCheck(checkID))
    dispatch(checkChecksLimits())
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.deleteCheckFailed(error.message)))
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

    const normLabel = normalize<Label, LabelEntities>(
      resp.data.label,
      labelSchema
    )

    dispatch(setLabelOnResource(checkID, normLabel))
  } catch (error) {
    console.error(error)
  }
}

export const deleteCheckLabel = (checkID: string, labelID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.deleteChecksLabel({
      checkID,
      labelID,
    })

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeLabelFromCheck(checkID, labelID))
  } catch (error) {
    console.error(error)
  }
}

export const cloneCheck = (check: Check) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkChecksLimits>
  >,
  getState: GetState
): Promise<void> => {
  try {
    const state = getState()
    const checks = getAll<Check>(state, ResourceType.Checks)
    const allCheckNames = checks.map(c => c.name)
    const clonedName = incrementCloneName(allCheckNames, check.name)

    const data = toPostCheck({...check, name: clonedName})

    const resp = await api.postCheck({data})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const normCheck = normalize<Check, CheckEntities, string>(
      resp.data,
      checkSchema
    )

    dispatch(setCheck(resp.data.id, RemoteDataState.Done, normCheck))
    dispatch(checkChecksLimits())
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.createCheckFailed(error.message)))
  }
}
