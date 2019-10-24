// Libraries
import {get, isEmpty} from 'lodash'

// API
import {
  runQuery,
  RunQueryResult,
  RunQuerySuccessResult,
} from 'src/shared/apis/query'
import {runStatusesQuery} from 'src/alerting/utils/statusEvents'

// Actions
import {
  refreshVariableValues,
  selectValue,
  setValues,
} from 'src/variables/actions'
import {notify} from 'src/shared/actions/notifications'

// Constants
import {rateLimitReached, resultTooLarge} from 'src/shared/copy/notifications'

// Utils
import {
  getActiveTimeMachine,
  getVariableAssignments,
} from 'src/timeMachine/selectors'
import {filterUnusedVars} from 'src/shared/utils/filterUnusedVars'
import {checkQueryResult} from 'src/shared/utils/checkQueryResult'
import {
  extractVariablesList,
  getVariable,
  getHydratedVariables,
} from 'src/variables/selectors'
import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Types
import {CancelBox} from 'src/types/promises'
import {RemoteDataState, StatusRow} from 'src/types'
import {GetState} from 'src/types'

export type Action = SetQueryResults | SaveDraftQueriesAction

interface SetQueryResults {
  type: 'SET_QUERY_RESULTS'
  payload: {
    status: RemoteDataState
    files?: string[]
    fetchDuration?: number
    errorMessage?: string
    statuses?: StatusRow[][]
  }
}

const setQueryResults = (
  status: RemoteDataState,
  files?: string[],
  fetchDuration?: number,
  errorMessage?: string,
  statuses?: StatusRow[][]
): SetQueryResults => ({
  type: 'SET_QUERY_RESULTS',
  payload: {
    status,
    files,
    fetchDuration,
    errorMessage,
    statuses,
  },
})

export const refreshTimeMachineVariableValues = (
  prevContextID?: string
) => async (dispatch, getState: GetState) => {
  const state = getState()
  const contextID = state.timeMachines.activeTimeMachineID

  if (prevContextID) {
    const values = get(state, `variables.values.${prevContextID}.values`) || {}
    if (!isEmpty(values)) {
      dispatch(setValues(contextID, RemoteDataState.Done, values))
      return
    }
  }
  // Find variables currently used by queries in the TimeMachine
  const {view, draftQueries} = getActiveTimeMachine(getState())
  const draftView = {
    ...view,
    properties: {...view.properties, queries: draftQueries},
  }
  const variables = extractVariablesList(getState())
  const variablesInUse = filterUnusedVars(variables, [view, draftView])

  // Find variables whose values have already been loaded by the TimeMachine
  // (regardless of whether these variables are currently being used)
  const hydratedVariables = getHydratedVariables(getState(), contextID)

  // Refresh values for all variables with existing values and in use variables
  const variablesToRefresh = variables.filter(
    v => variablesInUse.includes(v) || hydratedVariables.includes(v)
  )

  await dispatch(refreshVariableValues(contextID, variablesToRefresh))
}

let pendingResults: Array<CancelBox<RunQueryResult>> = []
let pendingCheckStatuses: CancelBox<StatusRow[][]> = null

export const executeQueries = (dashboardID?: string) => async (
  dispatch,
  getState: GetState
) => {
  const {
    view,
    alerting: {check},
  } = getActiveTimeMachine(getState())
  const queries = view.properties.queries.filter(({text}) => !!text.trim())

  if (!queries.length) {
    dispatch(setQueryResults(RemoteDataState.Done, [], null))
  }

  try {
    dispatch(setQueryResults(RemoteDataState.Loading, null, null, null))

    await dispatch(refreshTimeMachineVariableValues(dashboardID))

    const orgID = getState().orgs.org.id

    const variableAssignments = getVariableAssignments(getState())

    const startTime = Date.now()

    pendingResults.forEach(({cancel}) => cancel())

    pendingResults = queries.map(({text}) => {
      const windowVars = getWindowVars(text, variableAssignments)
      const extern = buildVarsOption([...variableAssignments, ...windowVars])

      return runQuery(orgID, text, extern)
    })

    const results = await Promise.all(pendingResults.map(r => r.promise))
    const duration = Date.now() - startTime

    let statuses = [[]] as StatusRow[][]
    if (check && isFlagEnabled('eventMarkers')) {
      const extern = buildVarsOption(variableAssignments)
      pendingCheckStatuses = runStatusesQuery(orgID, check.id, extern)
      statuses = await pendingCheckStatuses.promise // TODO handle errors
    }

    for (const result of results) {
      if (result.type === 'UNKNOWN_ERROR') {
        throw new Error(result.message)
      }

      if (result.type === 'RATE_LIMIT_ERROR') {
        dispatch(notify(rateLimitReached(result.retryAfter)))

        throw new Error(result.message)
      }

      if (result.didTruncate) {
        dispatch(notify(resultTooLarge(result.bytesRead)))
      }

      checkQueryResult(result.csv)
    }

    const files = (results as RunQuerySuccessResult[]).map(r => r.csv)

    dispatch(
      setQueryResults(RemoteDataState.Done, files, duration, null, statuses)
    )
  } catch (e) {
    if (e.name === 'CancellationError') {
      return
    }

    console.error(e)
    dispatch(setQueryResults(RemoteDataState.Error, null, null, e.message))
  }
}

interface SaveDraftQueriesAction {
  type: 'SAVE_DRAFT_QUERIES'
}

const saveDraftQueries = (): SaveDraftQueriesAction => ({
  type: 'SAVE_DRAFT_QUERIES',
})

export const saveAndExecuteQueries = () => dispatch => {
  dispatch(saveDraftQueries())
  dispatch(executeQueries())
}

export const addVariableToTimeMachine = (variableID: string) => async (
  dispatch,
  getState: GetState
) => {
  const contextID = getState().timeMachines.activeTimeMachineID

  const variable = getVariable(getState(), variableID)
  const variables = getHydratedVariables(getState(), contextID)

  if (!variables.includes(variable)) {
    variables.push(variable)
  }

  await dispatch(refreshVariableValues(contextID, variables))
}

export const selectVariableValue = (
  variableID: string,
  selectedValue: string
) => (dispatch, getState: GetState) => {
  const contextID = getState().timeMachines.activeTimeMachineID

  dispatch(selectValue(contextID, variableID, selectedValue))
  dispatch(executeQueries())
}
