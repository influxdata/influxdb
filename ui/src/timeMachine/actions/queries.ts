import {get} from 'lodash'

// API
import {
  executeQueryWithVars,
  ExecuteFluxQueryResult,
} from 'src/shared/apis/query'

// Actions
import {refreshVariableValues, selectValue} from 'src/variables/actions'
import {notify} from 'src/shared/actions/notifications'

// Constants
import {readLimitReached} from 'src/shared/copy/notifications'
import {RATE_LIMIT_ERROR_STATUS} from 'src/cloud/constants/index'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {getVariableAssignments} from 'src/variables/selectors'
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {filterUnusedVars} from 'src/shared/utils/filterUnusedVars'
import {checkQueryResult} from 'src/shared/utils/checkQueryResult'
import {
  extractVariablesList,
  getVariable,
  getHydratedVariables,
} from 'src/variables/selectors'

// Types
import {WrappedCancelablePromise, CancellationError} from 'src/types/promises'
import {RemoteDataState} from 'src/types'
import {GetState} from 'src/types'

export type Action = SetQueryResults | SaveDraftQueriesAction

interface SetQueryResults {
  type: 'SET_QUERY_RESULTS'
  payload: {
    status: RemoteDataState
    files?: string[]
    fetchDuration?: number
    errorMessage?: string
  }
}

const setQueryResults = (
  status: RemoteDataState,
  files?: string[],
  fetchDuration?: number,
  errorMessage?: string
): SetQueryResults => ({
  type: 'SET_QUERY_RESULTS',
  payload: {
    status,
    files,
    fetchDuration,
    errorMessage,
  },
})

export const refreshTimeMachineVariableValues = () => async (
  dispatch,
  getState: GetState
) => {
  const contextID = getState().timeMachines.activeTimeMachineID

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

let pendingResults: Array<WrappedCancelablePromise<ExecuteFluxQueryResult>> = []

export const executeQueries = () => async (dispatch, getState: GetState) => {
  const {view, timeRange} = getActiveTimeMachine(getState())
  const queries = view.properties.queries.filter(({text}) => !!text.trim())

  if (!queries.length) {
    dispatch(setQueryResults(RemoteDataState.Done, [], null))
  }

  try {
    dispatch(setQueryResults(RemoteDataState.Loading, null, null, null))

    await dispatch(refreshTimeMachineVariableValues())

    const orgID = getState().orgs.org.id
    const queryURL = getState().links.query.self
    const activeTimeMachineID = getState().timeMachines.activeTimeMachineID
    const variableAssignments = [
      ...getVariableAssignments(getState(), activeTimeMachineID),
      ...getTimeRangeVars(timeRange),
    ]

    const startTime = Date.now()

    pendingResults.forEach(({cancel}) => cancel())

    pendingResults = queries.map(({text}) =>
      executeQueryWithVars(queryURL, orgID, text, variableAssignments)
    )

    const results = await Promise.all(pendingResults.map(r => r.promise))

    const duration = Date.now() - startTime
    const files = results.map(r => r.csv)

    files.forEach(checkQueryResult)

    dispatch(setQueryResults(RemoteDataState.Done, files, duration))
  } catch (e) {
    if (e instanceof CancellationError) {
      return
    }

    if (get(e, 'xhr.status') === RATE_LIMIT_ERROR_STATUS) {
      dispatch(notify(readLimitReached()))
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

export const saveAndExecuteQueries = () => async dispatch => {
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
) => async (dispatch, getState: GetState) => {
  const contextID = getState().timeMachines.activeTimeMachineID

  dispatch(selectValue(contextID, variableID, selectedValue))
  dispatch(executeQueries())
}
