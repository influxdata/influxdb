// Libraries
import {get} from 'lodash'

// API
import {
  executeQueryWithVars,
  ExecuteFluxQueryResult,
} from 'src/shared/apis/query'

// Actions
import {refreshVariableValues} from 'src/variables/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {getActiveOrg} from 'src/organizations/selectors'
import {getVariableAssignments} from 'src/variables/selectors'
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {filterUnusedVars} from 'src/shared/utils/filterUnusedVars'
import {getVariablesForOrg} from 'src/variables/selectors'

// Types
import {WrappedCancelablePromise, CancellationError} from 'src/types/promises'
import {RemoteDataState} from 'src/types'
import {GetState} from 'src/types/v2'

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
  const orgID = getActiveOrg(getState()).id
  const variables = getVariablesForOrg(getState(), orgID)
  const variablesInUse = filterUnusedVars(variables, [view, draftView])

  // Find variables whose values have already been loaded by the TimeMachine
  // (regardless of whether these variables are currently being used)
  const existingVariableIDs = Object.keys(
    get(getState(), `variables.values.${contextID}.values`, {})
  )

  // Refresh values for all variables with existing values and in use variables
  const variablesToRefresh = variables.filter(
    v => variablesInUse.includes(v) || existingVariableIDs.includes(v.id)
  )

  await dispatch(refreshVariableValues(contextID, orgID, variablesToRefresh))
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

    const orgID = getActiveOrg(getState()).id
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

    dispatch(setQueryResults(RemoteDataState.Done, files, duration))
  } catch (e) {
    if (e instanceof CancellationError) {
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

export const saveAndExecuteQueries = () => async dispatch => {
  dispatch(saveDraftQueries())
  dispatch(executeQueries())
}
