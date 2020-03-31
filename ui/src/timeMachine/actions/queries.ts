// Libraries
import {parse} from '@influxdata/flux-parser'
import {get, isEmpty} from 'lodash'

// API
import {
  runQuery,
  RunQueryResult,
  RunQuerySuccessResult,
} from 'src/shared/apis/query'
import {runStatusesQuery} from 'src/alerting/utils/statusEvents'

// Actions
import {selectValue, setValues} from 'src/variables/actions/creators'
import {refreshVariableValues} from 'src/variables/actions/thunks'
import {notify} from 'src/shared/actions/notifications'

// Constants
import {rateLimitReached, resultTooLarge} from 'src/shared/copy/notifications'

// Utils
import {
  getActiveTimeMachine,
  getVariableAssignments,
  getActiveQuery,
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
import {findNodes} from 'src/shared/utils/ast'

// Types
import {CancelBox} from 'src/types/promises'
import {
  GetState,
  RemoteDataState,
  StatusRow,
  Node,
  ResourceType,
  Bucket,
} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors/index'

export type Action = SaveDraftQueriesAction | SetQueryResults

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
    const values = get(
      state,
      `resources.variables.values.${prevContextID}.values`,
      {}
    )
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

export const getOrgIDFromBuckets = (
  text: string,
  allBuckets: Bucket[]
): string | null => {
  const ast = parse(text)
  const bucketsInQuery: string[] = findNodes(ast, isFromBucket).map(node =>
    get(node, 'arguments.0.properties.0.value.value', '')
  )

  // if there are buckets from multiple orgs in a query, query will error, and user will receive error from query
  const bucketMatch = allBuckets.find(a => bucketsInQuery.includes(a.name))

  return get(bucketMatch, 'orgID', null)
}

const isFromBucket = (node: Node) => {
  return (
    get(node, 'type') === 'CallExpression' &&
    get(node, 'callee.type') === 'Identifier' &&
    get(node, 'callee.name') === 'from' &&
    get(node, 'arguments.0.properties.0.key.name') === 'bucket'
  )
}

export const executeQueries = (dashboardID?: string) => async (
  dispatch,
  getState: GetState
) => {
  const state = getState()

  const allBuckets = getAll<Bucket>(state, ResourceType.Buckets)

  const {view} = getActiveTimeMachine(state)
  const queries = view.properties.queries.filter(({text}) => !!text.trim())
  const {
    alertBuilder: {id: checkID},
  } = state

  if (!queries.length) {
    dispatch(setQueryResults(RemoteDataState.Done, [], null))
  }

  try {
    dispatch(setQueryResults(RemoteDataState.Loading, [], null))

    await dispatch(refreshTimeMachineVariableValues(dashboardID))
    // keeping getState() here ensures that the state we are working with
    // is the most current one. By having this set to state, we were creating a race
    // condition that was causing the following bug:
    // https://github.com/influxdata/idpe/issues/6240
    const variableAssignments = getVariableAssignments(getState())

    const startTime = window.performance.now()

    pendingResults.forEach(({cancel}) => cancel())

    pendingResults = queries.map(({text}) => {
      const orgID = getOrgIDFromBuckets(text, allBuckets) || getOrg(state).id
      const windowVars = getWindowVars(text, variableAssignments)
      const extern = buildVarsOption([...variableAssignments, ...windowVars])

      return runQuery(orgID, text, extern)
    })

    const results = await Promise.all(pendingResults.map(r => r.promise))
    const duration = window.performance.now() - startTime

    let statuses = [[]] as StatusRow[][]
    if (checkID) {
      const extern = buildVarsOption(variableAssignments)
      pendingCheckStatuses = runStatusesQuery(getOrg(state).id, checkID, extern)
      statuses = await pendingCheckStatuses.promise
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

export const executeCheckQuery = () => async (dispatch, getState: GetState) => {
  const state = getState()
  const {text} = getActiveQuery(state)
  const {id: orgID} = getOrg(state)

  if (text == '') {
    dispatch(setQueryResults(RemoteDataState.Done, [], null))
  }

  try {
    dispatch(setQueryResults(RemoteDataState.Loading, null, null, null))

    const startTime = Date.now()

    const extern = parse(
      'import "influxdata/influxdb/monitor"\noption monitor.write = yield'
    )

    const result = await runQuery(orgID, text, extern).promise
    const duration = Date.now() - startTime

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

    const file = result.csv

    dispatch(setQueryResults(RemoteDataState.Done, [file], duration, null))
  } catch (e) {
    if (e.name === 'CancellationError') {
      return
    }

    console.error(e)
    dispatch(setQueryResults(RemoteDataState.Error, null, null, e.message))
  }
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
  const state = getState()
  const currDash = state.currentDashboard
  const contextID = state.timeMachines.activeTimeMachineID

  if (currDash) {
    dispatch(selectValue(currDash.id, variableID, selectedValue))
  }

  dispatch(selectValue(contextID, variableID, selectedValue))
  dispatch(executeQueries())
}
