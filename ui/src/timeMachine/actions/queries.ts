// Libraries
import {parse} from '@influxdata/flux-parser'
import {get} from 'lodash'

// API
import {
  runQuery,
  RunQueryResult,
  RunQuerySuccessResult,
} from 'src/shared/apis/query'
import {runStatusesQuery} from 'src/alerting/utils/statusEvents'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {hydrateVariables} from 'src/variables/actions/thunks'

// Constants
import {
  rateLimitReached,
  resultTooLarge,
  demoDataSwitchedOff,
} from 'src/shared/copy/notifications'

// Utils
import {getActiveTimeMachine, getActiveQuery} from 'src/timeMachine/selectors'
import fromFlux from 'src/shared/utils/fromFlux'
import {getAllVariables, asAssignment} from 'src/variables/selectors'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {findNodes} from 'src/shared/utils/ast'
import {isDemoDataAvailabilityError} from 'src/cloud/utils/demoDataErrors'

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

export const executeQueries = () => async (dispatch, getState: GetState) => {
  const state = getState()

  const allBuckets = getAll<Bucket>(state, ResourceType.Buckets)

  const activeTimeMachine = getActiveTimeMachine(state)
  const queries = activeTimeMachine.view.properties.queries.filter(
    ({text}) => !!text.trim()
  )
  const {
    alertBuilder: {id: checkID},
  } = state

  if (!queries.length) {
    dispatch(setQueryResults(RemoteDataState.Done, [], null))
  }

  try {
    dispatch(setQueryResults(RemoteDataState.Loading, [], null))

    await dispatch(hydrateVariables())

    const variableAssignments = getAllVariables(state)
      .map(v => asAssignment(v))
      .filter(v => !!v)

    // keeping getState() here ensures that the state we are working with
    // is the most current one. By having this set to state, we were creating a race
    // condition that was causing the following bug:
    // https://github.com/influxdata/idpe/issues/6240

    const startTime = window.performance.now()

    pendingResults.forEach(({cancel}) => cancel())

    pendingResults = queries.map(({text}) => {
      const orgID = getOrgIDFromBuckets(text, allBuckets) || getOrg(state).id
      const extern = buildVarsOption(variableAssignments)

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
        if (isDemoDataAvailabilityError(result.code, result.message)) {
          dispatch(notify(demoDataSwitchedOff()))
        }

        throw new Error(result.message)
      }

      if (result.type === 'RATE_LIMIT_ERROR') {
        dispatch(notify(rateLimitReached(result.retryAfter)))

        throw new Error(result.message)
      }

      if (result.didTruncate) {
        dispatch(notify(resultTooLarge(result.bytesRead)))
      }

      // TODO: this is just here for validation. since we are already eating
      // the cost of parsing the results, we should store the output instead
      // of the raw input
      fromFlux(result.csv)
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

    // TODO: this is just here for validation. since we are already eating
    // the cost of parsing the results, we should store the output instead
    // of the raw input
    fromFlux(result.csv)

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
