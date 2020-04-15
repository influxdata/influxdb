// Libraries
import {Dispatch} from 'react'
import {extractBoxedCol} from 'src/timeMachine/apis/queryBuilder'
import moment from 'moment'

// Utils
import {postDelete} from 'src/client'
import {runQuery} from 'src/shared/apis/query'
import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {getVariables, asAssignment} from 'src/variables/selectors'
import {checkQueryResult} from 'src/shared/utils/checkQueryResult'
import {getOrg} from 'src/organizations/selectors'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import {
  predicateDeleteFailed,
  predicateDeleteSucceeded,
  setFilterKeyFailed,
  setFilterValueFailed,
} from 'src/shared/copy/notifications'
import {rateLimitReached, resultTooLarge} from 'src/shared/copy/notifications'

// Types
import {RemoteDataState, Filter, CustomTimeRange, GetState} from 'src/types'
import {Action as NotifyAction} from 'src/shared/actions/notifications'

export type Action =
  | DeleteFilter
  | ResetFilters
  | SetBucketName
  | SetDeletionStatus
  | SetFiles
  | SetFilter
  | SetIsSerious
  | SetKeysByBucket
  | SetPredicateToDefault
  | SetPreviewStatus
  | SetTimeRange
  | SetValuesByKey
  | NotifyAction

interface DeleteFilter {
  type: 'DELETE_FILTER'
  payload: {index: number}
}

export const deleteFilter = (index: number): DeleteFilter => ({
  type: 'DELETE_FILTER',
  payload: {index},
})

interface ResetFilters {
  type: 'RESET_FILTERS'
}

export const resetFilters = (): ResetFilters => ({
  type: 'RESET_FILTERS',
})

interface SetPredicateToDefault {
  type: 'SET_PREDICATE_DEFAULT'
}

export const resetPredicateState = (): SetPredicateToDefault => ({
  type: 'SET_PREDICATE_DEFAULT',
})

interface SetBucketName {
  type: 'SET_BUCKET_NAME'
  payload: {bucketName: string}
}

export const setBucketName = (bucketName: string): SetBucketName => ({
  type: 'SET_BUCKET_NAME',
  payload: {bucketName},
})

interface SetDeletionStatus {
  type: 'SET_DELETION_STATUS'
  payload: {deletionStatus: RemoteDataState}
}

export const setDeletionStatus = (
  status: RemoteDataState
): SetDeletionStatus => ({
  type: 'SET_DELETION_STATUS',
  payload: {deletionStatus: status},
})

interface SetFiles {
  type: 'SET_FILES'
  payload: {files: string[]}
}

export const setFiles = (files: string[]): SetFiles => ({
  type: 'SET_FILES',
  payload: {files},
})

interface SetFilter {
  type: 'SET_FILTER'
  payload: {
    filter: Filter
    index: number
  }
}

export const setFilter = (filter: Filter, index: number): SetFilter => ({
  type: 'SET_FILTER',
  payload: {filter, index},
})

interface SetIsSerious {
  type: 'SET_IS_SERIOUS'
  payload: {isSerious: boolean}
}

export const setIsSerious = (isSerious: boolean): SetIsSerious => ({
  type: 'SET_IS_SERIOUS',
  payload: {isSerious},
})

interface SetKeysByBucket {
  type: 'SET_KEYS_BY_BUCKET'
  payload: {keys: string[]}
}

const setKeys = (keys: string[]): SetKeysByBucket => ({
  type: 'SET_KEYS_BY_BUCKET',
  payload: {keys},
})

interface SetPreviewStatus {
  type: 'SET_PREVIEW_STATUS'
  payload: {previewStatus: RemoteDataState}
}

export const setPreviewStatus = (
  status: RemoteDataState
): SetPreviewStatus => ({
  type: 'SET_PREVIEW_STATUS',
  payload: {previewStatus: status},
})

interface SetTimeRange {
  type: 'SET_DELETE_TIME_RANGE'
  payload: {timeRange: CustomTimeRange}
}

export const setTimeRange = (timeRange: CustomTimeRange): SetTimeRange => ({
  type: 'SET_DELETE_TIME_RANGE',
  payload: {timeRange},
})

interface SetValuesByKey {
  type: 'SET_VALUES_BY_KEY'
  payload: {values: string[]}
}

const setValues = (values: string[]): SetValuesByKey => ({
  type: 'SET_VALUES_BY_KEY',
  payload: {values},
})

const formatFilters = (filters: Filter[]) =>
  filters.map(f => `${f.key} ${f.equality} ${f.value}`).join(' AND ')

export const deleteWithPredicate = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  dispatch(setDeletionStatus(RemoteDataState.Loading))

  const {
    predicates: {timeRange, bucketName, filters},
  } = getState()
  const orgID = getOrg(getState()).id

  const data = {
    start: moment(timeRange.lower).toISOString(),
    stop: moment(timeRange.upper).toISOString(),
  }

  if (filters.length > 0) {
    data['predicate'] = formatFilters(filters)
  }

  try {
    const resp = await postDelete({
      data,
      query: {
        orgID,
        bucket: bucketName,
      },
    })

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(setDeletionStatus(RemoteDataState.Done))
    dispatch(notify(predicateDeleteSucceeded()))
    dispatch(resetPredicateState())
  } catch {
    dispatch(notify(predicateDeleteFailed()))
    dispatch(setDeletionStatus(RemoteDataState.Error))
    dispatch(resetPredicateState())
  }
}

export const executePreviewQuery = (query: string) => async (
  dispatch,
  getState: GetState
) => {
  dispatch(setPreviewStatus(RemoteDataState.Loading))
  try {
    const state = getState()
    const orgID = getOrg(state).id

    // TODO figure out how to do this better
    // for some reason we can't use the time range variables
    // for preview query, which means we can't use getAllVariables
    // which means we have to drag around all this asAssignment
    // garbage to be able to run a query instead of just being able
    // to executeQuery as normal
    const variableAssignments = getVariables(state)
      .map(v => asAssignment(v))
      .filter(v => !!v)
    const windowVars = getWindowVars(query, variableAssignments)
    const extern = buildVarsOption([...variableAssignments, ...windowVars])
    const result = await runQuery(orgID, query, extern).promise

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

    const files = [result.csv]
    dispatch(setFiles(files))
  } catch (e) {
    if (e.name === 'CancellationError') {
      return
    }

    console.error(e)
    dispatch(setPreviewStatus(RemoteDataState.Error))
  }
}

export const setBucketAndKeys = (bucketName: string) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const orgID = getOrg(getState()).id

  try {
    const query = `import "influxdata/influxdb/v1"
    v1.tagKeys(bucket: "${bucketName}")
    |> filter(fn: (r) => r._value != "_stop" and r._value != "_start")`
    const keys = await extractBoxedCol(runQuery(orgID, query), '_value').promise
    keys.sort()
    dispatch(setBucketName(bucketName))
    dispatch(setKeys(keys))
  } catch {
    dispatch(notify(setFilterKeyFailed()))
    dispatch(setDeletionStatus(RemoteDataState.Error))
  }
}

export const setValuesByKey = (bucketName: string, keyName: string) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const orgID = getOrg(getState()).id

  try {
    const query = `import "influxdata/influxdb/v1" v1.tagValues(bucket: "${bucketName}", tag: "${keyName}")`
    const values = await extractBoxedCol(runQuery(orgID, query), '_value')
      .promise
    values.sort()
    dispatch(setValues(values))
  } catch {
    dispatch(notify(setFilterValueFailed()))
    dispatch(setDeletionStatus(RemoteDataState.Error))
  }
}
