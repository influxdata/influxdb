// Libraries
import {Dispatch} from 'redux-thunk'
import {extractBoxedCol} from 'src/timeMachine/apis/queryBuilder'

// API
import * as api from 'src/client'
import {runQuery} from 'src/shared/apis/query'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Constants
import {
  predicateDeleteFailed,
  predicateDeleteSucceeded,
} from 'src/shared/copy/notifications'

// Types
import {RemoteDataState, Filter} from 'src/types'

export type Action =
  | DeleteFilter
  | ResetFilters
  | SetBucketName
  | SetDeletionStatus
  | SetFilter
  | SetIsSerious
  | SetKeysByBucket
  | SetPredicateToDefault
  | SetTimeRange
  | SetValuesByKey

interface DeleteFilter {
  type: 'DELETE_FILTER'
  index: number
}

export const deleteFilter = (index: number): DeleteFilter => ({
  type: 'DELETE_FILTER',
  index,
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
  bucketName: string
}

export const setBucketName = (bucketName: string): SetBucketName => ({
  type: 'SET_BUCKET_NAME',
  bucketName,
})

interface SetDeletionStatus {
  type: 'SET_DELETION_STATUS'
  deletionStatus: RemoteDataState
}

export const setDeletionStatus = (
  status: RemoteDataState
): SetDeletionStatus => ({
  type: 'SET_DELETION_STATUS',
  deletionStatus: status,
})

interface SetFilter {
  type: 'SET_FILTER'
  filter: Filter
  index: number
}

export const setFilter = (filter: Filter, index: number): SetFilter => ({
  type: 'SET_FILTER',
  filter,
  index,
})

interface SetIsSerious {
  type: 'SET_IS_SERIOUS'
  isSerious: boolean
}

export const setIsSerious = (isSerious: boolean): SetIsSerious => ({
  type: 'SET_IS_SERIOUS',
  isSerious,
})

interface SetKeysByBucket {
  type: 'SET_KEYS_BY_BUCKET'
  keys: string[]
}

const setKeys = (keys: string[]): SetKeysByBucket => ({
  type: 'SET_KEYS_BY_BUCKET',
  keys,
})

interface SetTimeRange {
  type: 'SET_DELETE_TIME_RANGE'
  timeRange: [number, number]
}

export const setTimeRange = (timeRange: [number, number]): SetTimeRange => ({
  type: 'SET_DELETE_TIME_RANGE',
  timeRange,
})

interface SetValuesByKey {
  type: 'SET_VALUES_BY_KEY'
  values: string[]
}

const setValues = (values: string[]): SetValuesByKey => ({
  type: 'SET_VALUES_BY_KEY',
  values,
})

export const deleteWithPredicate = params => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const resp = await api.postDelete(params)
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

export const setBucketAndKeys = (orgID: string, bucketName: string) => async (
  dispatch: Dispatch<Action>
) => {
  const query = `import "influxdata/influxdb/v1"
  v1.tagKeys(bucket: "${bucketName}")`
  const keys = await extractBoxedCol(runQuery(orgID, query), '_value').promise
  dispatch(setBucketName(bucketName))
  dispatch(setKeys(keys))
}

export const setValuesByKey = (
  orgID: string,
  bucketName: string,
  keyName: string
) => async (dispatch: Dispatch<Action>) => {
  const query = `import "influxdata/influxdb/v1" v1.tagValues(bucket: "${bucketName}", tag: "${keyName}")`
  const values = await extractBoxedCol(runQuery(orgID, query), '_value').promise
  dispatch(setValues(values))
}
