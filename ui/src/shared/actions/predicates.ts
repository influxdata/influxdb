// Redux
import {Dispatch} from 'redux-thunk'

// API
import * as api from 'src/client'

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
  | SetPredicateToDefault
  | SetTimeRange

interface SetIsSerious {
  type: 'SET_IS_SERIOUS'
  isSerious: boolean
}

export const setIsSerious = (isSerious: boolean): SetIsSerious => ({
  type: 'SET_IS_SERIOUS',
  isSerious,
})

interface ResetFilters {
  type: 'RESET_FILTERS'
}

export const resetFilters = (): ResetFilters => ({
  type: 'RESET_FILTERS',
})

interface SetBucketName {
  type: 'SET_BUCKET_NAME'
  bucketName: string
}

export const setBucketName = (bucketName: string): SetBucketName => ({
  type: 'SET_BUCKET_NAME',
  bucketName,
})

interface SetTimeRange {
  type: 'SET_DELETE_TIME_RANGE'
  timeRange: [number, number]
}

export const setTimeRange = (timeRange: [number, number]): SetTimeRange => ({
  type: 'SET_DELETE_TIME_RANGE',
  timeRange,
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

interface DeleteFilter {
  type: 'DELETE_FILTER'
  index: number
}

export const deleteFilter = (index: number): DeleteFilter => ({
  type: 'DELETE_FILTER',
  index,
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

interface SetPredicateToDefault {
  type: 'SET_PREDICATE_DEFAULT'
}

export const resetPredicateState = (): SetPredicateToDefault => ({
  type: 'SET_PREDICATE_DEFAULT',
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
