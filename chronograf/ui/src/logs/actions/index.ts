// Utils
import {getDeep} from 'src/utils/wrappers'
import {serverToUIConfig} from 'src/logs/utils/config'

// APIs
import {readViews as readViewsAJAX} from 'src/dashboards/apis/v2/view'
import {getSource} from 'src/sources/apis/v2'
import {getBuckets} from 'src/shared/apis/v2/buckets'

// Data
import {logViewData as defaultLogView} from 'src/logs/data/logViewData'

// Types
import {Dispatch} from 'redux'
import {View, ViewType} from 'src/types/v2/dashboards'
import {Filter, LogConfig, SearchStatus, LogsState} from 'src/types/logs'
import {Source, Bucket} from 'src/types/v2'

export const INITIAL_LIMIT = 1000

interface State {
  logs: LogsState
}

type GetState = () => State

export enum ActionTypes {
  SetSource = 'LOGS_SET_SOURCE',
  SetBuckets = 'LOGS_SET_BUCKETS',
  SetBucket = 'LOGS_SET_BUCKET',
  AddFilter = 'LOGS_ADD_FILTER',
  RemoveFilter = 'LOGS_REMOVE_FILTER',
  ChangeFilter = 'LOGS_CHANGE_FILTER',
  ClearFilters = 'LOGS_CLEAR_FILTERS',
  SetConfig = 'SET_CONFIG',
  SetSearchStatus = 'SET_SEARCH_STATUS',
}

const getIsTruncated = (state: State): boolean =>
  state.logs.logConfig.isTruncated

export interface AddFilterAction {
  type: ActionTypes.AddFilter
  payload: {
    filter: Filter
  }
}

export interface ChangeFilterAction {
  type: ActionTypes.ChangeFilter
  payload: {
    id: string
    operator: string
    value: string
  }
}

export interface ClearFiltersAction {
  type: ActionTypes.ClearFilters
}

export interface RemoveFilterAction {
  type: ActionTypes.RemoveFilter
  payload: {
    id: string
  }
}
interface SetSourceAction {
  type: ActionTypes.SetSource
  payload: {
    source: Source
  }
}

interface SetBucketsAction {
  type: ActionTypes.SetBuckets
  payload: {
    buckets: Bucket[]
  }
}

interface SetBucketAction {
  type: ActionTypes.SetBucket
  payload: {
    bucket: Bucket
  }
}

export interface SetConfigAction {
  type: ActionTypes.SetConfig
  payload: {
    logConfig: LogConfig
  }
}

interface SetSearchStatusAction {
  type: ActionTypes.SetSearchStatus
  payload: {
    searchStatus: SearchStatus
  }
}

export type Action =
  | SetSourceAction
  | SetBucketsAction
  | SetBucketAction
  | AddFilterAction
  | RemoveFilterAction
  | ChangeFilterAction
  | ClearFiltersAction
  | SetConfigAction
  | SetSearchStatusAction

/**
 * Sets the search status corresponding to the current fetch request.
 * @param searchStatus the state of the current Logs Page fetch request.
 */
export const setSearchStatus = (
  searchStatus: SearchStatus
): SetSearchStatusAction => ({
  type: ActionTypes.SetSearchStatus,
  payload: {searchStatus},
})

export const changeFilter = (id: string, operator: string, value: string) => ({
  type: ActionTypes.ChangeFilter,
  payload: {id, operator, value},
})

export const setSource = (source: Source): SetSourceAction => ({
  type: ActionTypes.SetSource,
  payload: {source},
})

export const addFilter = (filter: Filter): AddFilterAction => ({
  type: ActionTypes.AddFilter,
  payload: {filter},
})

export const clearFilters = (): ClearFiltersAction => ({
  type: ActionTypes.ClearFilters,
})

export const removeFilter = (id: string): RemoveFilterAction => ({
  type: ActionTypes.RemoveFilter,
  payload: {id},
})

export const setBucketAsync = (bucket: Bucket) => async (
  dispatch
): Promise<void> => {
  dispatch({
    type: ActionTypes.SetBucket,
    payload: {bucket},
  })
}

export const setBuckets = (buckets: Bucket[]): SetBucketsAction => ({
  type: ActionTypes.SetBuckets,
  payload: {
    buckets,
  },
})

export const populateBucketsAsync = (
  bucketsLink: string,
  source: Source = null
) => async (dispatch): Promise<void> => {
  try {
    const buckets = await getBuckets(bucketsLink)

    if (buckets && buckets.length > 0) {
      dispatch(setBuckets(buckets))
      if (source && source.telegraf) {
        const defaultBucket = buckets.find(
          bucket => bucket.name === source.telegraf
        )

        await dispatch(setBucketAsync(defaultBucket))
      } else {
        await dispatch(setBucketAsync(buckets[0]))
      }
    }
  } catch (e) {
    dispatch(setBuckets([]))
    dispatch(setBucketAsync(null))
    throw new Error('Failed to populate buckets')
  }
}

export const getSourceAndPopulateBucketsAsync = (sourceURL: string) => async (
  dispatch
): Promise<void> => {
  const source = await getSource(sourceURL)

  const bucketsLink = getDeep<string | null>(source, 'links.buckets', null)

  if (bucketsLink) {
    dispatch(setSource(source))

    try {
      await dispatch(populateBucketsAsync(bucketsLink, source))
      await dispatch(setSearchStatus(SearchStatus.UpdatingSource))
    } catch (e) {
      await dispatch(setSearchStatus(SearchStatus.SourceError))
    }
  }
}

export const getLogConfigAsync = (url: string) => async (
  dispatch: Dispatch<SetConfigAction>,
  getState: GetState
) => {
  const state = getState()
  const isTruncated = getIsTruncated(state)
  const views = await readViewsAJAX(url, {type: ViewType.LogViewer})
  const logView: View = getDeep(views, '0', defaultLogView)

  const logConfig = {
    ...serverToUIConfig(logView),
    isTruncated,
  }

  await dispatch(setConfig(logConfig))
}

export const setConfig = (logConfig: LogConfig): SetConfigAction => {
  return {
    type: ActionTypes.SetConfig,
    payload: {
      logConfig,
    },
  }
}
