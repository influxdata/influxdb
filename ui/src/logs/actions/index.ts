// Utils
import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'
import {serverToUIConfig, uiToServerConfig} from 'src/logs/utils/config'
import {
  buildInfiniteScrollLogQuery,
  buildTableQueryConfig,
} from 'src/logs/utils/queryBuilder'
import {transformFluxLogsResponse} from 'src/logs/utils'

// APIs
import {
  readViews as readViewsAJAX,
  createView as createViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis/v2/view'
import {getSource} from 'src/sources/apis/v2'
import {getBuckets} from 'src/shared/apis/v2/buckets'
import {executeQueryAsync} from 'src/logs/api/v2'

// Data
import {logViewData as defaultLogView} from 'src/logs/data/logViewData'

// Types
import {Dispatch} from 'redux'
import {ThunkDispatch} from 'redux-thunk'
import {View, ViewType} from 'src/types/v2/dashboards'
import {
  Filter,
  LogConfig,
  SearchStatus,
  State,
  GetState,
  TableData,
} from 'src/types/logs'
import {Source, Bucket} from 'src/types/v2'
import {QueryConfig} from 'src/types'

// Constants
import {
  DEFAULT_MAX_TAIL_BUFFER_DURATION_MS,
  defaultTableData,
} from 'src/logs/constants'

export const INITIAL_LIMIT = 1000

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
  SetTableQueryConfig = 'LOGS_SET_TABLE_QUERY_CONFIG',
  SetCurrentTailUpperBound = 'SET_CURRENT_TAIL_UPPER_BOUND',
  SetNextTailLowerBound = 'SET_NEXT_TAIL_LOWER_BOUND',
  ClearTableData = 'CLEAR_TABLE_DATA',
  SetTableForwardData = 'SET_TABLE_FORWARD_DATA',
  SetTableBackwardData = 'SET_TABLE_BACKWARD_DATA',
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

interface SetTableQueryConfigAction {
  type: ActionTypes.SetTableQueryConfig
  payload: {
    queryConfig: QueryConfig
  }
}

interface SetCurrentTailUpperBoundAction {
  type: ActionTypes.SetCurrentTailUpperBound
  payload: {
    upper: number | undefined
  }
}
interface SetNextTailLowerBoundAction {
  type: ActionTypes.SetNextTailLowerBound
  payload: {
    lower: number | undefined
  }
}

export interface ClearTableDataAction {
  type: ActionTypes.ClearTableData
}

export interface SetTableForwardDataAction {
  type: ActionTypes.SetTableForwardData
  payload: {
    data: TableData
  }
}

export interface SetTableBackwardDataAction {
  type: ActionTypes.SetTableBackwardData
  payload: {
    data: TableData
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
  | SetTableQueryConfigAction
  | SetCurrentTailUpperBoundAction
  | SetNextTailLowerBoundAction
  | ClearTableDataAction
  | SetTableForwardDataAction
  | SetTableBackwardDataAction

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

  dispatch(setTableQueryConfigAsync())
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
      await dispatch(clearSearchData(SearchStatus.UpdatingSource))
    } catch (e) {
      await dispatch(clearSearchData(SearchStatus.SourceError))
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

export const createLogConfigAsync = (
  url: string,
  newConfig: LogConfig
) => async (dispatch: Dispatch<SetConfigAction>) => {
  const {isTruncated} = newConfig
  const {id, ...newLogView} = uiToServerConfig(newConfig)
  const logView = await createViewAJAX(url, newLogView)
  const logConfig = {
    ...serverToUIConfig(logView),
    isTruncated,
  }
  await dispatch(setConfig(logConfig))
}

export const updateLogConfigAsync = (updatedConfig: LogConfig) => async (
  dispatch: Dispatch<SetConfigAction>
) => {
  const {isTruncated, link} = updatedConfig
  const updatedView = uiToServerConfig(updatedConfig)
  const logView = await updateViewAJAX(link, updatedView)

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

// Table Data
/**
 * Sets tableInfiniteData with empty 'forward' and 'backward' table data.
 */
export const clearTableData = () => ({
  type: ActionTypes.ClearTableData,
})

export const clearSearchData = (
  searchStatus: SearchStatus
) => async dispatch => {
  await dispatch(setSearchStatus(SearchStatus.Clearing))
  await dispatch(clearTableData())
  await dispatch(setSearchStatus(searchStatus))
}

/**
 * Gets the maximum duration that the tail buffer should accumulate
 *  before being flushed to the 'backward' table data.
 * @param state the application state
 */
const getMaxTailBufferDurationMs = (state: State): number => {
  return getDeep<number>(
    state,
    'logs.maxTailBufferDurationMs',
    DEFAULT_MAX_TAIL_BUFFER_DURATION_MS
  )
}

export const setTableForwardData = (
  data: TableData
): SetTableForwardDataAction => ({
  type: ActionTypes.SetTableForwardData,
  payload: {data},
})

export const setTableBackwardData = (
  data: TableData
): SetTableBackwardDataAction => ({
  type: ActionTypes.SetTableBackwardData,
  payload: {data},
})

/**
 * Sets the upper bound of time up until which logs were fetched for the current tail chunk.
 * @param upper the point in time up until which the current tail was fetched.
 */
export const setCurrentTailUpperBound = (
  upper: number
): SetCurrentTailUpperBoundAction => ({
  type: ActionTypes.SetCurrentTailUpperBound,
  payload: {upper},
})

/**
 * Sets the lower bound of time on the current tail chunk fetch query.
 * @param lower the point in time starting from which to begin the next tail fetch.
 */
export const setNextTailLowerBound = (
  lower: number
): SetNextTailLowerBoundAction => ({
  type: ActionTypes.SetNextTailLowerBound,
  payload: {lower},
})

export const setTableQueryConfig = (
  queryConfig: QueryConfig
): SetTableQueryConfigAction => ({
  type: ActionTypes.SetTableQueryConfig,
  payload: {queryConfig},
})

export const fetchTailAsync = () => async (
  dispatch:
    | Dispatch<
        | SetTableBackwardDataAction
        | SetTableForwardDataAction
        | SetNextTailLowerBoundAction
      >
    | ThunkDispatch<typeof flushTailBuffer>,
  getState: GetState
): Promise<void> => {
  const state = getState()

  const {
    logs: {
      nextTailLowerBound: tailLowerBound,
      tableQueryConfig,
      currentSource,
      filters,
    },
  } = getState()

  const params = [currentSource, tableQueryConfig]

  if (_.every(params)) {
    if (!tailLowerBound) {
      throw new Error('tail lower bound is not set')
    }
    const upper = new Date().toISOString()
    const lower = new Date(tailLowerBound).toISOString()

    const upperUTC = Date.parse(upper)
    dispatch(setCurrentTailUpperBound(upperUTC))

    const query = buildInfiniteScrollLogQuery(
      lower,
      upper,
      tableQueryConfig,
      filters
    )
    const {
      links: {query: queryLink},
    } = currentSource
    const response = await executeQueryAsync(queryLink, query)

    if (response.status !== SearchStatus.Loaded) {
      return
    }
    const columnNames: string[] = tableQueryConfig.fields.map(f => f.alias)
    const logSeries: TableData = transformFluxLogsResponse(
      response.tables,
      columnNames
    )

    const currentForwardBufferDuration = upperUTC - tailLowerBound
    const maxTailBufferDurationMs = getMaxTailBufferDurationMs(state)
    const isMaxTailBufferDurationExceeded =
      currentForwardBufferDuration >= maxTailBufferDurationMs

    if (isMaxTailBufferDurationExceeded) {
      dispatch(flushTailBuffer())
      await dispatch(setNextTailLowerBound(upperUTC))
    } else {
      await dispatch(setTableForwardData(logSeries))
    }
  } else {
    throw new Error(
      `Missing params required to fetch tail logs. Maybe there's a race condition with setting buckets?`
    )
  }
}

export const flushTailBuffer = () => (
  dispatch: Dispatch<SetTableBackwardDataAction | SetTableForwardDataAction>,
  getState: GetState
) => {
  const {
    logs: {
      tableInfiniteData: {
        forward: currentTailBuffer,
        backward: currentBackward,
      },
    },
  } = getState()

  const combinedBackward = combineTableData(currentTailBuffer, currentBackward)

  dispatch(setTableBackwardData(combinedBackward))
  dispatch(setTableForwardData(defaultTableData))
}

export const setTableQueryConfigAsync = () => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  const state = getState()
  const bucket = getDeep<Bucket | null>(state, 'logs.currentBucket', null)

  if (bucket) {
    const queryConfig = buildTableQueryConfig(bucket)

    dispatch(setTableQueryConfig(queryConfig))
  }
}

export const clearAllTimeBounds = () => (
  dispatch: Dispatch<
    SetCurrentTailUpperBoundAction | SetNextTailLowerBoundAction
  >
) => {
  dispatch(setCurrentTailUpperBound(undefined))
  dispatch(setNextTailLowerBound(undefined))
}

/**
 * Creates new TableData with the concatted TableData values from args of TableData
 * @param tableDatas
 */
const combineTableData = (...tableDatas: TableData[]) => ({
  columns: tableDatas[0].columns,
  values: _.flatMap(tableDatas, t => t.values),
})
