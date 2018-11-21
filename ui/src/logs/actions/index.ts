// Utils
import _ from 'lodash'
import uuid from 'uuid'
import {getDeep} from 'src/utils/wrappers'
import {serverToUIConfig, uiToServerConfig} from 'src/logs/utils/config'
import {
  getTableData,
  buildTableQueryConfig,
  validateTailQuery,
  validateOlderQuery,
} from 'src/logs/utils/logQuery'

// APIs
import {
  readViews as readViewsAJAX,
  createView as createViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis/v2/view'
import {readSource} from 'src/sources/apis'
import {getBuckets} from 'src/shared/apis/v2/buckets'
import {executeQueryAsync} from 'src/logs/api/v2'

// Data
import {logViewData as defaultLogView} from 'src/logs/data/logViewData'

// Types
import {Dispatch} from 'redux'
import {ThunkDispatch} from 'redux-thunk'
import {View, NewView, ViewType, TimeSeriesValue} from 'src/types/v2/dashboards'
import {
  Filter,
  LogConfig,
  SearchStatus,
  State,
  GetState,
  TableData,
  LogQuery,
} from 'src/types/logs'
import {Source, Bucket} from 'src/types/v2'
import {QueryConfig} from 'src/types'

// Constants
import {
  DEFAULT_MAX_TAIL_BUFFER_DURATION_MS,
  DEFAULT_TAIL_CHUNK_DURATION_MS,
  DEFAULT_OLDER_CHUNK_DURATION_MS,
  defaultTableData,
  OLDER_BATCH_SIZE_LIMIT,
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
  SetCurrentTailID = 'SET_CURRENT_TAIL_ID',
  SetNextOlderUpperBound = 'SET_NEXT_OLDER_UPPER_BOUND',
  SetNextOlderLowerBound = 'SET_NEXT_OLDER_LOWER_BOUND',
  SetCurrentOlderBatchID = 'SET_CURRENT_OLDER_BATCH_ID',
  ConcatMoreLogs = 'LOGS_CONCAT_MORE_LOGS',
  SetTableRelativeTime = 'SET_TABLE_RELATIVE_TIME',
  SetTableCustomTime = 'SET_TABLE_CUSTOM_TIME',
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

interface SetNextOlderUpperBoundAction {
  type: ActionTypes.SetNextOlderUpperBound
  payload: {
    upper: number | undefined
  }
}
interface SetNextOlderLowerBoundAction {
  type: ActionTypes.SetNextOlderLowerBound
  payload: {
    lower: number | undefined
  }
}

interface SetCurrentOlderBatchIDAction {
  type: ActionTypes.SetCurrentOlderBatchID
  payload: {
    currentOlderBatchID: string | undefined
  }
}

export interface ConcatMoreLogsAction {
  type: ActionTypes.ConcatMoreLogs
  payload: {
    series: TableData
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

export interface SetCurrentTailIDAction {
  type: ActionTypes.SetCurrentTailID
  payload: {
    currentTailID: number | undefined
  }
}

export interface SetTableRelativeTimeAction {
  type: ActionTypes.SetTableRelativeTime
  payload: {
    time: number
  }
}

export interface SetTableCustomTimeAction {
  type: ActionTypes.SetTableCustomTime
  payload: {
    time: string
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
  | SetCurrentTailIDAction
  | SetCurrentOlderBatchIDAction
  | SetNextOlderLowerBoundAction
  | SetNextOlderUpperBoundAction
  | ConcatMoreLogsAction
  | SetTableCustomTimeAction
  | SetTableRelativeTimeAction

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
  const source = await readSource(sourceURL)

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
  const logView: NewView | View = getDeep(views, '0', defaultLogView)

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

export const setTableCustomTime = (time: string): SetTableCustomTimeAction => ({
  type: ActionTypes.SetTableCustomTime,
  payload: {time},
})

export const setTableRelativeTime = (
  time: number
): SetTableRelativeTimeAction => ({
  type: ActionTypes.SetTableRelativeTime,
  payload: {time},
})

export const clearSearchData = (
  searchStatus: SearchStatus
) => async dispatch => {
  await dispatch(setSearchStatus(SearchStatus.Clearing))
  await dispatch(stopCurrentTail())
  await dispatch(stopFetchingOlder())
  await dispatch(clearAllTimeBounds())
  await dispatch(clearTableData())
  await dispatch(setSearchStatus(searchStatus))
}

const getCurrentTailID = (state: State): number => state.logs.currentTailID

const getCurrentOlderBatchID = (state: State): string =>
  state.logs.currentOlderBatchID

const getBackwardTableDataValues = (state: State): TimeSeriesValue[][] =>
  state.logs.tableInfiniteData.backward.values

const getSearchStatus = (state: State): SearchStatus => state.logs.searchStatus

const getNextTailLowerBound = (state: State): number | void =>
  state.logs.nextTailLowerBound

const getTailLogQuery = (state: State): LogQuery => {
  const {
    logs: {currentSource, nextTailLowerBound, filters, tableQueryConfig},
  } = state

  const lower = new Date(nextTailLowerBound).toISOString()
  const upper = new Date(Date.now()).toISOString()

  return {
    source: currentSource,
    config: tableQueryConfig,
    lower,
    upper,
    filters,
  }
}

const getOlderLogQuery = (state: State): LogQuery => {
  const {
    logs: {currentSource, filters, tableQueryConfig},
  } = state

  const olderUpperBound = getNextOlderUpperBound(state)
  const olderChunkDurationMs = getOlderChunkDurationMs(state)

  const nextOlderUpperBound = olderUpperBound - olderChunkDurationMs

  const upper = new Date(olderUpperBound).toISOString()
  const lower = new Date(nextOlderUpperBound).toISOString()

  return {
    source: currentSource,
    config: tableQueryConfig,
    lower,
    upper,
    filters,
  }
}

const getOlderChunkDurationMs = (state: State): number => {
  return getDeep<number>(
    state,
    'logs.olderChunkDurationMs',
    DEFAULT_OLDER_CHUNK_DURATION_MS
  )
}

const getTableSelectedTime = (state: State): number => {
  const custom = getDeep<string>(state, 'logs.tableTime.custom', '')

  if (!_.isEmpty(custom)) {
    return Date.parse(custom)
  }

  const relative = getDeep<number>(state, 'logs.tableTime.relative', 0)

  return Date.now() - relative * 1000
}

const getNextOlderUpperBound = (state: State): number => {
  const selectedTableTime = getTableSelectedTime(state)
  return getDeep<number>(state, 'logs.nextOlderUpperBound', selectedTableTime)
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
 * Sets the upper bound on the next older chunk fetch query.
 * @param upper the point in time up until which to fetch older logs.
 */
export const setNextOlderUpperBound = (
  upper: number
): SetNextOlderUpperBoundAction => ({
  type: ActionTypes.SetNextOlderUpperBound,
  payload: {upper},
})

/**
 * Sets the lower bound on the next older chunk fetch query.
 *  This is _not_ used in fetchOlderChunkAsync.
 * @param lower the point in time starting from which to fetch older logs.
 */
export const setNextOlderLowerBound = (
  lower: number
): SetNextOlderLowerBoundAction => ({
  type: ActionTypes.SetNextOlderLowerBound,
  payload: {lower},
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

export const setCurrentTailID = (
  currentTailID: number | undefined
): SetCurrentTailIDAction => ({
  type: ActionTypes.SetCurrentTailID,
  payload: {currentTailID},
})

export const setCurrentOlderBatchID = (
  currentOlderBatchID: string
): SetCurrentOlderBatchIDAction => ({
  type: ActionTypes.SetCurrentOlderBatchID,
  payload: {currentOlderBatchID},
})

export const stopCurrentTail = () => (
  dispatch:
    | Dispatch<SetCurrentTailIDAction>
    | ThunkDispatch<typeof flushTailBuffer>,
  getState: GetState
) => {
  const state = getState()
  const currentTailID = getCurrentTailID(state)

  dispatch(setCurrentTailID(undefined))
  clearInterval(currentTailID)
}

export const startLogsTail = () => (
  dispatch:
    | Dispatch<SetCurrentTailIDAction>
    | ThunkDispatch<typeof flushTailBuffer>
) => {
  dispatch(flushTailBuffer())
  dispatch(setNextTailLowerBound(Date.now()))

  const logTailID = setInterval(
    () => dispatch(fetchLogTailAsync(logTailID)),
    DEFAULT_TAIL_CHUNK_DURATION_MS
  )

  dispatch(setCurrentTailID(logTailID))
}

export const fetchLogTailAsync = (logTailID: number) => async (
  dispatch:
    | Dispatch<SetCurrentTailUpperBoundAction>
    | ThunkDispatch<typeof stopCurrentTail | typeof updateLogsTailData>,
  getState: GetState
) => {
  const state = getState()
  const currentTailID = getCurrentTailID(state)
  const tailLogQuery = getTailLogQuery(state)
  const {logQuery, error} = validateTailQuery(
    tailLogQuery,
    logTailID,
    currentTailID
  )

  if (error) {
    dispatch(stopCurrentTail())
    return
  }

  const upperUTC = Date.parse(logQuery.upper)
  dispatch(setCurrentTailUpperBound(upperUTC))

  const logSeries = await getTableData(executeQueryAsync, logQuery)

  dispatch(updateLogsTailData(logSeries, logTailID, upperUTC))
}

export const updateLogsTailData = (
  logSeries: TableData,
  logTailID: number,
  upperTailBound: number
) => async (
  dispatch:
    | Dispatch<
        | SetTableBackwardDataAction
        | SetTableForwardDataAction
        | SetNextTailLowerBoundAction
      >
    | ThunkDispatch<typeof flushTailBuffer>,
  getState: GetState
) => {
  const state = getState()
  const maxTailBufferDurationMs = getMaxTailBufferDurationMs(state)
  const tailLowerBound = getNextTailLowerBound(state) as number
  const currentTailID = getCurrentTailID(state)
  const searchStatus = getSearchStatus(state)

  if (!currentTailID || currentTailID !== logTailID) {
    return
  }

  const currentForwardBufferDuration = upperTailBound - tailLowerBound
  const isMaxTailBufferDurationExceeded =
    currentForwardBufferDuration >= maxTailBufferDurationMs

  if (
    searchStatus !== SearchStatus.Loaded &&
    logSeries.values &&
    logSeries.values.length > 0
  ) {
    dispatch(setSearchStatus(SearchStatus.Loaded))
  }

  if (isMaxTailBufferDurationExceeded) {
    dispatch(flushTailBuffer())
    await dispatch(setNextTailLowerBound(upperTailBound))
  } else {
    await dispatch(setTableForwardData(logSeries))
  }
}

export const flushTailBuffer = () => async (
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

  await dispatch(setTableBackwardData(combinedBackward))
  await dispatch(setTableForwardData(defaultTableData))
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
    | SetNextOlderUpperBoundAction
    | SetNextOlderLowerBoundAction
    | SetCurrentTailUpperBoundAction
    | SetNextTailLowerBoundAction
  >
) => {
  return Promise.all([
    dispatch(setNextOlderUpperBound(undefined)),
    dispatch(setNextOlderLowerBound(undefined)),
    dispatch(setCurrentTailUpperBound(undefined)),
    dispatch(setNextTailLowerBound(undefined)),
  ])
}

/**
 * Creates new TableData with the concatted TableData values from args of TableData
 * @param tableDatas
 */
const combineTableData = (...tableDatas: TableData[]) => ({
  columns: tableDatas[0].columns,
  values: _.flatMap(tableDatas, t => t.values),
})

// Fetch Older Data
export const startFetchingOlder = () => async (
  dispatch:
    | Dispatch<SetCurrentOlderBatchIDAction>
    | ThunkDispatch<typeof fetchOlderChunkAsync>,
  getState: GetState
) => {
  const state = getState()
  const currentOlderBatchID = getCurrentOlderBatchID(state)

  if (currentOlderBatchID !== undefined) {
    return
  }

  const olderBatchID = uuid.v4()
  const initialSize = getBackwardTableDataValues(state).length

  const isCurrentBatch = () =>
    olderBatchID === getCurrentOlderBatchID(getState())

  const isBatchLoading = () => {
    const currentSize = getBackwardTableDataValues(getState()).length

    return currentSize - initialSize <= OLDER_BATCH_SIZE_LIMIT
  }

  await dispatch(setCurrentOlderBatchID(olderBatchID))

  while (isCurrentBatch() && isBatchLoading()) {
    await dispatch(fetchOlderChunkAsync(olderBatchID))
  }

  if (isCurrentBatch()) {
    dispatch(setCurrentOlderBatchID(undefined))
  }
}

export const stopFetchingOlder = () => async (
  dispatch: Dispatch<SetCurrentOlderBatchIDAction>
) => {
  return dispatch(setCurrentOlderBatchID(undefined))
}

export const fetchOlderChunkAsync = (olderBatchID: string) => async (
  dispatch:
    | Dispatch<SetNextOlderUpperBoundAction | ConcatMoreLogsAction>
    | ThunkDispatch<typeof updateOlderLogs>,
  getState: GetState
): Promise<void> => {
  const state = getState()
  const olderLogQuery = getOlderLogQuery(state)
  const currentOlderBatchID = getCurrentOlderBatchID(state)
  const {logQuery, error} = validateOlderQuery(
    olderLogQuery,
    olderBatchID,
    currentOlderBatchID
  )

  if (error) {
    return
  }
  const nextOlderUpperBound = new Date(logQuery.lower).valueOf()
  await dispatch(setNextOlderUpperBound(nextOlderUpperBound))

  const logSeries = await getTableData(executeQueryAsync, logQuery)
  if (logSeries.values.length > 0) {
    await dispatch(updateOlderLogs(logSeries, olderBatchID))
  }
}

export const updateOlderLogs = (
  logSeries: TableData,
  olderBatchID: string
) => async (
  dispatch: Dispatch<ConcatMoreLogsAction | SetSearchStatusAction>,
  getState: GetState
) => {
  const state = getState()
  const searchStatus = getSearchStatus(state)
  const currentOlderBatchID = getCurrentOlderBatchID(state)

  if (!currentOlderBatchID || olderBatchID !== currentOlderBatchID) {
    return
  }

  await dispatch(concatMoreLogs(logSeries))

  if (
    searchStatus !== SearchStatus.Loaded &&
    logSeries.values &&
    logSeries.values.length > 0
  ) {
    dispatch(setSearchStatus(SearchStatus.Loaded))
  }
}

export const concatMoreLogs = (series: TableData): ConcatMoreLogsAction => ({
  type: ActionTypes.ConcatMoreLogs,
  payload: {series},
})
