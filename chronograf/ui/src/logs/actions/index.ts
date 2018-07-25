import moment from 'moment'
import _ from 'lodash'
import {Dispatch} from 'redux'

import {Source, Namespace, QueryConfig} from 'src/types'
import {getSource} from 'src/shared/apis'
import {getDatabasesWithRetentionPolicies} from 'src/shared/apis/databases'
import {
  buildHistogramQueryConfig,
  buildTableQueryConfig,
  buildLogQuery,
  buildForwardLogQuery,
  buildBackwardLogQuery,
  parseHistogramQueryResponse,
} from 'src/logs/utils'
import {logConfigServerToUI, logConfigUIToServer} from 'src/logs/utils/config'
import {getDeep} from 'src/utils/wrappers'
import {
  executeQueryAsync,
  getLogConfig as getLogConfigAJAX,
  updateLogConfig as updateLogConfigAJAX,
} from 'src/logs/api'

import {
  LogsState,
  Filter,
  TableData,
  LogConfig,
  TimeRange,
  TimeBounds,
  TimeWindow,
  TimeMarker,
} from 'src/types/logs'

export const INITIAL_LIMIT = 1000

const defaultTableData: TableData = {
  columns: [
    'time',
    'severity',
    'timestamp',
    'message',
    'facility',
    'procid',
    'appname',
    'host',
  ],
  values: [],
}

interface State {
  logs: LogsState
}

type GetState = () => State

export enum ActionTypes {
  SetSource = 'LOGS_SET_SOURCE',
  SetNamespaces = 'LOGS_SET_NAMESPACES',
  SetTimeBounds = 'LOGS_SET_TIMEBOUNDS',
  SetTimeWindow = 'LOGS_SET_TIMEWINDOW',
  SetTimeMarker = 'LOGS_SET_TIMEMARKER',
  SetNamespace = 'LOGS_SET_NAMESPACE',
  SetHistogramQueryConfig = 'LOGS_SET_HISTOGRAM_QUERY_CONFIG',
  SetHistogramData = 'LOGS_SET_HISTOGRAM_DATA',
  SetTableQueryConfig = 'LOGS_SET_TABLE_QUERY_CONFIG',
  SetTableData = 'LOGS_SET_TABLE_DATA',
  SetSearchTerm = 'LOGS_SET_SEARCH_TERM',
  AddFilter = 'LOGS_ADD_FILTER',
  RemoveFilter = 'LOGS_REMOVE_FILTER',
  ChangeFilter = 'LOGS_CHANGE_FILTER',
  IncrementQueryCount = 'LOGS_INCREMENT_QUERY_COUNT',
  DecrementQueryCount = 'LOGS_DECREMENT_QUERY_COUNT',
  ConcatMoreLogs = 'LOGS_CONCAT_MORE_LOGS',
  PrependMoreLogs = 'LOGS_PREPEND_MORE_LOGS',
  SetConfig = 'SET_CONFIG',
  SetTableRelativeTime = 'SET_TABLE_RELATIVE_TIME',
  SetTableCustomTime = 'SET_TABLE_CUSTOM_TIME',
  SetTableForwardData = 'SET_TABLE_FORWARD_DATA',
  SetTableBackwardData = 'SET_TABLE_BACKWARD_DATA',
  ClearRowsAdded = 'CLEAR_ROWS_ADDED',
}

export interface ClearRowsAddedAction {
  type: ActionTypes.ClearRowsAdded
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

export interface ConcatMoreLogsAction {
  type: ActionTypes.ConcatMoreLogs
  payload: {
    series: TableData
  }
}

export interface PrependMoreLogsAction {
  type: ActionTypes.PrependMoreLogs
  payload: {
    series: TableData
  }
}

export interface IncrementQueryCountAction {
  type: ActionTypes.IncrementQueryCount
}

export interface DecrementQueryCountAction {
  type: ActionTypes.DecrementQueryCount
}

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

interface SetNamespacesAction {
  type: ActionTypes.SetNamespaces
  payload: {
    namespaces: Namespace[]
  }
}

interface SetNamespaceAction {
  type: ActionTypes.SetNamespace
  payload: {
    namespace: Namespace
  }
}

interface SetTimeBoundsAction {
  type: ActionTypes.SetTimeBounds
  payload: {
    timeBounds: TimeBounds
  }
}

interface SetTimeWindowAction {
  type: ActionTypes.SetTimeWindow
  payload: {
    timeWindow: TimeWindow
  }
}

interface SetTimeMarkerAction {
  type: ActionTypes.SetTimeMarker
  payload: {
    timeMarker: TimeMarker
  }
}

interface SetHistogramQueryConfig {
  type: ActionTypes.SetHistogramQueryConfig
  payload: {
    queryConfig: QueryConfig
  }
}

interface SetHistogramData {
  type: ActionTypes.SetHistogramData
  payload: {
    data: object[]
  }
}

interface SetTableQueryConfig {
  type: ActionTypes.SetTableQueryConfig
  payload: {
    queryConfig: QueryConfig
  }
}

interface SetTableData {
  type: ActionTypes.SetTableData
  payload: {
    data: object
  }
}

interface SetSearchTerm {
  type: ActionTypes.SetSearchTerm
  payload: {
    searchTerm: string
  }
}

export interface SetConfigsAction {
  type: ActionTypes.SetConfig
  payload: {
    logConfig: LogConfig
  }
}

export type Action =
  | SetSourceAction
  | SetNamespacesAction
  | SetTimeBoundsAction
  | SetTimeWindowAction
  | SetTimeMarkerAction
  | SetNamespaceAction
  | SetHistogramQueryConfig
  | SetHistogramData
  | SetTableData
  | SetTableQueryConfig
  | SetSearchTerm
  | AddFilterAction
  | RemoveFilterAction
  | ChangeFilterAction
  | DecrementQueryCountAction
  | IncrementQueryCountAction
  | ConcatMoreLogsAction
  | PrependMoreLogsAction
  | SetConfigsAction
  | SetTableCustomTimeAction
  | SetTableRelativeTimeAction
  | SetTableForwardDataAction
  | SetTableBackwardDataAction
  | ClearRowsAddedAction

const getTimeRange = (state: State): TimeRange | null =>
  getDeep<TimeRange | null>(state, 'logs.timeRange', null)

const getNamespace = (state: State): Namespace | null =>
  getDeep<Namespace | null>(state, 'logs.currentNamespace', null)

const getProxyLink = (state: State): string | null =>
  getDeep<string | null>(state, 'logs.currentSource.links.proxy', null)

const getHistogramQueryConfig = (state: State): QueryConfig | null =>
  getDeep<QueryConfig | null>(state, 'logs.histogramQueryConfig', null)

const getTableQueryConfig = (state: State): QueryConfig | null =>
  getDeep<QueryConfig | null>(state, 'logs.tableQueryConfig', null)

const getSearchTerm = (state: State): string | null =>
  getDeep<string | null>(state, 'logs.searchTerm', null)

const getFilters = (state: State): Filter[] =>
  getDeep<Filter[]>(state, 'logs.filters', [])

const getTableSelectedTime = (state: State): string => {
  const custom = getDeep<string>(state, 'logs.tableTime.custom', '')

  if (!_.isEmpty(custom)) {
    return custom
  }

  const relative = getDeep<number>(state, 'logs.tableTime.relative', 0)

  return moment()
    .subtract(relative, 'seconds')
    .toISOString()
}

export const clearRowsAdded = () => ({
  type: ActionTypes.ClearRowsAdded,
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

export const setTimeWindow = (timeWindow: TimeWindow): SetTimeWindowAction => ({
  type: ActionTypes.SetTimeWindow,
  payload: {timeWindow},
})

export const setTimeMarker = (timeMarker: TimeMarker): SetTimeMarkerAction => ({
  type: ActionTypes.SetTimeMarker,
  payload: {timeMarker},
})

export const setTimeBounds = (timeBounds: TimeBounds): SetTimeBoundsAction => ({
  type: ActionTypes.SetTimeBounds,
  payload: {timeBounds},
})

export const executeTableForwardQueryAsync = () => async (
  dispatch,
  getState: GetState
) => {
  const state = getState()

  const time = getTableSelectedTime(state)
  const queryConfig = getTableQueryConfig(state)
  const namespace = getNamespace(state)
  const proxyLink = getProxyLink(state)
  const searchTerm = getSearchTerm(state)
  const filters = getFilters(state)

  if (!_.every([queryConfig, time, namespace, proxyLink])) {
    return
  }

  try {
    dispatch(incrementQueryCount())

    const query = buildForwardLogQuery(time, queryConfig, filters, searchTerm)
    const response = await executeQueryAsync(
      proxyLink,
      namespace,
      `${query} ORDER BY time ASC LIMIT ${INITIAL_LIMIT}`
    )

    const series = getDeep(response, 'results.0.series.0', defaultTableData)

    const result = {
      columns: series.columns,
      values: _.reverse(series.values),
    }

    dispatch(setTableForwardData(result))
  } finally {
    dispatch(decrementQueryCount())
  }
}

export const executeTableBackwardQueryAsync = () => async (
  dispatch,
  getState: GetState
) => {
  const state = getState()

  const time = getTableSelectedTime(state)
  const queryConfig = getTableQueryConfig(state)
  const namespace = getNamespace(state)
  const proxyLink = getProxyLink(state)
  const searchTerm = getSearchTerm(state)
  const filters = getFilters(state)

  if (!_.every([queryConfig, time, namespace, proxyLink])) {
    return
  }

  try {
    dispatch(incrementQueryCount())

    const query = buildBackwardLogQuery(time, queryConfig, filters, searchTerm)
    const response = await executeQueryAsync(
      proxyLink,
      namespace,
      `${query} ORDER BY time DESC LIMIT ${INITIAL_LIMIT}`
    )

    const series = getDeep(response, 'results.0.series.0', defaultTableData)

    dispatch(setTableBackwardData(series))
  } finally {
    dispatch(decrementQueryCount())
  }
}

export const setTableCustomTimeAsync = (time: string) => async dispatch => {
  await dispatch(setTableCustomTime(time))
  await dispatch(executeTableQueryAsync())
}

export const setTableRelativeTimeAsync = (time: number) => async dispatch => {
  await dispatch(setTableRelativeTime(time))
  await dispatch(executeTableQueryAsync())
}

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

export const removeFilter = (id: string): RemoveFilterAction => ({
  type: ActionTypes.RemoveFilter,
  payload: {id},
})

const setHistogramData = (data): SetHistogramData => ({
  type: ActionTypes.SetHistogramData,
  payload: {data},
})

export const executeHistogramQueryAsync = () => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  const state = getState()

  const queryConfig = getHistogramQueryConfig(state)
  const timeRange = getTimeRange(state)
  const namespace = getNamespace(state)
  const proxyLink = getProxyLink(state)
  const searchTerm = getSearchTerm(state)
  const filters = getFilters(state)

  if (!_.every([queryConfig, timeRange, namespace, proxyLink])) {
    return
  }

  try {
    dispatch(incrementQueryCount())

    const query = buildLogQuery(timeRange, queryConfig, filters, searchTerm)
    const response = await executeQueryAsync(proxyLink, namespace, query)
    const data = parseHistogramQueryResponse(response)

    dispatch(setHistogramData(data))
  } finally {
    dispatch(decrementQueryCount())
  }
}

export const executeTableQueryAsync = () => async (dispatch): Promise<void> => {
  await Promise.all([
    dispatch(executeTableForwardQueryAsync()),
    dispatch(executeTableBackwardQueryAsync()),
    dispatch(clearRowsAdded()),
  ])
}

export const decrementQueryCount = () => ({
  type: ActionTypes.DecrementQueryCount,
})

export const incrementQueryCount = () => ({
  type: ActionTypes.IncrementQueryCount,
})

export const executeQueriesAsync = () => async dispatch => {
  try {
    await Promise.all([
      dispatch(executeHistogramQueryAsync()),
      dispatch(executeTableQueryAsync()),
    ])
  } catch {
    console.error('Could not make query requests')
  }
}

export const setSearchTermAsync = (searchTerm: string) => async dispatch => {
  dispatch({
    type: ActionTypes.SetSearchTerm,
    payload: {searchTerm},
  })
  dispatch(executeQueriesAsync())
}

export const setHistogramQueryConfigAsync = () => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  const state = getState()
  const namespace = getDeep<Namespace | null>(
    state,
    'logs.currentNamespace',
    null
  )
  const timeRange = getDeep<TimeRange | null>(state, 'logs.timeRange', null)

  if (timeRange && namespace) {
    const queryTimeRange = {
      upper: timeRange.upper,
      lower: timeRange.lower,
      seconds: timeRange.seconds,
    }

    const queryConfig = buildHistogramQueryConfig(namespace, queryTimeRange)

    dispatch({
      type: ActionTypes.SetHistogramQueryConfig,
      payload: {queryConfig},
    })

    dispatch(executeHistogramQueryAsync())
  }
}

export const setTableQueryConfig = (queryConfig: QueryConfig) => ({
  type: ActionTypes.SetTableQueryConfig,
  payload: {queryConfig},
})

export const setTableQueryConfigAsync = () => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  const state = getState()
  const namespace = getDeep<Namespace | null>(
    state,
    'logs.currentNamespace',
    null
  )
  const timeRange = getDeep<TimeRange | null>(state, 'logs.timeRange', null)

  if (timeRange && namespace) {
    const queryConfig = buildTableQueryConfig(namespace, timeRange)

    dispatch(setTableQueryConfig(queryConfig))
    dispatch(executeTableQueryAsync())
  }
}

export const fetchMoreAsync = (queryTimeEnd: string) => async (
  dispatch,
  getState
): Promise<void> => {
  const state = getState()
  const tableQueryConfig = getTableQueryConfig(state)
  const timeRange = {lower: queryTimeEnd}
  const newQueryConfig = {
    ...tableQueryConfig,
    range: timeRange,
  }
  const namespace = getNamespace(state)
  const proxyLink = getProxyLink(state)
  const searchTerm = getSearchTerm(state)
  const filters = getFilters(state)
  const params = [namespace, proxyLink, tableQueryConfig]

  if (_.every(params)) {
    const query = buildBackwardLogQuery(
      queryTimeEnd,
      newQueryConfig,
      filters,
      searchTerm
    )

    const response = await executeQueryAsync(
      proxyLink,
      namespace,
      `${query} ORDER BY time DESC LIMIT ${INITIAL_LIMIT}`
    )

    const series = getDeep(response, 'results.0.series.0', defaultTableData)
    await dispatch(ConcatMoreLogs(series))
  }
}

export const fetchNewerAsync = (queryTimeStart: string) => async (
  dispatch,
  getState
): Promise<void> => {
  const state = getState()
  const tableQueryConfig = getTableQueryConfig(state)
  const timeRange = {lower: queryTimeStart}
  const newQueryConfig = {
    ...tableQueryConfig,
    range: timeRange,
  }
  const namespace = getNamespace(state)
  const proxyLink = getProxyLink(state)
  const searchTerm = getSearchTerm(state)
  const filters = getFilters(state)
  const params = [namespace, proxyLink, tableQueryConfig]

  if (_.every(params)) {
    const query = buildForwardLogQuery(
      queryTimeStart,
      newQueryConfig,
      filters,
      searchTerm
    )

    const response = await executeQueryAsync(
      proxyLink,
      namespace,
      `${query} ORDER BY time ASC LIMIT ${INITIAL_LIMIT}`
    )

    const series = getDeep(response, 'results.0.series.0', defaultTableData)
    await dispatch(
      PrependMoreLogs({
        columns: series.columns,
        values: _.reverse(series.values),
      })
    )
  }
}

export const ConcatMoreLogs = (series: TableData): ConcatMoreLogsAction => ({
  type: ActionTypes.ConcatMoreLogs,
  payload: {series},
})

export const PrependMoreLogs = (series: TableData): PrependMoreLogsAction => ({
  type: ActionTypes.PrependMoreLogs,
  payload: {series},
})

export const setNamespaceAsync = (namespace: Namespace) => async (
  dispatch
): Promise<void> => {
  dispatch({
    type: ActionTypes.SetNamespace,
    payload: {namespace},
  })

  dispatch(setHistogramQueryConfigAsync())
  dispatch(setTableQueryConfigAsync())
}

export const setNamespaces = (
  namespaces: Namespace[]
): SetNamespacesAction => ({
  type: ActionTypes.SetNamespaces,
  payload: {
    namespaces,
  },
})

export const setTimeRangeAsync = () => async (dispatch): Promise<void> => {
  dispatch(setHistogramQueryConfigAsync())
  dispatch(setTableQueryConfigAsync())
}

export const populateNamespacesAsync = (
  proxyLink: string,
  source: Source = null
) => async (dispatch): Promise<void> => {
  const namespaces = await getDatabasesWithRetentionPolicies(proxyLink)

  if (namespaces && namespaces.length > 0) {
    dispatch(setNamespaces(namespaces))
    if (source) {
      const defaultNamespace = namespaces.find(
        namespace => namespace.database === source.telegraf
      )

      dispatch(setNamespaceAsync(defaultNamespace))
    } else {
      dispatch(setNamespaceAsync(namespaces[0]))
    }
  }
}

export const getSourceAndPopulateNamespacesAsync = (sourceID: string) => async (
  dispatch
): Promise<void> => {
  const source = await getSource(sourceID)

  const proxyLink = getDeep<string | null>(source, 'links.proxy', null)

  if (proxyLink) {
    dispatch(setSource(source))
    dispatch(populateNamespacesAsync(proxyLink, source))
  }
}

export const getLogConfigAsync = (url: string) => async (
  dispatch: Dispatch<SetConfigsAction>
): Promise<void> => {
  try {
    const {data} = await getLogConfigAJAX(url)
    const logConfig = logConfigServerToUI(data)
    dispatch(setConfig(logConfig))
  } catch (error) {
    console.error(error)
  }
}

export const updateLogConfigAsync = (url: string, config: LogConfig) => async (
  dispatch: Dispatch<SetConfigsAction>
): Promise<void> => {
  try {
    const configForServer = logConfigUIToServer(config)
    await updateLogConfigAJAX(url, configForServer)
    dispatch(setConfig(config))
  } catch (error) {
    console.error(error)
  }
}

export const setConfig = (logConfig: LogConfig): SetConfigsAction => {
  return {
    type: ActionTypes.SetConfig,
    payload: {
      logConfig,
    },
  }
}
