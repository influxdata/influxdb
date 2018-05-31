import _ from 'lodash'
import {Source, Namespace, TimeRange, QueryConfig} from 'src/types'
import {getSource} from 'src/shared/apis'
import {getDatabasesWithRetentionPolicies} from 'src/shared/apis/databases'
import {
  buildHistogramQueryConfig,
  buildTableQueryConfig,
  buildLogQuery,
} from 'src/logs/utils'
import {getDeep} from 'src/utils/wrappers'
import buildQuery from 'src/utils/influxql'
import {executeQueryAsync} from 'src/logs/api'
import {LogsState} from 'src/types/localStorage'

interface TableData {
  columns: string[]
  values: string[]
}

const defaultTableData = {
  columns: [
    'time',
    'message',
    'facility_code',
    'procid',
    'severity_code',
    'timestamp',
    'version',
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
  SetTimeRange = 'LOGS_SET_TIMERANGE',
  SetNamespace = 'LOGS_SET_NAMESPACE',
  SetHistogramQueryConfig = 'LOGS_SET_HISTOGRAM_QUERY_CONFIG',
  SetHistogramData = 'LOGS_SET_HISTOGRAM_DATA',
  SetTableQueryConfig = 'LOGS_SET_TABLE_QUERY_CONFIG',
  SetTableData = 'LOGS_SET_TABLE_DATA',
  ChangeZoom = 'LOGS_CHANGE_ZOOM',
  SetSearchTerm = 'LOGS_SET_SEARCH_TERM',
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

interface SetTimeRangeAction {
  type: ActionTypes.SetTimeRange
  payload: {
    timeRange: TimeRange
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

interface ChangeZoomAction {
  type: ActionTypes.ChangeZoom
  payload: {
    data: object[]
    timeRange: TimeRange
  }
}

export type Action =
  | SetSourceAction
  | SetNamespacesAction
  | SetTimeRangeAction
  | SetNamespaceAction
  | SetHistogramQueryConfig
  | SetHistogramData
  | ChangeZoomAction
  | SetTableData
  | SetTableQueryConfig
  | SetSearchTerm

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

export const setSource = (source: Source): SetSourceAction => ({
  type: ActionTypes.SetSource,
  payload: {source},
})

const setHistogramData = (response): SetHistogramData => ({
  type: ActionTypes.SetHistogramData,
  payload: {data: [{response}]},
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

  if (_.every([queryConfig, timeRange, namespace, proxyLink])) {
    const query = buildLogQuery(timeRange, queryConfig, searchTerm)
    const response = await executeQueryAsync(proxyLink, namespace, query)

    dispatch(setHistogramData(response))
  }
}

const setTableData = (series: TableData): SetTableData => ({
  type: ActionTypes.SetTableData,
  payload: {data: {columns: series.columns, values: _.reverse(series.values)}},
})

export const executeTableQueryAsync = () => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  const state = getState()

  const queryConfig = getTableQueryConfig(state)
  const timeRange = getTimeRange(state)
  const namespace = getNamespace(state)
  const proxyLink = getProxyLink(state)
  const searchTerm = getSearchTerm(state)

  if (_.every([queryConfig, timeRange, namespace, proxyLink])) {
    const query = buildLogQuery(timeRange, queryConfig, searchTerm)
    const response = await executeQueryAsync(proxyLink, namespace, query)

    const series = getDeep(response, 'results.0.series.0', defaultTableData)

    dispatch(setTableData(series))
  }
}

export const executeQueriesAsync = () => async dispatch => {
  dispatch(executeHistogramQueryAsync())
  dispatch(executeTableQueryAsync())
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
    const queryConfig = buildHistogramQueryConfig(namespace, timeRange)

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

export const setTimeRangeAsync = (timeRange: TimeRange) => async (
  dispatch
): Promise<void> => {
  dispatch({
    type: ActionTypes.SetTimeRange,
    payload: {
      timeRange,
    },
  })
  dispatch(setHistogramQueryConfigAsync())
  dispatch(setTableQueryConfigAsync())
}

export const populateNamespacesAsync = (proxyLink: string) => async (
  dispatch
): Promise<void> => {
  const namespaces = await getDatabasesWithRetentionPolicies(proxyLink)

  if (namespaces && namespaces.length > 0) {
    dispatch(setNamespaces(namespaces))
    dispatch(setNamespaceAsync(namespaces[0]))
  }
}

export const getSourceAndPopulateNamespacesAsync = (sourceID: string) => async (
  dispatch
): Promise<void> => {
  const response = await getSource(sourceID)
  const source = response.data

  const proxyLink = getDeep<string | null>(source, 'links.proxy', null)

  if (proxyLink) {
    dispatch(setSource(source))
    dispatch(populateNamespacesAsync(proxyLink))
  }
}

export const changeZoomAsync = (timeRange: TimeRange) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  const state = getState()

  const namespace = getNamespace(state)
  const proxyLink = getProxyLink(state)

  if (namespace && proxyLink) {
    const queryConfig = buildHistogramQueryConfig(namespace, timeRange)
    const query = buildQuery(timeRange, queryConfig)
    const response = await executeQueryAsync(proxyLink, namespace, query)

    dispatch({
      type: ActionTypes.ChangeZoom,
      payload: {
        data: [{response}],
        timeRange,
      },
    })

    await dispatch(setTimeRangeAsync(timeRange))
    await dispatch(executeTableQueryAsync())
  }
}
