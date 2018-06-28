import moment from 'moment'
import _ from 'lodash'
import {Source, Namespace, TimeRange, QueryConfig} from 'src/types'
import {getSource} from 'src/shared/apis'
import {getDatabasesWithRetentionPolicies} from 'src/shared/apis/databases'
import {
  buildHistogramQueryConfig,
  buildTableQueryConfig,
  buildLogQuery,
  parseHistogramQueryResponse,
} from 'src/logs/utils'
import {getDeep} from 'src/utils/wrappers'
import {executeQueryAsync} from 'src/logs/api'
import {LogsState, Filter, TableData} from 'src/types/logs'

const defaultTableData: TableData = {
  columns: [
    'time',
    'severity',
    'timestamp',
    'severity_1',
    'facility',
    'procid',
    'application',
    'host',
    'message',
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
  AddFilter = 'LOGS_ADD_FILTER',
  RemoveFilter = 'LOGS_REMOVE_FILTER',
  ChangeFilter = 'LOGS_CHANGE_FILTER',
  IncrementQueryCount = 'LOGS_INCREMENT_QUERY_COUNT',
  DecrementQueryCount = 'LOGS_DECREMENT_QUERY_COUNT',
  ConcatMoreLogs = 'LOGS_CONCAT_MORE_LOGS',
}

export interface ConcatMoreLogsAction {
  type: ActionTypes.ConcatMoreLogs
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
  | AddFilterAction
  | RemoveFilterAction
  | ChangeFilterAction
  | DecrementQueryCountAction
  | IncrementQueryCountAction
  | ConcatMoreLogsAction

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

  if (_.every([queryConfig, timeRange, namespace, proxyLink])) {
    const query = buildLogQuery(timeRange, queryConfig, filters, searchTerm)

    const response = await executeQueryAsync(proxyLink, namespace, query)
    const data = parseHistogramQueryResponse(response)

    dispatch(setHistogramData(data))
  }
}

const setTableData = (series: TableData): SetTableData => ({
  type: ActionTypes.SetTableData,
  payload: {data: {columns: series.columns, values: series.values}},
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
  const filters = getFilters(state)

  if (_.every([queryConfig, timeRange, namespace, proxyLink])) {
    const query = buildLogQuery(timeRange, queryConfig, filters, searchTerm)
    const response = await executeQueryAsync(
      proxyLink,
      namespace,
      `${query} ORDER BY time DESC LIMIT 1000`
    )

    const series = getDeep(response, 'results.0.series.0', defaultTableData)

    dispatch(setTableData(series))
  }
}

export const decrementQueryCount = () => ({
  type: ActionTypes.DecrementQueryCount,
})

export const incrementQueryCount = () => ({
  type: ActionTypes.IncrementQueryCount,
})

export const executeQueriesAsync = () => async dispatch => {
  dispatch(incrementQueryCount())
  try {
    await Promise.all([
      dispatch(executeHistogramQueryAsync()),
      dispatch(executeTableQueryAsync()),
    ])
  } catch (ex) {
    console.error('Could not make query requests')
  } finally {
    dispatch(decrementQueryCount())
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

export const fetchMoreAsync = (
  queryTimeEnd: string,
  lastTime: number
) => async (dispatch, getState): Promise<void> => {
  const state = getState()
  const tableQueryConfig = getTableQueryConfig(state)
  const time = moment(lastTime).toISOString()
  const timeRange = {lower: queryTimeEnd, upper: time}
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
    const query = buildLogQuery(timeRange, newQueryConfig, filters, searchTerm)
    const response = await executeQueryAsync(
      proxyLink,
      namespace,
      `${query} ORDER BY time DESC LIMIT 1000`
    )

    const series = getDeep(response, 'results.0.series.0', defaultTableData)
    await dispatch(ConcatMoreLogs(series))
  }
}

export const ConcatMoreLogs = (series: TableData): ConcatMoreLogsAction => ({
  type: ActionTypes.ConcatMoreLogs,
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

export const changeZoomAsync = (timeRange: TimeRange) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  const state = getState()
  const namespace = getNamespace(state)
  const proxyLink = getProxyLink(state)

  if (namespace && proxyLink) {
    await dispatch(setTimeRangeAsync(timeRange))
    await dispatch(executeTableQueryAsync())
  }
}
