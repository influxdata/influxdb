import {Source, Namespace, TimeRange, QueryConfig} from 'src/types'
import {getSource} from 'src/shared/apis'
import {getDatabasesWithRetentionPolicies} from 'src/shared/apis/databases'
import {buildHistogramQueryConfig} from 'src/logs/utils'
import {getDeep} from 'src/utils/wrappers'
import buildQuery from 'src/utils/influxql'
import {executeQueryAsync} from 'src/logs/api'
import {LogsState} from 'src/types/localStorage'

type GetState = () => {logs: LogsState}

export enum ActionTypes {
  SetSource = 'LOGS_SET_SOURCE',
  SetNamespaces = 'LOGS_SET_NAMESPACES',
  SetTimeRange = 'LOGS_SET_TIMERANGE',
  SetNamespace = 'LOGS_SET_NAMESPACE',
  SetHistogramQueryConfig = 'LOGS_SET_HISTOGRAM_QUERY_CONFIG',
  SetHistogramData = 'LOGS_SET_HISTOGRAM_DATA',
  ChangeZoom = 'LOGS_CHANGE_ZOOM',
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

export const setSource = (source: Source): SetSourceAction => ({
  type: ActionTypes.SetSource,
  payload: {
    source,
  },
})

export const executeHistogramQueryAsync = () => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  const state = getState()
  const queryConfig = getDeep<QueryConfig | null>(
    state,
    'logs.histogramQueryConfig',
    null
  )
  const timeRange = getDeep<TimeRange | null>(state, 'logs.timeRange', null)
  const namespace = getDeep<Namespace | null>(
    state,
    'logs.currentNamespace',
    null
  )
  const proxyLink = getDeep<string | null>(
    state,
    'logs.currentSource.links.proxy',
    null
  )

  if (queryConfig && timeRange && namespace && proxyLink) {
    const query = buildQuery(timeRange, queryConfig)
    const response = await executeQueryAsync(proxyLink, namespace, query)

    dispatch({
      type: ActionTypes.SetHistogramData,
      payload: {
        data: [{response}],
      },
    })
  }
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

export const setNamespaceAsync = (namespace: Namespace) => async (
  dispatch
): Promise<void> => {
  dispatch({
    type: ActionTypes.SetNamespace,
    payload: {namespace},
  })

  dispatch(setHistogramQueryConfigAsync())
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
  const namespace = getDeep<Namespace | null>(
    state,
    'logs.currentNamespace',
    null
  )
  const proxyLink = getDeep<string | null>(
    state,
    'logs.currentSource.links.proxy',
    null
  )

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
  }
}
