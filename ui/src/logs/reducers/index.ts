import _ from 'lodash'

import {
  ActionTypes,
  Action,
  RemoveFilterAction,
  AddFilterAction,
  ChangeFilterAction,
  DecrementQueryCountAction,
  IncrementQueryCountAction,
  ConcatMoreLogsAction,
  SetConfigsAction,
} from 'src/logs/actions'

import {SeverityFormatOptions} from 'src/logs/constants'
import {LogsState} from 'src/types/logs'

export const defaultState: LogsState = {
  currentSource: null,
  currentNamespaces: [],
  timeRange: {
    upper: null,
    lower: 'now() - 1m',
    seconds: 60,
    windowOption: '1m',
    timeOption: 'now',
  },
  currentNamespace: null,
  histogramQueryConfig: null,
  tableQueryConfig: null,
  tableData: {columns: [], values: []},
  histogramData: [],
  searchTerm: '',
  filters: [],
  queryCount: 0,
  logConfig: {
    tableColumns: [],
    severityFormat: SeverityFormatOptions.dotText,
  },
}

const removeFilter = (
  state: LogsState,
  action: RemoveFilterAction
): LogsState => {
  const {id} = action.payload
  const filters = _.filter(
    _.get(state, 'filters', []),
    filter => filter.id !== id
  )

  return {...state, filters}
}

const addFilter = (state: LogsState, action: AddFilterAction): LogsState => {
  const {filter} = action.payload

  return {...state, filters: [..._.get(state, 'filters', []), filter]}
}

const changeFilter = (
  state: LogsState,
  action: ChangeFilterAction
): LogsState => {
  const {id, operator, value} = action.payload

  const mappedFilters = _.map(_.get(state, 'filters', []), f => {
    if (f.id === id) {
      return {...f, operator, value}
    }
    return f
  })

  return {...state, filters: mappedFilters}
}

const decrementQueryCount = (
  state: LogsState,
  __: DecrementQueryCountAction
) => {
  const {queryCount} = state
  return {...state, queryCount: Math.max(queryCount - 1, 0)}
}

const incrementQueryCount = (
  state: LogsState,
  __: IncrementQueryCountAction
) => {
  const {queryCount} = state
  return {...state, queryCount: queryCount + 1}
}

const concatMoreLogs = (
  state: LogsState,
  action: ConcatMoreLogsAction
): LogsState => {
  const {
    series: {values},
  } = action.payload
  const {tableData} = state
  const vals = [...tableData.values, ...values]
  return {
    ...state,
    tableData: {
      columns: tableData.columns,
      values: vals,
    },
  }
}

export const setConfigs = (state: LogsState, action: SetConfigsAction) => {
  const {logConfig} = state
  const {
    logConfig: {tableColumns, severityFormat},
  } = action.payload
  const updatedLogConfig = {...logConfig, tableColumns, severityFormat}
  return {...state, logConfig: updatedLogConfig}
}

export default (state: LogsState = defaultState, action: Action) => {
  switch (action.type) {
    case ActionTypes.SetSource:
      return {...state, currentSource: action.payload.source}
    case ActionTypes.SetNamespaces:
      return {...state, currentNamespaces: action.payload.namespaces}
    case ActionTypes.SetTimeBounds:
      const {upper, lower} = action.payload.timeBounds
      return {...state, timeRange: {...state.timeRange, upper, lower}}
    case ActionTypes.SetTimeWindow:
      const {windowOption, seconds} = action.payload.timeWindow
      return {...state, timeRange: {...state.timeRange, windowOption, seconds}}
    case ActionTypes.SetTimeMarker:
      const {timeOption} = action.payload.timeMarker
      return {...state, timeRange: {...state.timeRange, timeOption}}
    case ActionTypes.SetNamespace:
      return {...state, currentNamespace: action.payload.namespace}
    case ActionTypes.SetHistogramQueryConfig:
      return {...state, histogramQueryConfig: action.payload.queryConfig}
    case ActionTypes.SetHistogramData:
      return {...state, histogramData: action.payload.data}
    case ActionTypes.SetTableQueryConfig:
      return {...state, tableQueryConfig: action.payload.queryConfig}
    case ActionTypes.SetTableData:
      return {...state, tableData: action.payload.data}
    case ActionTypes.SetSearchTerm:
      const {searchTerm} = action.payload
      return {...state, searchTerm}
    case ActionTypes.AddFilter:
      return addFilter(state, action)
    case ActionTypes.RemoveFilter:
      return removeFilter(state, action)
    case ActionTypes.ChangeFilter:
      return changeFilter(state, action)
    case ActionTypes.IncrementQueryCount:
      return incrementQueryCount(state, action)
    case ActionTypes.DecrementQueryCount:
      return decrementQueryCount(state, action)
    case ActionTypes.ConcatMoreLogs:
      return concatMoreLogs(state, action)
    case ActionTypes.SetConfig:
      return setConfigs(state, action)
    default:
      return state
  }
}
