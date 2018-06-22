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
} from 'src/logs/actions'

import {RemoteDataState} from 'src/types'
import {LogsState} from 'src/types/logs'

const defaultState: LogsState = {
  currentSource: null,
  currentNamespaces: [],
  timeRange: {lower: 'now() - 1m', upper: null},
  currentNamespace: null,
  histogramQueryConfig: null,
  tableQueryConfig: null,
  tableData: {columns: [], values: []},
  histogramData: [],
  histogramDataStatus: RemoteDataState.NotStarted,
  searchTerm: '',
  filters: [],
  queryCount: 0,
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

export default (state: LogsState = defaultState, action: Action) => {
  switch (action.type) {
    case ActionTypes.SetSource:
      return {...state, currentSource: action.payload.source}
    case ActionTypes.SetNamespaces:
      return {...state, currentNamespaces: action.payload.namespaces}
    case ActionTypes.SetTimeRange:
      return {...state, timeRange: action.payload.timeRange}
    case ActionTypes.SetNamespace:
      return {...state, currentNamespace: action.payload.namespace}
    case ActionTypes.SetHistogramQueryConfig:
      return {...state, histogramQueryConfig: action.payload.queryConfig}
    case ActionTypes.SetHistogramData:
      return {...state, histogramData: action.payload.data}
    case ActionTypes.SetHistogramDataStatus:
      return {...state, histogramDataStatus: action.payload}
    case ActionTypes.SetTableQueryConfig:
      return {...state, tableQueryConfig: action.payload.queryConfig}
    case ActionTypes.SetTableData:
      return {...state, tableData: action.payload.data}
    case ActionTypes.ChangeZoom:
      return {...state, timeRange: action.payload.timeRange}
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
    default:
      return state
  }
}
