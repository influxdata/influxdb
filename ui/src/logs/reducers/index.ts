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
  PrependMoreLogsAction,
  SetConfigsAction,
} from 'src/logs/actions'

import {SeverityFormatOptions} from 'src/logs/constants'
import {LogsState, TableData} from 'src/types/logs'

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
    severityLevelColors: [],
  },
  tableTime: {},
  tableInfiniteData: {
    forward: defaultTableData,
    backward: defaultTableData,
  },
  newRowsAdded: 0,
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
  const {tableInfiniteData} = state
  const {backward} = tableInfiniteData
  const vals = [...backward.values, ...values]

  return {
    ...state,
    tableInfiniteData: {
      ...tableInfiniteData,
      backward: {
        columns: backward.columns,
        values: vals,
      },
    },
  }
}

const prependMoreLogs = (
  state: LogsState,
  action: PrependMoreLogsAction
): LogsState => {
  const {
    series: {values},
  } = action.payload
  const {tableInfiniteData} = state
  const {forward} = tableInfiniteData
  const vals = [...values, ...forward.values]

  const uniqueValues = _.uniqBy(vals, '0')
  const newRowsAdded = uniqueValues.length - forward.values.length

  return {
    ...state,
    newRowsAdded,
    tableInfiniteData: {
      ...tableInfiniteData,
      forward: {
        columns: forward.columns,
        values: uniqueValues,
      },
    },
  }
}

export const setConfigs = (state: LogsState, action: SetConfigsAction) => {
  const {logConfig} = state
  const {
    logConfig: {tableColumns, severityFormat, severityLevelColors},
  } = action.payload
  const updatedLogConfig = {
    ...logConfig,
    tableColumns,
    severityFormat,
    severityLevelColors,
  }
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
    case ActionTypes.ClearRowsAdded:
      return {...state, newRowsAdded: null}
    case ActionTypes.SetTableForwardData:
      return {
        ...state,
        tableInfiniteData: {
          ...state.tableInfiniteData,
          forward: action.payload.data,
        },
      }
    case ActionTypes.SetTableBackwardData:
      return {
        ...state,
        tableInfiniteData: {
          ...state.tableInfiniteData,
          backward: action.payload.data,
        },
      }
    case ActionTypes.SetSearchTerm:
      const {searchTerm} = action.payload
      return {...state, searchTerm}
    case ActionTypes.SetTableCustomTime:
      return {...state, tableTime: {custom: action.payload.time}}
    case ActionTypes.SetTableRelativeTime:
      return {...state, tableTime: {relative: action.payload.time}}
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
    case ActionTypes.PrependMoreLogs:
      return prependMoreLogs(state, action)
    case ActionTypes.SetConfig:
      return setConfigs(state, action)
    default:
      return state
  }
}
