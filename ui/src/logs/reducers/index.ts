import _ from 'lodash'

import {
  ActionTypes,
  Action,
  RemoveFilterAction,
  AddFilterAction,
  ChangeFilterAction,
  SetConfigAction,
} from 'src/logs/actions'

import {
  DEFAULT_TRUNCATION,
  DEFAULT_TAIL_CHUNK_DURATION_MS,
  defaultTableData,
} from 'src/logs/constants'
import {LogsState, SearchStatus, SeverityFormatOptions} from 'src/types/logs'

export const defaultState: LogsState = {
  currentSource: null,
  currentBuckets: [],
  currentBucket: null,
  tableQueryConfig: null,
  filters: [],
  queryCount: 0,
  logConfig: {
    id: null,
    link: null,
    tableColumns: [],
    severityFormat: SeverityFormatOptions.DotText,
    severityLevelColors: [],
    isTruncated: DEFAULT_TRUNCATION,
  },
  searchStatus: SearchStatus.None,
  tableInfiniteData: {
    forward: defaultTableData,
    backward: defaultTableData,
  },
  currentTailUpperBound: undefined,
  nextTailLowerBound: undefined,
  tailChunkDurationMs: DEFAULT_TAIL_CHUNK_DURATION_MS,
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

  return {...state, filters: [filter, ..._.get(state, 'filters', [])]}
}

const clearFilters = (state: LogsState): LogsState => {
  return {...state, filters: []}
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

export const setConfigs = (
  state: LogsState,
  action: SetConfigAction
): LogsState => {
  const {
    logConfig: {
      id,
      link,
      tableColumns,
      severityFormat,
      severityLevelColors,
      isTruncated,
    },
  } = action.payload
  const updatedLogConfig = {
    id,
    link,
    tableColumns,
    severityFormat,
    severityLevelColors,
    isTruncated,
  }
  return {...state, logConfig: updatedLogConfig}
}

const clearTableData = (state: LogsState) => {
  return {
    ...state,
    tableInfiniteData: {
      forward: defaultTableData,
      backward: defaultTableData,
    },
  }
}

export default (state: LogsState = defaultState, action: Action) => {
  switch (action.type) {
    case ActionTypes.SetSource:
      return {...state, currentSource: action.payload.source}
    case ActionTypes.SetBuckets:
      return {...state, currentBuckets: action.payload.buckets}
    case ActionTypes.SetBucket:
      return {...state, currentBucket: action.payload.bucket}
    case ActionTypes.SetSearchStatus:
      return {...state, searchStatus: action.payload.searchStatus}
    case ActionTypes.SetTableQueryConfig:
      return {...state, tableQueryConfig: action.payload.queryConfig}
    case ActionTypes.SetCurrentTailUpperBound:
      return {...state, currentTailUpperBound: action.payload.upper}
    case ActionTypes.SetNextTailLowerBound:
      return {...state, nextTailLowerBound: action.payload.lower}
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
    case ActionTypes.AddFilter:
      return addFilter(state, action)
    case ActionTypes.RemoveFilter:
      return removeFilter(state, action)
    case ActionTypes.ChangeFilter:
      return changeFilter(state, action)
    case ActionTypes.ClearFilters:
      return clearFilters(state)
    case ActionTypes.SetConfig:
      return setConfigs(state, action)
    case ActionTypes.ClearTableData:
      return clearTableData(state)
    default:
      return state
  }
}
