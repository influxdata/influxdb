import _ from 'lodash'

import {
  ActionTypes,
  Action,
  RemoveFilterAction,
  AddFilterAction,
  ChangeFilterAction,
  SetConfigAction,
} from 'src/logs/actions'

import {DEFAULT_TRUNCATION} from 'src/logs/constants'
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
    default:
      return state
  }
}
