import _ from 'lodash'
import {
  ActionTypes,
  Action,
  RemoveFilterAction,
  AddFilterAction,
  SetFilterOperatorAction,
} from 'src/logs/actions'
import {LogsState} from 'src/types/logs'

const defaultState: LogsState = {
  currentSource: null,
  currentNamespaces: [],
  timeRange: {lower: 'now() - 1m', upper: null},
  currentNamespace: null,
  histogramQueryConfig: null,
  tableQueryConfig: null,
  tableData: [],
  histogramData: [],
  searchTerm: null,
  filters: [],
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

const setFilterOperator = (
  state: LogsState,
  action: SetFilterOperatorAction
): LogsState => {
  const {id, operator} = action.payload

  const mappedFilters = _.map(_.get(state, 'filters', []), f => {
    if (f.id === id) {
      return {...f, operator}
    }
    return f
  })

  return {...state, filters: mappedFilters}
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
    case ActionTypes.SetTableQueryConfig:
      return {...state, tableQueryConfig: action.payload.queryConfig}
    case ActionTypes.SetTableData:
      return {...state, tableData: action.payload.data}
    case ActionTypes.ChangeZoom:
      const {timeRange, data} = action.payload
      return {...state, timeRange, histogramData: data}
    case ActionTypes.SetSearchTerm:
      const {searchTerm} = action.payload
      return {...state, searchTerm}
    case ActionTypes.AddFilter:
      return addFilter(state, action)
    case ActionTypes.RemoveFilter:
      return removeFilter(state, action)
    case ActionTypes.SetFilterOperator:
      return setFilterOperator(state, action)
    default:
      return state
  }
}
