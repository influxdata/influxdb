import {ActionTypes, Action} from 'src/logs/actions'
import {LogsState} from 'src/types/localStorage'

const defaultState: LogsState = {
  currentSource: null,
  currentNamespaces: [],
  timeRange: {lower: 'now() - 1m', upper: null},
  currentNamespace: null,
  histogramQueryConfig: null,
  histogramData: [],
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
    case ActionTypes.ChangeZoom:
      const {timeRange, data} = action.payload
      return {...state, timeRange, histogramData: data}
    default:
      return state
  }
}
