import {Source, Namespace, TimeRange} from 'src/types'
import {ActionTypes, Action} from 'src/logs/actions'

interface LogsState {
  currentSource: Source | null
  currentNamespaces: Namespace[]
  currentNamespace: Namespace | null
  timeRange: TimeRange
}

const defaultState = {
  currentSource: null,
  currentNamespaces: [],
  timeRange: {lower: 'now() - 1m', upper: null},
  currentNamespace: null,
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
    default:
      return state
  }
}
