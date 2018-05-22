import {Source, Namespace, TimeRange} from 'src/types'
import {getSource} from 'src/shared/apis'
import {getDatabasesWithRetentionPolicies} from 'src/shared/apis/databases'

export enum ActionTypes {
  SetSource = 'LOGS_SET_SOURCE',
  SetNamespaces = 'LOGS_SET_NAMESPACES',
  SetTimeRange = 'LOGS_SET_TIMERANGE',
  SetNamespace = 'LOGS_SET_NAMESPACE',
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

export type Action =
  | SetSourceAction
  | SetNamespacesAction
  | SetTimeRangeAction
  | SetNamespaceAction

export const setSource = (source: Source): SetSourceAction => ({
  type: ActionTypes.SetSource,
  payload: {
    source,
  },
})

export const setNamespace = (namespace: Namespace): SetNamespaceAction => ({
  type: ActionTypes.SetNamespace,
  payload: {
    namespace,
  },
})

export const setNamespaces = (
  namespaces: Namespace[]
): SetNamespacesAction => ({
  type: ActionTypes.SetNamespaces,
  payload: {
    namespaces,
  },
})

export const setTimeRange = (timeRange: TimeRange): SetTimeRangeAction => ({
  type: ActionTypes.SetTimeRange,
  payload: {
    timeRange,
  },
})

export const getSourceAsync = sourceID => async dispatch => {
  const response = await getSource(sourceID)
  const source = response.data
  const namespaces = await getDatabasesWithRetentionPolicies(source.links.proxy)

  dispatch(setSource(source))
  dispatch(setNamespaces(namespaces))
}
