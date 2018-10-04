import {Namespace} from 'src/types'
import {Source} from 'src/types/v2'
import {getSource} from 'src/sources/apis/v2'
import {getDeep} from 'src/utils/wrappers'
import AJAX from 'src/utils/ajax'

import {Filter, LogConfig, SearchStatus} from 'src/types/logs'

export const INITIAL_LIMIT = 1000

export enum ActionTypes {
  SetSource = 'LOGS_SET_SOURCE',
  SetNamespaces = 'LOGS_SET_NAMESPACES',
  SetNamespace = 'LOGS_SET_NAMESPACE',
  AddFilter = 'LOGS_ADD_FILTER',
  RemoveFilter = 'LOGS_REMOVE_FILTER',
  ChangeFilter = 'LOGS_CHANGE_FILTER',
  ClearFilters = 'LOGS_CLEAR_FILTERS',
  SetConfig = 'SET_CONFIG',
  SetSearchStatus = 'SET_SEARCH_STATUS',
}

export interface AddFilterAction {
  type: ActionTypes.AddFilter
  payload: {
    filter: Filter
  }
}

export interface ChangeFilterAction {
  type: ActionTypes.ChangeFilter
  payload: {
    id: string
    operator: string
    value: string
  }
}

export interface ClearFiltersAction {
  type: ActionTypes.ClearFilters
}

export interface RemoveFilterAction {
  type: ActionTypes.RemoveFilter
  payload: {
    id: string
  }
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

export interface SetConfigAction {
  type: ActionTypes.SetConfig
  payload: {
    logConfig: LogConfig
  }
}

interface SetSearchStatusAction {
  type: ActionTypes.SetSearchStatus
  payload: {
    searchStatus: SearchStatus
  }
}

export type Action =
  | SetSourceAction
  | SetNamespacesAction
  | SetNamespaceAction
  | AddFilterAction
  | RemoveFilterAction
  | ChangeFilterAction
  | ClearFiltersAction
  | SetConfigAction
  | SetSearchStatusAction

/**
 * Sets the search status corresponding to the current fetch request.
 * @param searchStatus the state of the current Logs Page fetch request.
 */
export const setSearchStatus = (
  searchStatus: SearchStatus
): SetSearchStatusAction => ({
  type: ActionTypes.SetSearchStatus,
  payload: {searchStatus},
})

export const changeFilter = (id: string, operator: string, value: string) => ({
  type: ActionTypes.ChangeFilter,
  payload: {id, operator, value},
})

export const setSource = (source: Source): SetSourceAction => ({
  type: ActionTypes.SetSource,
  payload: {source},
})

export const addFilter = (filter: Filter): AddFilterAction => ({
  type: ActionTypes.AddFilter,
  payload: {filter},
})

export const clearFilters = (): ClearFiltersAction => ({
  type: ActionTypes.ClearFilters,
})

export const removeFilter = (id: string): RemoveFilterAction => ({
  type: ActionTypes.RemoveFilter,
  payload: {id},
})

export const setNamespaceAsync = (namespace: Namespace) => async (
  dispatch
): Promise<void> => {
  dispatch({
    type: ActionTypes.SetNamespace,
    payload: {namespace},
  })
}

export const setNamespaces = (
  namespaces: Namespace[]
): SetNamespacesAction => ({
  type: ActionTypes.SetNamespaces,
  payload: {
    namespaces,
  },
})

export const populateNamespacesAsync = (
  bucketsLink: string,
  source: Source = null
) => async (dispatch): Promise<void> => {
  try {
    const {data: buckets} = await AJAX({url: bucketsLink, method: 'GET'})
    const namespaces: Namespace[] = buckets.map(b => ({
      database: b.name,
      retentionPolicy: b.rp,
    }))

    if (namespaces && namespaces.length > 0) {
      dispatch(setNamespaces(namespaces))
      if (source && source.telegraf) {
        const defaultNamespace = namespaces.find(
          namespace => namespace.database === source.telegraf
        )

        await dispatch(setNamespaceAsync(defaultNamespace))
      } else {
        await dispatch(setNamespaceAsync(namespaces[0]))
      }
    }
  } catch (e) {
    dispatch(setNamespaces([]))
    dispatch(setNamespaceAsync(null))
    throw new Error('Failed to populate namespaces')
  }
}

export const getSourceAndPopulateNamespacesAsync = (
  sourceURL: string
) => async (dispatch): Promise<void> => {
  const source = await getSource(sourceURL)

  const bucketsLink = getDeep<string | null>(source, 'links.buckets', null)

  if (bucketsLink) {
    dispatch(setSource(source))

    try {
      await dispatch(populateNamespacesAsync(bucketsLink, source))
      await dispatch(setSearchStatus(SearchStatus.UpdatingSource))
    } catch (e) {
      await dispatch(setSearchStatus(SearchStatus.SourceError))
    }
  }
}

export const setConfig = (logConfig: LogConfig): SetConfigAction => {
  return {
    type: ActionTypes.SetConfig,
    payload: {
      logConfig,
    },
  }
}
