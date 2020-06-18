// Libraries
import {produce} from 'immer'

// Actions
import {Action} from 'src/queryCache/actions'

export interface QueryCacheState {
  queryResultsByQueryID: {[queryID: string]: string[]}
}

export const initialState: QueryCacheState = {
  queryResultsByQueryID: {},
}

export const queryCacheReducer = (
  state: QueryCacheState = initialState,
  action: Action
): QueryCacheState => {
  switch (action.type) {
    case 'SET_QUERY_RESULTS_BY_QUERY': {
      return produce(state, draftState => {
        const {queryID, files} = action
        draftState.queryResultsByQueryID[queryID] = files
      })
    }

    case 'RESET_CACHED_QUERY_RESULTS': {
      return {queryResultsByQueryID: {}}
    }
  }

  return state
}
