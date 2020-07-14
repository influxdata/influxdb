// Libraries
import {produce} from 'immer'

// Actions
import {Action} from 'src/queryCache/actions'

// Types
import {CancelBox} from 'src/types/promises'
import {RunQueryResult} from 'src/shared/apis/query'

export interface QueryCacheState {
  queryResultsByQueryID: {[queryID: string]: CancelBox<RunQueryResult>}
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
        const {queryID, queryPromise} = action
        draftState.queryResultsByQueryID[queryID] = queryPromise
      })
    }

    case 'RESET_CACHED_QUERY_RESULTS': {
      return {queryResultsByQueryID: {}}
    }
  }

  return state
}
