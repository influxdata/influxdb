// Libraries
import {produce} from 'immer'

// Actions
import {Action} from 'src/data/actions'

export interface DataState {
  queryResultsByQueryID: {[queryID: string]: string[]}
}

export const initialState: DataState = {
  queryResultsByQueryID: {},
}

export const dataReducer = (
  state: DataState = initialState,
  action: Action
): DataState => {
  switch (action.type) {
    case 'SET_QUERY_RESULTS_BY_QUERY': {
      return produce(state, draftState => {
        const {queryID, files} = action
        if (queryID && files.length) {
          draftState.queryResultsByQueryID[queryID] = files
        }
      })
    }

    case 'RESET_CACHED_QUERY_RESULTS': {
      return initialState
    }
  }

  return state
}
