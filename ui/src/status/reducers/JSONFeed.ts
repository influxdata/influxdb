import {JSONFeedData} from 'src/types'
import {Action, ActionTypes} from 'src/status/actions'

export interface JSONFeedReducerState {
  hasCompletedFetchOnce: boolean
  isFetching: boolean
  isFailed: boolean
  data: JSONFeedData
}

const initialState: JSONFeedReducerState = {
  hasCompletedFetchOnce: false,
  isFetching: false,
  isFailed: false,
  data: null,
}

const JSONFeedReducer = (
  state: JSONFeedReducerState = initialState,
  action: Action
): JSONFeedReducerState => {
  switch (action.type) {
    case ActionTypes.FETCH_JSON_FEED_REQUESTED: {
      return {...state, isFetching: true, isFailed: false}
    }

    case ActionTypes.FETCH_JSON_FEED_COMPLETED: {
      const {data} = action.payload

      return {
        ...state,
        hasCompletedFetchOnce: true,
        isFetching: false,
        isFailed: false,
        data,
      }
    }

    case ActionTypes.FETCH_JSON_FEED_FAILED: {
      return {
        ...state,
        isFetching: false,
        isFailed: true,
        data: null,
      }
    }

    default: {
      return state
    }
  }
}

export default JSONFeedReducer
