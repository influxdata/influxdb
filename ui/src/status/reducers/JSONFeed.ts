import {JSONFeedData} from 'src/types'
import {Action, ActionTypes} from 'src/status/actions'

export interface State {
  hasCompletedFetchOnce: boolean
  isFetching: boolean
  isFailed: boolean
  data: JSONFeedData
}

const initialState: State = {
  hasCompletedFetchOnce: false,
  isFetching: false,
  isFailed: false,
  data: null,
}

const JSONFeedReducer = (state: State = initialState, action: Action) => {
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
