import * as actionTypes from 'src/status/constants/actionTypes'

const initialState = {
  hasCompletedFetchOnce: false,
  isFetching: false,
  isFailed: false,
  data: null,
}

const JSONFeedReducer = (state = initialState, action) => {
  switch (action.type) {
    case actionTypes.FETCH_JSON_FEED_REQUESTED: {
      return {...state, isFetching: true, isFailed: false}
    }

    case actionTypes.FETCH_JSON_FEED_COMPLETED: {
      const {data} = action.payload

      return {
        ...state,
        hasCompletedFetchOnce: true,
        isFetching: false,
        isFailed: false,
        data,
      }
    }

    case actionTypes.FETCH_JSON_FEED_FAILED: {
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
