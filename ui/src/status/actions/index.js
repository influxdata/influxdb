import {errorThrown} from 'shared/actions/errors'

import {actionTypes} from 'src/status/constants'

const getJSONFeedRequested = url => ({
  type: actionTypes.GET_JSON_FEED_REQUESTED,
  payload: {url},
})

const getJSONFeedSuccess = url => ({
  type: actionTypes.GET_JSON_FEED_SUCCESS,
  payload: {url},
})

export const getJSONFeedAsync = url => async dispatch => {
  dispatch(getJSONFeedRequested(url))
  try {
    dispatch(getJSONFeedSuccess(url))
  } catch (error) {
    dispatch(errorThrown(error, `Failed to get news feed from ${url}`))
  }
}
