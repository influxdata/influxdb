import {getJSONFeed as getJSONFeedAJAX} from 'src/status/apis'

import {errorThrown} from 'shared/actions/errors'

import * as actionTypes from 'src/status/constants/actionTypes'

const getJSONFeedRequested = () => ({
  type: actionTypes.GET_JSON_FEED_REQUESTED,
})

const getJSONFeedCompleted = data => ({
  type: actionTypes.GET_JSON_FEED_COMPLETED,
  payload: {data},
})

const getJSONFeedFailed = () => ({
  type: actionTypes.GET_JSON_FEED_FAILED,
})

export const getJSONFeedAsync = url => async dispatch => {
  dispatch(getJSONFeedRequested())
  try {
    const {data} = await getJSONFeedAJAX(url)
    dispatch(getJSONFeedCompleted(data))
  } catch (error) {
    dispatch(getJSONFeedFailed())
    dispatch(errorThrown(error, `Failed to get news feed from ${url}`))
  }
}
