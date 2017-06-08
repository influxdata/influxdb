import {getJSONFeed as getJSONFeedAJAX} from 'src/status/apis'

import {errorThrown} from 'shared/actions/errors'

import {actionTypes} from 'src/status/constants'

const getJSONFeedRequested = url => ({
  type: actionTypes.GET_JSON_FEED_REQUESTED,
  payload: {url},
})

const getJSONFeedCompleted = (url, data) => ({
  type: actionTypes.GET_JSON_FEED_COMPLETED,
  payload: {url, data},
})

const getJSONFeedFailed = url => ({
  type: actionTypes.GET_JSON_FEED_FAILED,
  payload: {url},
})

export const getJSONFeedAsync = url => async dispatch => {
  dispatch(getJSONFeedRequested(url))
  try {
    const {data} = await getJSONFeedAJAX(url)
    dispatch(getJSONFeedCompleted(url, data))
  } catch (error) {
    dispatch(getJSONFeedFailed(url))
    dispatch(errorThrown(error, `Failed to get news feed from ${url}`))
  }
}
