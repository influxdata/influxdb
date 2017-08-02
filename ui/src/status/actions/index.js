// he is a library for safely encoding and decoding HTML Entities
import he from 'he'

import {fetchJSONFeed as fetchJSONFeedAJAX} from 'src/status/apis'

import {errorThrown} from 'shared/actions/errors'

import * as actionTypes from 'src/status/constants/actionTypes'

const fetchJSONFeedRequested = () => ({
  type: actionTypes.FETCH_JSON_FEED_REQUESTED,
})

const fetchJSONFeedCompleted = data => ({
  type: actionTypes.FETCH_JSON_FEED_COMPLETED,
  payload: {data},
})

const fetchJSONFeedFailed = () => ({
  type: actionTypes.FETCH_JSON_FEED_FAILED,
})

export const fetchJSONFeedAsync = url => async dispatch => {
  dispatch(fetchJSONFeedRequested())
  try {
    const {data} = await fetchJSONFeedAJAX(url)
    // data could be from a webpage, and thus would be HTML
    if (typeof data === 'string' || !data) {
      dispatch(fetchJSONFeedFailed())
    } else {
      // decode HTML entities from response text
      const decodedData = {
        ...data,
        items: data.items.map(item => {
          item.title = he.decode(item.title)
          item.content_text = he.decode(item.content_text)
          return item
        }),
      }

      dispatch(fetchJSONFeedCompleted(decodedData))
    }
  } catch (error) {
    console.error(error)
    dispatch(fetchJSONFeedFailed())
    dispatch(
      errorThrown(
        error,
        `Failed to fetch JSON Feed for News Feed from '${url}'`
      )
    )
  }
}
