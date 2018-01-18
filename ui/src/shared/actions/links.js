import {getLinks as getLinksAJAX} from 'shared/apis/links'

import {errorThrown} from 'shared/actions/errors'

import * as actionTypes from 'shared/constants/actionTypes'
import {linksLink} from 'shared/constants'

const linksGetRequested = () => ({
  type: actionTypes.LINKS_GET_REQUESTED,
})

const linksGetCompleted = links => {
  console.log('linksGetCompleted', links, actionTypes.LINKS_GET_COMPLETED)
  return {
    type: actionTypes.LINKS_GET_COMPLETED,
    payload: {links},
  }
}

const linksGetFailed = () => ({
  type: actionTypes.LINKS_GET_FAILED,
})

export const getLinksAsync = () => async dispatch => {
  console.log('getLinksAsync dispatch', dispatch)
  dispatch(linksGetRequested())
  try {
    const {data} = await getLinksAJAX()
    console.log('getLinksAJAX links', data)
    dispatch(linksGetCompleted(data))
  } catch (error) {
    const message = `Failed to retrieve api links from ${linksLink}`
    dispatch(errorThrown(error, message))
    dispatch(linksGetFailed())
  }
}
