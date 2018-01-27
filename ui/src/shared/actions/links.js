import {getLinks as getLinksAJAX} from 'shared/apis/links'

import {errorThrown} from 'shared/actions/errors'

import {linksLink} from 'shared/constants'

const linksGetRequested = () => ({
  type: 'LINKS_GET_REQUESTED',
})

const linksGetCompleted = links => ({
  type: 'LINKS_GET_COMPLETED',
  payload: {links},
})

const linksGetFailed = () => ({
  type: 'LINKS_GET_FAILED',
})

export const getLinksAsync = () => async dispatch => {
  dispatch(linksGetRequested())
  try {
    const {data} = await getLinksAJAX()
    dispatch(linksGetCompleted(data))
  } catch (error) {
    const message = `Failed to retrieve api links from ${linksLink}`
    dispatch(errorThrown(error, message))
    dispatch(linksGetFailed())
  }
}
