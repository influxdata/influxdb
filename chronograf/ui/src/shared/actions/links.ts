import {Dispatch} from 'redux'

import {getLinks as getLinksAJAX} from 'src/shared/apis/links'

import {errorThrown} from 'src/shared/actions/errors'

import {linksLink} from 'src/shared/constants'

import {AuthLinks} from 'src/types/auth'

export enum ActionTypes {
  LinksGetRequested = 'LINKS_GET_REQUESTED',
  LinksGetCompleted = 'LINKS_GET_COMPLETED',
  LinksGetFailed = 'LINKS_GET_FAILED',
}

export interface LinksGetRequestedAction {
  type: ActionTypes.LinksGetRequested
}
const linksGetRequested = (): LinksGetRequestedAction => ({
  type: ActionTypes.LinksGetRequested,
})

export interface LinksGetCompletedAction {
  type: ActionTypes.LinksGetCompleted
  payload: {links: AuthLinks}
}
export const linksGetCompleted = (
  links: AuthLinks
): LinksGetCompletedAction => ({
  type: ActionTypes.LinksGetCompleted,
  payload: {links},
})

export interface LinksGetFailedAction {
  type: ActionTypes.LinksGetFailed
}
const linksGetFailed = (): LinksGetFailedAction => ({
  type: ActionTypes.LinksGetFailed,
})

export const getLinksAsync = () => async (
  dispatch: Dispatch<
    LinksGetRequestedAction | LinksGetCompletedAction | LinksGetFailedAction
  >
): Promise<void> => {
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
