import {Dispatch} from 'redux'

import {getLinks as getLinksAJAX} from 'src/shared/apis/links'

import {errorThrown} from 'src/shared/actions/errors'

export enum ActionTypes {
  LinksGetRequested = 'LINKS_GET_REQUESTED',
  LinksGetCompleted = 'LINKS_GET_COMPLETED',
  LinksGetFailed = 'LINKS_GET_FAILED',
}

export type Action = LinksGetCompletedAction

export interface LinksGetRequestedAction {
  type: ActionTypes.LinksGetRequested
}
const linksGetRequested = (): LinksGetRequestedAction => ({
  type: ActionTypes.LinksGetRequested,
})

export interface LinksGetCompletedAction {
  type: ActionTypes.LinksGetCompleted
  payload: {links}
}
export const linksGetCompleted = (links): LinksGetCompletedAction => ({
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
    const links = await getLinksAJAX()
    dispatch(linksGetCompleted(links))
  } catch (error) {
    const message = `Failed to retrieve links`
    dispatch(errorThrown(error, message))
    dispatch(linksGetFailed())
  }
}
