import {Dispatch} from 'redux'

import {getLinks as getLinksAJAX} from 'src/shared/apis/links'

// Types
import {Links} from 'src/types/v2/links'

export enum ActionTypes {
  LinksGetRequested = 'LINKS_GET_REQUESTED',
  LinksGetCompleted = 'LINKS_GET_COMPLETED',
  LinksGetFailed = 'LINKS_GET_FAILED',
  SetDefaultDashboardLink = 'SET_DEFAULT_DASHBOARD_LINK',
}

export type Action = LinksGetCompletedAction | SetDefaultDashboardAction

export interface SetDefaultDashboardAction {
  type: ActionTypes.SetDefaultDashboardLink
  payload: {
    defaultDashboard: string
  }
}

export const setDefaultDashboard = (
  defaultDashboard: string
): SetDefaultDashboardAction => ({
  type: ActionTypes.SetDefaultDashboardLink,
  payload: {
    defaultDashboard,
  },
})

export interface LinksGetRequestedAction {
  type: ActionTypes.LinksGetRequested
}
const linksGetRequested = (): LinksGetRequestedAction => ({
  type: ActionTypes.LinksGetRequested,
})

export interface LinksGetCompletedAction {
  type: ActionTypes.LinksGetCompleted
  payload: {links: Links}
}
export const linksGetCompleted = (links: Links): LinksGetCompletedAction => ({
  type: ActionTypes.LinksGetCompleted,
  payload: {links},
})

export interface LinksGetFailedAction {
  type: ActionTypes.LinksGetFailed
}
const linksGetFailed = (): LinksGetFailedAction => ({
  type: ActionTypes.LinksGetFailed,
})

export const getLinks = () => async (
  dispatch: Dispatch<
    LinksGetRequestedAction | LinksGetCompletedAction | LinksGetFailedAction
  >
): Promise<void> => {
  dispatch(linksGetRequested())
  try {
    const links = await getLinksAJAX()
    dispatch(linksGetCompleted(links))
  } catch (error) {
    dispatch(linksGetFailed())
  }
}
