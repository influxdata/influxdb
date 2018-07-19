import {Dispatch} from 'redux'

import {
  getMe as getMeAJAX,
  updateMe as updateMeAJAX,
} from 'src/shared/apis/auth'

import {getLinksAsync} from 'src/shared/actions/links'

import {notify} from 'src/shared/actions/notifications'
import {errorThrown} from 'src/shared/actions/errors'

import {notifyUserSwitchedOrgs} from 'src/shared/copy/notifications'

import {Me, Organization} from 'src/types/auth'

export type Action =
  | AuthExpiredAction
  | AuthRequestedAction
  | MeGetRequestedAction
  | MeGetCompletedAction
  | MeGetRequestedAction
  | MeGetFailedAction
  | MeChangeOrganizationCompletedAction
  | MeChangeOrganizationFailedAction
  | MeChangeOrganizationRequestedAction

export enum ActionTypes {
  AuthExpired = 'AUTH_EXPIRED',
  AuthRequested = 'AUTH_REQUESTED',
  MeGetRequested = 'ME_GET_REQUESTED',
  MeGetCompleted = 'ME_GET_COMPLETED',
  MeGetFailed = 'ME_GET_FAILED',
  MeChangeOrganizationRequested = 'ME_CHANGE_ORGANIZATION_REQUESTED',
  MeChangeOrganizationCompleted = 'ME_CHANGE_ORGANIZATION_COMPLETED',
  MeChangeOrganizationFailed = 'ME_CHANGE_ORGANIZATION_FAILED',
}

export interface AuthExpiredAction {
  type: ActionTypes.AuthExpired
  payload: {auth}
}
export const authExpired = (auth): AuthExpiredAction => ({
  type: ActionTypes.AuthExpired,
  payload: {
    auth,
  },
})

export interface AuthRequestedAction {
  type: ActionTypes.AuthRequested
}
export const authRequested = (): AuthRequestedAction => ({
  type: ActionTypes.AuthRequested,
})

export interface MeGetRequestedAction {
  type: ActionTypes.MeGetRequested
}
export const meGetRequested = (): MeGetRequestedAction => ({
  type: ActionTypes.MeGetRequested,
})

export interface MeGetCompletedAction {
  type: ActionTypes.MeGetCompleted
  payload: {
    me: Me
    auth
    logoutLink: string
  }
}
export const meGetCompleted = ({
  me,
  auth,
  logoutLink,
}): MeGetCompletedAction => ({
  type: ActionTypes.MeGetCompleted,
  payload: {
    me,
    auth,
    logoutLink,
  },
})

export interface MeGetFailedAction {
  type: ActionTypes.MeGetFailed
}
export const meGetFailed = (): MeGetFailedAction => ({
  type: ActionTypes.MeGetFailed,
})

export interface MeChangeOrganizationRequestedAction {
  type: ActionTypes.MeChangeOrganizationRequested
}
export const meChangeOrganizationRequested = (): MeChangeOrganizationRequestedAction => ({
  type: ActionTypes.MeChangeOrganizationRequested,
})

export interface MeChangeOrganizationCompletedAction {
  type: ActionTypes.MeChangeOrganizationCompleted
}
export const meChangeOrganizationCompleted = (): MeChangeOrganizationCompletedAction => ({
  type: ActionTypes.MeChangeOrganizationCompleted,
})

export interface MeChangeOrganizationFailedAction {
  type: ActionTypes.MeChangeOrganizationFailed
}
export const meChangeOrganizationFailed = (): MeChangeOrganizationFailedAction => ({
  type: ActionTypes.MeChangeOrganizationFailed,
})

// shouldResetMe protects against `me` being nullified in Redux temporarily,
// which currently causes the app to show a loading spinner until me is
// re-hydrated. if `getMeAsync` is only being used to refresh me after creating
// an organization, this is undesirable behavior
export const getMeAsync = ({shouldResetMe = false} = {}) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  if (shouldResetMe) {
    dispatch(authRequested())
    dispatch(meGetRequested())
  }
  try {
    // These non-me objects are added to every response by some AJAX trickery
    const {data: me, auth, logoutLink} = await getMeAJAX()
    // TODO: eventually, get the links for auth and logout out of here and into linksGetCompleted
    dispatch(
      meGetCompleted({
        me,
        auth,
        logoutLink,
      })
    )
  } catch (error) {
    dispatch(meGetFailed())
    dispatch(errorThrown(error))
  }
}

// meChangeOrganizationAsync is for switching the user's current organization.
//
// Global links state also needs to be refreshed upon organization change so
// that Admin Chronograf / Current Org User tab's link is valid, but this is
// happening automatically because we are using a browser redirect to reload
// the application. If at some point we stop using a redirect and instead
// make it a seamless SPA experience, a la issue #2463, we'll need to make sure
// links are still refreshed.
export const meChangeOrganizationAsync = (
  url: string,
  organization: Organization
) => async (dispatch): Promise<void> => {
  dispatch(meChangeOrganizationRequested())
  try {
    const {data: me, auth, logoutLink} = await updateMeAJAX(url, organization)
    const currentRole = me.roles.find(
      r => r.organization === me.currentOrganization.id
    )
    dispatch(
      notify(
        notifyUserSwitchedOrgs(me.currentOrganization.name, currentRole.name)
      )
    )
    dispatch(meChangeOrganizationCompleted())
    dispatch(meGetCompleted({me, auth, logoutLink}))

    // refresh links after every successful meChangeOrganization to refresh
    // /organizations/:id/users link for Admin / Current Org Users page to load
    dispatch(getLinksAsync())
    // TODO: reload sources upon me change org if non-refresh behavior preferred
    // instead of current behavior on both invocations of meChangeOrganization,
    // which is to refresh index via router.push('')
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(meChangeOrganizationFailed())
  }
}
