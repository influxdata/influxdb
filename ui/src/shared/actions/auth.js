import {getMe as getMeAJAX, updateMe as updateMeAJAX} from 'shared/apis/auth'

import {linksReceived} from 'shared/actions/links'

import {publishAutoDismissingNotification} from 'shared/dispatchers'
import {errorThrown} from 'shared/actions/errors'

export const authExpired = auth => ({
  type: 'AUTH_EXPIRED',
  payload: {
    auth,
  },
})

export const authRequested = () => ({
  type: 'AUTH_REQUESTED',
})

export const authReceived = auth => ({
  type: 'AUTH_RECEIVED',
  payload: {
    auth,
  },
})

export const meGetRequested = () => ({
  type: 'ME_GET_REQUESTED',
})

export const meGetCompletedNotUsingAuth = me => ({
  type: 'ME_GET_COMPLETED__NON_AUTH',
  payload: {
    me,
  },
})

export const meGetCompletedUsingAuth = me => ({
  type: 'ME_GET_COMPLETED__AUTH',
  payload: {
    me,
  },
})

export const meGetFailed = () => ({
  type: 'ME_GET_FAILED',
})

export const meChangeOrganizationRequested = () => ({
  type: 'ME_CHANGE_ORGANIZATION_REQUESTED',
})

export const meChangeOrganizationCompleted = () => ({
  type: 'ME_CHANGE_ORGANIZATION_COMPLETED',
})

export const meChangeOrganizationFailed = () => ({
  type: 'ME_CHANGE_ORGANIZATION_FAILED',
})

export const logoutLinkReceived = logoutLink => ({
  type: 'LOGOUT_LINK_RECEIVED',
  payload: {
    logoutLink,
  },
})

// shouldResetMe protects against `me` being nullified in Redux temporarily,
// which currently causes the app to show a loading spinner until me is
// re-hydrated. if `getMeAsync` is only being used to refresh me after creating
// an organization, this is undesirable behavior
export const getMeAsync = ({shouldResetMe = false} = {}) => async dispatch => {
  if (shouldResetMe) {
    dispatch(authRequested())
    dispatch(meGetRequested())
  }
  try {
    // These non-me objects are added to every response by some AJAX trickery
    const {
      data: me,
      auth,
      logoutLink,
      external,
      users,
      organizations,
      meLink,
    } = await getMeAJAX()
    const isUsingAuth = !!logoutLink
    dispatch(
      isUsingAuth ? meGetCompletedUsingAuth(me) : meGetCompletedNotUsingAuth(me)
    )
    dispatch(authReceived(auth))
    dispatch(logoutLinkReceived(logoutLink))
    dispatch(linksReceived({external, users, organizations, me: meLink}))
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(meGetFailed())
  }
}

export const meChangeOrganizationAsync = (
  url,
  organization
) => async dispatch => {
  dispatch(meChangeOrganizationRequested())
  try {
    const {data} = await updateMeAJAX(url, organization)
    dispatch(
      publishAutoDismissingNotification(
        'success',
        `Now signed into ${data.currentOrganization.name}`
      )
    )
    dispatch(meChangeOrganizationCompleted())
    dispatch(meGetCompletedUsingAuth(data))
    // TODO: reload sources upon me change org if non-refresh behavior preferred
    // instead of current behavior on both invocations of meChangeOrganization,
    // which is to refresh index via router.push('')
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(meChangeOrganizationFailed())
  }
}
