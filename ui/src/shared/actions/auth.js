import {getMe as getMeAJAX, updateMe as updateMeAJAX} from 'shared/apis/auth'

import {getLinksAsync} from 'shared/actions/links'

import {publishAutoDismissingNotification} from 'shared/dispatchers'
import {errorThrown} from 'shared/actions/errors'

import {NOTIFICATION_DISMISS_DELAY} from 'shared/constants'

export const authExpired = auth => ({
  type: 'AUTH_EXPIRED',
  payload: {
    auth,
  },
})

export const authRequested = () => ({
  type: 'AUTH_REQUESTED',
})

export const meGetRequested = () => ({
  type: 'ME_GET_REQUESTED',
})

export const meGetCompleted = ({me, auth, logoutLink}) => ({
  type: 'ME_GET_COMPLETED',
  payload: {
    me,
    auth,
    logoutLink,
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
  url,
  organization
) => async dispatch => {
  dispatch(meChangeOrganizationRequested())
  try {
    const {data: me, auth, logoutLink} = await updateMeAJAX(url, organization)
    const currentRole = me.roles.find(
      r => r.organization === me.currentOrganization.id
    )
    dispatch(
      publishAutoDismissingNotification(
        'success',
        `Now logged in to '${me.currentOrganization
          .name}' as '${currentRole.name}'`,
        NOTIFICATION_DISMISS_DELAY
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
