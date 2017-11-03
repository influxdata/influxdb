import {updateMe as updateMeAJAX} from 'shared/apis/auth'

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

export const meRequested = () => ({
  type: 'ME_REQUESTED',
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

export const meReceived = me => ({
  type: 'ME_RECEIVED',
  payload: {
    me,
  },
})

export const logoutLinkReceived = logoutLink => ({
  type: 'LOGOUT_LINK_RECEIVED',
  payload: {
    logoutLink,
  },
})

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
    dispatch(meReceived(data))
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(meChangeOrganizationFailed())
  }
}
