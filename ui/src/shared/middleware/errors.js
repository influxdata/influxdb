import _ from 'lodash'

import {authExpired} from 'shared/actions/auth'
import {publishNotification} from 'shared/actions/notifications'

import {HTTP_FORBIDDEN} from 'shared/constants'
import {
  NOTIFY_SESSION_TIMED_OUT,
  NOTIFY_ERR_WITH_ALT_TEXT,
  NOTIFY_NEW_VERSION,
  NOTIFY_ORG_IS_PRIVATE,
  NOTIFY_CURRENT_ORG_DELETED,
} from 'shared/copy/notifications'

const actionsAllowedDuringBlackout = [
  '@@',
  'AUTH_',
  'ME_',
  'PUBLISH_NOTIFICATION',
  'ERROR_',
  'LINKS_',
]
const notificationsBlackoutDuration = 5000
let allowNotifications = true // eslint-disable-line

const errorsMiddleware = store => next => action => {
  const {auth: {me}} = store.getState()

  if (action.type === 'ERROR_THROWN') {
    const {error, error: {status, auth}, altText, alertType = 'info'} = action

    if (status === HTTP_FORBIDDEN) {
      const message = _.get(error, 'data.message', '')

      const organizationWasRemoved =
        message === `user's current organization was not found` // eslint-disable-line quotes
      const wasSessionTimeout = me !== null

      store.dispatch(authExpired(auth))

      if (
        message ===
        `This organization is private. To gain access, you must be explicitly added by an administrator.` // eslint-disable-line quotes
      ) {
        store.dispatch(publishNotification(NOTIFY_ORG_IS_PRIVATE))
      }

      if (_.startsWith(message, 'Welcome to Chronograf')) {
        store.dispatch(publishNotification(NOTIFY_NEW_VERSION(message)))
      }

      if (organizationWasRemoved) {
        store.dispatch(publishNotification(NOTIFY_CURRENT_ORG_DELETED))

        allowNotifications = false
        setTimeout(() => {
          allowNotifications = true
        }, notificationsBlackoutDuration)
      } else if (wasSessionTimeout) {
        store.dispatch(publishNotification(NOTIFY_SESSION_TIMED_OUT))

        allowNotifications = false
        setTimeout(() => {
          allowNotifications = true
        }, notificationsBlackoutDuration)
      }
    } else if (altText) {
      store.dispatch(
        publishNotification(NOTIFY_ERR_WITH_ALT_TEXT(alertType, altText))
      )
    } else {
      // TODO: actually do proper error handling
      // store.dispatch(publishNotification({type: alertType, 'Cannot communicate with server.'))
    }
  }

  // If auth has expired, do not execute any further actions or redux state
  // changes not related to routing and auth. This allows the error notification
  // telling the user why they've been logged out to persist in the UI. It also
  // prevents further changes to redux state by actions that may have triggered
  // AJAX requests pre-auth expiration and whose response returns post-logout
  if (
    me === null &&
    !actionsAllowedDuringBlackout.some(allowedAction =>
      action.type.includes(allowedAction)
    )
  ) {
    return
  }
  next(action)
}

export default errorsMiddleware
