import {authExpired} from 'shared/actions/auth'
import {publishNotification as notify} from 'shared/actions/notifications'

import {HTTP_FORBIDDEN} from 'shared/constants'

const actionsAllowedDuringBlackout = [
  '@@',
  'AUTH_',
  'ME_',
  'NOTIFICATION_',
  'ERROR_',
]
const notificationsBlackoutDuration = 5000
let allowNotifications = true // eslint-disable-line

const errorsMiddleware = store => next => action => {
  const {auth: {me}} = store.getState()

  if (action.type === 'ERROR_THROWN') {
    const {
      error: {status, auth, data: {message}},
      altText,
      alertType = 'error',
    } = action

    if (status === HTTP_FORBIDDEN) {
      const organizationWasRemoved =
        message === `user's current organization was not found` // eslint-disable-line quotes
      const wasSessionTimeout = me !== null

      store.dispatch(authExpired(auth))

      if (
        message ===
        `This organization is private. To gain access, you must be explicitly added by an administrator.` // eslint-disable-line quotes
      ) {
        store.dispatch(notify(alertType, message))
      }

      if (organizationWasRemoved) {
        store.dispatch(
          notify(alertType, 'Your current organization was deleted.')
        )

        allowNotifications = false
        setTimeout(() => {
          allowNotifications = true
        }, notificationsBlackoutDuration)
      } else if (wasSessionTimeout) {
        store.dispatch(
          notify(alertType, 'Session timed out. Please login again.')
        )

        allowNotifications = false
        setTimeout(() => {
          allowNotifications = true
        }, notificationsBlackoutDuration)
      }
    } else if (altText) {
      store.dispatch(notify(alertType, altText))
    } else {
      // TODO: actually do proper error handling
      // store.dispatch(notify(alertType, 'Cannot communicate with server.'))
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
