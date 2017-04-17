import {authExpired} from 'shared/actions/auth'
import {publishNotification as notify} from 'shared/actions/notifications'

import {HTTP_FORBIDDEN} from 'shared/constants'

const actionsAllowedDuringBlackout = ['@@', 'AUTH_', 'ME_', 'NOTIFICATION_', 'ERROR_']
const notificationsBlackoutDuration = 5000
let allowNotifications = true // eslint-disable-line

const errorsMiddleware = store => next => action => {
  const {auth: {me}} = store.getState()

  if (action.type === 'ERROR_THROWN') {
    const {error: {status, auth}, altText} = action

    if (status === HTTP_FORBIDDEN) {
      const wasSessionTimeout = me !== null

      store.dispatch(authExpired(auth))

      if (wasSessionTimeout) {
        store.dispatch(notify('error', 'Session timed out. Please login again.'))

        allowNotifications = false
        setTimeout(() => {
          allowNotifications = true
        }, notificationsBlackoutDuration)
      } else {
        store.dispatch(notify('error', 'Please login to use Chronograf.'))
      }
    } else if (altText) {
      store.dispatch(notify('error', altText))
    } else {
      store.dispatch(notify('error', 'Cannot communicate with server.'))
    }
  }

  // if auth has expired, do not execute any further actions or redux state
  // changes in order to prevent changing notification that indiciates why
  // logout occurred and any attempts to change redux state by actions that may
  // have triggered AJAX requests prior to auth expiration and whose response
  // returns after logout
  if (me === null && !(actionsAllowedDuringBlackout.some((allowedAction) => action.type.includes(allowedAction)))) {
    return
  }
  next(action)
}

export default errorsMiddleware
