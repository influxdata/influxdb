// import {replace} from 'react-router-redux'
import {authExpired} from 'shared/actions/auth'
import {publishNotification as notify} from 'shared/actions/notifications'

import {HTTP_FORBIDDEN} from 'shared/constants'

const notificationsBlackoutDuration = 5000
let allowNotifications = true // eslint-disable-line

const errorsMiddleware = store => next => action => {
  if (action.type === 'ERROR_THROWN') {
    const {error, error: {status, auth}} = action

    console.error(error)

    if (status === HTTP_FORBIDDEN) {
      const {auth: {me}} = store.getState()
      const wasSessionTimeout = me !== null

      next(authExpired(auth))

      if (wasSessionTimeout) {
        store.dispatch(notify('error', 'Session timed out. Please login again.'))

        allowNotifications = false
        setTimeout(() => {
          allowNotifications = true
        }, notificationsBlackoutDuration)
      } else {
        store.dispatch(notify('error', 'Please login to use Chronograf.'))
      }
    } else {
      store.dispatch(notify('error', 'Cannot communicate with server.'))
    }
  }

  next(action)
}

export default errorsMiddleware
