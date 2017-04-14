// import {replace} from 'react-router-redux'

import {authReceived, meReceived} from 'shared/actions/auth'
import {publishNotification as notify} from 'shared/actions/notifications'

import {HTTP_FORBIDDEN} from 'shared/constants'

const errorsMiddleware = store => next => action => {
  if (action.type === 'ERROR_THROWN') {
    const {error, error: {status, auth}} = action

    console.error(error)

    if (status === HTTP_FORBIDDEN) {
      const {auth: {me}} = store.getState()
      const wasSessionTimeout = me === null

      store.dispatch(authReceived(auth))
      store.dispatch(meReceived(null))

      if (wasSessionTimeout) {
        store.dispatch(notify('error', 'Please login to use Chronograf.'))
      } else {
        store.dispatch(notify('error', 'Session timed out. Please login again.'))
      }
    } else {
      store.dispatch(notify('error', 'Cannot communicate with server.'))
    }
  }

  next(action)
}

export default errorsMiddleware
