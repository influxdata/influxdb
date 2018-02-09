import {
  publishNotification,
  dismissNotification,
} from 'shared/actions/notifications'
import {delayEnablePresentationMode} from 'shared/actions/app'

import {PRESENTATION_MODE_NOTIFICATION_DELAY} from 'shared/constants'
import {NOTIFICATION_DISMISS_DELAY} from 'shared/constants'

export function delayDismissNotification(type, delay) {
  return dispatch => {
    setTimeout(() => dispatch(dismissNotification(type)), delay)
  }
}

export const publishAutoDismissingNotification = (
  type,
  message,
  delay = NOTIFICATION_DISMISS_DELAY
) => dispatch => {
  dispatch(publishNotification(type, message))
  dispatch(delayDismissNotification(type, delay))
}

export const presentationButtonDispatcher = dispatch => () => {
  dispatch(delayEnablePresentationMode())
  dispatch(
    publishAutoDismissingNotification(
      'success',
      'Press ESC to disable presentation mode.',
      PRESENTATION_MODE_NOTIFICATION_DELAY
    )
  )
}
