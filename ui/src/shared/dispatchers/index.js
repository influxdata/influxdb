import {delayEnablePresentationMode} from 'shared/actions/ui'
import {publishNotification, delayDismissNotification} from 'shared/actions/notifications'
import {PRESENTATION_MODE_NOTIFICATION_DELAY} from 'shared/constants'

export const presentationButtonDispatcher = (dispatch) => () => {
  dispatch(delayEnablePresentationMode())
  dispatch(publishNotification('success', 'Press ESC to disable presentation mode.'))
  dispatch(delayDismissNotification('success', PRESENTATION_MODE_NOTIFICATION_DELAY))
}
