import {publishNotification} from 'shared/actions/notifications'
import {delayEnablePresentationMode} from 'shared/actions/app'

export const presentationButtonDispatcher = dispatch => () => {
  dispatch(delayEnablePresentationMode())
  dispatch(
    publishNotification({
      type: 'primary',
      icon: 'expand-b',
      duration: 7500,
      message: 'Press ESC to exit Presentation Mode.',
    })
  )
}
