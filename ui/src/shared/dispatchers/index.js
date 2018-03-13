import {publishNotification} from 'shared/actions/notifications'
import {delayEnablePresentationMode} from 'shared/actions/app'
import {enterPresentationModeNotification} from 'shared/copy/notifications'

export const presentationButtonDispatcher = dispatch => () => {
  dispatch(delayEnablePresentationMode())
  dispatch(publishNotification(enterPresentationModeNotification))
}
