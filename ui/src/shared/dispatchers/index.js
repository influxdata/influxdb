import {notify} from 'shared/actions/notifications'
import {delayEnablePresentationMode} from 'shared/actions/app'
import {notifyPresentationMode} from 'shared/copy/notifications'

export const presentationButtonDispatcher = dispatch => () => {
  dispatch(delayEnablePresentationMode())
  dispatch(notify(notifyPresentationMode()))
}
