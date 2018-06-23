import {Notification} from 'src/types'
import * as NotificationActions from 'src/types/actions/notifications'

export const notify: NotificationActions.PublishNotificationActionCreator = (
  notification: Notification
): NotificationActions.PublishNotificationAction => ({
  type: 'PUBLISH_NOTIFICATION',
  payload: {notification},
})
export const dismissNotification = (
  id: string
): NotificationActions.DismissNotificationAction => ({
  type: 'DISMISS_NOTIFICATION',
  payload: {id},
})
