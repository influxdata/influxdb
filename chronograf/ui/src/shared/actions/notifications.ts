import * as NotificationsActions from 'src/types/actions/notifications'
import * as NotificationsModels from 'src/types/notifications'

export const notify: NotificationsActions.PublishNotificationActionCreator = (
  notification: NotificationsModels.Notification
): NotificationsActions.PublishNotificationAction => ({
  type: 'PUBLISH_NOTIFICATION',
  payload: {notification},
})
export const dismissNotification = (
  id: string
): NotificationsActions.DismissNotificationAction => ({
  type: 'DISMISS_NOTIFICATION',
  payload: {id},
})
