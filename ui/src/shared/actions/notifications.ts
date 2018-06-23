import * as Types from 'src/types/modules'

export const notify: Types.Notifications.Actions.PublishNotificationActionCreator = (
  notification: Types.Notifications.Data.Notification
): Types.Notifications.Actions.PublishNotificationAction => ({
  type: 'PUBLISH_NOTIFICATION',
  payload: {notification},
})
export const dismissNotification = (
  id: string
): Types.Notifications.Actions.DismissNotificationAction => ({
  type: 'DISMISS_NOTIFICATION',
  payload: {id},
})
