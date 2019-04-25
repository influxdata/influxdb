import {Notification} from 'src/types'

export type Action =
  | PublishNotificationAction
  | DismissNotificationAction
  | DismissAllNotificationsAction

export interface PublishNotificationAction {
  type: 'PUBLISH_NOTIFICATION'
  payload: {
    notification: Notification
  }
}
export const notify = (
  notification: Notification
): PublishNotificationAction => ({
  type: 'PUBLISH_NOTIFICATION',
  payload: {notification},
})

export interface DismissNotificationAction {
  type: 'DISMISS_NOTIFICATION'
  payload: {
    id: string
  }
}
export const dismissNotification = (id: string): DismissNotificationAction => ({
  type: 'DISMISS_NOTIFICATION',
  payload: {id},
})

export interface DismissAllNotificationsAction {
  type: 'DISMISS_ALL_NOTIFICATIONS'
}
export const dismissAllNotifications = (): DismissAllNotificationsAction => ({
  type: 'DISMISS_ALL_NOTIFICATIONS',
})
