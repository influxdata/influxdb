import {Notification} from 'src/types'

export type Action =
  | PublishNotificationAction
  | DismissNotificationAction
  | DismissAllNotificationsAction

export enum ActionTypes {
  PUBLISH_NOTIFICATION = 'PUBLISH_NOTIFICATION',
  DISMISS_NOTIFICATION = 'DISMISS_NOTIFICATION',
  DISMISS_ALL_NOTIFICATIONS = 'DISMISS_ALL_NOTIFICATIONS',
}

export interface PublishNotificationAction {
  type: ActionTypes.PUBLISH_NOTIFICATION
  payload: {
    notification: Notification
  }
}
export const notify = (
  notification: Notification
): PublishNotificationAction => ({
  type: ActionTypes.PUBLISH_NOTIFICATION,
  payload: {notification},
})

export interface DismissNotificationAction {
  type: ActionTypes.DISMISS_NOTIFICATION
  payload: {
    id: string
  }
}
export const dismissNotification = (id: string): DismissNotificationAction => ({
  type: ActionTypes.DISMISS_NOTIFICATION,
  payload: {id},
})

export interface DismissAllNotificationsAction {
  type: ActionTypes.DISMISS_ALL_NOTIFICATIONS
}
export const dismissAllNotifications = (): DismissAllNotificationsAction => ({
  type: ActionTypes.DISMISS_ALL_NOTIFICATIONS,
})
