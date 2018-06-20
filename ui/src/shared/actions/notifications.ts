import {Notification} from 'src/types'

export type Action = PublishNotificationAction | ActionDismissNotification

// Publish notification
export type PublishNotificationActionCreator = (
  n: Notification
) => PublishNotificationAction

export interface PublishNotificationAction {
  type: 'PUBLISH_NOTIFICATION'
  payload: {
    notification: Notification
  }
}

export const notify: PublishNotificationActionCreator = (
  notification: Notification
): PublishNotificationAction => ({
  type: 'PUBLISH_NOTIFICATION',
  payload: {notification},
})

// Dismiss notification
export type DismissNotification = (id: string) => ActionDismissNotification
export interface ActionDismissNotification {
  type: 'DISMISS_NOTIFICATION'
  payload: {
    id: string
  }
}

export const dismissNotification = (id: string): ActionDismissNotification => ({
  type: 'DISMISS_NOTIFICATION',
  payload: {id},
})
