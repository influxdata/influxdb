import {Notification} from 'src/types'

export type Action = PublishNotificationAction | DismissNotificationAction

export type PublishNotificationActionCreator = (
  n: Notification
) => PublishNotificationAction

export interface PublishNotificationAction {
  type: 'PUBLISH_NOTIFICATION'
  payload: {
    notification: Notification
  }
}

export type DismissNotification = (id: string) => DismissNotificationAction

export interface DismissNotificationAction {
  type: 'DISMISS_NOTIFICATION'
  payload: {
    id: string
  }
}
