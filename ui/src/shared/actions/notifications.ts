import {Notification} from 'src/types'

export type Action = ActionPublishNotification | ActionDismissNotification

// Publish notification
export type PublishNotification = (n: Notification) => ActionPublishNotification
export interface ActionPublishNotification {
  type: 'PUBLISH_NOTIFICATION'
  payload: {
    notification: Notification
  }
}

export const notify = (
  notification: Notification
): ActionPublishNotification => ({
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
