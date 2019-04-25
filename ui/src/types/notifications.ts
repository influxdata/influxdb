import {Action} from 'src/shared/actions/notifications'

export type NotificationAction = Action

export interface Notification {
  id?: string
  type: string
  icon: string
  duration: number
  message: string
}
