import {Action} from 'src/shared/actions/notifications'

export type NotificationAction = Action

export interface Notification {
  id?: string
  style: NotificationStyle
  icon: string
  duration: number
  message: string
}

export enum NotificationStyle {
  Error = 'error',
  Success = 'success',
  Info = 'info',
  Primary = 'primary',
  Warning = 'warning',
}
