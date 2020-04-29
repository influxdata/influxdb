import {Action} from 'src/shared/actions/notifications'
import {IconFont} from '@influxdata/clockface'

export type NotificationAction = Action

export interface Notification {
  id?: string
  style: NotificationStyle
  icon: IconFont
  duration?: number
  message: string
  type?: string
  link?: string
  linkText?: string
}

export enum NotificationStyle {
  Error = 'error',
  Success = 'success',
  Info = 'info',
  Primary = 'primary',
  Warning = 'warning',
}
