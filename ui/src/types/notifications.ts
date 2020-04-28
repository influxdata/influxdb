import {Action} from 'src/shared/actions/notifications'
import {IconFont, ComponentColor} from '@influxdata/clockface'

export type NotificationAction = Action

export interface Notification {
  id?: string
  style: ComponentColor
  icon: IconFont
  duration?: number
  message: string
  type?: string
}
