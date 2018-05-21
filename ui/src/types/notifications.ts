export interface Notification {
  id?: string
  type: string
  icon: string
  duration: number
  message: string
}

export type NotificationFunc = (message: any) => Notification
