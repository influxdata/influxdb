export interface Notification {
  id?: string
  type: string
  icon: string
  duration: number
  message: string
}

interface AdditionalInfo {
  [x: string]: string
}
export type NotificationFunc = (info?: AdditionalInfo) => Notification
