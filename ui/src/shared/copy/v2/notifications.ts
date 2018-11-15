import {Notification} from 'src/types'
import {TEN_SECONDS} from 'src/shared/constants/index'

type NotificationExcludingMessage = Pick<
  Notification,
  Exclude<keyof Notification, 'message'>
>

const defaultErrorNotification: NotificationExcludingMessage = {
  type: 'error',
  icon: 'alert-triangle',
  duration: TEN_SECONDS,
}

export const taskNotCreated = (additionalMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to create new task: ${additionalMessage}`,
})

export const taskNotFound = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to find task',
})

export const tasksFetchFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to get tasks from server',
})

export const taskDeleteFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to delete task',
})

export const taskUpdateFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to update task',
})
