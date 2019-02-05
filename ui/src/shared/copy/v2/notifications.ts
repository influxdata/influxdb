import {Notification} from 'src/types'
import {FIVE_SECONDS, TEN_SECONDS, INFINITE} from 'src/shared/constants/index'

type NotificationExcludingMessage = Pick<
  Notification,
  Exclude<keyof Notification, 'message'>
>

const defaultErrorNotification: NotificationExcludingMessage = {
  type: 'error',
  icon: 'alert-triangle',
  duration: TEN_SECONDS,
}

const defaultSuccessNotification: NotificationExcludingMessage = {
  type: 'success',
  icon: 'checkmark',
  duration: FIVE_SECONDS,
}

export const taskNotCreated = (additionalMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to create new task: ${additionalMessage}`,
})

export const taskCreatedSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'New task created successfully',
})

export const taskNotFound = (additionalMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to find task: ${additionalMessage}`,
})

export const tasksFetchFailed = (additionalMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to get tasks from server: ${additionalMessage}`,
})

export const taskDeleteFailed = (additionalMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to delete task: ${additionalMessage}`,
})

export const taskDeleteSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Task was deleted successfully',
})

export const taskCloneSuccess = (taskName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Successfully cloned task ${taskName}`,
})

export const taskCloneFailed = (
  taskName: string,
  additionalMessage: string
): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to clone task ${taskName}: ${additionalMessage} `,
})

export const taskUpdateFailed = (additionalMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to update task: ${additionalMessage}`,
})

export const taskUpdateSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Task was updated successfully',
})

export const taskImportFailed = (
  fileName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: `Failed to import Task from file ${fileName}: ${errorMessage}.`,
})

export const taskImportSuccess = (fileName: string): Notification => ({
  ...defaultSuccessNotification,
  duration: FIVE_SECONDS,
  message: `Successfully imported file ${fileName}.`,
})

export const getTelegrafConfigFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to get telegraf config',
})

export const savingNoteFailed = (error: string): Notification => ({
  ...defaultErrorNotification,
  duration: FIVE_SECONDS,
  message: `Failed to save note: ${error}`,
})

export const writeLineProtocolFailed = (error: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to write line protocol ${error}`,
})

export const labelCreateFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to create label',
})

export const labelDeleteFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to delete label',
})

export const labelUpdateFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to update label',
})

export const bucketDeleted = (bucketName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Bucket ${bucketName} was successfully deleted`,
})
