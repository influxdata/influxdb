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

export const cantImportInvalidResource = (
  resourceName: string
): Notification => ({
  ...defaultErrorNotification,
  message: `Invalid JSON, could not create ${resourceName}`,
})

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

export const taskImportFailed = (errorMessage: string): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: `Failed to import Task: ${errorMessage}.`,
})

export const taskImportSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  duration: FIVE_SECONDS,
  message: `Successfully imported task.`,
})

export const taskRunSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  duration: FIVE_SECONDS,
  message: 'Task ran successfully',
})

export const taskGetFailed = (error: string): Notification => ({
  ...defaultErrorNotification,
  duration: FIVE_SECONDS,
  message: `Failed to get runs: ${error}`,
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

export const bucketDeleteSuccess = (bucketName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Bucket "${bucketName}" was successfully deleted`,
})

export const bucketDeleteFailed = (bucketName: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to delete bucket: "${bucketName}"`,
})

export const bucketCreateSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Bucket was successfully created',
})

export const bucketCreateFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to create bucket',
})

export const bucketUpdateSuccess = (bucketName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Bucket "${bucketName}" was successfully updated`,
})

export const bucketUpdateFailed = (bucketName: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to update bucket: "${bucketName}"`,
})

export const orgCreateSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Organization was successfully created',
})

export const orgCreateFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to create organization',
})

export const orgEditSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Organization was successfully updated',
})

export const orgEditFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to update organization',
})

export const scraperDeleteSuccess = (scraperName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Scraper "${scraperName}" was successfully deleted`,
})

export const scraperDeleteFailed = (scraperName: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to delete scraper: "${scraperName}"`,
})

export const scraperCreateSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Scraper was created successfully',
})

export const scraperCreateFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to create scraper',
})

export const scraperUpdateSuccess = (scraperName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Scraper "${scraperName}" was updated successfully`,
})

export const scraperUpdateFailed = (scraperName: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to update scraper: "${scraperName}"`,
})

export const telegrafUpdateSuccess = (telegrafName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Telegraf "${telegrafName}" was updated successfully`,
})

export const telegrafGetFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to get telegraf configs',
})

export const telegrafCreateFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to create telegraf',
})

export const telegrafUpdateFailed = (telegrafName: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to update telegraf: "${telegrafName}"`,
})

export const addTelelgrafLabelFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to add label to telegraf config`,
})

export const removeTelelgrafLabelFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to remove label from telegraf config`,
})

export const authorizationsGetFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to get tokens',
})

export const authorizationCreateFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to create tokens',
})

export const authorizationUpdateFailed = (desc: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to update token: "${desc}"`,
})

export const authorizationDeleteFailed = (desc: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to delete token: "${desc}"`,
})

export const telegrafDeleteSuccess = (telegrafName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Telegraf "${telegrafName}" was deleted successfully`,
})

export const telegrafDeleteFailed = (telegrafName: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to delete telegraf: "${telegrafName}"`,
})

export const memberAddSuccess = (username: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Member "${username}" was added successfully`,
})

export const memberAddFailed = (message: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to add members: "${message}"`,
})

export const memberRemoveSuccess = (memberName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Member "${memberName}" was removed successfully`,
})

export const memberRemoveFailed = (message: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to remove members: "${message}"`,
})

export const addVariableLabelFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to add label to variables`,
})

export const removeVariableLabelFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to remove label from variables`,
})

export const invalidMapType = (): Notification => ({
  ...defaultErrorNotification,
  message: `Variables of type map accept two comma separated values per line`,
})
