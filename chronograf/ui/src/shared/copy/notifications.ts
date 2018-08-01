// All copy for notifications should be stored here for easy editing
// and ensuring stylistic consistency
import {Notification} from 'src/types'
import {TemplateUpdate} from 'src/types/tempVars'

type NotificationExcludingMessage = Pick<
  Notification,
  Exclude<keyof Notification, 'message'>
>

import {FIVE_SECONDS, TEN_SECONDS, INFINITE} from 'src/shared/constants/index'
import {MAX_RESPONSE_BYTES} from 'src/flux/constants'

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

const defaultDeletionNotification: NotificationExcludingMessage = {
  type: 'primary',
  icon: 'trash',
  duration: FIVE_SECONDS,
}

//  Misc Notifications
//  ----------------------------------------------------------------------------
export const notifyGenericFail = (): string =>
  'Could not communicate with server.'

export const notifyNewVersion = (version: string): Notification => ({
  type: 'info',
  icon: 'cubo-uniform',
  duration: INFINITE,
  message: `Welcome to the latest Chronograf${version}. Local settings cleared.`,
})

export const notifyLoadLocalSettingsFailed = (error: string): Notification => ({
  ...defaultErrorNotification,
  message: `Loading local settings failed: ${error}`,
})

export const notifyErrorWithAltText = (
  type: string,
  message: string
): Notification => ({
  type,
  icon: 'triangle',
  duration: TEN_SECONDS,
  message,
})

export const notifyPresentationMode = (): Notification => ({
  type: 'primary',
  icon: 'expand-b',
  duration: 7500,
  message: 'Press ESC to exit Presentation Mode.',
})

export const notifyDataWritten = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Data was written successfully.',
})

export const notifyDataWriteFailed = (errorMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: `Data write failed: ${errorMessage}`,
})

export const notifySessionTimedOut = (): Notification => ({
  type: 'primary',
  icon: 'triangle',
  duration: INFINITE,
  message: 'Your session has timed out. Log in again to continue.',
})

export const notifyServerError: Notification = {
  ...defaultErrorNotification,
  message: 'Internal Server Error. Check API Logs.',
}

export const notifyCSVDownloadFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Unable to download .CSV file',
})

export const notifyCSVUploadFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Please upload a .csv file',
})

//  Hosts Page Notifications
//  ----------------------------------------------------------------------------
export const notifyUnableToGetHosts = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Unable to get Hosts.',
})

export const notifyUnableToGetApps = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Unable to get Apps for Hosts.',
})

//  InfluxDB Sources Notifications
//  ----------------------------------------------------------------------------
export const notifySourceCreationSucceeded = (
  sourceName: string
): Notification => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `Connected to InfluxDB ${sourceName} successfully.`,
})

export const notifySourceCreationFailed = (
  sourceName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Unable to connect to InfluxDB ${sourceName}: ${errorMessage}`,
})

export const notifySourceUpdated = (sourceName: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `Updated InfluxDB ${sourceName} Connection successfully.`,
})

export const notifySourceUpdateFailed = (
  sourceName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Failed to update InfluxDB ${sourceName} Connection: ${errorMessage}`,
})

export const notifySourceDeleted = (sourceName: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `${sourceName} deleted successfully.`,
})

export const notifySourceDeleteFailed = (sourceName: string): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `There was a problem deleting ${sourceName}.`,
})

export const notifySourceNoLongerAvailable = (
  sourceName: string
): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Source ${sourceName} is no longer available. Please ensure InfluxDB is running.`,
})

export const notifyErrorConnectingToSource = (
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Unable to connect to InfluxDB source: ${errorMessage}`,
})

//  Multitenancy User Notifications
//  ----------------------------------------------------------------------------
export const notifyUserRemovedFromAllOrgs = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message:
    'You have been removed from all organizations. Please contact your administrator.',
})

export const notifyUserRemovedFromCurrentOrg = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'You were removed from your current organization.',
})

export const notifyOrgHasNoSources = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'Organization has no sources configured.',
})

export const notifyUserSwitchedOrgs = (
  orgName: string,
  roleName: string
): Notification => ({
  ...defaultSuccessNotification,
  type: 'primary',
  message: `Now logged in to '${orgName}' as '${roleName}'.`,
})

export const notifyOrgIsPrivate = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message:
    'This organization is private. To gain access, you must be explicitly added by an administrator.',
})

export const notifyCurrentOrgDeleted = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'Your current organization was deleted.',
})

export const notifyJSONFeedFailed = (url: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to fetch JSON Feed for News Feed from '${url}'`,
})

//  Chronograf Admin Notifications
//  ----------------------------------------------------------------------------
export const notifyMappingDeleted = (
  id: string,
  scheme: string
): Notification => ({
  ...defaultSuccessNotification,
  message: `Mapping ${id}/${scheme} deleted successfully.`,
})

export const notifyChronografUserAddedToOrg = (
  user: string,
  organization: string
): string => `${user} has been added to ${organization} successfully.`

export const notifyChronografUserRemovedFromOrg = (
  user: string,
  organization: string
): string => `${user} has been removed from ${organization} successfully.`

export const notifyChronografUserUpdated = (message: string): Notification => ({
  ...defaultSuccessNotification,
  message,
})

export const notifyChronografOrgDeleted = (orgName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Organization ${orgName} deleted successfully.`,
})

export const notifyChronografUserDeleted = (
  user: string,
  isAbsoluteDelete: boolean
): Notification => ({
  ...defaultSuccessNotification,
  message: `${user} has been removed from ${
    isAbsoluteDelete
      ? 'all organizations and deleted.'
      : 'the current organization.'
  }`,
})

export const notifyChronografUserMissingNameAndProvider = (): Notification => ({
  ...defaultErrorNotification,
  type: 'warning',
  message: 'User must have a Name and Provider.',
})

//  InfluxDB Admin Notifications
//  ----------------------------------------------------------------------------
export const notifyDBUserCreated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'User created successfully.',
})

export const notifyDBUserCreationFailed = (errorMessage: string): string =>
  `Failed to create User: ${errorMessage}`

export const notifyDBUserDeleted = (userName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `User "${userName}" deleted successfully.`,
})

export const notifyDBUserDeleteFailed = (errorMessage: string): string =>
  `Failed to delete User: ${errorMessage}`

export const notifyDBUserPermissionsUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'User Permissions updated successfully.',
})

export const notifyDBUserPermissionsUpdateFailed = (
  errorMessage: string
): string => `Failed to update User Permissions: ${errorMessage}`

export const notifyDBUserRolesUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'User Roles updated successfully.',
})

export const notifyDBUserRolesUpdateFailed = (errorMessage: string): string =>
  `Failed to update User Roles: ${errorMessage}`

export const notifyDBUserPasswordUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'User Password updated successfully.',
})

export const notifyDBUserPasswordUpdateFailed = (
  errorMessage: string
): string => `Failed to update User Password: ${errorMessage}`

export const notifyDatabaseCreated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Database created successfully.',
})

export const notifyDBCreationFailed = (errorMessage: string): string =>
  `Failed to create Database: ${errorMessage}`

export const notifyDBDeleted = (databaseName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Database "${databaseName}" deleted successfully.`,
})

export const notifyDBDeleteFailed = (errorMessage: string): string =>
  `Failed to delete Database: ${errorMessage}`

export const notifyRoleCreated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Role created successfully.',
})

export const notifyRoleCreationFailed = (errorMessage: string): string =>
  `Failed to create Role: ${errorMessage}`

export const notifyRoleDeleted = (roleName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Role "${roleName}" deleted successfully.`,
})

export const notifyRoleDeleteFailed = (errorMessage: string): string =>
  `Failed to delete Role: ${errorMessage}`

export const notifyRoleUsersUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Role Users updated successfully.',
})

export const notifyRoleUsersUpdateFailed = (errorMessage: string): string =>
  `Failed to update Role Users: ${errorMessage}`

export const notifyRolePermissionsUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Role Permissions updated successfully.',
})

export const notifyRolePermissionsUpdateFailed = (
  errorMessage: string
): string => `Failed to update Role Permissions: ${errorMessage}`

export const notifyRetentionPolicyCreated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Retention Policy created successfully.',
})

export const notifyRetentionPolicyCreationError = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to create Retention Policy. Please check name and duration.',
})

export const notifyRetentionPolicyCreationFailed = (
  errorMessage: string
): string => `Failed to create Retention Policy: ${errorMessage}`

export const notifyRetentionPolicyDeleted = (rpName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Retention Policy "${rpName}" deleted successfully.`,
})

export const notifyRetentionPolicyDeleteFailed = (
  errorMessage: string
): string => `Failed to delete Retention Policy: ${errorMessage}`

export const notifyRetentionPolicyUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Retention Policy updated successfully.',
})

export const notifyRetentionPolicyUpdateFailed = (
  errorMessage: string
): string => `Failed to update Retention Policy: ${errorMessage}`

export const notifyQueriesError = (errorMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: errorMessage,
})

export const notifyRetentionPolicyCantHaveEmptyFields = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Fields cannot be empty.',
})

export const notifyDatabaseDeleteConfirmationRequired = (
  databaseName: string
): Notification => ({
  ...defaultErrorNotification,
  message: `Type "DELETE ${databaseName}" to confirm. This action cannot be undone.`,
})

export const notifyDBUserNamePasswordInvalid = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Username and/or Password too short.',
})

export const notifyRoleNameInvalid = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Role name is too short.',
})

export const notifyDatabaseNameInvalid = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Database name cannot be blank.',
})

export const notifyDatabaseNameAlreadyExists = (): Notification => ({
  ...defaultErrorNotification,
  message: 'A Database by this name already exists.',
})

//  Dashboard Notifications
//  ----------------------------------------------------------------------------
export const notifyTempVarAlreadyExists = (
  tempVarName: string
): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Variable '${tempVarName}' already exists. Please enter a new value.`,
})

export const notifyDashboardNotFound = (dashboardID: number): Notification => ({
  ...defaultErrorNotification,
  icon: 'dash-h',
  message: `Dashboard ${dashboardID} could not be found`,
})

export const notifyDashboardDeleted = (name: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `Dashboard ${name} deleted successfully.`,
})

export const notifyDashboardExported = (name: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `Dashboard ${name} exported successfully.`,
})

export const notifyDashboardExportFailed = (
  name: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: `Failed to export Dashboard ${name}: ${errorMessage}.`,
})

export const notifyDashboardImported = (name: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `Dashboard ${name} imported successfully.`,
})

export const notifyDashboardImportFailed = (
  fileName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: `Failed to import Dashboard from file ${fileName}: ${errorMessage}.`,
})

export const notifyDashboardDeleteFailed = (
  name: string,
  errorMessage: string
): string => `Failed to delete Dashboard ${name}: ${errorMessage}.`

export const notifyCellAdded = (name: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  duration: 1900,
  message: `Added "${name}" to dashboard.`,
})

export const notifyCellDeleted = (name: string): Notification => ({
  ...defaultDeletionNotification,
  icon: 'dash-h',
  duration: 1900,
  message: `Deleted "${name}" from dashboard.`,
})

export const notifyBuilderDisabled = (): Notification => ({
  type: 'info',
  icon: 'graphline',
  duration: 7500,
  message: `Your query contains a user-defined Template Variable. The Schema Explorer cannot render the query and is disabled.`,
})

//  Template Variables & URL Queries
//  ----------------------------------------------------------------------------
export const notifyInvalidTempVarValueInMetaQuery = (
  tempVar: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  duration: 7500,
  message: `Invalid query supplied for template variable ${tempVar}: ${errorMessage}`,
})

export const notifyInvalidTempVarValueInURLQuery = ({
  key,
  value,
}: TemplateUpdate): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Invalid URL query value of '${value}' supplied for template variable '${key}'.`,
})

export const notifyInvalidTimeRangeValueInURLQuery = (): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Invalid URL query value supplied for lower or upper time range.`,
})

export const notifyInvalidMapType = (): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Template Variables of map type accept two comma separated values per line`,
})

export const notifyInvalidZoomedTimeRangeValueInURLQuery = (): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Invalid URL query value supplied for zoomed lower or zoomed upper time range.`,
})

//  Rule Builder Notifications
//  ----------------------------------------------------------------------------
export const notifyAlertRuleCreated = (ruleName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} created successfully.`,
})

export const notifyAlertRuleCreateFailed = (
  ruleName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  message: `There was a problem creating ${ruleName}: ${errorMessage}`,
})

export const notifyAlertRuleUpdated = (ruleName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} saved successfully.`,
})

export const notifyAlertRuleUpdateFailed = (
  ruleName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  message: `There was a problem saving ${ruleName}: ${errorMessage}`,
})

export const notifyAlertRuleDeleted = (ruleName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} deleted successfully.`,
})

export const notifyAlertRuleDeleteFailed = (
  ruleName: string
): Notification => ({
  ...defaultErrorNotification,
  message: `${ruleName} could not be deleted.`,
})

export const notifyAlertRuleStatusUpdated = (
  ruleName: string,
  updatedStatus: string
): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} ${updatedStatus} successfully.`,
})

export const notifyAlertRuleStatusUpdateFailed = (
  ruleName: string,
  updatedStatus: string
): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} could not be ${updatedStatus}.`,
})

export const notifyAlertRuleRequiresQuery = (): string =>
  'Please select a Database, Measurement, and Field.'

export const notifyAlertRuleRequiresConditionValue = (): string =>
  'Please enter a value in the Conditions section.'

export const notifyAlertRuleDeadmanInvalid = (): string =>
  'Deadman rules require a Database and Measurement.'

// Flux notifications
export const validateSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'No errors found. Happy Happy Joy Joy!',
})

export const notifyCopyToClipboardSuccess = (text: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `'${text}' has been copied to clipboard.`,
})

export const notifyCopyToClipboardFailed = (text: string): Notification => ({
  ...defaultErrorNotification,
  message: `'${text}' was not copied to clipboard.`,
})

export const notifyFluxNameAlreadyTaken = (fluxName: string): Notification => ({
  ...defaultErrorNotification,
  message: `There is already a Flux Connection named "${fluxName}."`,
})

// Service notifications
export const couldNotGetFluxService = (id: string): Notification => ({
  ...defaultErrorNotification,
  message: `Could not find Flux with id ${id}.`,
})

export const couldNotGetServices: Notification = {
  ...defaultErrorNotification,
  message: 'We could not get services',
}

export const fluxCreated: Notification = {
  ...defaultSuccessNotification,
  message: 'Flux Connection Created.  Script your heart out!',
}

export const fluxNotCreated = (message: string): Notification => ({
  ...defaultErrorNotification,
  message,
})

export const fluxNotUpdated = (message: string): Notification => ({
  ...defaultErrorNotification,
  message,
})

export const fluxUpdated: Notification = {
  ...defaultSuccessNotification,
  message: 'Connection Updated. Rejoice!',
}

export const fluxTimeSeriesError = (message: string): Notification => ({
  ...defaultErrorNotification,
  message: `Could not get data: ${message}`,
})

export const fluxResponseTruncatedError = (): Notification => {
  const BYTES_TO_MB = 1 / 1e6
  const APPROX_MAX_RESPONSE_MB = +(MAX_RESPONSE_BYTES * BYTES_TO_MB).toFixed(2)

  return {
    ...defaultErrorNotification,
    message: `Large response truncated to first ${APPROX_MAX_RESPONSE_MB} MB`,
  }
}
