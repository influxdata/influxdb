// All copy for notifications should be stored here for easy editing
// and ensuring stylistic consistency
import {Notification} from 'src/types'

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
export const genericFail = (): string => 'Could not communicate with server.'

export const newVersion = (version: string): Notification => ({
  type: 'info',
  icon: 'cubo-uniform',
  duration: INFINITE,
  message: `Welcome to the latest Chronograf${version}. Local settings cleared.`,
})

export const loadLocalSettingsFailed = (error: string): Notification => ({
  ...defaultErrorNotification,
  message: `Loading local settings failed: ${error}`,
})

export const errorWithAltText = (
  type: string,
  message: string
): Notification => ({
  type,
  icon: 'triangle',
  duration: TEN_SECONDS,
  message,
})

export const presentationMode = (): Notification => ({
  type: 'primary',
  icon: 'expand-b',
  duration: 7500,
  message: 'Press ESC to exit Presentation Mode.',
})

export const dataWritten = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Data was written successfully.',
})

export const dataWriteFailed = (errorMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: `Data write failed: ${errorMessage}`,
})

export const sessionTimedOut = (): Notification => ({
  type: 'primary',
  icon: 'triangle',
  duration: INFINITE,
  message: 'Your session has timed out. Log in again to continue.',
})

export const serverError: Notification = {
  ...defaultErrorNotification,
  message: 'Internal Server Error. Check API Logs.',
}

export const csvDownloadFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Unable to download .CSV file',
})

export const csvUploadFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Please upload a .csv file',
})

// Onboarding notifications
export const SetupSuccess: Notification = {
  ...defaultSuccessNotification,
  message: 'Admin User details have been successfully set',
}

export const SetupError: Notification = {
  ...defaultErrorNotification,
  message: `Could not Setup Admin User at this time.`,
}

export const SetupNotAllowed: Notification = {
  ...defaultErrorNotification,
  message: `Defaults have already been set on this account.`,
}

export const SigninSuccessful: Notification = {
  ...defaultSuccessNotification,
  message: `YAY! You're good to go, RELOAD to continue`,
}
export const SigninError: Notification = {
  ...defaultErrorNotification,
  message: `OH Noes! Sign In did not work. :(`,
}

//  Hosts Page Notifications
//  ----------------------------------------------------------------------------
export const unableToGetHosts = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Unable to get Hosts.',
})

export const unableToGetApps = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Unable to get Apps for Hosts.',
})

//  InfluxDB Sources Notifications
//  ----------------------------------------------------------------------------
export const sourceCreationSucceeded = (sourceName: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `Connected to InfluxDB ${sourceName} successfully.`,
})

export const sourceCreationFailed = (
  sourceName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Unable to connect to InfluxDB ${sourceName}: ${errorMessage}`,
})

export const sourceUpdated = (sourceName: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `Updated InfluxDB ${sourceName} Connection successfully.`,
})

export const sourceUpdateFailed = (
  sourceName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Failed to update InfluxDB ${sourceName} Connection: ${errorMessage}`,
})

export const sourceDeleted = (sourceName: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `${sourceName} deleted successfully.`,
})

export const sourceDeleteFailed = (sourceName: string): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `There was a problem deleting ${sourceName}.`,
})

export const sourceNoLongerAvailable = (sourceName: string): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Source ${sourceName} is no longer available. Please ensure InfluxDB is running.`,
})

export const errorConnectingToSource = (
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Unable to connect to InfluxDB source: ${errorMessage}`,
})

//  Multitenancy User Notifications
//  ----------------------------------------------------------------------------
export const userRemovedFromAllOrgs = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message:
    'You have been removed from all organizations. Please contact your administrator.',
})

export const userRemovedFromCurrentOrg = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'You were removed from your current organization.',
})

export const orgHasNoSources = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'Organization has no sources configured.',
})

export const userSwitchedOrgs = (
  orgName: string,
  roleName: string
): Notification => ({
  ...defaultSuccessNotification,
  type: 'primary',
  message: `Now logged in to '${orgName}' as '${roleName}'.`,
})

export const orgIsPrivate = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message:
    'This organization is private. To gain access, you must be explicitly added by an administrator.',
})

export const currentOrgDeleted = (): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'Your current organization was deleted.',
})

export const jsonFeedFailed = (url: string): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to fetch JSON Feed for News Feed from '${url}'`,
})

//  Chronograf Admin Notifications
//  ----------------------------------------------------------------------------
export const mappingDeleted = (id: string, scheme: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Mapping ${id}/${scheme} deleted successfully.`,
})

export const chronografUserAddedToOrg = (
  user: string,
  organization: string
): string => `${user} has been added to ${organization} successfully.`

export const chronografUserRemovedFromOrg = (
  user: string,
  organization: string
): string => `${user} has been removed from ${organization} successfully.`

export const chronografUserUpdated = (message: string): Notification => ({
  ...defaultSuccessNotification,
  message,
})

export const chronografOrgDeleted = (orgName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Organization ${orgName} deleted successfully.`,
})

export const chronografUserDeleted = (
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

export const chronografUserMissingNameAndProvider = (): Notification => ({
  ...defaultErrorNotification,
  type: 'warning',
  message: 'User must have a Name and Provider.',
})

//  InfluxDB Admin Notifications
//  ----------------------------------------------------------------------------
export const dbUserCreated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'User created successfully.',
})

export const dbUserCreationFailed = (errorMessage: string): string =>
  `Failed to create User: ${errorMessage}`

export const dbUserDeleted = (userName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `User "${userName}" deleted successfully.`,
})

export const dbUserDeleteFailed = (errorMessage: string): string =>
  `Failed to delete User: ${errorMessage}`

export const dbUserPermissionsUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'User Permissions updated successfully.',
})

export const dbUserPermissionsUpdateFailed = (errorMessage: string): string =>
  `Failed to update User Permissions: ${errorMessage}`

export const dbUserRolesUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'User Roles updated successfully.',
})

export const dbUserRolesUpdateFailed = (errorMessage: string): string =>
  `Failed to update User Roles: ${errorMessage}`

export const dbUserPasswordUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'User Password updated successfully.',
})

export const dbUserPasswordUpdateFailed = (errorMessage: string): string =>
  `Failed to update User Password: ${errorMessage}`

export const DatabaseCreated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Database created successfully.',
})

export const dbCreationFailed = (errorMessage: string): string =>
  `Failed to create Database: ${errorMessage}`

export const dbDeleted = (databaseName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Database "${databaseName}" deleted successfully.`,
})

export const dbDeleteFailed = (errorMessage: string): string =>
  `Failed to delete Database: ${errorMessage}`

export const roleCreated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Role created successfully.',
})

export const roleCreationFailed = (errorMessage: string): string =>
  `Failed to create Role: ${errorMessage}`

export const roleDeleted = (roleName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Role "${roleName}" deleted successfully.`,
})

export const roleDeleteFailed = (errorMessage: string): string =>
  `Failed to delete Role: ${errorMessage}`

export const roleUsersUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Role Users updated successfully.',
})

export const roleUsersUpdateFailed = (errorMessage: string): string =>
  `Failed to update Role Users: ${errorMessage}`

export const rolePermissionsUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Role Permissions updated successfully.',
})

export const rolePermissionsUpdateFailed = (errorMessage: string): string =>
  `Failed to update Role Permissions: ${errorMessage}`

export const retentionPolicyCreated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Retention Policy created successfully.',
})

export const retentionPolicyCreationError = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to create Retention Policy. Please check name and duration.',
})

export const retentionPolicyCreationFailed = (errorMessage: string): string =>
  `Failed to create Retention Policy: ${errorMessage}`

export const retentionPolicyDeleted = (rpName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `Retention Policy "${rpName}" deleted successfully.`,
})

export const retentionPolicyDeleteFailed = (errorMessage: string): string =>
  `Failed to delete Retention Policy: ${errorMessage}`

export const retentionPolicyUpdated = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'Retention Policy updated successfully.',
})

export const retentionPolicyUpdateFailed = (errorMessage: string): string =>
  `Failed to update Retention Policy: ${errorMessage}`

export const QueriesError = (errorMessage: string): Notification => ({
  ...defaultErrorNotification,
  message: errorMessage,
})

export const retentionPolicyCantHaveEmptyFields = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Fields cannot be empty.',
})

export const databaseDeleteConfirmationRequired = (
  databaseName: string
): Notification => ({
  ...defaultErrorNotification,
  message: `Type "DELETE ${databaseName}" to confirm. This action cannot be undone.`,
})

export const dbUserNamePasswordInvalid = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Username and/or Password too short.',
})

export const roleNameInvalid = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Role name is too short.',
})

export const databaseNameInvalid = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Database name cannot be blank.',
})

export const databaseNameAlreadyExists = (): Notification => ({
  ...defaultErrorNotification,
  message: 'A Database by this name already exists.',
})

//  Dashboard Notifications
//  ----------------------------------------------------------------------------
export const tempVarAlreadyExists = (tempVarName: string): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Variable '${tempVarName}' already exists. Please enter a new value.`,
})

export const dashboardNotFound = (dashboardID: string): Notification => ({
  ...defaultErrorNotification,
  icon: 'dash-h',
  message: `Dashboard ${dashboardID} could not be found`,
})

export const dashboardUpdateFailed = (): Notification => ({
  ...defaultErrorNotification,
  icon: 'dash-h',
  message: 'Could not update dashboard',
})

export const dashboardDeleted = (name: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `Dashboard ${name} deleted successfully.`,
})

export const dashboardExported = (name: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `Dashboard ${name} exported successfully.`,
})

export const dashboardExportFailed = (
  name: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: `Failed to export Dashboard ${name}: ${errorMessage}.`,
})

export const dashboardCreateFailed = () => ({
  ...defaultErrorNotification,
  message: 'Failed to created dashboard.',
})

export const dashboardSetDefaultFailed = (name: string) => ({
  ...defaultErrorNotification,
  message: `Failed to set ${name} to default dashboard.`,
})

export const dashboardImported = (name: string): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `Dashboard ${name} imported successfully.`,
})

export const dashboardImportFailed = (
  fileName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: `Failed to import Dashboard from file ${fileName}: ${errorMessage}.`,
})

export const dashboardDeleteFailed = (
  name: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  message: `Failed to delete Dashboard ${name}: ${errorMessage}.`,
})

export const cellAdded = (): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  duration: 1900,
  message: `Added new cell to dashboard.`,
})

export const cellAddFailed = (): Notification => ({
  ...defaultErrorNotification,
  message: 'Failed to add cell to dashboard',
})

export const cellDeleted = (): Notification => ({
  ...defaultDeletionNotification,
  icon: 'dash-h',
  duration: 1900,
  message: `Cell deleted from dashboard.`,
})

export const builderDisabled = (): Notification => ({
  type: 'info',
  icon: 'graphline',
  duration: 7500,
  message: `Your query contains a user-defined Template Variable. The Schema Explorer cannot render the query and is disabled.`,
})

//  Template Variables & URL Queries
//  ----------------------------------------------------------------------------
export const invalidTimeRangeValueInURLQuery = (): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Invalid URL query value supplied for lower or upper time range.`,
})

export const invalidMapType = (): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Template Variables of map type accept two comma separated values per line`,
})

export const invalidZoomedTimeRangeValueInURLQuery = (): Notification => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Invalid URL query value supplied for zoomed lower or zoomed upper time range.`,
})

//  Rule Builder Notifications
//  ----------------------------------------------------------------------------
export const alertRuleCreated = (ruleName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} created successfully.`,
})

export const alertRuleCreateFailed = (
  ruleName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  message: `There was a problem creating ${ruleName}: ${errorMessage}`,
})

export const alertRuleUpdated = (ruleName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} saved successfully.`,
})

export const alertRuleUpdateFailed = (
  ruleName: string,
  errorMessage: string
): Notification => ({
  ...defaultErrorNotification,
  message: `There was a problem saving ${ruleName}: ${errorMessage}`,
})

export const alertRuleDeleted = (ruleName: string): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} deleted successfully.`,
})

export const alertRuleDeleteFailed = (ruleName: string): Notification => ({
  ...defaultErrorNotification,
  message: `${ruleName} could not be deleted.`,
})

export const alertRuleStatusUpdated = (
  ruleName: string,
  updatedStatus: string
): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} ${updatedStatus} successfully.`,
})

export const alertRuleStatusUpdateFailed = (
  ruleName: string,
  updatedStatus: string
): Notification => ({
  ...defaultSuccessNotification,
  message: `${ruleName} could not be ${updatedStatus}.`,
})

export const alertRuleRequiresQuery = (): string =>
  'Please select a Database, Measurement, and Field.'

export const alertRuleRequiresConditionValue = (): string =>
  'Please enter a value in the Conditions section.'

export const alertRuleDeadmanInvalid = (): string =>
  'Deadman rules require a Database and Measurement.'

// Flux notifications
export const validateSuccess = (): Notification => ({
  ...defaultSuccessNotification,
  message: 'No errors found. Happy Happy Joy Joy!',
})

export const copyToClipboardSuccess = (
  text: string,
  title: string = ''
): Notification => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `${title}'${text}' has been copied to clipboard.`,
})

export const copyToClipboardFailed = (
  text: string,
  title: string = ''
): Notification => ({
  ...defaultErrorNotification,
  message: `${title}'${text}' was not copied to clipboard.`,
})

export const fluxNameAlreadyTaken = (fluxName: string): Notification => ({
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
