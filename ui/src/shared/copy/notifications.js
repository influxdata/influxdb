// All copy for notifications should be stored here for easy editing
// and ensuring stylistic consistency

import {FIVE_SECONDS, TEN_SECONDS, INFINITE} from 'shared/constants/index'

const defaultErrorNotification = {
  type: 'error',
  icon: 'alert-triangle',
  duration: TEN_SECONDS,
}

const defaultSuccessNotification = {
  type: 'success',
  icon: 'checkmark',
  duration: FIVE_SECONDS,
}

const defaultDeletionNotification = {
  type: 'primary',
  icon: 'trash',
  duration: FIVE_SECONDS,
}

//  Misc Notifications
//  ----------------------------------------------------------------------------
export const notifyGenericFail = () => 'Could not communicate with server.'

export const notifyNewVersion = version => ({
  type: 'info',
  icon: 'cubo-uniform',
  duration: INFINITE,
  message: `Welcome to the latest Chronograf${version}. Local settings cleared.`,
})

export const notifyLoadLocalSettingsFailed = error => ({
  ...defaultErrorNotification,
  message: `Loading local settings failed: ${error}`,
})

export const notifyErrorWithAltText = (type, message) => ({
  type,
  icon: 'triangle',
  duration: TEN_SECONDS,
  message,
})

export const notifyPresentationMode = () => ({
  type: 'primary',
  icon: 'expand-b',
  duration: 7500,
  message: 'Press ESC to exit Presentation Mode.',
})

export const notifyDataWritten = () => ({
  ...defaultSuccessNotification,
  message: 'Data was written successfully.',
})

export const notifyDataWriteFailed = errorMessage => ({
  ...defaultErrorNotification,
  message: `Data write failed: ${errorMessage}`,
})

export const notifySessionTimedOut = () => ({
  type: 'primary',
  icon: 'triangle',
  duration: INFINITE,
  message: 'Your session has timed out. Log in again to continue.',
})

export const notifyServerError = () => ({
  ...defaultErrorNotification,
  mesasage: 'Internal Server Error. Check API Logs.',
})

export const notifyCouldNotRetrieveKapacitors = sourceID => ({
  ...defaultErrorNotification,
  mesasage: `Internal Server Error. Could not retrieve Kapacitor Connections for source ${sourceID}.`,
})

export const notifyCouldNotRetrieveKapacitorServices = kapacitor => ({
  ...defaultErrorNotification,
  message: `Interanl Server Error. Could not retrieve services for Kapacitor ${kapacitor}`,
})

export const notifyCouldNotDeleteKapacitor = () => ({
  ...defaultErrorNotification,
  message: 'Internal Server Error. Could not delete Kapacitor Connection.',
})

export const notifyCSVDownloadFailed = () => ({
  ...defaultErrorNotification,
  message: 'Unable to download .CSV file',
})

//  Hosts Page Notifications
//  ----------------------------------------------------------------------------
export const notifyUnableToGetHosts = () => ({
  ...defaultErrorNotification,
  message: 'Unable to get Hosts.',
})

export const notifyUnableToGetApps = () => ({
  ...defaultErrorNotification,
  message: 'Unable to get Apps for Hosts.',
})

//  InfluxDB Sources Notifications
//  ----------------------------------------------------------------------------
export const notifySourceCreationSucceeded = sourceName => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `Connected to InfluxDB ${sourceName} successfully.`,
})

export const notifySourceCreationFailed = (sourceName, errorMessage) => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Unable to connect to InfluxDB ${sourceName}: ${errorMessage}`,
})

export const notifySourceUdpated = sourceName => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `Updated InfluxDB ${sourceName} Connection successfully.`,
})

export const notifySourceUdpateFailed = (sourceName, errorMessage) => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Failed to update InfluxDB ${sourceName} Connection: ${errorMessage}`,
})

export const notifySourceDeleted = sourceName => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `${sourceName} deleted successfully.`,
})

export const notifySourceDeleteFailed = sourceName => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `There was a problem deleting ${sourceName}.`,
})

export const notifySourceNoLongerAvailable = sourceName =>
  `Source ${sourceName} is no longer available. Please ensure InfluxDB is running.`

export const notifyNoSourcesAvailable = sourceName =>
  `Unable to connect to source ${sourceName}. No other sources available.`

export const notifyUnableToRetrieveSources = () => 'Unable to retrieve sources.'

export const notifyUnableToConnectSource = sourceName =>
  `Unable to connect to source ${sourceName}.`

export const notifyErrorConnectingToSource = errorMessage =>
  `Unable to connect to InfluxDB source: ${errorMessage}`

//  Multitenancy User Notifications
//  ----------------------------------------------------------------------------
export const notifyUserRemovedFromAllOrgs = () => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message:
    'You have been removed from all organizations. Please contact your administrator.',
})

export const notifyUserRemovedFromCurrentOrg = () => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'You were removed from your current organization.',
})

export const notifyOrgHasNoSources = () => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'Organization has no sources configured.',
})

export const notifyUserSwitchedOrgs = (orgName, roleName) => ({
  ...defaultSuccessNotification,
  type: 'primary',
  message: `Now logged in to '${orgName}' as '${roleName}'.`,
})

export const notifyOrgIsPrivate = () => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message:
    'This organization is private. To gain access, you must be explicitly added by an administrator.',
})

export const notifyCurrentOrgDeleted = () => ({
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'Your current organization was deleted.',
})

export const notifyJSONFeedFailed = url => ({
  ...defaultErrorNotification,
  message: `Failed to fetch JSON Feed for News Feed from '${url}'`,
})

//  Chronograf Admin Notifications
//  ----------------------------------------------------------------------------
export const notifyMappingDeleted = (id, scheme) => ({
  ...defaultSuccessNotification,
  message: `Mapping ${id}/${scheme} deleted successfully.`,
})

export const notifyChronografUserAddedToOrg = (user, organization) =>
  `${user} has been added to ${organization} successfully.`

export const notifyChronografUserRemovedFromOrg = (user, organization) =>
  `${user} has been removed from ${organization} successfully.`

export const notifyChronografUserUpdated = message => ({
  ...defaultSuccessNotification,
  message,
})

export const notifyChronografOrgDeleted = orgName => ({
  ...defaultSuccessNotification,
  message: `Organization ${orgName} deleted successfully.`,
})

export const notifyChronografUserDeleted = (user, isAbsoluteDelete) => ({
  ...defaultSuccessNotification,
  message: `${user} has been removed from ${
    isAbsoluteDelete
      ? 'all organizations and deleted.'
      : 'the current organization.'
  }`,
})

export const notifyChronografUserMissingNameAndProvider = () => ({
  ...defaultErrorNotification,
  type: 'warning',
  message: 'User must have a Name and Provider.',
})

//  InfluxDB Admin Notifications
//  ----------------------------------------------------------------------------
export const notifyDBUserCreated = () => ({
  ...defaultSuccessNotification,
  message: 'User created successfully.',
})

export const notifyDBUserCreationFailed = errorMessage =>
  `Failed to create User: ${errorMessage}`

export const notifyDBUserDeleted = userName => ({
  ...defaultSuccessNotification,
  message: `User "${userName}" deleted successfully.`,
})

export const notifyDBUserDeleteFailed = errorMessage =>
  `Failed to delete User: ${errorMessage}`

export const notifyDBUserPermissionsUpdated = () => ({
  ...defaultSuccessNotification,
  message: 'User Permissions updated successfully.',
})

export const notifyDBUserPermissionsUpdateFailed = errorMessage =>
  `Failed to update User Permissions: ${errorMessage}`

export const notifyDBUserRolesUpdated = () => ({
  ...defaultSuccessNotification,
  message: 'User Roles updated successfully.',
})

export const notifyDBUserRolesUpdateFailed = errorMessage =>
  `Failed to update User Roles: ${errorMessage}`

export const notifyDBUserPasswordUpdated = () => ({
  ...defaultSuccessNotification,
  message: 'User Password updated successfully.',
})

export const notifyDBUserPasswordUpdateFailed = errorMessage =>
  `Failed to update User Password: ${errorMessage}`

export const notifyDatabaseCreated = () => ({
  ...defaultSuccessNotification,
  message: 'Database created successfully.',
})

export const notifyDBCreationFailed = errorMessage =>
  `Failed to create Database: ${errorMessage}`

export const notifyDBDeleted = databaseName => ({
  ...defaultSuccessNotification,
  message: `Database "${databaseName}" deleted successfully.`,
})

export const notifyDBDeleteFailed = errorMessage =>
  `Failed to delete Database: ${errorMessage}`

export const notifyRoleCreated = () => ({
  ...defaultSuccessNotification,
  message: 'Role created successfully.',
})

export const notifyRoleCreationFailed = errorMessage =>
  `Failed to create Role: ${errorMessage}`

export const notifyRoleDeleted = roleName => ({
  ...defaultSuccessNotification,
  message: `Role "${roleName}" deleted successfully.`,
})

export const notifyRoleDeleteFailed = errorMessage =>
  `Failed to delete Role: ${errorMessage}`

export const notifyRoleUsersUpdated = () => ({
  ...defaultSuccessNotification,
  message: 'Role Users updated successfully.',
})

export const notifyRoleUsersUpdateFailed = errorMessage =>
  `Failed to update Role Users: ${errorMessage}`

export const notifyRolePermissionsUpdated = () => ({
  ...defaultSuccessNotification,
  message: 'Role Permissions updated successfully.',
})

export const notifyRolePermissionsUpdateFailed = errorMessage =>
  `Failed to update Role Permissions: ${errorMessage}`

export const notifyRetentionPolicyCreated = () => ({
  ...defaultSuccessNotification,
  message: 'Retention Policy created successfully.',
})

export const notifyRetentionPolicyCreationError = () => ({
  ...defaultErrorNotification,
  message: 'Failed to create Retention Policy. Please check name and duration.',
})

export const notifyRetentionPolicyCreationFailed = errorMessage =>
  `Failed to create Retention Policy: ${errorMessage}`

export const notifyRetentionPolicyDeleted = rpName => ({
  ...defaultSuccessNotification,
  message: `Retention Policy "${rpName}" deleted successfully.`,
})

export const notifyRetentionPolicyDeleteFailed = errorMessage =>
  `Failed to delete Retention Policy: ${errorMessage}`

export const notifyRetentionPolicyUpdated = () => ({
  ...defaultSuccessNotification,
  message: 'Retention Policy updated successfully.',
})

export const notifyRetentionPolicyUpdateFailed = errorMessage =>
  `Failed to update Retention Policy: ${errorMessage}`

export const notifyQueriesError = errorMessage => ({
  ...defaultErrorNotification,
  errorMessage,
})

export const notifyRetentionPolicyCantHaveEmptyFields = () => ({
  ...defaultErrorNotification,
  message: 'Fields cannot be empty.',
})

export const notifyDatabaseDeleteConfirmationRequired = databaseName => ({
  ...defaultErrorNotification,
  message: `Type "DELETE ${databaseName}" to confirm. This action cannot be undone.`,
})

export const notifyDBUserNamePasswordInvalid = () => ({
  ...defaultErrorNotification,
  message: 'Username and/or Password too short.',
})

export const notifyRoleNameInvalid = () => ({
  ...defaultErrorNotification,
  message: 'Role name is too short.',
})

export const notifyDatabaseNameInvalid = () => ({
  ...defaultErrorNotification,
  message: 'Database name cannot be blank.',
})

export const notifyDatabaseNameAlreadyExists = () => ({
  ...defaultErrorNotification,
  message: 'A Database by this name already exists.',
})

//  Dashboard Notifications
//  ----------------------------------------------------------------------------
export const notifyTempVarAlreadyExists = tempVarName => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Variable '${tempVarName}' already exists. Please enter a new value.`,
})

export const notifyDashboardNotFound = dashboardID => ({
  ...defaultErrorNotification,
  icon: 'dash-h',
  message: `Dashboard ${dashboardID} could not be found`,
})

export const notifyDashboardDeleted = name => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `Dashboard ${name} deleted successfully.`,
})

export const notifyDashboardDeleteFailed = (name, errorMessage) =>
  `Failed to delete Dashboard ${name}: ${errorMessage}.`

export const notifyCellAdded = name => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  duration: 1900,
  message: `Added "${name}" to dashboard.`,
})

export const notifyCellDeleted = name => ({
  ...defaultDeletionNotification,
  icon: 'dash-h',
  duration: 1900,
  message: `Deleted "${name}" from dashboard.`,
})

export const notifyBuilderDisabled = () => ({
  type: 'info',
  icon: 'graphline',
  duration: 7500,
  message: `Your query contains a user-defined Template Variable. The Schema Explorer cannot render the query and is disabled.`,
})

//  Rule Builder Notifications
//  ----------------------------------------------------------------------------
export const notifyAlertRuleCreated = () => ({
  ...defaultSuccessNotification,
  message: 'Alert Rule created successfully.',
})

export const notifyAlertRuleCreateFailed = () => ({
  ...defaultErrorNotification,
  message: 'Alert Rule could not be created.',
})

export const notifyAlertRuleUpdated = ruleName => ({
  ...defaultSuccessNotification,
  message: `${ruleName} saved successfully.`,
})

export const notifyAlertRuleUpdateFailed = (ruleName, errorMessage) => ({
  ...defaultErrorNotification,
  message: `There was a problem saving ${ruleName}: ${errorMessage}`,
})

export const notifyAlertRuleDeleted = ruleName => ({
  ...defaultSuccessNotification,
  message: `${ruleName} deleted successfully.`,
})

export const notifyAlertRuleDeleteFailed = ruleName => ({
  ...defaultErrorNotification,
  message: `${ruleName} could not be deleted.`,
})

export const notifyAlertRuleStatusUpdated = (ruleName, updatedStatus) => ({
  ...defaultSuccessNotification,
  message: `${ruleName} ${updatedStatus} successfully.`,
})

export const notifyAlertRuleStatusUpdateFailed = (ruleName, updatedStatus) => ({
  ...defaultSuccessNotification,
  message: `${ruleName} could not be ${updatedStatus}.`,
})

export const notifyAlertRuleRequiresQuery = () =>
  'Please select a Database, Measurement, and Field.'

export const notifyAlertRuleRequiresConditionValue = () =>
  'Please enter a value in the Conditions section.'

export const notifyAlertRuleDeadmanInvalid = () =>
  'Deadman rules require a Database and Measurement.'

//  Kapacitor Configuration Notifications
//  ----------------------------------------------------------------------------
export const notifyKapacitorNameAlreadyTaken = kapacitorName => ({
  ...defaultErrorNotification,
  message: `There is already a Kapacitor Connection named "${kapacitorName}".`,
})

export const notifyCouldNotFindKapacitor = () => ({
  ...defaultErrorNotification,
  message: 'We could not find a Kapacitor configuration for this source.',
})

export const notifyRefreshKapacitorFailed = () => ({
  ...defaultErrorNotification,
  message: 'There was an error getting the Kapacitor configuration.',
})

export const notifyAlertEndpointSaved = endpoint => ({
  ...defaultSuccessNotification,
  message: `Alert configuration for ${endpoint} saved successfully.`,
})

export const notifyAlertEndpointSaveFailed = (endpoint, errorMessage) => ({
  ...defaultErrorNotification,
  message: `There was an error saving the alert configuration for ${endpoint}: ${errorMessage}`,
})

export const notifyAlertEndpointDeleteFailed = (
  endpoint,
  config,
  errorMessage
) => ({
  ...defaultErrorNotification,
  message: `There was an error deleting the alert configuration for ${endpoint}/${config}: ${errorMessage}`,
})

export const notifyAlertEndpointDeleted = (endpoint, config) => ({
  ...defaultSuccessNotification,
  message: `Alert configuration for ${endpoint}/${config} deleted successfully.`,
})

export const notifyTestAlertSent = endpoint => ({
  ...defaultSuccessNotification,
  duration: TEN_SECONDS,
  message: `Test Alert sent to ${endpoint}. If the Alert does not reach its destination, please check your endpoint configuration settings.`,
})

export const notifyTestAlertFailed = (endpoint, errorMessage) => ({
  ...defaultErrorNotification,
  message: `There was an error sending a Test Alert to ${endpoint}${
    errorMessage ? `: ${errorMessage}` : '.'
  }`,
})

export const notifyInvalidBatchSizeValue = () => ({
  ...defaultErrorNotification,
  message: 'Batch Size cannot be empty.',
})

export const notifyKapacitorConnectionFailed = () => ({
  ...defaultErrorNotification,
  message:
    'Could not connect to Kapacitor. Check your connection settings in the Configuration page.',
})

export const notifyKapacitorCreated = () => ({
  ...defaultSuccessNotification,
  message:
    'Connected to Kapacitor successfully! Configuring endpoints is optional.',
})

export const notifyKapacitorCreateFailed = () => ({
  ...defaultErrorNotification,
  message: 'There was a problem connecting to Kapacitor.',
})

export const notifyKapacitorUpdated = () => ({
  ...defaultSuccessNotification,
  message: 'Kapacitor Connection updated successfully.',
})

export const notifyKapacitorUpdateFailed = () => ({
  ...defaultErrorNotification,
  message: 'There was a problem updating the Kapacitor Connection.',
})

//  TICKscript Notifications
//  ----------------------------------------------------------------------------
export const notifyTickScriptCreated = () => ({
  ...defaultSuccessNotification,
  message: 'TICKscript successfully created.',
})

export const notifyTickscriptCreationFailed = () =>
  'Failed to create TICKscript.'

export const notifyTickscriptUpdated = () => ({
  ...defaultSuccessNotification,
  message: 'TICKscript successfully updated.',
})

export const notifyTickscriptUpdateFailed = () => 'Failed to update TICKscript.'

export const notifyTickscriptLoggingUnavailable = () => ({
  type: 'warning',
  icon: 'alert-triangle',
  duration: INFINITE,
  message: 'Kapacitor version 1.4 required to view TICKscript logs',
})

export const notifyTickscriptLoggingError = () => ({
  ...defaultErrorNotification,
  message: 'Could not collect kapacitor logs',
})

export const notifyKapacitorNotFound = () => ({
  ...defaultErrorNotification,
  message: 'We could not find a Kapacitor configuration for this source.',
})

// IFQL notifications
export const analyzeSuccess = {
  ...defaultSuccessNotification,
  message: 'No errors found. Happy Happy Joy Joy!',
}
