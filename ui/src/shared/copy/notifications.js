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

//  Misc Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_GENERIC_FAIL = 'Could not communicate with server.'

export const NOTIFY_NEW_VERSION = message => ({
  type: 'info',
  icon: 'cubo-uniform',
  duration: INFINITE,
  message,
})

export const NOTIFY_ERR_WITH_ALT_TEXT = (type, message) => ({
  type,
  icon: 'triangle',
  duration: TEN_SECONDS,
  message,
})

export const NOTIFY_PRESENTATION_MODE = {
  type: 'primary',
  icon: 'expand-b',
  duration: 7500,
  message: 'Press ESC to exit Presentation Mode.',
}

export const NOTIFY_DATA_WRITTEN = {
  ...defaultSuccessNotification,
  message: 'Data was written successfully.',
}

export const NOTIFY_SESSION_TIMED_OUT = {
  type: 'primary',
  icon: 'triangle',
  duration: INFINITE,
  message: 'Your session has timed out. Log in again to continue.',
}

export const NOTIFY_SERVER_ERROR = {
  ...defaultErrorNotification,
  mesasage: 'Internal Server Error. Check API Logs.',
}

export const NOTIFY_COULD_NOT_RETRIEVE_KAPACITORS = sourceID => ({
  ...defaultErrorNotification,
  mesasage: `Internal Server Error. Could not retrieve Kapacitor Connections for source ${sourceID}.`,
})

export const NOTIFY_COULD_NOT_DELETE_KAPACITOR = {
  ...defaultErrorNotification,
  message: 'Internal Server Error. Could not delete Kapacitor Connection.',
}

//  Hosts Page Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_UNABLE_TO_GET_HOSTS = {
  ...defaultErrorNotification,
  message: 'Unable to get Hosts.',
}

export const NOTIFY_UNABLE_TO_GET_APPS = {
  ...defaultErrorNotification,
  message: 'Unable to get Apps for Hosts.',
}

//  InfluxDB Sources Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_SOURCE_CREATION_SUCCEEDED = sourceName => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `Connected to InfluxDB ${sourceName} successfully.`,
})

export const NOTIFY_SOURCE_CREATION_FAILED = (sourceName, errorMessage) => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Unable to connect to InfluxDB ${sourceName}: ${errorMessage}`,
})

export const NOTIFY_SOURCE_UPDATED = sourceName => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `Updated InfluxDB ${sourceName} Connection successfully.`,
})

export const NOTIFY_SOURCE_UPDATE_FAILED = (sourceName, errorMessage) => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `Failed to update InfluxDB ${sourceName} Connection: ${errorMessage}`,
})

export const NOTIFY_SOURCE_DELETED = sourceName => ({
  ...defaultSuccessNotification,
  icon: 'server2',
  message: `${sourceName} deleted successfully.`,
})

export const NOTIFY_SOURCE_DELETE_FAILED = sourceName => ({
  ...defaultErrorNotification,
  icon: 'server2',
  message: `There was a problem deleting ${sourceName}.`,
})

export const NOTIFY_SOURCE_NO_LONGER_AVAILABLE = sourceName =>
  `Source ${sourceName} is no longer available. Successfully connected to another source.`

export const NOTIFY_NO_SOURCES_AVAILABLE = sourceName =>
  `Unable to connect to source ${sourceName}. No other sources available.`

export const NOTIFY_UNABLE_TO_RETRIEVE_SOURCES = 'Unable to retrieve sources.'

export const NOTIFY_UNABLE_TO_CONNECT_SOURCE = sourceName =>
  `Unable to connect to source ${sourceName}.`

export const NOTIFY_ERROR_CONNECTING_TO_SOURCE = errorMessage =>
  `Unable to connect to InfluxDB source: ${errorMessage}`

//  Multitenancy User Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_USER_REMOVED_FROM_ALL_ORGS = {
  ...defaultErrorNotification,
  duration: INFINITE,
  message:
    'You have been removed from all organizations. Please contact your administrator.',
}

export const NOTIFY_USER_REMOVED_FROM_CURRENT_ORG = {
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'You were removed from your current organization.',
}

export const NOTIFY_ORG_HAS_NO_SOURCES = {
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'Organization has no sources configured.',
}

export const NOTIFY_USER_SWITCHED_ORGS = (orgName, roleName) => ({
  ...defaultSuccessNotification,
  type: 'primary',
  message: `Now logged in to '${orgName}' as '${roleName}'.`,
})

export const NOTIFY_ORG_IS_PRIVATE = {
  ...defaultErrorNotification,
  duration: INFINITE,
  message:
    'This organization is private. To gain access, you must be explicitly added by an administrator.',
}

export const NOTIFY_CURRENT_ORG_DELETED = {
  ...defaultErrorNotification,
  duration: INFINITE,
  message: 'Your current organization was deleted.',
}

//  Chronograf Admin Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_MAPPING_DELETED = (id, scheme) => ({
  ...defaultSuccessNotification,
  message: `Mapping ${id}/${scheme} deleted successfully.`,
})

export const NOTIFY_CHRONOGRAF_USER_ADDED_TO_ORG = (user, organization) =>
  `${user} has been added to ${organization} successfully.`

export const NOTIFY_CHRONOGRAF_USER_REMOVED_FROM_ORG = (user, organization) =>
  `${user} has been removed from ${organization} successfully.`

export const NOTIFY_CHRONOGRAF_USER_UPDATED = message => ({
  ...defaultSuccessNotification,
  message,
})

export const NOTIFY_CHRONOGRAF_ORG_DELETED = orgName => ({
  ...defaultSuccessNotification,
  message: `Organization ${orgName} deleted successfully.`,
})

export const NOTIFY_CHRONOGRAF_USER_DELETED = (user, isAbsoluteDelete) => ({
  ...defaultSuccessNotification,
  message: `${user} has been removed from ${
    isAbsoluteDelete
      ? 'all organizations and deleted.'
      : 'the current organization.'
  }`,
})

export const NOTIFY_CHRONOGRAF_USER_MISSING_NAME_AND_PROVIDER = {
  ...defaultErrorNotification,
  type: 'warning',
  message: 'User must have a Name and Provider.',
}

//  InfluxDB Admin Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_DB_USER_CREATED = {
  ...defaultSuccessNotification,
  message: 'User created successfully.',
}

export const NOTIFY_DB_USER_CREATION_FAILED = errorMessage =>
  `Failed to create User: ${errorMessage}`

export const NOTIFY_DB_USER_DELETED = userName => ({
  ...defaultSuccessNotification,
  message: `User "${userName}" deleted successfully.`,
})

export const NOTIFY_DB_USER_DELETION_FAILED = errorMessage =>
  `Failed to delete User: ${errorMessage}`

export const NOTIFY_DB_USER_PERMISSIONS_UPDATED = {
  ...defaultSuccessNotification,
  message: 'User Permissions updated successfully.',
}

export const NOTIFY_DB_USER_PERMISSIONS_UPDATE_FAILED = errorMessage =>
  `Failed to update User Permissions: ${errorMessage}`

export const NOTIFY_DB_USER_ROLES_UPDATED = {
  ...defaultSuccessNotification,
  message: 'User Roles updated successfully.',
}

export const NOTIFY_DB_USER_ROLES_UPDATE_FAILED = errorMessage =>
  `Failed to update User Roles: ${errorMessage}`

export const NOTIFY_DB_USER_PASSWORD_UPDATED = {
  ...defaultSuccessNotification,
  message: 'User Password updated successfully.',
}

export const NOTIFY_DB_USER_PASSWORD_UPDATE_FAILED = errorMessage =>
  `Failed to update User Password: ${errorMessage}`

export const NOTIFY_DATABASE_CREATED = {
  ...defaultSuccessNotification,
  message: 'Database created successfully.',
}

export const NOTIFY_DATABASE_CREATION_FAILED = errorMessage =>
  `Failed to create Database: ${errorMessage}`

export const NOTIFY_DATABASE_DELETED = databaseName => ({
  ...defaultSuccessNotification,
  message: `Database "${databaseName}" deleted successfully.`,
})

export const NOTIFY_DATABASE_DELETION_FAILED = errorMessage =>
  `Failed to delete Database: ${errorMessage}`

export const NOTIFY_ROLE_CREATED = {
  ...defaultSuccessNotification,
  message: 'Role created successfully.',
}

export const NOTIFY_ROLE_CREATION_FAILED = errorMessage =>
  `Failed to create Role: ${errorMessage}`

export const NOTIFY_ROLE_DELETED = roleName => ({
  ...defaultSuccessNotification,
  message: `Role "${roleName}" deleted successfully.`,
})

export const NOTIFY_ROLE_DELETION_FAILED = errorMessage =>
  `Failed to delete Role: ${errorMessage}`

export const NOTIFY_ROLE_USERS_UPDATED = {
  ...defaultSuccessNotification,
  message: 'Role Users updated successfully.',
}

export const NOTIFY_ROLE_USERS_UPDATE_FAILED = errorMessage =>
  `Failed to update Role Users: ${errorMessage}`

export const NOTIFY_ROLE_PERMISSIONS_UPDATED = {
  ...defaultSuccessNotification,
  message: 'Role Permissions updated successfully.',
}

export const NOTIFY_ROLE_PERMISSIONS_UPDATE_FAILED = errorMessage =>
  `Failed to update Role Permissions: ${errorMessage}`

export const NOTIFY_RETENTION_POLICY_CREATED = {
  ...defaultSuccessNotification,
  message: 'Retention Policy created successfully.',
}

export const NOTIFY_RETENTION_POLICY_CREATION_FAILED = errorMessage =>
  `Failed to create Retention Policy: ${errorMessage}`

export const NOTIFY_RETENTION_POLICY_DELETED = rpName => ({
  ...defaultSuccessNotification,
  message: `Retention Policy "${rpName}" deleted successfully.`,
})

export const NOTIFY_RETENTION_POLICY_DELETION_FAILED = errorMessage =>
  `Failed to delete Retention Policy: ${errorMessage}`

export const NOTIFY_RETENTION_POLICY_UPDATED = {
  ...defaultSuccessNotification,
  message: 'Retention Policy updated successfully.',
}

export const NOTIFY_RETENTION_POLICY_UPDATE_FAILED = errorMessage =>
  `Failed to update Retention Policy: ${errorMessage}`

export const NOTIFY_QUERIES_ERROR = errorMessage => ({
  ...defaultErrorNotification,
  errorMessage,
})

export const NOTIFY_RETENTION_POLICY_CANT_HAVE_EMPTY_FIELDS = {
  ...defaultErrorNotification,
  message: 'Fields cannot be empty.',
}

export const NOTIFY_DATABASE_DELETE_CONFIRMATION_REQUIRED = databaseName => ({
  ...defaultErrorNotification,
  message: `Type "DELETE ${databaseName}" to confirm.`,
})

export const NOTIFY_DB_USER_NAME_PASSWORD_INVALID = {
  ...defaultErrorNotification,
  message: 'Username and/or Password too short.',
}

export const NOTIFY_ROLE_NAME_INVALID = {
  ...defaultErrorNotification,
  message: 'Role name is too short.',
}

export const NOTIFY_DATABASE_NAME_INVALID = {
  ...defaultErrorNotification,
  message: 'Database name cannot be blank.',
}

export const NOTIFY_DATABASE_NAME_ALREADY_EXISTS = {
  ...defaultErrorNotification,
  message: 'A Database by this name already exists.',
}

//  Dashboard Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_TEMP_VAR_ALREADY_EXISTS = tempVarName => ({
  ...defaultErrorNotification,
  icon: 'cube',
  message: `Variable '${tempVarName}' already exists. Please enter a new value.`,
})

export const NOTIFY_DASHBOARD_NOT_FOUND = dashboardID => ({
  ...defaultErrorNotification,
  icon: 'dash-h',
  message: `Dashboard ${dashboardID} could not be found`,
})

export const NOTIFY_DASHBOARD_DELETED = name => ({
  ...defaultSuccessNotification,
  icon: 'dash-h',
  message: `Dashboard ${name} deleted successfully.`,
})

export const NOTIFY_DASHBOARD_DELETE_FAILED = (name, errorMessage) =>
  `Failed to delete Dashboard ${name}: ${errorMessage}.`

//  Rule Builder Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_ALERT_RULE_CREATED = {
  ...defaultSuccessNotification,
  message: 'Alert Rule created successfully.',
}

export const NOTIFY_ALERT_RULE_CREATION_FAILED = {
  ...defaultErrorNotification,
  message: 'Alert Rule could not be created.',
}

export const NOTIFY_ALERT_RULE_UPDATED = ruleName => ({
  ...defaultSuccessNotification,
  message: `${ruleName} saved successfully.`,
})

export const NOTIFY_ALERT_RULE_UPDATE_FAILED = (ruleName, errorMessage) => ({
  ...defaultErrorNotification,
  message: `There was a problem saving ${ruleName}: ${errorMessage}`,
})

export const NOTIFY_ALERT_RULE_DELETED = ruleName => ({
  ...defaultSuccessNotification,
  message: `${ruleName} deleted successfully.`,
})

export const NOTIFY_ALERT_RULE_DELETION_FAILED = ruleName => ({
  ...defaultErrorNotification,
  message: `${ruleName} could not be deleted.`,
})

export const NOTIFY_ALERT_RULE_STATUS_UPDATED = (ruleName, updatedStatus) => ({
  ...defaultSuccessNotification,
  message: `${ruleName} ${updatedStatus} successfully.`,
})

export const NOTIFY_ALERT_RULE_STATUS_UPDATE_FAILED = (
  ruleName,
  updatedStatus
) => ({
  ...defaultSuccessNotification,
  message: `${ruleName} could not be ${updatedStatus}.`,
})

export const NOTIFY_ALERT_RULE_REQUIRES_QUERY =
  'Please select a Database, Measurement, and Field.'

export const NOTIFY_ALERT_RULE_REQUIRES_CONDITION_VALUE =
  'Please enter a value in the Conditions section.'

export const NOTIFY_ALERT_RULE_DEADMAN_INVALID =
  'Deadman rules require a Database and Measurement.'

//  Kapacitor Configuration Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_KAPACITOR_NAME_ALREADY_TAKEN = kapacitorName => ({
  ...defaultErrorNotification,
  message: `There is already a Kapacitor Connection named "${kapacitorName}".`,
})

export const NOTIFY_COULD_NOT_FIND_KAPACITOR = {
  ...defaultErrorNotification,
  message: 'We could not find a Kapacitor configuration for this source.',
}

export const NOTIFY_REFRESH_KAPACITOR_FAILED = {
  ...defaultErrorNotification,
  message: 'There was an error getting the Kapacitor configuration.',
}

export const NOTIFY_ALERT_ENDPOINT_SAVED = endpoint => ({
  ...defaultSuccessNotification,
  message: `Alert configuration for ${endpoint} saved successfully.`,
})

export const NOTIFY_ALERT_ENDPOINT_SAVE_FAILED = (endpoint, errorMessage) => ({
  ...defaultErrorNotification,
  message: `There was an error saving the alert configuration for ${endpoint}: ${errorMessage}`,
})

export const NOTIFY_TEST_ALERT_SENT = endpoint => ({
  ...defaultSuccessNotification,
  duration: TEN_SECONDS,
  message: `Test Alert sent to ${endpoint}. If the Alert does not reach its destination, please check your endpoint configuration settings.`,
})

export const NOTIFY_TEST_ALERT_FAILED = (endpoint, errorMessage) => ({
  ...defaultErrorNotification,
  message: `There was an error sending a Test Alert to ${endpoint}${
    errorMessage ? `: ${errorMessage}` : '.'
  }`,
})

export const NOTIFY_KAPACITOR_CONNECTION_FAILED = {
  ...defaultErrorNotification,
  message:
    'Could not connect to Kapacitor. Check your connection settings in the Configuration page.',
}

export const NOTIFY_KAPACITOR_CREATED = {
  ...defaultSuccessNotification,
  message:
    'Connected to Kapacitor successfully! Configuring endpoints is optional.',
}

export const NOTIFY_KAPACITOR_CREATION_FAILED = {
  ...defaultErrorNotification,
  message: 'There was a problem connecting to Kapacitor.',
}

export const NOTIFY_KAPACITOR_UPDATED = {
  ...defaultSuccessNotification,
  message: 'Kapacitor Connection updated successfully.',
}

export const NOTIFY_KAPACITOR_UPDATE_FAILED = {
  ...defaultErrorNotification,
  message: 'There was a problem updating the Kapacitor Connection.',
}

//  TICKscript Notifications
//  ----------------------------------------------------------------------------
export const NOTIFY_TICKSCRIPT_CREATED = {
  ...defaultSuccessNotification,
  message: 'TICKscript successfully created.',
}

export const NOTIFY_TICKSCRIPT_CREATION_FAILED = 'Failed to create TICKscript.'

export const NOTIFY_TICKSCRIPT_UPDATED = {
  ...defaultSuccessNotification,
  message: 'TICKscript successfully updated.',
}

export const NOTIFY_TICKSCRIPT_UPDATE_FAILED = 'Failed to update TICKscript.'

export const NOTIFY_TICKSCRIPT_LOGGING_UNAVAILABLE = {
  type: 'warning',
  icon: 'alert-triangle',
  duration: INFINITE,
  message: 'Kapacitor version 1.4 required to view TICKscript logs',
}

export const NOTIFY_TICKSCRIPT_LOGGING_ERROR = message => ({
  ...defaultErrorNotification,
  message,
})

export const NOTIFY_KAPACITOR_NOT_FOUND =
  'We could not find a Kapacitor configuration for this source.'
