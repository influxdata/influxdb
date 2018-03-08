// All copy for notifications should be stored here for easy editing and comparison

// Text Formatting helper
const toTitleCase = str => {
  return str.replace(/\w\S*/g, function(txt) {
    return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase()
  })
}

// App Wide Notifications
export const enterPresentationModeNotification = {
  type: 'primary',
  icon: 'expand-b',
  duration: 7500,
  message: 'Press ESC to exit Presentation Mode.',
}

// Chronograf Admin Notifications
export const mappingDeletedNotification = (id, scheme) => {
  return {
    type: 'success',
    icon: 'checkmark',
    duration: 5000,
    message: `Mapping deleted: ${id} ${scheme}`,
  }
}

export const userAddedToOrgMessage = (user, organization) => {
  return `${user} has been added to ${organization}`
}

export const userRemovedFromOrgMessage = (user, organization) => {
  return `${user} has been removed from ${organization}`
}

export const userRemovedFromOrgNotification = (user, isAbsoluteDelete) => {
  return {
    type: 'success',
    icon: 'checkmark',
    duration: 5000,
    message: `${user} has been removed from ${isAbsoluteDelete
      ? 'all organizations and deleted'
      : 'the current organization'}`,
  }
}

export const updateUserSuccessNotification = message => {
  return {
    type: 'success',
    icon: 'checkmark',
    duration: 5000,
    message,
  }
}

export const deleteOrgSuccessNotification = orgName => {
  return {
    type: 'success',
    icon: 'checkmark',
    duration: 5000,
    message: `Organization deleted: ${orgName}`,
  }
}

// InfluxDB Admin Notifications
const dbAdminCreate = (success, objectType, errorMessage) => {
  return success
    ? {
        type: 'success',
        icon: 'checkmark',
        duration: 5000,
        message: `${toTitleCase(objectType)} created successfully`,
      }
    : `Failed to create ${toTitleCase(objectType)}: ${errorMessage}`
}

const dbAdminUpdate = (success, objectType, errorMessage) => {
  return success
    ? {
        type: 'success',
        icon: 'checkmark',
        duration: 5000,
        message: `${toTitleCase(objectType)} updated successfully`,
      }
    : `Failed to update ${toTitleCase(objectType)}: ${errorMessage}`
}

const dbAdminDelete = (success, objectType, name, errorMessage) => {
  return success
    ? {
        type: 'success',
        icon: 'checkmark',
        duration: 5000,
        message: `${toTitleCase(objectType)} ${name} deleted successfully`,
      }
    : `Failed to delete ${toTitleCase(objectType)}: ${errorMessage}`
}
export const dbAdminNotifications = {
  createNotification: dbAdminCreate,
  updateNotification: dbAdminUpdate,
  deleteNotification: dbAdminDelete,
}
