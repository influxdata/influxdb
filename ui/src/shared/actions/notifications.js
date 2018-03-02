export function publishNotification(notification) {
  return {
    type: 'PUBLISH_NOTIFICATION',
    payload: {
      notification,
    },
  }
}

export function dismissNotification(notificationID) {
  return {
    type: 'DISMISS_NOTIFICATION',
    payload: {
      notificationID,
    },
  }
}

export function deleteNotification(notificationID) {
  return {
    type: 'DELETE_NOTIFICATION',
    payload: {
      notificationID,
    },
  }
}
