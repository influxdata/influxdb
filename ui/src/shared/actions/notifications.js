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
    type: 'NOTIFICATION_DISMISSED',
    payload: {
      notificationID,
    },
  }
}
