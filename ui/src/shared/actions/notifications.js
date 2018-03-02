<<<<<<< HEAD
export const publishNotification = notification => ({
  type: 'PUBLISH_NOTIFICATION',
  payload: {notification},
})

export const dismissNotification = id => ({
  type: 'DISMISS_NOTIFICATION',
  payload: {id},
})
=======
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
>>>>>>> WIP Refactor Notifications
