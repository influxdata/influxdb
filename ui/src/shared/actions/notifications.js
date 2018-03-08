// add comment
export function publishNotification(notification) {
  return {
    type: 'PUBLISH_NOTIFICATION',
    payload: {notification},
  }
}

export function dismissNotification(id) {
  return {
    type: 'DISMISS_NOTIFICATION',
    payload: {id},
  }
}
