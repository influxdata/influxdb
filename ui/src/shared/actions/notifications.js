export function publishNotification(
  type,
  message,
  duration = 4000,
  icon = 'zap'
) {
  // TODO: Refactor where notifications are published from and use correct shape
  // This acts as a temporary means to transform the old shape into the new one
  const notification = {
    type,
    message,
    duration,
    icon,
  }
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
