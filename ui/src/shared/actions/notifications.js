export function publishNotification(type, message, options = {once: false}) {
  // this validator is purely for development purposes. It might make sense to move this to a middleware.
  const validTypes = ['error', 'success', 'warning']
  if (!validTypes.includes(type) || message === undefined) {
    console.error('handleNotification must have a valid type and text') // eslint-disable-line no-console
  }

  return {
    type: 'NOTIFICATION_RECEIVED',
    payload: {
      type,
      message,
      once: options.once,
    },
  }
}

export function dismissNotification(type) {
  return {
    type: 'NOTIFICATION_DISMISSED',
    payload: {
      type,
    },
  }
}

export function dismissAllNotifications() {
  return {
    type: 'ALL_NOTIFICATIONS_DISMISSED',
  }
}
